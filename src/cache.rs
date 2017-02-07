use std::collections::HashMap;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::mpsc::{self, TryRecvError};
use std::time::Duration;

use futures::{self, Async, Fuse, Future, IntoFuture, Poll};
use futures::sync::oneshot::{self, Sender, Receiver};

use linked_hash_map::LinkedHashMap;

use mio::timer::{Builder as TimerBuilder, Timer};

use tokio_core::reactor::{Handle, PollEvented};

// for the Checker error case
const LEADER: bool = true;
const WAITER: bool = !LEADER;

type Checker<V> = Sender<Result<Arc<V>, bool>>;
type Loader<V, E> = Receiver<Result<Arc<V>, E>>;
type Waiter<V, E> = Sender<Result<Option<Arc<V>>, E>>;

enum Message<K, V, E> {
    Stats(Sender<CacheStats>),
    Get(K, bool, Waiter<V, E>),
    Load(K, Checker<V>, Loader<V, E>, Waiter<V, E>),
    Evict(K, Sender<()>),
}

pub trait Weighted {
    fn weight(&self) -> usize;
}

pub struct ReactorCache<K, V, E> {
    tx: mpsc::Sender<Message<K, V, E>>,
}

#[derive(Debug)]
pub struct CacheStats {
    pub entries: usize,
    pub remaining: usize,
    pub capacity: usize,
}

pub struct GetHandle<V, E> {
    rx: Receiver<Result<Option<Arc<V>>, E>>,
}

enum LoadState<F: Future, V, E> {
    Empty, // used for state transitions
    Checking(Receiver<Result<Arc<V>, bool>>, Fuse<F>, Sender<Result<Arc<V>, E>>, GetHandle<V, E>),
    Loading(Fuse<F>, Sender<Result<Arc<V>, E>>, GetHandle<V, E>),
    Waiting(GetHandle<V, E>),
}

pub struct LoadHandle<F: Future, V, E> {
    state: LoadState<F, V, E>,
}

struct CacheEntry<V> {
    inner: Arc<V>,
    weight: usize,
    marked: bool,
}

struct Inner<K, V, E> {
    rx: mpsc::Receiver<Message<K, V, E>>,
    timer: PollEvented<Timer<()>>,
    fetch_map: HashMap<K, (Loader<V, E>, Vec<Waiter<V, E>>)>,
    cache_map: LinkedHashMap<K, CacheEntry<V>>,
    usage: (usize, usize), // (remaining, capacity)
}

impl<V, E> Future for GetHandle<V, E> {
    type Item = Option<Arc<V>>;
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll().expect("get canceled") {
            Async::Ready(Ok(res)) => Ok(res.into()),
            Async::Ready(Err(e)) => Err(e),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

impl<F, V, E: Clone> Future for LoadHandle<F, V, E>
    where F: Future<Item = V, Error = E>
{
    type Item = Arc<V>;
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("loadhandle - start");

        let mut state = ::std::mem::replace(&mut self.state, LoadState::Empty);

        if let LoadState::Checking(mut checker, loader, resolver, waiter) = state {
            trace!("loadhandle - checking");
            match checker.poll().expect("check canceled") {
                Async::Ready(Ok(res)) => {
                    trace!("loadhandle - hit");
                    return Ok(res.into());
                }
                Async::Ready(Err(LEADER)) => {
                    trace!("loadhandle - miss:leader");
                    state = LoadState::Loading(loader, resolver, waiter);
                }
                Async::Ready(Err(WAITER)) => {
                    trace!("loadhandle - miss:waiter");
                    state = LoadState::Waiting(waiter);
                }
                Async::NotReady => {
                    self.state = LoadState::Checking(checker, loader, resolver, waiter);
                    return Ok(Async::NotReady);
                }
            }
        }

        if let LoadState::Loading(mut loader, resolver, waiter) = state {
            trace!("loadhandle - loading");
            match loader.poll() {
                Ok(Async::Ready(res)) => {
                    trace!("loadhandle - success");
                    resolver.complete(Ok(Arc::new(res)));
                    state = LoadState::Waiting(waiter);
                }
                Err(e) => {
                    trace!("loadhandle - failure");
                    resolver.complete(Err(e));
                    state = LoadState::Waiting(waiter);
                }
                Ok(Async::NotReady) => {
                    self.state = LoadState::Loading(loader, resolver, waiter);
                    return Ok(Async::NotReady);
                }
            }
        }

        if let LoadState::Waiting(mut waiter) = state {
            trace!("loadhandle - waiting");
            return match waiter.poll() {
                Ok(Async::Ready(Some(res))) => {
                    trace!("loadhandle - ok");
                    Ok(res.into())
                }
                Ok(Async::Ready(None)) => unreachable!(),
                Ok(Async::NotReady) => {
                    self.state = LoadState::Waiting(waiter);
                    Ok(Async::NotReady)
                }
                Err(e) => {
                    trace!("loadhandle - err");
                    Err(e)
                }
            };
        }

        unreachable!("invalid state transition")
    }
}

/// Core methods for interacting with the cache
impl<K: Clone + Eq + Hash, V: Weighted, E: Clone + Debug> ReactorCache<K, V, E> {
    /// Creates a new Reactor Cache with the given capacity, which runs in `handle`'s event-loop
    pub fn new(capacity: usize, handle: Handle) -> Self
        where K: 'static,
              V: 'static,
              E: 'static
    {
        let (tx, rx) = mpsc::channel();
        let mut timer = TimerBuilder::default().tick_duration(Duration::from_millis(10)).build();
        timer.set_timeout(Duration::from_millis(10), ()).unwrap();
        let poll = PollEvented::new(timer, &handle).unwrap();
        handle.spawn_fn(move || Inner::new(capacity, rx, poll).map_err(|e| panic!("{:?}", e)));
        ReactorCache { tx: tx }
    }

    /// Returns a future with a snapshot of the cache's stats. No guarnatees are made about
    /// when the snapshot is taken.
    ///
    /// # Example
    ///
    /// Run a future that logs the cache stats:
    ///
    /// ```
    /// # extern crate futures;
    /// # extern crate reactor_cache;
    /// # extern crate tokio_core;
    /// #
    /// # use futures::Future;
    /// # use reactor_cache::*;
    /// # use tokio_core::reactor::Core;
    /// #
    /// # #[derive(Clone, Eq, Hash, PartialEq)] struct Int(i64);
    /// # impl Weighted for Int { fn weight(&self) -> usize { 8 } }
    /// #
    /// # fn main() {
    ///     let mut core = Core::new().expect("meltdown");
    ///     let cache = ReactorCache::<Int, Int, ()>::new(10, core.handle());
    ///     core.run(cache.stats().map(|s| println!("{:?}",s))).unwrap();
    /// # }
    pub fn stats(&self) -> Receiver<CacheStats> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Stats(tx)).unwrap();
        rx
    }

    /// Returns a future with a snapshot of the cache's stats. No guarnatees are made about
    /// when the snapshot is taken.
    ///
    /// # Example
    ///
    /// Run a future that logs the cache stats:
    ///
    /// ```
    /// # extern crate futures;
    /// # extern crate reactor_cache;
    /// # extern crate tokio_core;
    /// #
    /// # use futures::Future;
    /// # use reactor_cache::*;
    /// # use tokio_core::reactor::Core;
    /// #
    /// # #[derive(Clone, Eq, Hash, PartialEq)] struct Int(i64);
    /// # impl Weighted for Int { fn weight(&self) -> usize { 8 } }
    /// #
    /// # fn main() {
    ///     let mut core = Core::new().expect("meltdown");
    ///     let cache = ReactorCache::<Int, Int, ()>::new(10, core.handle());
    ///     core.run(cache.load_fn(Int(1), ||
    ///     core.run(cache.stats().map(|s| println!("{:?}",s))).unwrap();
    /// # }
    pub fn get(&self, k: K) -> GetHandle<V, E> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Get(k, true, tx)).unwrap();
        GetHandle { rx: rx }
    }

    pub fn get_if_resident(&self, k: K) -> GetHandle<V, E> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Get(k, false, tx)).unwrap();
        GetHandle { rx: rx }
    }

    pub fn load_fn<F, T>(&self, k: K, f: F) -> LoadHandle<futures::Lazy<F, T>, V, E>
        where F: 'static + Send + FnOnce() -> T,
              T: 'static + IntoFuture<Item = V, Error = E>,
              T::Future: 'static + Send
    {
        self.load(k, futures::lazy(f))
    }

    pub fn load<F>(&self, k: K, f: F) -> LoadHandle<F, V, E>
        where F: Future<Item = V, Error = E>
    {
        let (check_tx, check_rx) = oneshot::channel();
        let (load_tx, load_rx) = oneshot::channel();
        let (get_tx, get_rx) = oneshot::channel();
        self.tx.send(Message::Load(k, check_tx, load_rx, get_tx)).unwrap();

        let state = LoadState::Checking(check_rx, f.fuse(), load_tx, GetHandle { rx: get_rx });
        LoadHandle { state: state }
    }

    pub fn evict(&self, k: K) -> Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Evict(k, tx)).unwrap();
        rx
    }
}

impl<V: Weighted> CacheEntry<V> {
    fn new(v: Arc<V>) -> Self {
        CacheEntry {
            weight: v.weight(),
            inner: v,
            marked: false,
        }
    }
}

// TODO - remove req of K: Clone
impl<K: Clone + Eq + Hash, V: Weighted, E: Clone> Inner<K, V, E> {
    fn new(capacity: usize,
           rx: mpsc::Receiver<Message<K, V, E>>,
           timer: PollEvented<Timer<()>>)
           -> Self {
        Inner {
            rx: rx,
            timer: timer,
            fetch_map: HashMap::new(),
            cache_map: LinkedHashMap::new(),
            usage: (capacity, capacity),
        }
    }

    fn upgrade_fetches(&mut self) -> Result<(), ()> {
        trace!("upgrade -- start");
        if self.fetch_map.is_empty() {
            trace!("upgrade -- empty");
            return Ok(());
        }

        let mut to_upgrade = vec![];
        for (k, &mut (ref mut f, _)) in self.fetch_map.iter_mut() {
            match f.poll() {
                Ok(Async::Ready(r)) => to_upgrade.push((k.clone(), Some(r))),
                Ok(Async::NotReady) => continue,
                Err(_) => to_upgrade.push((k.clone(), None)),
            };
        }

        for (k, r_opt) in to_upgrade.into_iter() {
            let (_, waiters) = self.fetch_map.remove(&k).unwrap();
            if let Some(r) = r_opt {
                for waiter in waiters.into_iter() {
                    trace!("upgrade -- waiter");
                    waiter.complete(r.clone().map(Some));
                }

                if let Ok(v) = r {
                    self.try_cache(k, v);
                }
            }
        }

        trace!("upgrade -- end");
        Ok(())
    }

    fn try_cache(&mut self, k: K, v: Arc<V>) {
        trace!("trycache -- start");
        let (ref mut remaining, capacity) = self.usage;

        let entry = CacheEntry::new(v);
        if entry.weight >= capacity {
            trace!("trycache -- toobig");
            return;
        }

        loop {
            if self.cache_map.is_empty() || *remaining >= entry.weight {
                *remaining -= entry.weight;
                self.cache_map.insert(k, entry);
                break;
            }

            let (k2, mut v2) = self.cache_map.pop_front().expect("cache should be non-empty");
            if v2.marked {
                *remaining += v2.weight;
            } else {
                v2.marked = true;
                self.cache_map.insert(k2, v2);
            }
        }
        trace!("trycache -- end");
    }

    fn handle(&mut self, msg: Message<K, V, E>) -> Result<(), ()> {
        trace!("handle -- start");
        match msg {
            Message::Stats(tx) => self.stats(tx),
            Message::Get(k, w, tx) => self.get(k, w, tx),
            Message::Load(k, ck, rx, tx) => self.load(k, ck, rx, tx),
            Message::Evict(k, tx) => self.evict(k, tx),
        };
        trace!("handle -- end");
        Ok(())
    }

    fn stats(&mut self, tx: Sender<CacheStats>) {
        trace!("stats -- start");
        let (remaining, capacity) = self.usage;
        tx.complete(CacheStats {
            entries: self.cache_map.len(),
            remaining: remaining,
            capacity: capacity,
        });
        trace!("stats -- end");
    }

    fn get(&mut self, k: K, wait: bool, tx: Waiter<V, E>) {
        trace!("get -- start");
        if let Some(mut entry) = self.cache_map.get_refresh(&k) {
            entry.marked = false;
            trace!("get -- hit");
            return tx.complete(Ok(Some(entry.inner.clone())));
        }

        if wait {
            if let Some(&mut (_, ref mut waiters)) = self.fetch_map.get_mut(&k) {
                trace!("get -- wait");
                return waiters.push(tx);
            }
        }

        trace!("get -- miss");
        tx.complete(Ok(None));
    }

    fn load(&mut self, k: K, checker: Checker<V>, f: Loader<V, E>, tx: Waiter<V, E>) {
        trace!("load -- start");

        if let Some(mut entry) = self.cache_map.get_refresh(&k) {
            trace!("load -- hit");
            entry.marked = false;
            return checker.complete(Ok(entry.inner.clone()));
        }
        trace!("load -- miss");

        let &mut (_, ref mut waiters) = self.fetch_map.entry(k).or_insert((f, vec![]));
        checker.complete(Err(waiters.is_empty())); // if there are no waiters, we're the leader
        waiters.push(tx);

        trace!("load -- end");
    }

    fn evict(&mut self, k: K, tx: Sender<()>) {
        trace!("evict -- start");
        self.fetch_map.remove(&k);
        if let Some(entry) = self.cache_map.remove(&k) {
            self.usage.0 += entry.weight;
        }
        tx.complete(());
        trace!("evict -- end");
    }
}

impl<K: Clone + Eq + Hash, V: Weighted, E: Clone> Future for Inner<K, V, E> {
    type Item = ();
    type Error = (); // TODO - E doesn't work out of the box...

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("poll -- start");
        if let Async::NotReady = self.timer.poll_read() {
            trace!("poll -- not ready");
            return Ok(Async::NotReady);
        }

        // remove scheduled bits and schedule Inner for the next loop
        while let Some(_) = self.timer.get_mut().poll() {}
        self.timer.need_read();

        self.upgrade_fetches()?;

        loop {
            match self.rx.try_recv() {
                Ok(msg) => self.handle(msg)?,
                Err(TryRecvError::Empty) => {
                    trace!("poll -- end");
                    self.timer.get_mut().set_timeout(Duration::from_millis(10), ()).unwrap();
                    return Ok(Async::NotReady);
                }
                Err(TryRecvError::Disconnected) => {
                    trace!("poll -- terminate");
                    return Ok(().into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio_core::reactor::Core;

    impl Weighted for i64 {
        fn weight(&self) -> usize {
            8
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Scale(usize);

    impl Weighted for Scale {
        fn weight(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn basic_cache_ops() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, ()>::new(16, core.handle());

        // insert two entries to fill the cache
        assert_eq!(10, *core.run(cache.load_fn(1, || Ok(10))).unwrap());
        assert_eq!(20, *core.run(cache.load_fn(2, || Ok(20))).unwrap());

        let stats = core.run(cache.stats()).unwrap();
        assert_eq!(2, stats.entries);
        assert_eq!(0, stats.remaining);

        // evict (1, 10) and insert (3, 30)
        assert_eq!(30, *core.run(cache.load_fn(3, || Ok(30))).unwrap());
        assert_eq!(None, core.run(cache.get(1)).unwrap());
        assert_eq!(20, *core.run(cache.get(2)).unwrap().unwrap());

        let stats = core.run(cache.stats()).unwrap();
        assert_eq!(2, stats.entries);
        assert_eq!(0, stats.remaining);

        // evict (3, 30)
        assert_eq!((), core.run(cache.evict(3)).unwrap());
        assert_eq!(None, core.run(cache.get(1)).unwrap());

        let stats = core.run(cache.stats()).unwrap();
        assert_eq!(1, stats.entries);
        assert_eq!(8, stats.remaining);
    }

    #[test]
    fn waiters() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, i64>::new(16, core.handle());

        let counter = Arc::new(AtomicUsize::new(10));
        let c1 = counter.clone();
        let c2 = counter.clone();

        let l1 = cache.load_fn(1, move || Ok(c1.fetch_add(1, Ordering::SeqCst) as i64));
        let g1 = cache.get(1);
        let l2 = cache.load_fn(1, move || Ok(c2.fetch_add(1, Ordering::SeqCst) as i64));

        assert_eq!(10, *core.run(l1).unwrap());
        assert_eq!(10, *core.run(g1).unwrap().unwrap());
        assert_eq!(10, *core.run(l2).unwrap());
        assert_eq!(11, counter.load(Ordering::SeqCst));
    }

    #[test]
    fn get_if_resident() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, i64>::new(16, core.handle());

        let l1 = cache.load_fn(1, || Ok(10));
        let g1 = cache.get_if_resident(1);
        let g2 = cache.get_if_resident(1);

        // because of how events are processed, both gets are misses
        assert_eq!(None, core.run(g1).unwrap());
        assert_eq!(10, *core.run(l1).unwrap());
        assert_eq!(None, core.run(g2).unwrap());

        // since the load has resolved, this will be a cache hit
        let g3 = cache.get_if_resident(1);
        assert_eq!(10, *core.run(g3).unwrap().unwrap());
    }

    #[test]
    fn errors() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, i64>::new(16, core.handle());

        // errors should not be cached
        assert!(core.run(cache.load_fn(1, || Err(10))).is_err());
        assert_eq!(None, core.run(cache.get(1)).unwrap());
        assert!(core.run(cache.load_fn(1, || Ok(10))).is_ok());
        assert_eq!(10, *core.run(cache.get(1)).unwrap().unwrap());
    }

    #[test]
    #[should_panic]
    #[allow(unreachable_code)]
    fn panic() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, i64>::new(16, core.handle());
        assert!(core.run(cache.load_fn(1, || Ok(panic!()))).is_err());
    }

    #[test]
    fn lru_and_marking() {
        let mut core = Core::new().unwrap();
        let cache: ReactorCache<i64, Scale, ()> = ReactorCache::new(16, core.handle());

        // load two entries and touch the first one
        assert_eq!(Scale(8),
                   *core.run(cache.load_fn(1, || Ok(Scale(8)))).unwrap());
        assert_eq!(Scale(8),
                   *core.run(cache.load_fn(2, || Ok(Scale(8)))).unwrap());
        assert_eq!(Scale(8), *core.run(cache.get(1)).unwrap().unwrap());

        // push out (2, 20)
        assert_eq!(Scale(8),
                   *core.run(cache.load_fn(3, || Ok(Scale(8)))).unwrap());
        assert_eq!(None, core.run(cache.get(2)).unwrap());
    }
}
