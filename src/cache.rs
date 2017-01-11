use std::collections::HashMap;
use std::hash::Hash;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::mpsc::{self, TryRecvError};
use std::time::Duration;

use futures::{self, future, Async, Future, IntoFuture, Poll};
use futures::sync::oneshot::{self, Sender, Receiver};

use linked_hash_map::LinkedHashMap;

use mio::timer::{Builder as TimerBuilder, Timer};

use tokio_core::reactor::{Handle, PollEvented};

const IGNORE_CACHED: bool = false;
const LOOKUP_CACHED: bool = true;

type Loader<V, E> = mpsc::Receiver<Result<Arc<V>, E>>;
type Waiter<V, E> = Sender<Result<Option<Arc<V>>, E>>;

enum Message<K, V, E> {
    Stats(Sender<CacheStats>),
    Get(K, Waiter<V, E>),
    Load(K, Loader<V, E>, Waiter<V, E>, bool),
    Evict(K, Sender<()>),
}

pub trait Weighted {
    fn weight(&self) -> usize;
}

#[derive(Clone)]
pub struct ReactorCache<K, V, E> {
    tx: mpsc::Sender<Message<K, V, E>>,
}

pub struct CacheStats {
    pub entries: usize,
    pub remaining: usize,
    pub capacity: usize,
}

pub struct GetHandle<V, E> {
    rx: Receiver<Result<Option<Arc<V>>, E>>,
}

pub struct LoadHandle<F: Future, V, E> {
    inner: future::Fuse<F>,
    tx: mpsc::Sender<Result<Arc<V>, E>>,
    get: GetHandle<V, E>,
    loaded: bool,
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
        println!("loadhandle - start");
        match self.get.poll() {
            Ok(Async::Ready(Some(res))) => {
                println!("loadhandle - end+ok");
                return Ok(res.into());
            }
            Ok(Async::Ready(None)) => unreachable!(),
            Ok(Async::NotReady) => (), // noop
            Err(e) => {
                println!("loadhandle - end+err");
                return Err(e);
            }
        };

        if self.loaded {
            println!("loadhandle - waiting");
            return Ok(Async::NotReady);
        }

        match self.inner.poll() {
            Ok(Async::Ready(res)) => {
                println!("loadhandle - success");
                self.loaded = true;
                let arc = Arc::new(res);
                self.tx.send(Ok(arc.clone())).unwrap();
                Ok(Async::NotReady)
            }
            Err(e) => {
                println!("loadhandle - failure");
                self.loaded = true;
                self.tx.send(Err(e.clone())).unwrap();
                Ok(Async::NotReady)
            }
            _ => Ok(Async::NotReady),
        }
    }
}

impl<K: Clone + Eq + Hash, V: Weighted, E: Clone + Debug> ReactorCache<K, V, E> {
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

    pub fn stats(&self) -> Receiver<CacheStats> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Stats(tx)).unwrap();
        rx
    }

    pub fn get(&self, k: K) -> GetHandle<V, E> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Message::Get(k, tx)).unwrap();
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
        let (load_tx, load_rx) = mpsc::channel();
        let (get_tx, get_rx) = oneshot::channel();
        self.tx.send(Message::Load(k, load_rx, get_tx, IGNORE_CACHED)).unwrap();
        LoadHandle {
            inner: f.fuse(),
            tx: load_tx,
            get: GetHandle { rx: get_rx },
            loaded: false,
        }
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
        println!("upgrade -- start");
        if self.fetch_map.is_empty() {
            println!("upgrade -- empty");
            return Ok(());
        }

        let mut to_upgrade = vec![];
        for (k, &(ref f, _)) in self.fetch_map.iter() {
            match f.try_recv() {
                Ok(r) => to_upgrade.push((k.clone(), Some(r))),
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => to_upgrade.push((k.clone(), None)),
            };
        }

        for (k, r_opt) in to_upgrade.into_iter() {
            let (_, waiters) = self.fetch_map.remove(&k).unwrap();
            if let Some(r) = r_opt {
                for waiter in waiters.into_iter() {
                    waiter.complete(r.clone().map(Some));
                }

                if let Ok(v) = r {
                    self.try_cache(k, v);
                }
            }
        }

        println!("upgrade -- end");
        Ok(())
    }

    fn try_cache(&mut self, k: K, v: Arc<V>) {
        println!("trycache -- start");
        let (ref mut remaining, capacity) = self.usage;

        let entry = CacheEntry::new(v);
        if entry.weight >= capacity {
            println!("trycache -- toobig");
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
        println!("trycache -- end");
    }

    fn handle(&mut self, msg: Message<K, V, E>) -> Result<(), ()> {
        println!("handle -- start");
        match msg {
            Message::Stats(tx) => self.stats(tx),
            Message::Get(k, tx) => self.get(k, tx),
            Message::Load(k, rx, tx, lookup) => self.load(k, rx, tx, lookup),
            Message::Evict(k, tx) => self.evict(k, tx),
        };
        println!("handle -- end");
        Ok(())
    }

    fn stats(&mut self, tx: Sender<CacheStats>) {
        println!("stats -- start");
        let (remaining, capacity) = self.usage;
        tx.complete(CacheStats {
            entries: self.cache_map.len(),
            remaining: remaining,
            capacity: capacity,
        });
        println!("stats -- end");
    }

    fn get(&mut self, k: K, tx: Waiter<V, E>) {
        println!("get -- start");
        if let Some(mut entry) = self.cache_map.get_refresh(&k) {
            entry.marked = false;
            println!("get -- hit");
            return tx.complete(Ok(Some(entry.inner.clone())));
        }

        if let Some(&mut (_, ref mut waiters)) = self.fetch_map.get_mut(&k) {
            println!("get -- wait");
            return waiters.push(tx);
        }

        println!("get -- miss");
        tx.complete(Ok(None));
    }

    fn load(&mut self, k: K, f: Loader<V, E>, tx: Waiter<V, E>, lookup: bool) {
        println!("load -- start");
        // if lookup == LOOKUP_CACHED {
        // if let Some(mut entry) = self.cache_map.get_refresh(&k) {
        // entry.marked = false;
        // return tx.complete(Ok(Some(entry.inner.clone())));
        // }
        // }
        //
        self.fetch_map.insert(k, (f, vec![tx]));
        println!("load -- end");
    }

    fn evict(&mut self, k: K, tx: Sender<()>) {
        println!("evict -- start");
        self.fetch_map.remove(&k);
        if let Some(entry) = self.cache_map.remove(&k) {
            self.usage.0 += entry.weight;
        }
        tx.complete(());
        println!("evict -- end");
    }
}

impl<K: Clone + Eq + Hash, V: Weighted, E: Clone> Future for Inner<K, V, E> {
    type Item = ();
    type Error = (); // TODO - E doesn't work out of the box...

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("poll -- start");
        if let Async::NotReady = self.timer.poll_read() {
            println!("poll -- not ready");
            return Ok(Async::NotReady);
        }

        // schedule Inner for the next loop
        self.timer.need_read();

        self.upgrade_fetches()?;

        loop {
            match self.rx.try_recv() {
                Ok(msg) => self.handle(msg)?,
                Err(TryRecvError::Empty) => {
                    println!("poll -- end");
                    self.timer.get_mut().set_timeout(Duration::from_millis(10), ()).unwrap();
                    return Ok(Async::NotReady);
                }
                Err(TryRecvError::Disconnected) => {
                    println!("poll -- terminate");
                    return Ok(().into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;

    use futures;
    use tokio_core::reactor::{Core, Remote};

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

    // #[test]
    // fn load_if_absent() {
    // let core = Core::new().unwrap();
    // let mut cache: ReactorCache<_, _, ()> = ReactorCache::new(16, core.handle());
    //
    // assert_eq!(10, *cache.load_if_absent_fn(1, || Ok(10)).wait().unwrap());
    // assert_eq!(10, *cache.load_if_absent_fn(1, || Ok(11)).wait().unwrap());
    // }

    #[test]
    fn errors() {
        let mut core = Core::new().unwrap();
        let cache = ReactorCache::<i64, i64, i64>::new(16, core.handle());

        // errors should not be cached
        assert!(core.run(cache.load_fn(1, || Err(10))).is_err());
        assert_eq!(None, core.run(cache.get(1)).unwrap());

        // failed loads should not overwrite existing values
        assert!(core.run(cache.load_fn(1, || Ok(10))).is_ok());
        assert!(core.run(cache.load_fn(1, || Err(10))).is_err());
        assert_eq!(10, *core.run(cache.get(1)).unwrap().unwrap());
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
