use crate::hist::{encode_hist, new_hist, record};
use crate::Snapshot;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SHARDS: usize = 64;

#[derive(Default)]
struct Shard {
    hists: HashMap<String, Histogram<u64>>,
    ops: HashMap<String, u64>,
    counters: HashMap<String, u64>,
    errors: HashMap<String, u64>,
}

fn bump(map: &mut HashMap<String, u64>, key: &str, n: u64) {
    match map.get_mut(key) {
        Some(v) => *v += n,
        None => {
            map.insert(key.to_string(), n);
        }
    }
}

struct Inner {
    shards: Vec<Mutex<Shard>>,
    gauges: Mutex<BTreeMap<String, Arc<AtomicI64>>>,
}

impl Inner {
    fn gauge(&self, name: &str) -> Gauge {
        Gauge(
            self.gauges
                .lock()
                .entry(name.to_string())
                .or_default()
                .clone(),
        )
    }
}

/// Process-wide metric sink. VUs record through a `VuRecorder` bound to one of 64 shards.
#[derive(Clone)]
pub struct Recorder {
    inner: Arc<Inner>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    pub fn new() -> Self {
        Recorder {
            inner: Arc::new(Inner {
                shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
                gauges: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn vu(&self, vu: u64) -> VuRecorder {
        VuRecorder {
            inner: self.inner.clone(),
            shard: (vu % SHARDS as u64) as usize,
        }
    }

    pub fn gauge(&self, name: &str) -> Gauge {
        self.inner.gauge(name)
    }

    /// Drains counters/histograms recorded since the previous snapshot; gauges are read, not reset.
    pub fn snapshot(&self, at_ms: i64) -> Snapshot {
        let mut hists: BTreeMap<String, Histogram<u64>> = BTreeMap::new();
        let mut snap = Snapshot {
            at_ms,
            ..Default::default()
        };
        for shard in &self.inner.shards {
            let s = std::mem::take(&mut *shard.lock());
            for (k, h) in s.hists {
                hists
                    .entry(k)
                    .or_insert_with(new_hist)
                    .add(&h)
                    .expect("same bounds");
            }
            for (k, v) in s.ops {
                *snap.ops.entry(k).or_default() += v;
            }
            for (k, v) in s.counters {
                *snap.counters.entry(k).or_default() += v;
            }
            for (k, v) in s.errors {
                *snap.errors.entry(k).or_default() += v;
            }
        }
        snap.hists = hists
            .iter()
            .map(|(k, h)| (k.clone(), encode_hist(h)))
            .collect();
        snap.gauges = self
            .inner
            .gauges
            .lock()
            .iter()
            .map(|(k, g)| (k.clone(), g.load(Ordering::Relaxed)))
            .collect();
        snap
    }
}

#[derive(Clone)]
pub struct VuRecorder {
    inner: Arc<Inner>,
    shard: usize,
}

impl VuRecorder {
    /// Same registry as [`Recorder::gauge`] (e.g. the `connections` gauge scenarios hold).
    pub fn gauge(&self, name: &str) -> Gauge {
        self.inner.gauge(name)
    }
    pub fn latency(&self, name: &str, d: Duration) {
        let mut s = self.inner.shards[self.shard].lock();
        match s.hists.get_mut(name) {
            Some(h) => record(h, d),
            None => {
                let mut h = new_hist();
                record(&mut h, d);
                s.hists.insert(name.to_string(), h);
            }
        }
    }
    /// An operation attempt; the denominator of the error rate.
    pub fn op(&self, name: &str) {
        bump(&mut self.inner.shards[self.shard].lock().ops, name, 1);
    }
    /// A plain counter (not part of the error rate).
    pub fn count(&self, name: &str, n: u64) {
        bump(&mut self.inner.shards[self.shard].lock().counters, name, n);
    }
    pub fn error(&self, code: &str) {
        bump(&mut self.inner.shards[self.shard].lock().errors, code, 1);
    }
}

#[derive(Clone, Default)]
pub struct Gauge(Arc<AtomicI64>);

impl Gauge {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn set(&self, n: i64) {
        self.0.store(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
