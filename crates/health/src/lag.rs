use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Measures how late the tokio scheduler wakes a sleeping task (runtime lag).
///
/// A stalled runtime records nothing until the stall ends, so `take()` also folds in the
/// time since the last wakeup (minus the interval) as a lower bound on current lag.
pub struct LagProbe {
    hist: Arc<Mutex<Histogram<u64>>>,
    base: Instant,
    /// Microseconds since `base` of the last probe wakeup.
    last_tick: Arc<AtomicU64>,
    interval: Duration,
    task: JoinHandle<()>,
}

impl LagProbe {
    pub fn start(interval: Duration) -> Self {
        let hist = Arc::new(Mutex::new(
            Histogram::<u64>::new_with_bounds(1, 60_000_000, 2).expect("valid histogram bounds"),
        ));
        let base = Instant::now();
        let last_tick = Arc::new(AtomicU64::new(0));
        let (h, lt) = (hist.clone(), last_tick.clone());
        let task = tokio::spawn(async move {
            loop {
                let t0 = Instant::now();
                tokio::time::sleep(interval).await;
                let now = Instant::now();
                let lag = now.duration_since(t0).saturating_sub(interval);
                h.lock().saturating_record((lag.as_micros() as u64).max(1));
                lt.store(
                    now.duration_since(base).as_micros() as u64,
                    Ordering::Relaxed,
                );
            }
        });
        LagProbe {
            hist,
            base,
            last_tick,
            interval,
            task,
        }
    }

    /// (p95 ms, max ms) since the previous call; resets the window.
    pub fn take(&self) -> (f64, f64) {
        let now = self.base.elapsed().as_micros() as u64;
        let since_tick = now.saturating_sub(self.last_tick.load(Ordering::Relaxed));
        let stall = since_tick.saturating_sub(self.interval.as_micros() as u64);
        let mut h = self.hist.lock();
        if stall > 0 {
            h.saturating_record(stall.max(1));
        }
        if h.is_empty() {
            return (0.0, 0.0);
        }
        let r = (
            h.value_at_quantile(0.95) as f64 / 1000.0,
            h.max() as f64 / 1000.0,
        );
        h.reset();
        r
    }
}

impl Drop for LagProbe {
    fn drop(&mut self) {
        self.task.abort();
    }
}
