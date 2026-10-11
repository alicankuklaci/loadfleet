use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

/// Open outbound connections per destination (each destination ip:port has its own ephemeral port pool).
#[derive(Default, Clone)]
pub struct ConnTracker {
    inner: Arc<Mutex<HashMap<SocketAddr, u64>>>,
}

pub struct ConnGuard {
    tracker: ConnTracker,
    dest: SocketAddr,
}

impl ConnTracker {
    pub fn open(&self, dest: SocketAddr) -> ConnGuard {
        *self.inner.lock().entry(dest).or_insert(0) += 1;
        ConnGuard {
            tracker: self.clone(),
            dest,
        }
    }
    pub fn max_per_dest(&self) -> u64 {
        self.inner.lock().values().copied().max().unwrap_or(0)
    }
    /// Largest number of open connections drawing on one ephemeral-port pool: per destination
    /// (Linux), or per address family when `per_family` (macOS/BSD).
    pub fn max_per_pool(&self, per_family: bool) -> u64 {
        let m = self.inner.lock();
        if !per_family {
            return m.values().copied().max().unwrap_or(0);
        }
        let (v4, v6) = m.iter().fold(
            (0, 0),
            |(a, b), (d, n)| {
                if d.is_ipv4() {
                    (a + n, b)
                } else {
                    (a, b + n)
                }
            },
        );
        v4.max(v6)
    }
    pub fn total(&self) -> u64 {
        self.inner.lock().values().sum()
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut m = self.tracker.inner.lock();
        if let Some(n) = m.get_mut(&self.dest) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.dest);
            }
        }
    }
}

static GLOBAL: LazyLock<ConnTracker> = LazyLock::new(ConnTracker::default);

/// The process-wide tracker used by every scenario connector.
pub fn conns() -> &'static ConnTracker {
    &GLOBAL
}
