//! loadfleet health: client-side saturation signals (CPU, runtime lag, fds, ephemeral ports, client errors).
mod conns;
mod errors;
mod lag;
mod sys;

pub use conns::{conns, ConnGuard, ConnTracker};
pub use errors::{classify_connect_error, classify_io_error, client_error_count};
pub use lag::LagProbe;

/// Wall-clock unix milliseconds: the one `now_ms` used across the workspace.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
pub use sys::{
    ephemeral_port_range, fd_count, fd_limit, parse_port_range, raise_fd_limit, HostInfo,
};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HealthSample {
    pub cpu_pct: f64,
    pub lag_p95_ms: f64,
    pub lag_max_ms: f64,
    pub fds: u64,
    pub fd_limit: u64,
    pub ports_used_max: u64,
    pub ports_range: u64,
    pub ports_pct: f64,
    /// `client:*` errors since the previous sample.
    pub client_errors: u64,
}

pub const CPU_LIMIT_PCT: f64 = 85.0;
pub const LAG_LIMIT_MS: f64 = 50.0;
pub const PORTS_LIMIT_PCT: f64 = 80.0;
/// macOS picks an ephemeral port that is free on every local address of the family (one pool
/// for all IPv4 connections, one for IPv6, whatever the destination; measured, see
/// docs/bench.md); Linux only needs the 4-tuple to be unique, so its pool is per destination
/// ip:port. The BSDs are left at per destination: FreeBSD 13+ picks ports per destination and
/// the others are unmeasured.
const PORTS_PER_FAMILY: bool = cfg!(any(target_os = "macos", target_os = "ios"));

impl HealthSample {
    pub fn reasons(&self) -> Vec<String> {
        let mut r = Vec::new();
        if self.cpu_pct > CPU_LIMIT_PCT {
            r.push(format!("cpu {:.0}% > 85%", self.cpu_pct));
        }
        if self.lag_p95_ms > LAG_LIMIT_MS {
            r.push(format!("runtime lag p95 {:.0} ms > 50 ms", self.lag_p95_ms));
        }
        if self.ports_pct >= PORTS_LIMIT_PCT {
            r.push(format!("ephemeral ports {:.0}% used", self.ports_pct));
        }
        if self.client_errors > 0 {
            r.push(format!("{} client-side socket errors", self.client_errors));
        }
        r
    }
    pub fn saturated(&self) -> bool {
        !self.reasons().is_empty()
    }
}

/// Samples process health; call `sample()` once per second.
pub struct HealthMonitor {
    lag: LagProbe,
    cores: f64,
    ports: (u16, u16),
    cpu_prev: Option<(Duration, Instant)>,
    client_prev: u64,
    /// Open fd count, refreshed off the runtime by a background thread (enumerating
    /// /dev/fd is O(open fds) and must never run on the load runtime).
    fds: Arc<AtomicU64>,
}

const FD_REFRESH: Duration = Duration::from_secs(5);
const LAG_INTERVAL: Duration = Duration::from_millis(10);

fn spawn_fd_refresher(fds: &Arc<AtomicU64>) {
    let weak = Arc::downgrade(fds);
    let _ = std::thread::Builder::new()
        .name("loadfleet-fds".into())
        .spawn(move || loop {
            std::thread::sleep(FD_REFRESH);
            match weak.upgrade() {
                Some(a) => refresh_fds(&a, fd_count()),
                None => return,
            }
        });
}

/// A failed count (listing /dev/fd needs a free fd itself, so it fails with EMFILE exactly when
/// the process is out of fds) keeps the last good value instead of reporting 0.
fn refresh_fds(slot: &AtomicU64, read: Option<u64>) {
    if let Some(n) = read {
        slot.store(n, Ordering::Relaxed);
    }
}

/// One CPU-usage step. Returns the percentage and the next `cpu_prev`; re-seeds when
/// there is no previous reading, and keeps the old seed if the current read fails.
fn cpu_step(
    prev: Option<(Duration, Instant)>,
    now: Option<Duration>,
    cores: f64,
) -> (f64, Option<(Duration, Instant)>) {
    match (prev, now) {
        (Some((c0, w0)), Some(c1)) => {
            let wall = w0.elapsed().as_secs_f64().max(1e-6);
            let pct = (c1.saturating_sub(c0).as_secs_f64() / wall / cores * 100.0).min(100.0);
            (pct, Some((c1, Instant::now())))
        }
        (None, Some(c1)) => (0.0, Some((c1, Instant::now()))),
        (prev, None) => (0.0, prev),
    }
}

impl HealthMonitor {
    pub fn start() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1) as f64;
        let fds = Arc::new(AtomicU64::new(fd_count().unwrap_or(0)));
        spawn_fd_refresher(&fds);
        HealthMonitor {
            fds,
            lag: LagProbe::start(LAG_INTERVAL),
            cores,
            ports: ephemeral_port_range(),
            cpu_prev: cpu_now().map(|c| (c, Instant::now())),
            client_prev: client_error_count(),
        }
    }

    pub fn sample(&mut self) -> HealthSample {
        let (lag_p95_ms, lag_max_ms) = self.lag.take();
        let (cpu_pct, next) = cpu_step(self.cpu_prev, cpu_now(), self.cores);
        self.cpu_prev = next;
        let ports_range = (self.ports.1 as u64).saturating_sub(self.ports.0 as u64) + 1;
        let used = conns().max_per_pool(PORTS_PER_FAMILY);
        let client_now = client_error_count();
        let client_errors = client_now.saturating_sub(self.client_prev);
        self.client_prev = client_now;
        HealthSample {
            cpu_pct,
            lag_p95_ms,
            lag_max_ms,
            fds: self.fds.load(Ordering::Relaxed),
            fd_limit: fd_limit().unwrap_or(0),
            ports_used_max: used,
            ports_range,
            ports_pct: used as f64 / ports_range as f64 * 100.0,
            client_errors,
        }
    }
}

fn cpu_now() -> Option<Duration> {
    cpu_time::ProcessTime::try_now()
        .ok()
        .map(|t| t.as_duration())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_step_reseeds_after_failed_first_read() {
        let (pct, next) = cpu_step(None, Some(Duration::from_secs(1)), 1.0);
        assert_eq!(pct, 0.0);
        assert!(
            next.is_some(),
            "must re-seed when there was no previous reading"
        );
        let (_, kept) = cpu_step(next, None, 1.0);
        assert_eq!(kept, next, "a failed read keeps the old seed");
    }

    #[test]
    fn cpu_step_computes_percentage() {
        let w0 = Instant::now() - Duration::from_secs(1);
        let (pct, _) = cpu_step(
            Some((Duration::ZERO, w0)),
            Some(Duration::from_millis(500)),
            2.0,
        );
        assert!((24.0..=26.0).contains(&pct), "pct {pct}");
    }

    #[test]
    fn fd_refresh_keeps_last_good_value() {
        let fds = AtomicU64::new(42);
        refresh_fds(&fds, None); // e.g. EMFILE while listing /dev/fd
        assert_eq!(fds.load(Ordering::Relaxed), 42);
        refresh_fds(&fds, Some(7));
        assert_eq!(fds.load(Ordering::Relaxed), 7);
    }
}
