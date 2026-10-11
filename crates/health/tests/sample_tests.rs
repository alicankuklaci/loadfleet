use loadfleet_health::*;
use std::io;
use std::time::{Duration, Instant};

#[tokio::test]
async fn blocked_runtime_shows_high_lag_through_sample() {
    let mut m = HealthMonitor::start();
    tokio::time::sleep(Duration::from_millis(50)).await;
    m.sample();
    std::thread::sleep(Duration::from_millis(300)); // probe task cannot run
    let s = m.sample(); // called before the probe task gets a turn
    assert!(s.lag_max_ms >= 250.0, "max lag {}", s.lag_max_ms);
    assert!(s.lag_p95_ms > 0.0 && s.saturated(), "{s:?}");
}

#[tokio::test]
async fn idle_runtime_shows_low_lag() {
    let mut m = HealthMonitor::start();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let s = m.sample();
    assert!(s.lag_max_ms < 100.0, "{s:?}");
}

#[tokio::test]
async fn cpu_pct_reflects_busy_work() {
    let mut m = HealthMonitor::start();
    let t = Instant::now();
    let mut x = 0u64;
    while t.elapsed() < Duration::from_millis(300) {
        x = x.wrapping_add(std::hint::black_box(1));
    }
    std::hint::black_box(x);
    let s = m.sample();
    assert!(s.cpu_pct > 1.0, "cpu_pct {}", s.cpu_pct);
}

#[tokio::test]
async fn client_errors_are_per_sample_delta() {
    let mut m = HealthMonitor::start();
    classify_io_error(&io::Error::from_raw_os_error(24));
    classify_io_error(&io::Error::from(io::ErrorKind::AddrNotAvailable));
    classify_io_error(&io::Error::from(io::ErrorKind::ConnectionRefused)); // not a client error
    assert_eq!(m.sample().client_errors, 2);
    assert_eq!(m.sample().client_errors, 0);
}

#[tokio::test]
async fn sample_cost_does_not_scale_with_open_fds() {
    raise_fd_limit();
    let mut m = HealthMonitor::start();
    let _files: Vec<_> = (0..200)
        .filter_map(|_| std::fs::File::open("/dev/null").ok())
        .collect();
    m.sample();
    let t = Instant::now();
    for _ in 0..10 {
        m.sample();
    }
    assert!(
        t.elapsed() < Duration::from_millis(50),
        "10 samples took {:?}",
        t.elapsed()
    );
}
