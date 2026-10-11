use loadfleet_health::*;
use std::io;
use std::time::Duration;

#[test]
fn classify_client_errors() {
    assert_eq!(
        classify_io_error(&io::Error::from_raw_os_error(24)),
        "client:too_many_files"
    );
    assert_eq!(
        classify_io_error(&io::Error::from_raw_os_error(23)),
        "client:too_many_files"
    );
    assert_eq!(
        classify_io_error(&io::Error::from(io::ErrorKind::AddrNotAvailable)),
        "client:addr_not_available"
    );
    assert_eq!(
        classify_io_error(&io::Error::from(io::ErrorKind::ConnectionRefused)),
        "net:refused"
    );
    assert_eq!(
        classify_io_error(&io::Error::from(io::ErrorKind::TimedOut)),
        "net:timeout"
    );
}

/// macOS fails `connect()` with EAGAIN (not EADDRNOTAVAIL) once the IPv6 ephemeral ports are
/// used up; `cargo xtask bench tcp-hold` reported 16k of those as `net:io` target errors.
#[test]
fn connect_eagain_is_port_exhaustion() {
    let before = client_error_count();
    assert_eq!(
        classify_connect_error(&io::Error::from(io::ErrorKind::WouldBlock)),
        "client:addr_not_available"
    );
    assert!(client_error_count() > before);
    assert_eq!(
        classify_connect_error(&io::Error::from(io::ErrorKind::ConnectionRefused)),
        "net:refused"
    );
}

#[test]
fn port_range_parsing() {
    assert_eq!(parse_port_range("32768\t60999\n"), Some((32768, 60999)));
    assert_eq!(parse_port_range("49152\n65535\n"), Some((49152, 65535)));
    assert_eq!(parse_port_range("garbage"), None);
    let (lo, hi) = ephemeral_port_range();
    assert!(lo < hi);
}

#[test]
fn conn_tracker_counts_per_destination() {
    let t = ConnTracker::default();
    let a = "10.0.0.1:80".parse().unwrap();
    let b = "10.0.0.2:80".parse().unwrap();
    let g1 = t.open(a);
    let g2 = t.open(a);
    let _g3 = t.open(b);
    assert_eq!(t.max_per_dest(), 2);
    assert_eq!(t.total(), 3);
    drop(g1);
    drop(g2);
    assert_eq!(t.max_per_dest(), 1);
}

#[test]
fn saturation_rules() {
    let ok = HealthSample {
        cpu_pct: 50.0,
        lag_p95_ms: 5.0,
        ports_pct: 10.0,
        ..Default::default()
    };
    assert!(!ok.saturated());
    assert!(HealthSample {
        cpu_pct: 86.0,
        ..ok.clone()
    }
    .saturated());
    assert!(HealthSample {
        lag_p95_ms: 51.0,
        ..ok.clone()
    }
    .saturated());
    assert!(HealthSample {
        ports_pct: 80.0,
        ..ok.clone()
    }
    .saturated());
    assert!(HealthSample {
        client_errors: 1,
        ..ok.clone()
    }
    .saturated());
}

#[tokio::test]
async fn lag_probe_sees_a_blocked_runtime() {
    let probe = LagProbe::start(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(30)).await;
    std::thread::sleep(Duration::from_millis(200)); // blocks the current-thread runtime
    tokio::time::sleep(Duration::from_millis(30)).await;
    let (_p95, max) = probe.take();
    assert!(max >= 150.0, "max lag {max} ms");
}

#[tokio::test]
async fn monitor_samples() {
    let mut m = HealthMonitor::start();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let s = m.sample();
    assert!(s.fds > 0 && s.fd_limit >= s.fds);
    assert!(s.ports_range > 1000);
    assert!(HostInfo::detect().cores >= 1);
}

#[test]
fn port_pools_follow_the_kernel_rule() {
    let t = ConnTracker::default();
    let mut g = Vec::new();
    for d in [
        "10.0.0.1:80",
        "10.0.0.1:80",
        "10.0.0.2:80",
        "10.0.0.3:81",
        "[::1]:80",
    ] {
        g.push(t.open(d.parse().unwrap()));
    }
    // Linux: a source port only has to be unique per destination ip:port.
    assert_eq!(t.max_per_pool(false), 2);
    // macOS: connect() picks a port free for every local address of the family.
    assert_eq!(t.max_per_pool(true), 4);
    drop(g);
    assert_eq!(t.max_per_pool(true), 0);
}
