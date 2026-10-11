//! argon2 runs behind a small global gate; the failure map stays bounded.
use loadfleet_server::{Auth, LoginError};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_two_verifications_run_at_once() {
    let a = Arc::new(Auth::new("pw", false).with_gate(2, Duration::from_secs(30)));
    let tasks: Vec<_> = (1..=8)
        .map(|i| {
            let a = a.clone();
            tokio::spawn(
                async move { a.login_gated(ip(&format!("192.0.2.{i}")), "x".into()).await },
            )
        })
        .collect();
    for t in tasks {
        assert_eq!(t.await.unwrap(), Err(LoginError::Bad));
    }
    let peak = a.peak_verifications();
    assert!((1..=2).contains(&peak), "peak {peak}");
    assert!(a.login_gated(ip("192.0.2.99"), "pw".into()).await.is_ok());
}

#[tokio::test]
async fn a_full_gate_answers_busy_and_does_not_charge_the_client() {
    let a = Arc::new(Auth::new("pw", false).with_gate(0, Duration::from_millis(50)));
    let t0 = std::time::Instant::now();
    for _ in 0..7 {
        assert_eq!(
            a.login_gated(ip("192.0.2.1"), "pw".into()).await,
            Err(LoginError::Busy)
        );
    }
    assert!(
        t0.elapsed() >= Duration::from_millis(350),
        "each waited for the gate"
    );
    assert_eq!(a.tracked_clients(), 0, "busy attempts are refunded");
    assert_eq!(a.peak_verifications(), 0);
}

#[test]
fn failure_map_is_bounded_and_evicts_the_oldest() {
    let a = Auth::new("pw", false).with_failure_cap(8);
    for i in 1..=20 {
        assert_eq!(
            a.login(ip(&format!("192.0.2.{i}")), "x"),
            Err(LoginError::Bad)
        );
        assert!(a.tracked_clients() <= 8, "{}", a.tracked_clients());
    }
    assert_eq!(a.tracked_clients(), 8);
    // the newest client kept its count: 1 + 4 more failures hit the limit
    for _ in 0..4 {
        assert_eq!(a.login(ip("192.0.2.20"), "x"), Err(LoginError::Bad));
    }
    assert_eq!(
        a.login(ip("192.0.2.20"), "pw"),
        Err(LoginError::RateLimited)
    );
}

#[test]
fn absurdly_long_passwords_are_refused_without_hashing() {
    let a = Auth::new("pw", false);
    let long = "p".repeat(4096);
    assert_eq!(a.login(ip("192.0.2.1"), &long), Err(LoginError::Bad));
    assert_eq!(a.peak_verifications(), 0);
}
