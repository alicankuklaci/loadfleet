//! Minimum admin password length, and the global failed-login budget: past it, logins are
//! throttled server-wide (one per 10 s) rather than locked.
use axum::http::HeaderMap;
use loadfleet_server::*;
use reqwest::StatusCode;
use std::net::IpAddr;
use std::time::Duration;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn cookie(session: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("cookie", format!("lf_session={session}").parse().unwrap());
    h
}

#[test]
fn past_the_global_budget_logins_are_throttled_not_locked() {
    let a = Auth::new("pw", false)
        .with_global_budget(3, Duration::from_secs(60))
        .with_throttle_spacing(Duration::from_secs(1));
    let s = a.login(ip("192.0.2.200"), "pw").unwrap();
    // successes do not spend the budget
    for _ in 0..5 {
        a.login(ip("192.0.2.201"), "pw").unwrap();
    }
    for i in 1..=3 {
        assert_eq!(
            a.login(ip(&format!("198.51.100.{i}")), "x"),
            Err(LoginError::Bad)
        );
    }
    // budget spent: one attempt per spacing, server-wide, whatever the IP or the password
    assert_eq!(a.login(ip("198.51.100.9"), "x"), Err(LoginError::Bad));
    assert_eq!(a.login(ip("192.0.2.9"), "pw"), Err(LoginError::Throttled));
    assert!(a.check(&cookie(&s)), "issued sessions keep working");
    std::thread::sleep(Duration::from_millis(1100));
    assert!(
        a.login(ip("192.0.2.9"), "pw").is_ok(),
        "the next slot is open"
    );
    assert_eq!(a.login(ip("192.0.2.10"), "pw"), Err(LoginError::Throttled));
    std::thread::sleep(Duration::from_millis(1100));
    // the throttled slot is never a lockout: the admin always gets in on a free slot
    assert!(a.login(ip("192.0.2.9"), "pw").is_ok());
}

#[test]
fn the_failure_window_rolls() {
    let a = Auth::new("pw", false)
        .with_global_budget(2, Duration::from_secs(3))
        .with_throttle_spacing(Duration::from_secs(3600));
    for i in 1..=2 {
        assert_eq!(
            a.login(ip(&format!("198.51.100.{i}")), "x"),
            Err(LoginError::Bad)
        );
    }
    assert_eq!(a.login(ip("198.51.100.3"), "x"), Err(LoginError::Bad));
    assert_eq!(a.login(ip("192.0.2.9"), "pw"), Err(LoginError::Throttled));
    std::thread::sleep(Duration::from_millis(3100));
    // every failure aged out of the window: back to normal speed
    assert!(a.login(ip("192.0.2.9"), "pw").is_ok());
    assert!(a.login(ip("192.0.2.9"), "pw").is_ok());
}

#[test]
fn a_per_client_refusal_does_not_spend_the_global_budget() {
    let a = Auth::new("pw", false).with_global_budget(6, Duration::from_secs(60));
    for _ in 0..5 {
        assert_eq!(a.login(ip("192.0.2.1"), "x"), Err(LoginError::Bad));
    }
    for _ in 0..20 {
        assert_eq!(a.login(ip("192.0.2.1"), "x"), Err(LoginError::RateLimited));
    }
    assert!(a.login(ip("192.0.2.2"), "pw").is_ok());
}

#[test]
fn a_full_failure_map_still_evicts_the_oldest_after_pruning() {
    let a = Auth::new("pw", false)
        .with_failure_cap(4)
        .with_global_budget(10_000, Duration::from_secs(60));
    for round in 0..50 {
        for i in 1..=6 {
            let _ = a.login(ip(&format!("10.{round}.0.{i}")), "x");
        }
        assert!(a.tracked_clients() <= 4);
    }
}

async fn server_with(pw: &str) -> anyhow::Result<ServerHandle> {
    let dir = tempfile::tempdir().unwrap().keep();
    serve(ServerConfig {
        db: dir.join("b.db"),
        admin_password: Some(pw.into()),
        ..ServerConfig::for_tests()
    })
    .await
}

#[tokio::test]
async fn a_short_admin_password_refuses_to_start() {
    let err = server_with("short-pw-11").await.err().expect("refused");
    let msg = err.to_string();
    assert!(
        msg.contains("--admin-password") && msg.contains("LOADFLEET_ADMIN_PASSWORD"),
        "{msg}"
    );
    assert!(msg.contains("12"), "{msg}");
    assert!(server_with("long-enough-12").await.is_ok());
}

#[tokio::test]
async fn a_throttled_server_answers_429_with_its_own_message_code() {
    let dir = tempfile::tempdir().unwrap().keep();
    let h = serve(ServerConfig {
        db: dir.join("l.db"),
        login_failure_budget: 2,
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap();
    let c = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let post = |pw: &'static str, html: bool| {
        let mut rq = c
            .post(format!("http://{}/login", h.addr))
            .form(&[("password", pw)]);
        if html {
            rq = rq.header("accept", "text/html");
        }
        rq.send()
    };
    for _ in 0..2 {
        assert_eq!(
            post("nope", false).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    // the first attempt past the budget takes the throttled slot ...
    assert_eq!(
        post("nope", false).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    // ... the next ones within 10 s are refused, even with the right password
    assert_eq!(
        post("test-password", false).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let r = post("test-password", true).await.unwrap();
    assert_eq!(r.headers()["location"], "/login?e=throttled");
    let js = reqwest::get(format!("http://{}/static/app.js", h.addr))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(js.contains("throttled:"), "app.js maps the code");
}
