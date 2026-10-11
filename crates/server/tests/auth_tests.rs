//! Admin login: password check, rate limit, sessions and the cookie.
use axum::http::HeaderMap;
use loadfleet_server::{Auth, LoginError};
use std::net::IpAddr;
use std::sync::Arc;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn with_cookie(v: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("cookie", v.parse().unwrap());
    h
}

#[test]
fn login_check_logout() {
    let a = Auth::new("s3cret", false);
    assert_eq!(a.login(ip("192.0.2.1"), "wrong"), Err(LoginError::Bad));
    let s = a.login(ip("192.0.2.1"), "s3cret").unwrap();
    assert_eq!(s.len(), 64, "256-bit hex session id");
    assert!(a.check(&with_cookie(&format!("theme=dark; lf_session={s}"))));
    assert!(!a.check(&with_cookie("lf_session=deadbeef")));
    assert!(!a.check(&HeaderMap::new()));
    a.logout(&with_cookie(&format!("lf_session={s}")));
    assert!(!a.check(&with_cookie(&format!("lf_session={s}"))));
}

#[test]
fn empty_password_never_logs_in() {
    let a = Auth::new("", false);
    assert_eq!(a.login(ip("192.0.2.1"), ""), Err(LoginError::Bad));
}

#[test]
fn five_failures_lock_that_ip_only() {
    let a = Auth::new("pw", false);
    for _ in 0..5 {
        assert_eq!(a.login(ip("192.0.2.1"), "x"), Err(LoginError::Bad));
    }
    assert_eq!(a.login(ip("192.0.2.1"), "pw"), Err(LoginError::RateLimited));
    assert!(a.login(ip("192.0.2.2"), "pw").is_ok());
}

#[test]
fn ipv6_failures_count_per_64() {
    let a = Auth::new("pw", false);
    for i in 1..=5 {
        let addr = format!("2001:db8:1:2::{i}");
        assert_eq!(a.login(ip(&addr), "x"), Err(LoginError::Bad));
    }
    assert_eq!(
        a.login(ip("2001:db8:1:2::99"), "pw"),
        Err(LoginError::RateLimited)
    );
    assert!(a.login(ip("2001:db8:1:3::1"), "pw").is_ok());
}

#[test]
fn concurrent_guesses_cannot_exceed_the_limit() {
    let a = Arc::new(Auth::new("pw", false));
    let threads: Vec<_> = (0..12)
        .map(|_| {
            let a = a.clone();
            std::thread::spawn(move || a.login(ip("192.0.2.7"), "x"))
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let bad = results
        .iter()
        .filter(|r| **r == Err(LoginError::Bad))
        .count();
    assert_eq!(bad, 5, "{results:?}");
    assert!(results
        .iter()
        .all(|r| matches!(r, Err(LoginError::Bad) | Err(LoginError::RateLimited))));
}

#[test]
fn cookie_attributes() {
    let plain = Auth::new("pw", false).cookie("abc");
    assert!(plain.starts_with("lf_session=abc;"), "{plain}");
    for part in ["HttpOnly", "SameSite=Strict", "Path=/"] {
        assert!(plain.contains(part), "{plain}");
    }
    assert!(!plain.contains("Secure"), "{plain}");
    assert!(Auth::new("pw", true).cookie("abc").ends_with("; Secure"));
}

#[test]
fn stored_hash_round_trips() {
    let h = loadfleet_server::hash_password("pw");
    assert!(h.starts_with("$argon2"), "{h}");
    assert!(!h.contains("pw$"), "hash, not the password");
    let a = Auth::from_hash(h, false);
    assert!(a.login(ip("192.0.2.1"), "pw").is_ok());
}
