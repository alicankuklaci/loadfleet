//! Login hardening: spoofed CF-Connecting-IP, browser login messages.
use loadfleet_server::*;
use reqwest::StatusCode;

async fn server_with(f: impl FnOnce(&mut ServerConfig)) -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut cfg = ServerConfig {
        db: dir.join("l.db"),
        ..ServerConfig::for_tests()
    };
    f(&mut cfg);
    serve(cfg).await.unwrap()
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn attempt(base: &str, cf: &str, password: &str) -> StatusCode {
    client()
        .post(format!("{base}/login"))
        .header("cf-connecting-ip", cf)
        .form(&[("password", password)])
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn rotating_a_spoofed_cf_header_behind_a_local_proxy_still_hits_the_limit() {
    // nginx on 127.0.0.1 is trusted, but the CF header is not switched on
    let h = server_with(|c| c.trusted_proxies = vec![parse_proxy("127.0.0.1").unwrap()]).await;
    let base = format!("http://{}", h.addr);
    for i in 1..=5 {
        let cf = format!("198.51.100.{i}");
        assert_eq!(attempt(&base, &cf, "nope").await, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        attempt(&base, "198.51.100.6", "test-password").await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn cf_header_counts_with_the_switch_behind_a_trusted_proxy() {
    let h = server_with(|c| {
        c.trusted_proxies = vec![parse_proxy("127.0.0.1").unwrap()];
        c.trust_cf_header = true;
    })
    .await;
    let base = format!("http://{}", h.addr);
    for _ in 0..5 {
        assert_eq!(
            attempt(&base, "198.51.100.1", "nope").await,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        attempt(&base, "198.51.100.1", "test-password").await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        attempt(&base, "198.51.100.2", "test-password").await,
        StatusCode::SEE_OTHER
    );
}

async fn browser_login(base: &str, password: &str) -> (StatusCode, String) {
    let r = client()
        .post(format!("{base}/login"))
        .header("accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
        .form(&[("password", password)])
        .send()
        .await
        .unwrap();
    let loc = r
        .headers()
        .get("location")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    (r.status(), loc)
}

#[tokio::test]
async fn a_browser_form_gets_redirected_back_with_a_message_code() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    assert_eq!(
        browser_login(&base, "test-password").await,
        (StatusCode::SEE_OTHER, "/agents".into())
    );
    for _ in 0..5 {
        assert_eq!(
            browser_login(&base, "nope").await,
            (StatusCode::SEE_OTHER, "/login?e=bad".into())
        );
    }
    assert_eq!(
        browser_login(&base, "test-password").await,
        (StatusCode::SEE_OTHER, "/login?e=rate".into())
    );
    // the page has a slot the panel script fills from `?e=` (fixed texts, never the query)
    let page = client()
        .get(format!("{base}/login?e=bad"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(page.contains("id=\"login-error\""), "{page}");
    let js = client()
        .get(format!("{base}/static/app.js"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for code in ["bad:", "rate:", "busy:"] {
        assert!(js.contains(code), "app.js maps {code}");
    }
}

#[tokio::test]
async fn head_needs_a_session_and_pages_redirect_to_login() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let r = client()
        .head(format!("{base}/api/active"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let r = client()
        .get(format!("{base}/history"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    assert_eq!(r.headers()["location"], "/login");
}

#[tokio::test]
async fn a_flag_password_replaces_the_stored_hash() {
    let dir = tempfile::tempdir().unwrap().keep();
    let cfg = |pw: Option<&str>| ServerConfig {
        db: dir.join("f.db"),
        admin_password: pw.map(str::to_string),
        ..ServerConfig::for_tests()
    };
    let h = serve(cfg(None)).await.unwrap();
    let generated = h.state.store.setting("admin_hash").await.unwrap().unwrap();
    h.kill();
    let h = serve(cfg(Some("test-password"))).await.unwrap();
    let stored = h.state.store.setting("admin_hash").await.unwrap().unwrap();
    assert_ne!(stored, generated);
    assert!(!stored.contains("test-password"));
    h.kill();
    // flag dropped: the last flag password still works, the generated one is gone
    let h = serve(cfg(None)).await.unwrap();
    let base = format!("http://{}", h.addr);
    let r = client()
        .post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
}
