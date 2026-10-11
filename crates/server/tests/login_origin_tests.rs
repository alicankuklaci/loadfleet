//! POST /login refuses cross-origin posts (login CSRF / lockout abuse).
use loadfleet_server::*;
use reqwest::StatusCode;

async fn server_with(f: impl FnOnce(&mut ServerConfig)) -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut cfg = ServerConfig {
        db: dir.join("o.db"),
        ..ServerConfig::for_tests()
    };
    f(&mut cfg);
    serve(cfg).await.unwrap()
}

async fn login(base: &str, headers: &[(&str, &str)], password: &str) -> StatusCode {
    let mut rq = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .post(format!("{base}/login"));
    for (k, v) in headers {
        rq = rq.header(*k, *v);
    }
    rq.form(&[("password", password)])
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn cross_origin_posts_are_refused_before_the_password_check() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let other = [
        vec![("origin", "https://evil.example")],
        vec![("origin", "null")],
        vec![("origin", "https://127.0.0.1:1")],
        // right host, wrong scheme (the server is plain http here)
        vec![("origin", base.replace("http://", "https://").leak() as &str)],
        vec![("referer", "https://evil.example/login")],
        vec![("sec-fetch-site", "cross-site")],
        vec![("sec-fetch-site", "same-site")],
    ];
    for hs in &other {
        assert_eq!(
            login(&base, hs, "test-password").await,
            StatusCode::FORBIDDEN,
            "{hs:?}"
        );
    }
    // refused posts never touch the per-client failure budget
    for _ in 0..10 {
        login(&base, &[("origin", "https://evil.example")], "nope").await;
    }
    assert_eq!(
        login(&base, &[("origin", base.as_str())], "test-password").await,
        StatusCode::SEE_OTHER
    );
}

#[tokio::test]
async fn same_origin_and_non_browser_posts_are_accepted() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let referer = format!("{base}/login?e=bad");
    let ok = [
        vec![("origin", base.as_str())],
        vec![("origin", base.as_str()), ("sec-fetch-site", "same-origin")],
        vec![("referer", referer.as_str())],
        vec![],                           // curl, scripts
        vec![("sec-fetch-site", "none")], // typed into the address bar
        vec![("sec-fetch-site", "same-origin")],
    ];
    for hs in &ok {
        assert_eq!(
            login(&base, hs, "test-password").await,
            StatusCode::SEE_OTHER,
            "{hs:?}"
        );
    }
}

#[tokio::test]
async fn behind_a_trusted_tls_proxy_the_public_origin_is_expected() {
    let h = server_with(|c| c.trusted_proxies = vec![parse_proxy("127.0.0.1").unwrap()]).await;
    let base = format!("http://{}", h.addr);
    let fwd = [
        ("x-forwarded-proto", "https"),
        ("x-forwarded-host", "lf.example.com"),
    ];
    let with = |origin: &'static str| {
        let mut v = fwd.to_vec();
        v.push(("origin", origin));
        v
    };
    assert_eq!(
        login(&base, &with("https://lf.example.com"), "test-password").await,
        StatusCode::SEE_OTHER
    );
    assert_eq!(
        login(&base, &with("http://lf.example.com"), "test-password").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        login(&base, &with("https://evil.example"), "test-password").await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn forwarded_host_is_ignored_from_an_untrusted_peer() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let hs = [
        ("x-forwarded-host", "evil.example"),
        ("origin", "http://evil.example"),
    ];
    assert_eq!(
        login(&base, &hs, "test-password").await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn panel_pages_keep_the_origin_on_same_origin_posts() {
    // `no-referrer` would make browsers send `Origin: null` on the login form post
    let h = server_with(|_| {}).await;
    let r = reqwest::get(format!("http://{}/login", h.addr))
        .await
        .unwrap();
    assert_eq!(r.headers()["referrer-policy"], "same-origin");
}
