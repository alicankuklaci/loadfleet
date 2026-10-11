//! Session, CSRF, cookie and proxy-IP rules of the HTTP API; what it does and does not return.
mod support;

use loadfleet_metrics::RunMeta;
use loadfleet_server::*;
use reqwest::header::SET_COOKIE;
use reqwest::StatusCode;
use serde_json::Value;

async fn server_with(f: impl FnOnce(&mut ServerConfig)) -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut cfg = ServerConfig {
        db: dir.join("s.db"),
        ..ServerConfig::for_tests()
    };
    f(&mut cfg);
    serve(cfg).await.unwrap()
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn login(c: &reqwest::Client, base: &str) -> reqwest::Response {
    c.post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap()
}

const GETS: [&str; 10] = [
    "/api/agents",
    "/api/token",
    "/api/runs",
    "/api/runs/x",
    "/api/runs/x/series?since=0",
    "/api/runs/x/export.json",
    "/api/compare?a=x&b=y",
    "/api/templates",
    "/api/active",
    "/api/runs/x/export.md",
];
const POSTS: [&str; 4] = [
    "/api/token/rotate",
    "/api/runs/validate",
    "/api/runs",
    "/api/runs/x/stop",
];

#[tokio::test]
async fn every_api_route_needs_a_session_and_every_post_the_csrf_header() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let c = client();
    for p in GETS {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "GET {p}");
    }
    for p in POSTS {
        let r = c
            .post(format!("{base}{p}"))
            .header("x-loadfleet", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "POST {p}");
    }
    let before = h.state.hub.token();
    assert_eq!(login(&c, &base).await.status(), StatusCode::SEE_OTHER);
    for p in POSTS {
        for hdr in [None, Some("0")] {
            let mut rq = c.post(format!("{base}{p}"));
            if let Some(v) = hdr {
                rq = rq.header("x-loadfleet", v);
            }
            assert_eq!(
                rq.send().await.unwrap().status(),
                StatusCode::FORBIDDEN,
                "POST {p} {hdr:?}"
            );
        }
    }
    assert_eq!(
        h.state.hub.token(),
        before,
        "a refused rotate changed nothing"
    );
}

#[tokio::test]
async fn login_cookie_flags_and_logout_ends_the_session() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let c = client();
    let r = login(&c, &base).await;
    assert_eq!(r.headers()["location"], "/agents");
    let ck = r.headers()[SET_COOKIE].to_str().unwrap().to_string();
    for part in ["lf_session=", "HttpOnly", "SameSite=Strict", "Path=/"] {
        assert!(ck.contains(part), "{ck}");
    }
    assert!(
        !ck.contains("Secure"),
        "plain http, no --secure-cookies: {ck}"
    );
    assert_eq!(
        c.get(format!("{base}/api/active"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let out = c.post(format!("{base}/logout")).send().await.unwrap();
    assert_eq!(out.status(), StatusCode::SEE_OTHER);
    let cleared = out.headers()[SET_COOKIE].to_str().unwrap();
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
    // even a client that kept the old cookie is out
    let sid = ck.split(';').next().unwrap().to_string();
    let r = reqwest::Client::new()
        .get(format!("{base}/api/active"))
        .header("cookie", sid)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn secure_cookie_with_flag_or_tls_terminating_trusted_proxy() {
    let h = server_with(|c| c.secure_cookies = true).await;
    let r = login(&client(), &format!("http://{}", h.addr)).await;
    assert!(r.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .contains("; Secure"));

    let h = server_with(|c| c.trusted_proxies = vec![parse_proxy("127.0.0.1").unwrap()]).await;
    let r = client()
        .post(format!("http://{}/login", h.addr))
        .header("x-forwarded-proto", "https")
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    assert!(r.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .contains("; Secure"));

    // the same header from an untrusted peer is ignored
    let h = server_with(|_| {}).await;
    let r = client()
        .post(format!("http://{}/login", h.addr))
        .header("x-forwarded-proto", "https")
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    assert!(!r.headers()[SET_COOKIE].to_str().unwrap().contains("Secure"));
}

async fn fail_five(base: &str, xff: &str) {
    for _ in 0..5 {
        let r = client()
            .post(format!("{base}/login"))
            .header("x-forwarded-for", xff)
            .form(&[("password", "nope")])
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }
}

async fn login_from(base: &str, xff: &str) -> StatusCode {
    client()
        .post(format!("{base}/login"))
        .header("x-forwarded-for", xff)
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn rate_limit_keys_on_the_real_client_only_behind_a_trusted_proxy() {
    // no trusted proxy: a forged X-Forwarded-For cannot dodge the limit
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    fail_five(&base, "203.0.113.1").await;
    assert_eq!(
        login_from(&base, "203.0.113.2").await,
        StatusCode::TOO_MANY_REQUESTS
    );

    // behind a trusted proxy each forwarded client has its own budget
    let h = server_with(|c| c.trusted_proxies = vec![parse_proxy("127.0.0.0/8").unwrap()]).await;
    let base = format!("http://{}", h.addr);
    fail_five(&base, "203.0.113.1").await;
    assert_eq!(
        login_from(&base, "203.0.113.1").await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        login_from(&base, "203.0.113.2").await,
        StatusCode::SEE_OTHER
    );
}

#[tokio::test]
async fn run_list_hides_plans_and_errors_map_to_status_codes() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let c = client();
    login(&c, &base).await;
    let meta = RunMeta {
        id: "r1".into(),
        name: "old".into(),
        plan_yaml: "headers: { authorization: literal-secret }".into(),
        status: "completed".into(),
        ..Default::default()
    };
    h.state.store.create_run(&meta).await.unwrap();
    let list = c
        .get(format!("{base}/api/runs"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(list.contains("\"r1\""), "{list}");
    assert!(!list.contains("literal-secret"), "{list}");
    let body = c
        .get(format!("{base}/api/agents"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !body.contains(&h.token),
        "the agent token is only served by /api/token"
    );

    let post = |p: &str, body: &str| {
        c.post(format!("{base}{p}"))
            .header("x-loadfleet", "1")
            .body(body.to_string())
            .send()
    };
    let r = post("/api/runs", "name: [").await.unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    assert!(r.json::<Value>().await.unwrap()["error"]
        .as_str()
        .unwrap()
        .contains("invalid plan"));
    let plan = "name: p\ntarget: { allow: [\"127.0.0.1\"] }\nscenario: { type: http, http: { url: \"http://127.0.0.1:9/\" } }\nload: { stages: [{target: 1, ramp: 0s, hold: 1s}] }\n";
    let v: Value = post("/api/runs/validate", plan)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!({ "ok": true, "errors": [] }));
    let off_list = plan
        .replace("http://127.0.0.1:9/", "http://10.9.9.9/")
        .replace("127.0.0.1", "10.9.9.9");
    let v: Value = post("/api/runs/validate", &off_list)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["ok"], false, "server allow-list applies: {v}");
    let r = post("/api/runs", plan).await.unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT, "no agents connected");
    assert_eq!(
        post("/api/runs/r1/stop", "").await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let get = |p: &str| c.get(format!("{base}{p}")).send();
    assert_eq!(
        get("/api/runs/nope").await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get("/api/runs/r1/export.exe").await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get("/api/compare?a=r1&b=nope").await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let r = get("/api/runs/r1/export.json").await.unwrap();
    assert_eq!(
        r.headers()["content-disposition"],
        "attachment; filename=\"loadfleet-r1-export.json\""
    );
    let v: Value = get("/api/active").await.unwrap().json().await.unwrap();
    assert_eq!(v, serde_json::json!({ "id": null }));
}

#[tokio::test]
async fn rotate_persists_and_only_new_agents_need_the_new_token() {
    let h = server_with(|_| {}).await;
    let base = format!("http://{}", h.addr);
    let c = client();
    login(&c, &base).await;
    let _old = support::FakeAgent::connect(h.addr, &h.token, "keep", 0).await;
    let hub = h.state.hub.clone();
    assert!(support::wait_until(|| hub.agents().len() == 1, 5).await);
    let v: Value = c
        .post(format!("{base}/api/token/rotate"))
        .header("x-loadfleet", "1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let new = v["token"].as_str().unwrap().to_string();
    assert_eq!(new.len(), 48);
    assert_eq!(h.state.hub.token(), new);
    assert_eq!(
        h.state
            .store
            .setting("agent_token")
            .await
            .unwrap()
            .as_deref(),
        Some(new.as_str())
    );
    let v: Value = c
        .get(format!("{base}/api/token"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["token"], new);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(h.state.hub.agents().len(), 1, "connected agents stay");
}

#[tokio::test]
async fn generated_admin_password_is_stored_only_as_a_hash() {
    let dir = tempfile::tempdir().unwrap().keep();
    let cfg = || ServerConfig {
        db: dir.join("g.db"),
        admin_password: None,
        ..ServerConfig::for_tests()
    };
    let h = serve(cfg()).await.unwrap();
    let hash = h.state.store.setting("admin_hash").await.unwrap().unwrap();
    assert!(hash.starts_with("$argon2"), "{hash}");
    h.kill();
    let h = serve(cfg()).await.unwrap();
    assert_eq!(
        h.state.store.setting("admin_hash").await.unwrap().unwrap(),
        hash,
        "reused"
    );
    let r = login(&client(), &format!("http://{}", h.addr)).await;
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}
