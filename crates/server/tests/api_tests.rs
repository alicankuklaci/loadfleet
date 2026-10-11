mod support;

use loadfleet_server::*;
use reqwest::StatusCode;
use serde_json::Value;
use support::*;

async fn server() -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    serve(ServerConfig {
        db: dir.join("a.db"),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap()
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn login(c: &reqwest::Client, base: &str) {
    let r = c
        .post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn api_requires_session_and_rate_limits_logins() {
    let h = server().await;
    let base = format!("http://{}", h.addr);
    let c = client();
    assert_eq!(
        c.get(format!("{base}/api/agents"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for _ in 0..5 {
        let r = c
            .post(format!("{base}/login"))
            .form(&[("password", "nope")])
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }
    let r = c
        .post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn agents_cloudflare_list_and_csrf_guard() {
    let h = server().await;
    let base = format!("http://{}", h.addr);
    let c = client();
    login(&c, &base).await;
    let _a = FakeAgent::connect(h.addr, &h.token, "m5", 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let v: Value = c
        .get(format!("{base}/api/agents"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["agents"][0]["name"], "m5");
    assert_eq!(v["cloudflare"], "(ip.src in {127.0.0.1})");
    let r = c
        .post(format!("{base}/api/runs/validate"))
        .body("name: x")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "missing x-loadfleet header"
    );
    let v: Value = c
        .post(format!("{base}/api/runs/validate"))
        .header("x-loadfleet", "1")
        .body("name: x")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["ok"], false);
    let old = h.token.clone();
    let v: Value = c
        .post(format!("{base}/api/token/rotate"))
        .header("x-loadfleet", "1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(v["token"].as_str().unwrap(), old);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_watch_export_compare() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let h = server().await;
    let base = format!("http://{}", h.addr);
    let c = client();
    login(&c, &base).await;
    let (addr, _) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let stop = loadfleet_engine::CancellationToken::new();
    let cfg =
        loadfleet_agent::AgentConfig::new(format!("ws://{}", h.addr), h.token.clone(), "a1".into());
    let s = stop.clone();
    tokio::spawn(async move {
        let _ = loadfleet_agent::run_agent(cfg, s).await;
    });
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents().iter().any(|a| a.readiness.is_some()), 5).await);
    let plan = format!("name: api\ntarget: {{ allow: [\"127.0.0.1\"] }}\nscenario: {{ type: http, http: {{ url: \"http://{addr}/\" }} }}\nload: {{ stages: [{{target: 5, ramp: 0s, hold: 2s}}] }}\n");
    let mut ids = Vec::new();
    for _ in 0..2 {
        let r = c
            .post(format!("{base}/api/runs"))
            .header("x-loadfleet", "1")
            .body(plan.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        let id = r.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let busy = c
            .post(format!("{base}/api/runs"))
            .header("x-loadfleet", "1")
            .body(plan.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(busy.status(), StatusCode::CONFLICT);
        for _ in 0..150 {
            let v: Value = c
                .get(format!("{base}/api/runs/{id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if v["meta"]["status"] != "running" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        ids.push(id);
    }
    let md = c
        .get(format!("{base}/api/runs/{}/export.md", ids[0]))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        md.contains("**verdict:** completed all stages up to 5"),
        "{md}"
    );
    let csv = c
        .get(format!("{base}/api/runs/{}/export.csv", ids[0]))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(csv.starts_with("t_s,agents,ops"));
    let series: Value = c
        .get(format!("{base}/api/runs/{}/series?since=0", ids[0]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(series.as_array().unwrap().len() >= 2);
    let cmp = c
        .get(format!("{base}/api/compare?a={}&b={}", ids[0], ids[1]))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(cmp.contains(" → "));
    let t: Value = c
        .get(format!("{base}/api/templates"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // the four built-in templates; plugin templates come only with a registered plugin
    assert_eq!(t.as_array().unwrap().len(), 4);
    stop.cancel();
}
