mod support;

use loadfleet_metrics::{AgentMeta, RunMeta};
use loadfleet_server::*;
use support::*;

const PAGES: [&str; 6] = [
    "/agents",
    "/new",
    "/live/abc",
    "/history",
    "/runs/abc",
    "/compare?a=x&b=y",
];

async fn server() -> (ServerHandle, reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap().keep();
    let h = serve(ServerConfig {
        db: dir.join("p.db"),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap();
    let c = reqwest::Client::builder()
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let base = format!("http://{}", h.addr);
    (h, c, base)
}

async fn logged_in() -> (ServerHandle, reqwest::Client, String) {
    let (h, c, base) = server().await;
    c.post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    (h, c, base)
}

#[tokio::test]
async fn pages_and_assets_are_served() {
    let (_h, c, base) = logged_in().await;
    for p in PAGES.iter().chain(&["/login"]) {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 200, "{p}");
        let csp = r.headers()["content-security-policy"].to_str().unwrap();
        assert!(csp.contains("script-src 'self'"), "{p}: {csp}");
        assert!(r.text().await.unwrap().contains("<title>"), "{p}");
    }
    let r = c
        .get(format!("{base}/static/vendor/htmx.min.js"))
        .send()
        .await
        .unwrap();
    assert!(r.headers()["content-type"]
        .to_str()
        .unwrap()
        .contains("javascript"));
    let r = c.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(r.headers()["location"], "/agents");
    let r = c
        .get(format!("{base}/static/nope.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    for p in [
        "/static/..%2fCargo.toml",
        "/static/..%2f..%2fCargo.toml",
        "/static/%2fetc%2fpasswd",
    ] {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 404, "{p}");
    }
}

#[tokio::test]
async fn pages_and_fragments_require_a_session() {
    let (_h, c, base) = server().await;
    for p in PAGES.iter().chain(&["/"]) {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 303, "{p}");
        assert_eq!(r.headers()["location"], "/login", "{p}");
    }
    for p in [
        "/ui/agents",
        "/ui/cloudflare",
        "/ui/runs",
        "/ui/runs/x/summary",
        "/ui/runs/x/agents",
        "/ui/compare?a=x&b=y",
        "/ui/template?name=WS+echo",
    ] {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 401, "{p}");
    }
    let r = c
        .post(format!("{base}/ui/start"))
        .header("x-loadfleet", "1")
        .form(&[("plan", "name: p")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // the login page and the assets it needs stay public
    for p in ["/login", "/static/app.css"] {
        let r = c.get(format!("{base}{p}")).send().await.unwrap();
        assert_eq!(r.status(), 200, "{p}");
    }
}

#[tokio::test]
async fn panel_posts_need_the_csrf_header() {
    let (_h, c, base) = logged_in().await;
    for p in ["/ui/validate", "/ui/start"] {
        let r = c
            .post(format!("{base}{p}"))
            .form(&[("plan", "name: p")])
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403, "{p}");
    }
}

#[tokio::test]
async fn fragments_escape_agent_supplied_text() {
    let (h, c, base) = logged_in().await;
    let _a = FakeAgent::connect(h.addr, &h.token, "<img src=x onerror=alert(1)>", 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let html = c
        .get(format!("{base}/ui/agents"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // the hub rejects such a name outright; either way it must never reach the page raw
    assert!(!html.contains("<img src=x"), "{html}");
    let _b = FakeAgent::connect(h.addr, &h.token, "m5", 0).await;
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents().iter().any(|a| a.name == "m5"), 5).await);
    let html = c
        .get(format!("{base}/ui/agents"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("m5") && html.contains("127.0.0.1"), "{html}");
    let cf = c
        .get(format!("{base}/ui/cloudflare"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(cf.contains("(ip.src in {127.0.0.1})"), "{cf}");
}

#[test]
fn esc_covers_html_and_attribute_metacharacters() {
    assert_eq!(
        esc(r#"<a href="x" onclick='y'>&</a>"#),
        "&lt;a href=&quot;x&quot; onclick=&#39;y&#39;&gt;&amp;&lt;/a&gt;"
    );
}

#[tokio::test]
async fn stored_run_text_is_escaped_and_plan_text_is_not_shown() {
    let (h, c, base) = logged_in().await;
    let evil = "<script>alert(1)</script>";
    let meta = RunMeta {
        id: "r1".into(),
        name: evil.into(),
        plan_yaml: "secret-header-value".into(),
        status: "completed".into(),
        verdict: evil.into(),
        agents: vec![AgentMeta {
            name: evil.into(),
            ip: evil.into(),
            ..Default::default()
        }],
        missing_agents: vec![evil.into()],
        ..Default::default()
    };
    h.state.store.create_run(&meta).await.unwrap();
    for p in [
        "/ui/runs",
        "/ui/runs/r1/summary",
        "/ui/runs/r1/agents",
        "/ui/compare?a=r1&b=r1",
    ] {
        let html = c
            .get(format!("{base}{p}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!html.contains("<script"), "{p}: {html}");
        assert!(html.contains("&lt;script&gt;"), "{p}: {html}");
        assert!(!html.contains("secret-header-value"), "{p}");
    }
    let r = c.get(format!("{base}/ui/compare")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("History"));
}

#[tokio::test]
async fn plan_errors_are_escaped() {
    let (_h, c, base) = logged_in().await;
    let plan = "name: \"<b>x</b>\"\ntarget: { allow: [\"<i>evil</i>\"] }\n";
    for p in ["/ui/validate", "/ui/start"] {
        let html = c
            .post(format!("{base}{p}"))
            .header("x-loadfleet", "1")
            .form(&[("plan", plan)])
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(html.contains("class=\"errors\""), "{p}: {html}");
        assert!(
            !html.contains("<b>") && !html.contains("<i>"),
            "{p}: {html}"
        );
    }
}

#[tokio::test]
async fn templates_fill_the_editor() {
    let (_h, c, base) = logged_in().await;
    let html = c
        .get(format!("{base}/ui/template?name=TCP+connect+%26+hold"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.starts_with("<textarea id=\"plan\""), "{html}");
    assert!(html.contains("type: tcp"), "{html}");
    let empty = c
        .get(format!("{base}/ui/template?name=nope"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(empty.contains("></textarea>"), "{empty}");
}

/// The template picker comes from the server's template list (built-ins + registered plugins),
/// so the static page names no scenario a build may not have.
#[tokio::test]
async fn the_template_picker_is_rendered_from_the_registry() {
    let (_h, c, base) = logged_in().await;
    let page = c
        .get(format!("{base}/new"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(page.contains("hx-get=\"/ui/template-select\""), "{page}");
    assert!(
        !page.contains("<option>"),
        "no hard-coded templates: {page}"
    );
    let select = c
        .get(format!("{base}/ui/template-select"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(select.starts_with("<select name=\"name\""), "{select}");
    assert!(select.contains("hx-get=\"/ui/template\""), "{select}");
    for name in [
        "HTTP GET",
        "HTTP POST JSON",
        "TCP connect &amp; hold",
        "WS echo",
    ] {
        assert!(
            select.contains(&format!("<option>{name}</option>")),
            "{select}"
        );
    }
}

#[tokio::test]
async fn start_from_panel_redirects_to_live() {
    let (h, c, base) = logged_in().await;
    let _a = FakeAgent::connect(h.addr, &h.token, "a", 0).await;
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents().iter().any(|a| a.readiness.is_some()), 5).await);
    let plan = "name: p\ntarget: { allow: [\"127.0.0.1\"] }\nscenario: { type: http, http: { url: \"http://127.0.0.1:9/\" } }\nload: { stages: [{target: 1, ramp: 0s, hold: 30s}] }\n";
    let bad = c
        .post(format!("{base}/ui/validate"))
        .header("x-loadfleet", "1")
        .form(&[("plan", "name: [")])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(bad.contains("class=\"errors\""));
    let ok = c
        .post(format!("{base}/ui/validate"))
        .header("x-loadfleet", "1")
        .form(&[("plan", plan)])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(ok.contains("plan is valid"), "{ok}");
    let r = c
        .post(format!("{base}/ui/start"))
        .header("x-loadfleet", "1")
        .form(&[("name", "HTTP GET"), ("plan", plan)])
        .send()
        .await
        .unwrap();
    let to = r.headers()["hx-redirect"].to_str().unwrap().to_string();
    assert!(to.starts_with("/live/"));
    let id = to.trim_start_matches("/live/").to_string();
    let summary = c
        .get(format!("{base}/ui/runs/{id}/summary"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(summary.contains(&id), "{summary}");
    h.state.coord.stop_run(&id, "test");
}

#[tokio::test]
async fn finished_runs_stop_htmx_polling_and_fragments_carry_csp() {
    let (h, c, base) = logged_in().await;
    for (id, status, code) in [("done", "completed", 286), ("live", "running", 200)] {
        let meta = RunMeta {
            id: id.into(),
            name: "n".into(),
            status: status.into(),
            ..Default::default()
        };
        h.state.store.create_run(&meta).await.unwrap();
        for p in [
            format!("/ui/runs/{id}/summary"),
            format!("/ui/runs/{id}/agents"),
        ] {
            let r = c.get(format!("{base}{p}")).send().await.unwrap();
            assert_eq!(r.status().as_u16(), code, "{p}");
            let csp = r.headers()["content-security-policy"].to_str().unwrap();
            assert!(csp.contains("script-src 'self'"), "{p}");
            assert_eq!(r.headers()["x-content-type-options"], "nosniff");
        }
        let html = c
            .get(format!("{base}/ui/runs/{id}/summary"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            html.contains(&format!("data-status=\"{status}\"")),
            "{html}"
        );
    }
}

#[tokio::test]
async fn agents_fragment_escapes_agent_reported_host_fields() {
    use futures_util::SinkExt;
    use loadfleet_protocol::{encode, AgentMsg, PROTOCOL_VERSION};
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let (h, c, base) = logged_in().await;
    let mut req = format!("ws://{}/agent", h.addr)
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "authorization",
        format!("Bearer {}", h.token).parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let evil = "<script>alert(1)</script>";
    let mut host = loadfleet_health::HostInfo::detect();
    host.os = evil.into();
    host.arch = evil.into();
    let hello = AgentMsg::Hello {
        proto: PROTOCOL_VERSION,
        version: evil.into(),
        name: "evil".into(),
        host,
        max_conns: 0,
        public_ip: None,
    };
    ws.send(Message::text(encode(&hello))).await.unwrap();
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents().iter().any(|a| a.name == "evil"), 5).await);
    let html = c
        .get(format!("{base}/ui/agents"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!html.contains("<script"), "{html}");
    assert!(
        html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
        "{html}"
    );
}
