//! An agent's self-reported `--public-ip` is only shown as "reported": the address list and the
//! Cloudflare WAF expression use the address the server observed, so an agent-token holder
//! cannot put an arbitrary IP into the rule the admin pastes.
use futures_util::SinkExt;
use loadfleet_protocol::{encode, AgentMsg, PROTOCOL_VERSION};
use loadfleet_server::*;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

mod support;
use support::wait_until;

#[tokio::test]
async fn a_reported_public_ip_never_reaches_the_waf_expression() {
    let dir = tempfile::tempdir().unwrap().keep();
    let h = serve(ServerConfig {
        db: dir.join("r.db"),
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
    c.post(format!("{base}/login"))
        .form(&[("password", "test-password")])
        .send()
        .await
        .unwrap();
    let mut req = format!("ws://{}/agent", h.addr)
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "authorization",
        format!("Bearer {}", h.token).parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let hello = AgentMsg::Hello {
        proto: PROTOCOL_VERSION,
        version: "t".into(),
        name: "liar".into(),
        host: loadfleet_health::HostInfo::detect(),
        max_conns: 0,
        public_ip: Some("198.51.100.66".into()),
    };
    ws.send(Message::text(encode(&hello))).await.unwrap();
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents().iter().any(|a| a.name == "liar"), 5).await);
    let a = hub.agents().into_iter().find(|a| a.name == "liar").unwrap();
    assert_eq!(a.ip, "127.0.0.1");
    assert_eq!(a.reported_ip.as_deref(), Some("198.51.100.66"));
    let get = |p: &str| c.get(format!("{base}{p}")).send();
    let api: serde_json::Value = get("/api/agents").await.unwrap().json().await.unwrap();
    assert_eq!(api["cloudflare"], "(ip.src in {127.0.0.1})");
    assert_eq!(api["ips"], serde_json::json!(["127.0.0.1"]));
    let cf = get("/ui/cloudflare").await.unwrap().text().await.unwrap();
    assert!(
        cf.contains("127.0.0.1") && !cf.contains("198.51.100.66"),
        "{cf}"
    );
    // the claim is still visible to the admin, labelled as such
    let rows = get("/ui/agents").await.unwrap().text().await.unwrap();
    assert!(rows.contains("reported 198.51.100.66"), "{rows}");
}
