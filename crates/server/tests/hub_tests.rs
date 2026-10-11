use futures_util::{SinkExt, StreamExt};
use loadfleet_health::HostInfo;
use loadfleet_protocol::*;
use loadfleet_server::*;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn start() -> (
    SocketAddr,
    String,
    tokio_util::sync::CancellationToken,
    tempfile::TempDir,
) {
    let stop = tokio_util::sync::CancellationToken::new();
    let dir = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        db: dir.path().join("lf.db"),
        ..ServerConfig::for_tests()
    };
    let (addr, token) = serve_for_tests(cfg, stop.clone()).await.unwrap();
    (addr, token, stop, dir)
}

/// Serves a router over a hand-built state so the test can watch hub events.
async fn start_with_events(token: &str) -> (SocketAddr, Hub, UnboundedReceiver<HubEvent>) {
    start_with_cfg(token, ServerConfig::for_tests()).await
}

async fn start_with_cfg(
    token: &str,
    cfg: ServerConfig,
) -> (SocketAddr, Hub, UnboundedReceiver<HubEvent>) {
    let (hub, events) = Hub::new(token.into(), Default::default());
    let (cfg, store) = (Arc::new(cfg), Store::memory().unwrap());
    let coord = Coordinator::new(hub.clone(), store.clone(), cfg.clone());
    let state = AppState {
        cfg,
        store,
        hub: hub.clone(),
        coord,
        auth: Arc::new(Auth::new("test-password", false)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, app).await });
    (addr, hub, events)
}

async fn dial(addr: SocketAddr, token: &str) -> Result<Ws, String> {
    let mut req = format!("ws://{addr}/agent").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(req)
        .await
        .map(|(ws, _)| ws)
        .map_err(|e| e.to_string())
}

fn hello_named(proto: u32, name: &str) -> String {
    encode(&AgentMsg::Hello {
        proto,
        version: "0.1.0".into(),
        name: name.into(),
        host: HostInfo::detect(),
        max_conns: 50_000,
        public_ip: None,
    })
}

fn hello(proto: u32) -> String {
    hello_named(proto, "m5")
}

/// Next text frame decoded as a server message, or None when the socket ends / times out.
async fn next_msg(ws: &mut Ws) -> Option<ServerMsg> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return decode::<ServerMsg>(t.as_str()).ok(),
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
            Ok(Some(Ok(_))) => continue,
        }
    }
}

/// Waits for the socket to end, skipping server traffic. True if it ended within 5 s.
async fn closes(ws: &mut Ws) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => return true,
            Ok(Some(Ok(_))) => continue,
            Err(_) => return false,
        }
    }
    false
}

async fn next_event(ev: &mut UnboundedReceiver<HubEvent>) -> HubEvent {
    tokio::time::timeout(Duration::from_secs(5), ev.recv())
        .await
        .expect("event in time")
        .expect("hub alive")
}

#[tokio::test]
async fn wrong_token_is_rejected_before_upgrade() {
    let (addr, _token, _stop, _dir) = start().await;
    let err = dial(addr, "nope").await.unwrap_err();
    assert!(err.contains("401"), "{err}");
}

#[tokio::test]
async fn empty_token_never_authenticates() {
    let (addr, _hub, _ev) = start_with_events("").await;
    let err = dial(addr, "").await.unwrap_err();
    assert!(err.contains("401"), "{err}");
}

#[tokio::test]
async fn protocol_mismatch_gets_update_message() {
    let (addr, token, _stop, _dir) = start().await;
    let mut ws = dial(addr, &token).await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION + 1)))
        .await
        .unwrap();
    let Some(Ok(Message::Text(t))) = ws.next().await else {
        panic!("no reply")
    };
    match decode::<ServerMsg>(t.as_str()).unwrap() {
        ServerMsg::Reject { reason } => assert!(reason.contains("update"), "{reason}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn old_hello_shape_still_gets_update_message() {
    let (addr, token, _stop, _dir) = start().await;
    let mut ws = dial(addr, &token).await.unwrap();
    // an agent from another protocol generation whose hello lacks today's fields
    ws.send(Message::text(r#"{"t":"hello","proto":0,"agent":"x"}"#))
        .await
        .unwrap();
    match next_msg(&mut ws).await {
        Some(ServerMsg::Reject { reason }) => assert!(reason.contains("update"), "{reason}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn hello_fields_beyond_proto_and_name_default() {
    let m: AgentMsg = decode(r#"{"t":"hello","proto":1,"name":"m5"}"#).unwrap();
    let AgentMsg::Hello {
        name,
        max_conns,
        public_ip,
        version,
        ..
    } = m
    else {
        panic!()
    };
    assert_eq!(
        (name.as_str(), max_conns, public_ip, version.as_str()),
        ("m5", 0, None, "")
    );
}

#[tokio::test]
async fn clock_sync_measures_offset_and_lists_agent() {
    let (addr, token, _stop, _dir) = start().await;
    let mut ws = dial(addr, &token).await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    let mut pongs = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pongs < CLOCK_ROUNDS && tokio::time::Instant::now() < deadline {
        let Some(Ok(Message::Text(t))) = ws.next().await else {
            break;
        };
        if let Ok(ServerMsg::Ping { id, server_ms }) = decode::<ServerMsg>(t.as_str()) {
            // pretend our clock is 200 ms ahead
            let pong = AgentMsg::Pong {
                id,
                server_ms,
                agent_ms: now_ms() + 200,
            };
            ws.send(Message::text(encode(&pong))).await.unwrap();
            pongs += 1;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agents: serde_json::Value = reqwest_like_get(addr, "/healthz/agents").await;
    let a = &agents[0];
    assert_eq!(a["name"], "m5");
    let off = a["offset_ms"].as_f64().unwrap();
    assert!((150.0..250.0).contains(&off), "offset {off}");
    assert_eq!(a["readiness"], "warn");
    assert_eq!(a["ip"], "127.0.0.1");
}

#[tokio::test]
async fn forged_pong_server_ms_is_ignored() {
    let (addr, hub, _ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    let mut pongs = 0;
    while pongs < CLOCK_ROUNDS {
        if let Some(ServerMsg::Ping { id, .. }) = next_msg(&mut ws).await {
            // echo a made-up send time: only the server's own record may be used
            let pong = AgentMsg::Pong {
                id,
                server_ms: 0,
                agent_ms: now_ms(),
            };
            ws.send(Message::text(encode(&pong))).await.unwrap();
            pongs += 1;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let off = hub.agents()[0].offset_ms.unwrap();
    assert!(off.abs() < 50.0, "offset {off}");
}

#[tokio::test]
async fn same_name_reconnect_replaces_and_surfaces_left() {
    let (addr, hub, mut ev) = start_with_events("tok").await;
    let mut first = dial(addr, "tok").await.unwrap();
    first
        .send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    assert!(matches!(next_event(&mut ev).await, HubEvent::Joined(id) if id == "m5"));
    let mut second = dial(addr, "tok").await.unwrap();
    second
        .send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    // the stale socket's departure is reported before the new one joins
    assert!(matches!(next_event(&mut ev).await, HubEvent::Left(id) if id == "m5"));
    assert!(matches!(next_event(&mut ev).await, HubEvent::Joined(id) if id == "m5"));
    assert!(closes(&mut first).await, "stale socket must be closed");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(hub.agents().len(), 1);
    assert!(hub.send("m5", ServerMsg::Heartbeat { server_ms: 1 }));
    // the stale socket ending must not remove the live entry or emit a second Left
    assert!(ev.try_recv().is_err());
    drop(second);
    assert!(matches!(next_event(&mut ev).await, HubEvent::Left(id) if id == "m5"));
    assert!(hub.agents().is_empty());
}

#[tokio::test]
async fn bad_names_are_rejected() {
    let (addr, _hub, _ev) = start_with_events("tok").await;
    for name in ["", "a b", &"x".repeat(65)] {
        let mut ws = dial(addr, "tok").await.unwrap();
        ws.send(Message::text(hello_named(PROTOCOL_VERSION, name)))
            .await
            .unwrap();
        assert!(
            matches!(next_msg(&mut ws).await, Some(ServerMsg::Reject { .. })),
            "{name:?}"
        );
    }
}

#[tokio::test]
async fn message_flood_disconnects_agent() {
    let (addr, hub, _ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    let hb = encode(&AgentMsg::Heartbeat { agent_ms: 1 });
    for _ in 0..2_000 {
        if ws.send(Message::text(hb.clone())).await.is_err() {
            break;
        }
    }
    assert!(closes(&mut ws).await, "flooding agent must be dropped");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.agents().is_empty());
}

#[tokio::test]
async fn silent_agent_with_stalled_writer_leaves_within_silence() {
    let cfg = ServerConfig {
        agent_silence: Duration::from_secs(2),
        ..ServerConfig::for_tests()
    };
    let (addr, hub, mut ev) = start_with_cfg("tok", cfg).await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    assert!(matches!(next_event(&mut ev).await, HubEvent::Joined(_)));
    // the agent stops reading (and sending); big frames fill the socket so the writer stalls
    let big = "x".repeat(1 << 20);
    let t0 = std::time::Instant::now();
    for _ in 0..64 {
        let stop = ServerMsg::Stop {
            run_id: big.clone(),
            reason: "fill".into(),
        };
        if !hub.send("m5", stop) {
            break;
        }
    }
    let left = tokio::time::timeout(Duration::from_secs(6), ev.recv()).await;
    assert!(matches!(left, Ok(Some(HubEvent::Left(_)))), "{left:?}");
    assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
    assert!(hub.agents().is_empty());
    drop(ws);
}

#[tokio::test]
async fn binary_frame_is_rejected() {
    let (addr, hub, _ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    ws.send(Message::binary(vec![1u8, 2, 3])).await.unwrap();
    let reason = loop {
        match next_msg(&mut ws).await {
            Some(ServerMsg::Reject { reason }) => break reason,
            Some(_) => continue,
            None => panic!("closed without a reject"),
        }
    };
    assert!(reason.contains("binary"), "{reason}");
    assert!(closes(&mut ws).await);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(hub.agents().is_empty());
}

#[tokio::test]
async fn ws_ping_flood_disconnects_agent() {
    let (addr, _hub, _ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    for _ in 0..2_000 {
        if ws.send(Message::Ping(vec![0u8; 8].into())).await.is_err() {
            break;
        }
    }
    assert!(
        closes(&mut ws).await,
        "ping flood must count against the rate limit"
    );
}

#[tokio::test]
async fn oversized_frame_disconnects_agent() {
    let (addr, _hub, _ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    let _ = ws
        .send(Message::text("x".repeat(MAX_FRAME_BYTES + 1)))
        .await;
    assert!(closes(&mut ws).await);
}

#[tokio::test]
async fn metrics_with_absurd_rel_ms_are_dropped() {
    let (addr, _hub, mut ev) = start_with_events("tok").await;
    let mut ws = dial(addr, "tok").await.unwrap();
    ws.send(Message::text(hello(PROTOCOL_VERSION)))
        .await
        .unwrap();
    assert!(matches!(next_event(&mut ev).await, HubEvent::Joined(_)));
    let metrics = |seq: u64, rel_ms: i64| {
        encode(&AgentMsg::Metrics {
            run_id: "r".into(),
            seq,
            rel_ms,
            snap: Default::default(),
            health: Default::default(),
        })
    };
    ws.send(Message::text(metrics(1, i64::MAX))).await.unwrap();
    ws.send(Message::text(metrics(2, -10_000_000)))
        .await
        .unwrap();
    ws.send(Message::text(metrics(3, 1_000))).await.unwrap();
    match next_event(&mut ev).await {
        HubEvent::Msg(id, AgentMsg::Metrics { seq, .. }) => {
            assert_eq!((id.as_str(), seq), ("m5", 3))
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn token_can_be_rotated() {
    let (addr, hub, _ev) = start_with_events("old").await;
    hub.set_token("new".into());
    assert_eq!(hub.token(), "new");
    assert!(dial(addr, "old").await.unwrap_err().contains("401"));
    assert!(dial(addr, "new").await.is_ok());
}

#[test]
fn ip_and_cloudflare_expression() {
    use axum::http::HeaderMap;
    let peer = "10.0.0.5:5000".parse().unwrap();
    let proxy = [parse_proxy("10.0.0.0/8").unwrap()];
    let mut h = HeaderMap::new();
    h.insert("cf-connecting-ip", "203.0.113.7".parse().unwrap());
    h.insert("x-forwarded-for", "198.51.100.1, 10.0.0.1".parse().unwrap());
    assert_eq!(client_ip(&h, peer, &proxy, true).to_string(), "203.0.113.7");
    assert_eq!(client_ip(&h, peer, &[], true).to_string(), "10.0.0.5");
    h.remove("cf-connecting-ip");
    assert_eq!(
        client_ip(&h, peer, &proxy, false).to_string(),
        "198.51.100.1"
    );
    h.insert("x-forwarded-for", "not-an-ip".parse().unwrap());
    assert_eq!(client_ip(&h, peer, &proxy, false).to_string(), "10.0.0.5");
    // IPv4 first (numeric order), then IPv6 widened to its /64; duplicates collapse
    let e = cloudflare_expression(&[
        "203.0.113.7".into(),
        "2001:db8::1".into(),
        "203.0.113.7".into(),
    ]);
    assert_eq!(e, "(ip.src in {203.0.113.7 2001:db8::/64})");
    let e = cloudflare_expression(&[
        "2a00:1450:4001:82a:1234::9".into(),
        "2a00:1450:4001:82a::7".into(),
        "::ffff:198.51.100.20".into(),
        "9.9.9.9".into(),
        "junk".into(),
    ]);
    assert_eq!(
        e,
        "(ip.src in {9.9.9.9 198.51.100.20 2a00:1450:4001:82a::/64})"
    );
    assert_eq!(cloudflare_expression(&["junk".into()]), "");
}

/// Minimal HTTP GET without extra deps (hyper client over a TcpStream).
async fn reqwest_like_get(addr: SocketAddr, path: &str) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.unwrap();
    let body = buf.split("\r\n\r\n").nth(1).unwrap();
    serde_json::from_str(body).unwrap()
}
