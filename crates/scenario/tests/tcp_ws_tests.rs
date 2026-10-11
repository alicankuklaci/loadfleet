use loadfleet_engine::{execute, CancellationToken, Schedule, Work};
use loadfleet_metrics::{decode_hist, Recorder, Snapshot};
use loadfleet_plan::{parse, Model, Stage};
use loadfleet_scenario::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

fn scenario(yaml: &str, vus: u64) -> std::sync::Arc<dyn loadfleet_engine::Scenario> {
    let p = parse(yaml).unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    build(
        &p.scenario,
        BuildCtx {
            guard,
            run_id: "r".into(),
            agent: "a".into(),
            max_vus: vus,
        },
    )
    .unwrap()
}

fn work(vus: u64, hold_ms: u64) -> Work {
    let stages = [Stage {
        target: vus,
        ramp: Duration::ZERO,
        hold: Duration::from_millis(hold_ms),
    }];
    Work {
        schedule: Schedule::new(&stages),
        model: Model::Closed,
        max_vus: vus,
        vu_offset: 0,
        id_prefix: "run-r-a".into(),
        seed: 3,
    }
}

async fn run(yaml: &str, vus: u64, hold_ms: u64) -> Snapshot {
    let s = scenario(yaml, vus);
    let rec = Recorder::new();
    execute(
        work(vus, hold_ms),
        s,
        rec.clone(),
        CancellationToken::new(),
        Instant::now(),
    )
    .await
    .unwrap();
    rec.snapshot(0)
}

fn build_err(yaml: &str) -> String {
    let p = parse(yaml).unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    let ctx = BuildCtx {
        guard,
        run_id: "r".into(),
        agent: "a".into(),
        max_vus: 1,
    };
    build(&p.scenario, ctx).err().expect("build must fail")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_hold_keeps_connections_open() {
    let (a1, open1) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (a2, open2) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = format!(
        r#"
name: hold
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: ["{a1}", "{a2}"], hold_ms: 0 }} }}
load: {{ stages: [{{target: 40, ramp: 0s, hold: 1s}}] }}
"#
    );
    let h = tokio::spawn(async move { run(&yaml, 40, 1500).await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        open1.load(Ordering::SeqCst) + open2.load(Ordering::SeqCst),
        40
    );
    assert_eq!(
        open1.load(Ordering::SeqCst),
        20,
        "VUs spread over the address list"
    );
    let snap = h.await.unwrap();
    assert_eq!(snap.ops["tcp.connect"], 40);
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        open1.load(Ordering::SeqCst) + open2.load(Ordering::SeqCst),
        0,
        "closed on stage end"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_send_expect_measures_rtt() {
    let (a, _) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = format!(
        r#"
name: rtt
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: "{a}", send: {{ text: "PING {{{{vu}}}}\n" }}, expect: {{ regex: "PING run-r-a-\\d+" }}, hold_ms: 10 }} }}
load: {{ stages: [{{target: 4, ramp: 0s, hold: 1s}}] }}
"#
    );
    let snap = run(&yaml, 4, 1000).await;
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
    assert!(decode_hist(&snap.hists["tcp.rtt"]).unwrap().len() > 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_hex_expect_and_peer_close() {
    // Server that answers the first read with 0xBEEF and then closes the socket.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut b = [0u8; 64];
                let _ = s.read(&mut b).await;
                let _ = s.write_all(&[0xbe, 0xef]).await;
            });
        }
    });
    let yaml = format!(
        r#"
name: hex
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: "{a}", send: {{ hex: "01ff" }}, expect: {{ hex: "beef" }}, hold_ms: 0 }} }}
load: {{ stages: [{{target: 1, ramp: 0s, hold: 1s}}] }}
"#
    );
    let snap = run(&yaml, 1, 300).await;
    assert!(snap.ops["tcp.rtt"] >= 1);
    assert!(!decode_hist(&snap.hists["tcp.rtt"]).unwrap().is_empty());
    assert!(
        snap.errors.get("tcp:closed_by_peer").copied().unwrap_or(0) >= 1,
        "{:?}",
        snap.errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_stop_closes_held_connections_fast() {
    let (a, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = format!(
        r#"
name: stop
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: "{a}", hold_ms: 0 }} }}
load: {{ stages: [{{target: 10, ramp: 0s, hold: 30s}}] }}
"#
    );
    let s = scenario(&yaml, 10);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let h = tokio::spawn(async move {
        execute(work(10, 30_000), s, Recorder::new(), c, Instant::now())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(open.load(Ordering::SeqCst), 10);
    let t = Instant::now();
    cancel.cancel();
    h.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_eq!(open.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn degenerate_payloads_are_rejected_at_build() {
    let tcp = |extra: &str| {
        format!(
            r#"
name: x
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: "127.0.0.1:1"{extra} }} }}
load: {{ stages: [{{target: 1, ramp: 0s, hold: 1s}}] }}
"#
        )
    };
    let e = build_err(&tcp(r#", send: { text: "x" }, expect: { hex: "" }"#));
    assert!(e.contains("scenario.tcp.expect"), "{e}");
    let e = build_err(&tcp(r#", send: { hex: "" }"#));
    assert!(e.contains("scenario.tcp.send"), "{e}");
    let e = build_err(&tcp(r#", send: { hex: "zz" }"#));
    assert!(e.contains("scenario.tcp.send"), "{e}");
    let e = build_err(&tcp(r#", send: { text: "x" }, expect: { regex: "(" }"#));
    assert!(e.contains("scenario.tcp.expect"), "{e}");
    let ws = r#"
name: x
target: { allow: ["127.0.0.1"] }
scenario: { type: ws, ws: { url: "ws://127.0.0.1:1/ws", steps: [{expect: {hex: "00"}}] } }
load: { stages: [{target: 1, ramp: 0s, hold: 1s}] }
"#;
    let e = build_err(ws);
    assert!(e.contains("hex"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_echo_steps() {
    let (a, open) = loadfleet_testserver::spawn_ws_echo("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = format!(
        r#"
name: ws
target: {{ allow: ["127.0.0.1"] }}
scenario:
  type: ws
  ws:
    url: "ws://{a}/ws"
    on_open: [{{send: "hello {{{{vu}}}}"}}, {{expect: {{regex: "^hello "}}}}]
    steps: [{{send: "ping {{{{iter}}}}"}}, {{expect: {{regex: "^ping \\d+$"}}}}, {{sleep_ms: 50}}]
load: {{ stages: [{{target: 5, ramp: 0s, hold: 1s}}] }}
"#
    );
    let snap = run(&yaml, 5, 1000).await;
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
    assert_eq!(snap.ops["ws.connect"], 5);
    assert!(decode_hist(&snap.hists["ws.rtt"]).unwrap().len() >= 5 * 10);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(open.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connect_and_hold_closes_on_stop() {
    let (a, open) = loadfleet_testserver::spawn_ws_echo("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = format!(
        r#"
name: wshold
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: ws, ws: {{ url: "ws://{a}/ws" }} }}
load: {{ stages: [{{target: 6, ramp: 0s, hold: 30s}}] }}
"#
    );
    let s = scenario(&yaml, 6);
    let rec = Recorder::new();
    let cancel = CancellationToken::new();
    let (c, r) = (cancel.clone(), rec.clone());
    let h = tokio::spawn(async move {
        execute(work(6, 30_000), s, r, c, Instant::now())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(open.load(Ordering::SeqCst), 6);
    assert_eq!(rec.gauge("connections").get(), 6);
    let t = Instant::now();
    cancel.cancel();
    h.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_eq!(open.load(Ordering::SeqCst), 0);
    assert_eq!(rec.gauge("connections").get(), 0);
    assert_eq!(rec.snapshot(0).total_errors(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_target_is_not_retried() {
    // "localhost" passes the name check but resolves to loopback with no IP entry -> blocked
    // after DNS. A guard denial is permanent: each VU records it once and stops.
    let (a, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let port = a.port();
    for (kind, scenario) in [
        (
            "tcp",
            format!(r#"{{ type: tcp, tcp: {{ addr: "localhost:{port}", hold_ms: 0 }} }}"#),
        ),
        (
            "ws",
            format!(r#"{{ type: ws, ws: {{ url: "ws://localhost:{port}/ws" }} }}"#),
        ),
    ] {
        let yaml = format!(
            r#"
name: guard
target: {{ allow: ["localhost"] }}
scenario: {scenario}
load: {{ stages: [{{target: 3, ramp: 0s, hold: 2s}}] }}
"#
        );
        let snap = run(&yaml, 3, 1500).await;
        assert_eq!(open.load(Ordering::SeqCst), 0, "{kind}");
        assert_eq!(snap.total_errors(), 3, "{kind}: {:?}", snap.errors);
        assert!(
            snap.errors.keys().all(|k| k.starts_with("blocked:")),
            "{kind}: {:?}",
            snap.errors
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_target_backs_off() {
    // A dead port for 3 s: a fixed 500 ms retry makes 7 attempts per VU (70 for 10 VUs).
    // Exponential backoff with full jitter (ceilings 0.5, 1, 2, 4 s) makes ~4 per VU.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    let yaml = format!(
        r#"
name: refused
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: tcp, tcp: {{ addr: "{a}", hold_ms: 0 }} }}
load: {{ stages: [{{target: 10, ramp: 0s, hold: 3s}}] }}
"#
    );
    let snap = run(&yaml, 10, 3000).await;
    let errs = snap.total_errors();
    assert!((20..=55).contains(&errs), "{errs}: {:?}", snap.errors);
    assert_eq!(snap.ops["tcp.connect"], errs);
}
