//! `run_assignment` driven through a fake outbound channel: refusals, start
//! alignment with the server clock offset, metrics sequencing, cancel, engine errors.
use loadfleet_agent::run_assignment;
use loadfleet_engine::CancellationToken;
use loadfleet_health::now_ms;
use loadfleet_plan::AllowList;
use loadfleet_protocol::{AgentMsg, Assignment};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

fn tcp_plan(addr: SocketAddr, hold: &str, model: &str) -> loadfleet_plan::Plan {
    let text = format!(
        "name: x\ntarget: {{ allow: [\"127.0.0.1\"] }}\nscenario: {{ type: tcp, tcp: {{ addr: \"{addr}\", hold_ms: 0 }} }}\nload: {{ model: {model}, stages: [{{target: 5, ramp: 0s, hold: {hold}}}] }}\n"
    );
    loadfleet_plan::parse(&text).unwrap()
}

fn assignment(
    plan: loadfleet_plan::Plan,
    share: u64,
    start_in_ms: i64,
    offset_ms: f64,
) -> Assignment {
    Assignment {
        run_id: "r1".into(),
        plan,
        allow: vec!["127.0.0.1".into()],
        stage_targets: vec![share],
        vu_offset: 0,
        agent_index: 0,
        max_vus: 100,
        seed: 7,
        start_at_server_ms: now_ms() + start_in_ms,
        offset_ms,
    }
}

type Run = (mpsc::Receiver<AgentMsg>, CancellationToken);

fn spawn(a: Assignment, local: Option<AllowList>, max_conns: u64) -> Run {
    let (tx, rx) = mpsc::channel(256);
    let cancel = CancellationToken::new();
    tokio::spawn(run_assignment(a, local, max_conns, tx, cancel.clone()));
    (rx, cancel)
}

/// Every message until the task drops its sender.
async fn drain(rx: &mut mpsc::Receiver<AgentMsg>, secs: u64) -> Vec<AgentMsg> {
    let mut v = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    while let Ok(Some(m)) = tokio::time::timeout_at(end, rx.recv()).await {
        v.push(m);
    }
    assert!(
        tokio::time::Instant::now() < end,
        "assignment task did not end: {v:?}"
    );
    v
}

fn only_failure(msgs: &[AgentMsg]) -> String {
    match msgs {
        [AgentMsg::Failed { run_id, error }] if run_id == "r1" => error.clone(),
        other => panic!("expected a single Failed, got {other:?}"),
    }
}

/// Listener that records the wall-clock time of every accept and holds the sockets.
async fn recording_listener() -> (SocketAddr, Arc<Mutex<Vec<i64>>>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = l.accept().await {
            s.lock().unwrap().push(now_ms());
            tokio::spawn(async move {
                let mut b = [0u8; 256];
                while matches!(sock.read(&mut b).await, Ok(n) if n > 0) {}
            });
        }
    });
    (addr, seen)
}

#[tokio::test]
async fn late_assignment_is_refused() {
    let (addr, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (mut rx, _c) = spawn(
        assignment(tcp_plan(addr, "5s", "closed"), 5, -2000, 0.0),
        None,
        1000,
    );
    let err = only_failure(&drain(&mut rx, 3).await);
    assert!(err.contains("late join"), "{err}");
    assert_eq!(open.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn endpoint_outside_local_allow_is_refused() {
    let (addr, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let local = AllowList::parse(&["10.0.0.0/8".to_string()]).unwrap();
    let (mut rx, _c) = spawn(
        assignment(tcp_plan(addr, "5s", "closed"), 5, 500, 0.0),
        Some(local),
        1000,
    );
    let err = only_failure(&drain(&mut rx, 3).await);
    assert!(
        err.contains("scenario.tcp.addr[0]") && err.contains("not allowed"),
        "{err}"
    );
    assert_eq!(open.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn endpoint_outside_server_allow_is_refused() {
    let (addr, seen) = recording_listener().await;
    for server_allow in [vec![], vec!["10.0.0.0/8".to_string()]] {
        let mut a = assignment(tcp_plan(addr, "5s", "closed"), 5, 500, 0.0);
        a.allow = server_allow.clone();
        let (mut rx, _c) = spawn(a, None, 1000);
        let err = only_failure(&drain(&mut rx, 3).await);
        assert!(err.contains("not allowed"), "{server_allow:?}: {err}");
    }
    assert!(
        seen.lock().unwrap().is_empty(),
        "the refused target was contacted"
    );
}

#[tokio::test]
async fn share_over_max_conns_is_refused() {
    let (addr, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (mut rx, _c) = spawn(
        assignment(tcp_plan(addr, "5s", "closed"), 50, 500, 0.0),
        None,
        10,
    );
    let err = only_failure(&drain(&mut rx, 3).await);
    assert!(err.contains("--max-conns 10"), "{err}");
    assert_eq!(open.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn open_model_on_tcp_fails_instead_of_completing() {
    let (addr, _) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (mut rx, _c) = spawn(
        assignment(tcp_plan(addr, "2s", "open"), 5, 200, 0.0),
        None,
        1000,
    );
    let msgs = drain(&mut rx, 5).await;
    assert!(
        matches!(msgs.first(), Some(AgentMsg::Ready { .. })),
        "{msgs:?}"
    );
    match msgs.last() {
        Some(AgentMsg::Failed { error, .. }) => assert!(error.contains("open model"), "{error}"),
        other => panic!("expected Failed last, got {other:?}"),
    }
    assert!(!msgs.iter().any(|m| matches!(m, AgentMsg::Done { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_follows_the_server_clock_offset() {
    // offset = agent clock − server clock; the agent starts at start_at + offset on its own clock
    for offset in [400.0, -400.0] {
        let (addr, seen) = recording_listener().await;
        let t0 = now_ms();
        let mut a = assignment(tcp_plan(addr, "1s", "closed"), 5, 600, offset);
        a.start_at_server_ms = t0 + 600;
        let (mut rx, _c) = spawn(a, None, 1000);
        let msgs = drain(&mut rx, 6).await;
        assert!(
            matches!(msgs.last(), Some(AgentMsg::Done { reason, .. }) if reason == "completed"),
            "{msgs:?}"
        );
        let first = *seen.lock().unwrap().iter().min().expect("no connection");
        let want = t0 + 600 + offset as i64;
        assert!(
            first.abs_diff(want) <= 200,
            "offset {offset}: started at {} ms, want {} ms",
            first - t0,
            want - t0
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_are_sequenced_and_end_with_done() {
    let (addr, _) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (mut rx, _c) = spawn(
        assignment(tcp_plan(addr, "3s", "closed"), 5, 200, 0.0),
        None,
        1000,
    );
    let msgs = drain(&mut rx, 8).await;
    assert!(
        matches!(msgs.first(), Some(AgentMsg::Ready { run_id }) if run_id == "r1"),
        "{msgs:?}"
    );
    assert!(
        matches!(msgs.last(), Some(AgentMsg::Done { reason, .. }) if reason == "completed"),
        "{msgs:?}"
    );
    let metrics: Vec<(u64, i64, i64, u64)> = msgs
        .iter()
        .filter_map(|m| match m {
            AgentMsg::Metrics {
                seq, rel_ms, snap, ..
            } => Some((*seq, *rel_ms, snap.at_ms, snap.ops.values().sum())),
            _ => None,
        })
        .collect();
    assert!(metrics.len() >= 3, "{metrics:?}");
    assert_eq!(metrics[0].0, 1);
    for (k, w) in metrics.windows(2).enumerate() {
        assert!(
            w[1].0 > w[0].0 && w[1].2 > w[0].2,
            "seq/at_ms not increasing: {metrics:?}"
        );
        assert_eq!(w[0].1, (k as i64 + 1) * 1000, "{metrics:?}");
    }
    assert!(metrics.iter().map(|m| m.3).sum::<u64>() >= 5, "{metrics:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_closes_connections_within_a_second() {
    let (addr, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let (mut rx, cancel) = spawn(
        assignment(tcp_plan(addr, "60s", "closed"), 5, 100, 0.0),
        None,
        1000,
    );
    for _ in 0..100 {
        if open.load(Ordering::SeqCst) == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(open.load(Ordering::SeqCst), 5);
    let t0 = std::time::Instant::now();
    cancel.cancel();
    let msgs = drain(&mut rx, 3).await;
    while open.load(Ordering::SeqCst) > 0 && t0.elapsed() < Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "closed after {:?}",
        t0.elapsed()
    );
    assert!(
        matches!(msgs.last(), Some(AgentMsg::Done { reason, .. }) if reason == "cancelled"),
        "{msgs:?}"
    );
}
