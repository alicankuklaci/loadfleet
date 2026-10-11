//! `run_agent` against a scripted fake server: the watchdog stops the load
//! through the real session path, any socket loss cancels the share, a redial to a listener
//! that never answers neither hangs nor resumes the load, fatal refusals end the agent.
use futures_util::{SinkExt, StreamExt};
use loadfleet_agent::{run_agent, AgentConfig};
use loadfleet_engine::CancellationToken;
use loadfleet_health::now_ms;
use loadfleet_protocol::{decode, encode, AgentMsg, Assignment, ServerMsg};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Assign, then never send another frame (half-open / hung server).
    Silent,
    /// Assign, then close the socket after the given delay.
    DropAfter(u64),
    /// Assign, then send Stop after the given delay and keep serving.
    StopAfter(u64),
    /// Assign, then assign another run while the first is running.
    DoubleAssign,
    /// Accept TCP, never answer the WebSocket handshake.
    Blackhole,
    /// Answer the handshake with HTTP 401.
    Unauthorized,
    /// Reject the hello.
    Reject,
}

struct Fake {
    addr: SocketAddr,
    accepts: Arc<AtomicUsize>,
    got: Arc<Mutex<Vec<AgentMsg>>>,
    /// When the scripted event happened: Silent = last frame sent, DropAfter = socket
    /// dropped, StopAfter = Stop sent. Timing assertions are anchored here.
    fired: Fired,
}

type Fired = Arc<Mutex<Option<Instant>>>;

impl Fake {
    /// The scripted event's instant (waits for it).
    async fn fired(&self) -> Instant {
        for _ in 0..500 {
            if let Some(t) = *self.fired.lock().unwrap() {
                return t;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the fake server never fired its event");
    }
}

fn assignment(target: SocketAddr) -> Assignment {
    let text = format!(
        "name: x\ntarget: {{ allow: [\"127.0.0.1\"] }}\nscenario: {{ type: tcp, tcp: {{ addr: \"{target}\", hold_ms: 0 }} }}\nload: {{ stages: [{{target: 20, ramp: 0s, hold: 60s}}] }}\n"
    );
    Assignment {
        run_id: "r1".into(),
        plan: loadfleet_plan::parse(&text).unwrap(),
        allow: vec!["127.0.0.1".into()],
        stage_targets: vec![20],
        vu_offset: 0,
        agent_index: 0,
        max_vus: 100,
        seed: 1,
        start_at_server_ms: now_ms() + 200,
        offset_ms: 0.0,
    }
}

/// Connection `i` uses `modes[i]` (the last mode repeats).
async fn fake_server(modes: Vec<Mode>, target: SocketAddr) -> Fake {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let got = Arc::new(Mutex::new(Vec::new()));
    let fired: Fired = Arc::new(Mutex::new(None));
    let (acc, g, f) = (accepts.clone(), got.clone(), fired.clone());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = l.accept().await {
            let i = acc.fetch_add(1, Ordering::SeqCst);
            let mode = modes[i.min(modes.len() - 1)];
            if matches!(mode, Mode::Blackhole) {
                held.push(sock);
                continue;
            }
            let (g, f) = (g.clone(), f.clone());
            tokio::spawn(async move { serve(sock, mode, target, g, f).await });
        }
    });
    Fake {
        addr,
        accepts,
        got,
        fired,
    }
}

async fn serve(
    sock: tokio::net::TcpStream,
    mode: Mode,
    target: SocketAddr,
    got: Arc<Mutex<Vec<AgentMsg>>>,
    fired: Fired,
) {
    // the callback's Result type is fixed by tungstenite
    #[allow(clippy::result_large_err)]
    let auth = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        let ok = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            == Some("Bearer tok");
        if ok && !matches!(mode, Mode::Unauthorized) {
            return Ok(resp);
        }
        let mut e = ErrorResponse::new(Some("bad agent token".into()));
        *e.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
        Err(e)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(sock, auth).await else {
        return;
    };
    let (mut tx, mut rx) = ws.split();
    let send = |m: ServerMsg| Message::text(encode(&m));
    let Some(Ok(Message::Text(hello))) = rx.next().await else {
        return;
    };
    assert!(matches!(
        decode::<AgentMsg>(hello.as_str()),
        Ok(AgentMsg::Hello { .. })
    ));
    if matches!(mode, Mode::Reject) {
        let _ = tx
            .send(send(ServerMsg::Reject {
                reason: "protocol mismatch".into(),
            }))
            .await;
        return;
    }
    let _ = tx
        .send(send(ServerMsg::Welcome {
            agent_id: "a".into(),
            server_ms: now_ms(),
        }))
        .await;
    let _ = tx
        .send(send(ServerMsg::Ping {
            id: 9,
            server_ms: now_ms(),
        }))
        .await;
    let _ = tx
        .send(send(ServerMsg::Assign(Box::new(assignment(target)))))
        .await;
    if matches!(mode, Mode::DoubleAssign) {
        let mut second = assignment(target);
        second.run_id = "r2".into();
        let _ = tx.send(send(ServerMsg::Assign(Box::new(second)))).await;
    }
    let started = Instant::now();
    if matches!(mode, Mode::Silent) {
        *fired.lock().unwrap() = Some(Instant::now());
    }
    loop {
        let wait = match mode {
            Mode::DropAfter(ms) | Mode::StopAfter(ms) => {
                Duration::from_millis(ms).saturating_sub(started.elapsed())
            }
            _ => Duration::from_secs(3600),
        };
        tokio::select! {
            m = rx.next() => match m {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(m) = decode::<AgentMsg>(t.as_str()) { got.lock().unwrap().push(m); }
                }
                Some(Ok(_)) => {}
                _ => return,
            },
            _ = tokio::time::sleep(wait) => match mode {
                Mode::DropAfter(_) => {
                    drop((tx, rx));
                    *fired.lock().unwrap() = Some(Instant::now());
                    return;
                }
                Mode::StopAfter(_) => {
                    let _ = tx.send(send(ServerMsg::Stop { run_id: "r1".into(), reason: "panel".into() })).await;
                    *fired.lock().unwrap() = Some(Instant::now());
                    // keep serving; never fire again
                    loop {
                        match rx.next().await {
                            Some(Ok(Message::Text(t))) => {
                                if let Ok(m) = decode::<AgentMsg>(t.as_str()) { got.lock().unwrap().push(m); }
                            }
                            Some(Ok(_)) => {}
                            _ => return,
                        }
                    }
                }
                _ => {}
            },
        }
    }
}

fn start_agent(
    addr: SocketAddr,
    silence: Duration,
) -> (
    CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let stop = CancellationToken::new();
    let mut cfg = AgentConfig::new(format!("ws://{addr}"), "tok".into(), "a".into());
    cfg.silence = silence;
    let h = tokio::spawn(run_agent(cfg, stop.clone()));
    (stop, h)
}

async fn until(open: &AtomicU64, want: u64, secs: u64) -> Duration {
    let t0 = Instant::now();
    while open.load(Ordering::SeqCst) != want && t0.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        open.load(Ordering::SeqCst),
        want,
        "after {:?}",
        t0.elapsed()
    );
    t0.elapsed()
}

/// Time from the fake's scripted event until the target has no connection left.
async fn closed_after(fake: &Fake, open: &AtomicU64, secs: u64) -> Duration {
    until(open, 0, secs).await;
    fake.fired().await.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_server_trips_the_watchdog_and_stops_the_load() {
    let (target, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let fake = fake_server(vec![Mode::Silent], target).await;
    let (_stop, _h) = start_agent(fake.addr, Duration::from_secs(2));
    until(&open, 20, 5).await;
    // the socket stays open but nothing arrives after the Assign: only the 2 s watchdog can
    // stop the load, so it must end no earlier than the silence limit and soon after it
    let took = closed_after(&fake, &open, 8).await;
    assert!(
        took >= Duration::from_millis(1900) && took < Duration::from_millis(3500),
        "load stopped {took:?} after the last server frame (silence 2 s)"
    );
    let got = fake.got.lock().unwrap().clone();
    assert!(
        got.iter()
            .any(|m| matches!(m, AgentMsg::Pong { id: 9, .. })),
        "{got:?}"
    );
    assert!(
        got.iter().any(|m| matches!(m, AgentMsg::Ready { .. })),
        "{got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_loss_cancels_the_share_at_once() {
    let (target, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let fake = fake_server(vec![Mode::DropAfter(1000), Mode::Blackhole], target).await;
    // a 15 s silence: the watchdog cannot be what stops the load here
    let (stop, h) = start_agent(fake.addr, Duration::from_secs(15));
    until(&open, 20, 5).await;
    let took = closed_after(&fake, &open, 5).await;
    assert!(
        took < Duration::from_secs(1),
        "load outlived the dropped socket by {took:?}"
    );
    // the redial hangs on a listener that never answers; the load must not come back
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(open.load(Ordering::SeqCst), 0, "load resumed");
    let t1 = Instant::now();
    stop.cancel();
    let r = tokio::time::timeout(Duration::from_secs(1), h)
        .await
        .expect("agent did not shut down during a pending dial");
    assert!(r.unwrap().is_ok());
    assert!(t1.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_redial_is_given_up_and_retried() {
    let (target, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let fake = fake_server(vec![Mode::DropAfter(500), Mode::Blackhole], target).await;
    // silence 2 s also bounds each dial at 2 s
    let (stop, h) = start_agent(fake.addr, Duration::from_secs(2));
    until(&open, 20, 5).await;
    let t0 = Instant::now();
    while fake.accepts.load(Ordering::SeqCst) < 3 && t0.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        fake.accepts.load(Ordering::SeqCst) >= 3,
        "dial hung: {} accepts",
        fake.accepts.load(Ordering::SeqCst)
    );
    assert_eq!(open.load(Ordering::SeqCst), 0);
    let t1 = Instant::now();
    stop.cancel();
    let r = tokio::time::timeout(Duration::from_secs(1), h)
        .await
        .expect("agent did not shut down during a pending dial");
    assert!(r.unwrap().is_ok());
    assert!(t1.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_cancels_within_a_second_and_reports_done() {
    let (target, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let fake = fake_server(vec![Mode::StopAfter(1000)], target).await;
    let (_stop, _h) = start_agent(fake.addr, Duration::from_secs(15));
    until(&open, 20, 5).await;
    let took = closed_after(&fake, &open, 5).await;
    assert!(took < Duration::from_secs(1), "closed {took:?} after Stop");
    let t0 = Instant::now();
    loop {
        let got = fake.got.lock().unwrap().clone();
        if got
            .iter()
            .any(|m| matches!(m, AgentMsg::Done { reason, .. } if reason == "cancelled"))
        {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(2), "no Done: {got:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let got = fake.got.lock().unwrap().clone();
    let seqs: Vec<u64> = got
        .iter()
        .filter_map(|m| match m {
            AgentMsg::Metrics { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect();
    assert!(
        !seqs.is_empty() && seqs.windows(2).all(|w| w[1] > w[0]),
        "{seqs:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_assignment_while_busy_is_refused() {
    let (target, open) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    let fake = fake_server(vec![Mode::DoubleAssign], target).await;
    let (stop, _h) = start_agent(fake.addr, Duration::from_secs(15));
    until(&open, 20, 5).await;
    let got = fake.got.lock().unwrap().clone();
    assert!(
        !got.iter()
            .any(|m| matches!(m, AgentMsg::Failed { run_id, .. } if run_id == "r1")),
        "{got:?}"
    );
    assert!(got.iter().any(|m| matches!(m, AgentMsg::Failed { run_id, error } if run_id == "r2" && error.contains("busy with run r1"))), "{got:?}");
    stop.cancel();
    until(&open, 0, 2).await;
}

#[tokio::test]
async fn unauthorized_and_rejected_agents_give_up() {
    let (target, _) = loadfleet_testserver::spawn_tcp_echo("127.0.0.1:0")
        .await
        .unwrap();
    for mode in [Mode::Unauthorized, Mode::Reject] {
        let fake = fake_server(vec![mode], target).await;
        let (_stop, h) = start_agent(fake.addr, Duration::from_secs(15));
        let r = tokio::time::timeout(Duration::from_secs(3), h)
            .await
            .expect("agent kept retrying");
        let err = r
            .unwrap()
            .expect_err("fatal refusal must be an error")
            .to_string();
        assert!(
            err.contains("401") || err.contains("protocol mismatch"),
            "{mode:?}: {err}"
        );
        assert_eq!(fake.accepts.load(Ordering::SeqCst), 1, "{mode:?}");
    }
}
