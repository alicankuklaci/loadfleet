#![allow(dead_code)]
//! A scripted fake agent shared by the coordinator, API and UI tests.
use futures_util::{SinkExt, StreamExt};
use loadfleet_health::{HealthSample, HostInfo};
use loadfleet_metrics::Snapshot;
use loadfleet_protocol::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

pub struct FakeAgent {
    pub got: Arc<Mutex<Vec<ServerMsg>>>,
    pub out: mpsc::UnboundedSender<AgentMsg>,
    pub task: tokio::task::JoinHandle<()>,
    seq: AtomicU64,
}

impl FakeAgent {
    /// Connects, says hello, answers pings with a clock `skew_ms` ahead, records every server message.
    pub async fn connect(
        addr: std::net::SocketAddr,
        token: &str,
        name: &str,
        skew_ms: i64,
    ) -> FakeAgent {
        let mut req = format!("ws://{addr}/agent").into_client_request().unwrap();
        req.headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let hello = AgentMsg::Hello {
            proto: PROTOCOL_VERSION,
            version: "t".into(),
            name: name.into(),
            host: HostInfo::detect(),
            max_conns: 1_000_000,
            public_ip: None,
        };
        sink.send(Message::text(encode(&hello))).await.unwrap();
        let got = Arc::new(Mutex::new(Vec::new()));
        let (out, mut out_rx) = mpsc::unbounded_channel::<AgentMsg>();
        let g = got.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    m = out_rx.recv() => match m {
                        Some(m) => { if sink.send(Message::text(encode(&m))).await.is_err() { break; } }
                        None => break,
                    },
                    m = stream.next() => {
                        let Some(Ok(Message::Text(t))) = m else { break };
                        let Ok(msg) = decode::<ServerMsg>(t.as_str()) else { continue };
                        if let ServerMsg::Ping { id, server_ms } = &msg {
                            let pong = AgentMsg::Pong { id: *id, server_ms: *server_ms, agent_ms: now_ms() + skew_ms };
                            if sink.send(Message::text(encode(&pong))).await.is_err() { break; }
                        }
                        g.lock().push(msg);
                    }
                }
            }
        });
        FakeAgent {
            got,
            out,
            task,
            seq: AtomicU64::new(0),
        }
    }

    pub fn assignment(&self) -> Option<Assignment> {
        self.got.lock().iter().find_map(|m| match m {
            ServerMsg::Assign(a) => Some((**a).clone()),
            _ => None,
        })
    }

    pub fn stopped(&self) -> bool {
        self.got
            .lock()
            .iter()
            .any(|m| matches!(m, ServerMsg::Stop { .. }))
    }

    /// One 1 s metrics window ending at `rel_ms`; `seq` and `at_ms` increase per call.
    pub fn send_metrics(&self, run_id: &str, rel_ms: i64, ops: u64, errors: u64) {
        let mut snap = Snapshot {
            at_ms: rel_ms,
            ..Default::default()
        };
        snap.ops.insert("x".into(), ops);
        if errors > 0 {
            snap.errors.insert("x:fail".into(), errors);
        }
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.out.send(AgentMsg::Metrics {
            run_id: run_id.into(),
            seq,
            rel_ms,
            snap,
            health: HealthSample::default(),
        });
    }

    /// Sends `snap` as one metrics window ending at `rel_ms` (`seq` increases per call).
    pub fn send_snapshot(&self, run_id: &str, rel_ms: i64, snap: Snapshot) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.out.send(AgentMsg::Metrics {
            run_id: run_id.into(),
            seq,
            rel_ms,
            snap,
            health: HealthSample::default(),
        });
    }

    pub fn send_done(&self, run_id: &str) {
        let _ = self.out.send(AgentMsg::Done {
            run_id: run_id.into(),
            reason: "completed".into(),
        });
    }

    pub fn send_failed(&self, run_id: &str, error: &str) {
        let _ = self.out.send(AgentMsg::Failed {
            run_id: run_id.into(),
            error: error.into(),
        });
    }
}

pub async fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let end = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while tokio::time::Instant::now() < end {
        if f() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    f()
}
