mod clock;
mod wire;

use axum::extract::ws::{Message, WebSocket};
use clock::ClockSync;
use futures_util::StreamExt;
use loadfleet_health::HostInfo;
use loadfleet_protocol::*;
use parking_lot::RwLock;
use rand::RngCore;
use serde::Serialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Sustained agent→server messages per second (a healthy agent sends ~3: metrics, pong,
/// heartbeat) and the burst allowed above it. Exceeding the bucket drops the agent.
pub const AGENT_MSG_RATE: f64 = 100.0;
pub const AGENT_MSG_BURST: f64 = 400.0;
pub const MAX_AGENT_NAME: usize = 64;
/// Slack around a run's window for agent-supplied `Metrics.rel_ms`.
pub const REL_MS_SLACK: i64 = 120_000;
const PING_SPACING: Duration = Duration::from_millis(100);
/// Floors for config-supplied timers (`interval(0)` panics; zero silence drops everyone).
const MIN_HEARTBEAT: Duration = Duration::from_millis(100);
const MIN_SILENCE: Duration = Duration::from_secs(1);
/// How long `serve_socket` waits for its writer after unregistering before aborting it.
const WRITER_GRACE: Duration = Duration::from_secs(2);

/// `Msg` is much larger than the other variants; it is allowed rather than boxed because
/// events are a few per agent per second and the variant shape is the coordinator's interface.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum HubEvent {
    Joined(String),
    Left(String),
    Msg(String, AgentMsg),
}

#[derive(Serialize, Clone, Debug)]
pub struct AgentView {
    pub id: String,
    pub name: String,
    pub version: String,
    pub host: HostInfo,
    /// The address the server observed (socket peer, or the client IP a trusted proxy gave).
    pub ip: String,
    /// The agent's own `--public-ip` claim: display only, never used for allow-lists.
    pub reported_ip: Option<String>,
    pub offset_ms: Option<f64>,
    pub rtt_ms: Option<f64>,
    pub readiness: Option<Readiness>,
    pub busy_run: Option<String>,
    pub max_conns: u64,
    pub connected_ms: i64,
}

/// Per-socket limits, taken from the server config.
#[derive(Clone, Copy, Debug)]
pub struct SocketOpts {
    pub heartbeat: Duration,
    pub silence: Duration,
    /// Largest accepted `Metrics.rel_ms` (max run duration + slack).
    pub max_rel_ms: i64,
    /// Server cap on an agent's connections; the agent's own `max_conns` is clamped to it.
    pub max_conns: u64,
}

struct Entry {
    view: AgentView,
    conn: u64,
    tx: mpsc::Sender<ServerMsg>,
    kill: CancellationToken,
}

#[derive(Clone)]
pub struct Hub {
    agents: Arc<RwLock<HashMap<String, Entry>>>,
    token: Arc<RwLock<String>>,
    events: mpsc::UnboundedSender<HubEvent>,
    next_conn: Arc<AtomicU64>,
    /// Server shutdown: every agent socket closes when it fires.
    stop: CancellationToken,
}

/// A fresh agent registration token: 24 random bytes, hex encoded. The single generator
/// for every token the server mints (first start and panel rotation).
pub(crate) fn new_token() -> String {
    let mut b = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

fn valid_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= MAX_AGENT_NAME
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

impl Hub {
    /// `stop` is the server's shutdown token: when it fires every agent socket is closed
    /// and new ones are refused.
    pub fn new(token: String, stop: CancellationToken) -> (Hub, mpsc::UnboundedReceiver<HubEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let hub = Hub {
            agents: Default::default(),
            token: Arc::new(RwLock::new(token)),
            events: tx,
            next_conn: Default::default(),
            stop,
        };
        (hub, rx)
    }

    pub fn token(&self) -> String {
        self.token.read().clone()
    }

    pub fn set_token(&self, t: String) {
        *self.token.write() = t;
    }

    pub fn agents(&self) -> Vec<AgentView> {
        let mut v: Vec<AgentView> = self
            .agents
            .read()
            .values()
            .map(|e| e.view.clone())
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn get(&self, id: &str) -> Option<AgentView> {
        self.agents.read().get(id).map(|e| e.view.clone())
    }

    pub fn send(&self, id: &str, m: ServerMsg) -> bool {
        match self.agents.read().get(id) {
            Some(e) => e.tx.try_send(m).is_ok(),
            None => false,
        }
    }

    pub fn mark_busy(&self, id: &str, run: Option<String>) {
        if let Some(e) = self.agents.write().get_mut(id) {
            e.view.busy_run = run;
        }
    }

    /// Registers a connection under its agent name. A live entry of the same name is a stale
    /// socket of a reconnecting agent: it is closed and reported as `Left` before `Joined`,
    /// so a run that counted on it sees the loss instead of waiting for its `Done`.
    fn register(
        &self,
        view: AgentView,
        tx: mpsc::Sender<ServerMsg>,
        kill: CancellationToken,
    ) -> u64 {
        let conn = self.next_conn.fetch_add(1, Ordering::Relaxed);
        let (id, ip) = (view.id.clone(), view.ip.clone());
        tracing::info!(
            agent = %view.name, ip = %view.ip, version = %view.version,
            cores = view.host.cores, os = %view.host.os, "agent joined"
        );
        let mut agents = self.agents.write();
        if let Some(old) = agents.insert(
            id.clone(),
            Entry {
                view,
                conn,
                tx,
                kill,
            },
        ) {
            old.kill.cancel();
            tracing::warn!(
                agent = %id, old_ip = %old.view.ip, new_ip = %ip,
                "agent reconnected under a live name; replacing the old socket \
                 (two machines sharing a name evict each other)"
            );
            let _ = self.events.send(HubEvent::Left(id.clone()));
        }
        // sent under the lock so Left/Joined of competing sockets cannot interleave
        let _ = self.events.send(HubEvent::Joined(id));
        conn
    }

    fn unregister(&self, id: &str, conn: u64) {
        let mut agents = self.agents.write();
        if agents.get(id).map(|e| e.conn) == Some(conn) {
            if let Some(e) = agents.remove(id) {
                let v = &e.view;
                tracing::info!(
                    agent = %v.name, ip = %v.ip, version = %v.version,
                    run = v.busy_run.as_deref().unwrap_or("-"), "agent left"
                );
            }
            let _ = self.events.send(HubEvent::Left(id.to_string()));
        }
    }

    /// Serves one agent socket until it closes. `ip` is the client IP as seen by the server.
    pub async fn serve_socket(&self, mut ws: WebSocket, ip: String, opts: SocketOpts) {
        if self.stop.is_cancelled() {
            return;
        }
        let hello = match wire::read_hello(&mut ws).await {
            Ok(h) => h,
            Err(reason) => return wire::reject(ws, reason).await,
        };
        let AgentMsg::Hello {
            proto,
            version,
            name,
            host,
            max_conns,
            public_ip,
        } = hello
        else {
            return wire::reject(ws, "expected hello".into()).await;
        };
        if !compatible(proto) {
            return wire::reject(ws, wire::update_reason(proto)).await;
        }
        if !valid_name(&name) {
            let reason = format!("agent name must be 1-{MAX_AGENT_NAME} of [A-Za-z0-9._-]");
            return wire::reject(ws, reason).await;
        }
        let reported_ip = public_ip
            .and_then(|p| p.trim().parse::<IpAddr>().ok())
            .map(|p| p.to_string())
            .filter(|p| *p != ip);
        let max_conns = match max_conns {
            0 => opts.max_conns,
            n => n.min(opts.max_conns),
        };
        let id = name.clone();
        let view = AgentView {
            id: id.clone(),
            name,
            version,
            host,
            ip,
            reported_ip,
            offset_ms: None,
            rtt_ms: None,
            readiness: None,
            busy_run: None,
            max_conns,
            connected_ms: now_ms(),
        };
        let (tx, rx) = mpsc::channel::<ServerMsg>(1024);
        let kill = CancellationToken::new();
        let conn = self.register(view, tx.clone(), kill.clone());
        let (sink, stream) = ws.split();
        let mut writer = tokio::spawn(wire::write_loop(sink, rx, kill.clone()));
        let _ = tx.try_send(ServerMsg::Welcome {
            agent_id: id.clone(),
            server_ms: now_ms(),
        });
        self.read_loop(stream, &id, conn, &tx, &kill, opts).await;
        // report the loss first: a writer stuck on a peer that stopped reading must not keep
        // a dead agent listed or hide its Left from a running test
        self.unregister(&id, conn);
        kill.cancel();
        if tokio::time::timeout(WRITER_GRACE, &mut writer)
            .await
            .is_err()
        {
            writer.abort();
        }
    }

    async fn read_loop(
        &self,
        mut stream: futures_util::stream::SplitStream<WebSocket>,
        id: &str,
        conn: u64,
        tx: &mpsc::Sender<ServerMsg>,
        kill: &CancellationToken,
        opts: SocketOpts,
    ) {
        let mut clock = ClockSync::default();
        let mut budget = wire::FrameBudget::new();
        let silence = opts.silence.max(MIN_SILENCE);
        let mut last_rx = Instant::now();
        let mut ping_iv = tokio::time::interval(PING_SPACING);
        let mut hb_iv = tokio::time::interval(opts.heartbeat.max(MIN_HEARTBEAT));
        loop {
            let frame = tokio::select! {
                _ = kill.cancelled() => return,
                _ = self.stop.cancelled() => return,
                _ = ping_iv.tick() => {
                    if last_rx.elapsed() > silence {
                        return;
                    }
                    if let Some(p) = clock.next_ping() {
                        let _ = tx.try_send(p);
                    }
                    continue;
                }
                _ = hb_iv.tick() => {
                    let _ = tx.try_send(ServerMsg::Heartbeat { server_ms: now_ms() });
                    continue;
                }
                f = stream.next() => f,
            };
            let frame = match frame {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(f)) => f,
            };
            last_rx = Instant::now();
            // every frame costs: control and binary frames are allocated and answered too
            if !budget.charge() {
                tracing::warn!(
                    agent = id,
                    "agent exceeded the message rate limit; dropping it"
                );
                let reason = "message rate limit exceeded".to_string();
                let _ = tx.try_send(ServerMsg::Reject { reason });
                return;
            }
            let text = match frame {
                Message::Text(t) => t,
                Message::Binary(_) => {
                    let reason = "binary frames are not supported: the agent protocol is JSON text; update the agent".to_string();
                    let _ = tx.try_send(ServerMsg::Reject { reason });
                    return;
                }
                _ => continue,
            };
            let Ok(msg) = decode::<AgentMsg>(text.as_str()) else {
                continue;
            };
            match msg {
                AgentMsg::Pong {
                    id: ping, agent_ms, ..
                } => {
                    if let Some((e, full)) = clock.pong(ping, agent_ms) {
                        let mut agents = self.agents.write();
                        // a replaced socket must not write into its successor's entry
                        if let Some(entry) = agents.get_mut(id).filter(|e| e.conn == conn) {
                            entry.view.offset_ms = Some((e.offset_ms * 10.0).round() / 10.0);
                            entry.view.rtt_ms = Some(e.rtt_ms);
                            entry.view.readiness = full.then(|| readiness(e.offset_ms));
                        }
                    }
                }
                AgentMsg::Heartbeat { .. } => {}
                AgentMsg::Hello { .. } => {}
                AgentMsg::Metrics { rel_ms, .. }
                    if !(-REL_MS_SLACK..=opts.max_rel_ms).contains(&rel_ms) =>
                {
                    tracing::warn!(
                        agent = id,
                        rel_ms,
                        "dropping metrics with out-of-range rel_ms"
                    );
                }
                other => {
                    // under the lock and only while current, so a replaced socket's last
                    // message can never follow its successor's Joined
                    let agents = self.agents.read();
                    if agents.get(id).is_some_and(|e| e.conn == conn) {
                        let _ = self.events.send(HubEvent::Msg(id.to_string(), other));
                    }
                }
            }
        }
    }
}
