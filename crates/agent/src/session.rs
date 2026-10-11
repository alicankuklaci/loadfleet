//! The agent's connection loop: dial with backoff, hello, answer pings, run at most one
//! assignment, forward its messages, and drop the share whenever the control link is lost.
use crate::conn::{dial, DialError, Sink, Stream};
use crate::exec::run_assignment;
use crate::Watchdog;
use anyhow::{bail, Result};
use futures_util::{SinkExt, StreamExt};
use loadfleet_engine::CancellationToken;
use loadfleet_health::{now_ms, HostInfo};
use loadfleet_plan::AllowList;
use loadfleet_protocol::{decode, encode, AgentMsg, Assignment, ServerMsg, PROTOCOL_VERSION};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Upper bound on TCP connect + TLS + WebSocket handshake.
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
/// A frame that cannot be written within this long means the link is dead.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// A session must last this long before the reconnect backoff starts over: a server that
/// welcomes and then drops the agent at once is retried with growing delays.
const STABLE_SESSION: Duration = Duration::from_secs(10);
const HEARTBEAT: Duration = Duration::from_secs(5);
const WATCHDOG_CHECK: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub server: String,
    pub token: String,
    pub name: String,
    pub max_conns: u64,
    pub local_allow: Option<AllowList>,
    pub public_ip: Option<String>,
    /// Server silence after which the load is stopped and the link redialed.
    pub silence: Duration,
}

impl AgentConfig {
    pub fn new(server: String, token: String, name: String) -> AgentConfig {
        AgentConfig {
            server,
            token,
            name,
            max_conns: 100_000,
            local_allow: None,
            public_ip: None,
            silence: Duration::from_secs(15),
        }
    }
}

/// The one assignment this agent is running.
struct Running {
    run_id: String,
    cancel: CancellationToken,
    rx: mpsc::Receiver<AgentMsg>,
}

impl Running {
    fn spawn(a: Assignment, cfg: &AgentConfig) -> Running {
        let (tx, rx) = mpsc::channel(256);
        let cancel = CancellationToken::new();
        let run_id = a.run_id.clone();
        tokio::spawn(run_assignment(
            a,
            cfg.local_allow.clone(),
            cfg.max_conns,
            tx,
            cancel.clone(),
        ));
        Running { run_id, cancel, rx }
    }
}

impl Drop for Running {
    /// Losing track of a share always stops it: no load runs without a live control link.
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

enum End {
    Shutdown,
    Fatal(String),
    /// The link died; `stable` = the server welcomed us and the session lasted at least
    /// [`STABLE_SESSION`] (resets the backoff).
    Lost {
        stable: bool,
    },
}

/// Connects to the server and serves it until `shutdown`. Reconnects with jittered backoff
/// (1 s → 30 s); returns `Err` when the server refuses the token (401) or rejects the agent.
pub async fn run_agent(cfg: AgentConfig, shutdown: CancellationToken) -> Result<()> {
    let host = HostInfo::detect();
    let dial_limit = DIAL_TIMEOUT
        .min(cfg.silence)
        .max(Duration::from_millis(500));
    let mut backoff = BACKOFF_MIN;
    loop {
        let dialed = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            d = tokio::time::timeout(dial_limit, dial(&cfg.server, &cfg.token)) => d,
        };
        let stable = match dialed {
            Ok(Ok((sink, stream))) => match session(&cfg, &host, sink, stream, &shutdown).await {
                End::Shutdown => return Ok(()),
                End::Fatal(e) => bail!(e),
                End::Lost { stable } => {
                    tracing::warn!(server = %cfg.server, "lost the server; reconnecting");
                    stable
                }
            },
            Ok(Err(DialError::Fatal(e))) => bail!(e),
            Ok(Err(DialError::Retry(e))) => {
                tracing::warn!(server = %cfg.server, error = %e, "server unreachable; retrying in {backoff:?}");
                false
            }
            Err(_) => {
                tracing::warn!(server = %cfg.server, "no answer within {dial_limit:?}; retrying in {backoff:?}");
                false
            }
        };
        if stable {
            backoff = BACKOFF_MIN;
        }
        let jitter = Duration::from_millis(rand::random::<u64>() % 500);
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(backoff + jitter) => {}
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// Writes one frame. Bounded by the watchdog's remaining time as well as [`SEND_TIMEOUT`],
/// so a send blocked on a dead link cannot stretch the silence rule.
async fn send(sink: &mut Sink, m: &AgentMsg, dog: &Watchdog) -> bool {
    let limit = SEND_TIMEOUT
        .min(dog.remaining())
        .max(Duration::from_millis(100));
    matches!(
        tokio::time::timeout(limit, sink.send(Message::text(encode(m)))).await,
        Ok(Ok(()))
    )
}

async fn next_out(running: &mut Option<Running>) -> Option<AgentMsg> {
    match running {
        Some(r) => r.rx.recv().await,
        None => std::future::pending().await,
    }
}

/// One connected session. Whatever ends it, the running share (if any) is cancelled.
async fn session(
    cfg: &AgentConfig,
    host: &HostInfo,
    mut sink: Sink,
    mut stream: Stream,
    shutdown: &CancellationToken,
) -> End {
    let hello = AgentMsg::Hello {
        proto: PROTOCOL_VERSION,
        version: env!("CARGO_PKG_VERSION").into(),
        name: cfg.name.clone(),
        host: host.clone(),
        max_conns: cfg.max_conns,
        public_ip: cfg.public_ip.clone(),
    };
    let mut dog = Watchdog::new(cfg.silence);
    if !send(&mut sink, &hello, &dog).await {
        return End::Lost { stable: false };
    }
    let started = tokio::time::Instant::now();
    let mut running: Option<Running> = None;
    let mut welcomed = false;
    let mut beat = tokio::time::interval(HEARTBEAT);
    let mut check = tokio::time::interval(WATCHDOG_CHECK);
    let end = loop {
        tokio::select! {
            _ = shutdown.cancelled() => break End::Shutdown,
            m = stream.next() => {
                let text = match m {
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break End::Lost { stable: false },
                    Some(Ok(_)) => { dog.feed(); continue; }
                };
                dog.feed();
                let msg = match decode::<ServerMsg>(text.as_str()) {
                    Ok(m) => m,
                    Err(e) => { tracing::debug!(error = %e, "ignoring unknown server message"); continue; }
                };
                match msg {
                    ServerMsg::Welcome { agent_id, .. } => {
                        tracing::info!(%agent_id, "connected to the server");
                        welcomed = true;
                    }
                    ServerMsg::Ping { id, server_ms } => {
                        if !send(&mut sink, &AgentMsg::Pong { id, server_ms, agent_ms: now_ms() }, &dog).await {
                            break End::Lost { stable: false };
                        }
                    }
                    ServerMsg::Reject { reason } => break End::Fatal(format!("server rejected this agent: {reason}")),
                    ServerMsg::Assign(a) => {
                        match &running {
                            // a resend of the run already in progress
                            Some(r) if r.run_id == a.run_id => {}
                            Some(r) => {
                                let busy = AgentMsg::Failed { run_id: a.run_id.clone(), error: format!("busy with run {}", r.run_id) };
                                if !send(&mut sink, &busy, &dog).await { break End::Lost { stable: false }; }
                            }
                            None => {
                                tracing::info!(run = %a.run_id, share = ?a.stage_targets, "assignment received");
                                running = Some(Running::spawn(*a, cfg));
                            }
                        }
                    }
                    ServerMsg::Stop { run_id, reason } => {
                        if let Some(r) = running.as_ref().filter(|r| r.run_id == run_id) {
                            tracing::info!(run = %run_id, %reason, "stop");
                            r.cancel.cancel();
                        }
                    }
                    ServerMsg::Heartbeat { .. } => {}
                }
            }
            m = next_out(&mut running) => match m {
                Some(msg) => {
                    let last = matches!(msg, AgentMsg::Done { .. } | AgentMsg::Failed { .. });
                    if !send(&mut sink, &msg, &dog).await { break End::Lost { stable: false }; }
                    if last { running = None; }
                }
                // the task ended without a final message
                None => running = None,
            },
            _ = beat.tick() => {
                if !send(&mut sink, &AgentMsg::Heartbeat { agent_ms: now_ms() }, &dog).await {
                    break End::Lost { stable: false };
                }
            }
            _ = check.tick() => {
                if dog.expired() {
                    tracing::warn!(silence = ?cfg.silence, "server silent: stopping load and reconnecting");
                    break End::Lost { stable: false };
                }
            }
        }
    };
    let end = match end {
        End::Lost { .. } => End::Lost {
            stable: welcomed && started.elapsed() >= STABLE_SESSION,
        },
        other => other,
    };
    // any end of the session ends the share: after a server restart the run
    // is gone server-side, so an agent that kept loading would be a runaway
    if let Some(r) = running.take() {
        tracing::warn!(run = %r.run_id, "control link closed: cancelling the share");
    }
    if matches!(end, End::Shutdown | End::Fatal(_)) {
        let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(Message::Close(None))).await;
    }
    end
}
