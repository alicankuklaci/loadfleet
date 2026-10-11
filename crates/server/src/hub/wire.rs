//! Socket-level pieces of the hub: hello handshake, rejects, the bounded writer and the
//! per-agent frame budget.
use super::{AGENT_MSG_BURST, AGENT_MSG_RATE};
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use loadfleet_protocol::{decode, encode, AgentMsg, ServerMsg, PROTOCOL_VERSION};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest a single frame may take to reach the agent before the socket is given up.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on the final flush (queued Reject + Close) after the socket is told to stop.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) fn update_reason(proto: impl std::fmt::Display) -> String {
    format!(
        "protocol mismatch: server speaks v{PROTOCOL_VERSION}, agent v{proto}; update the agent to loadfleet {}",
        env!("CARGO_PKG_VERSION")
    )
}

/// Reads the hello (skipping WS ping/pong control frames): Ok(hello) or Err(reject reason).
pub(super) async fn read_hello(ws: &mut WebSocket) -> Result<AgentMsg, String> {
    let deadline = Instant::now() + HELLO_TIMEOUT;
    let text = loop {
        match tokio::time::timeout_at(deadline, ws.recv()).await {
            Ok(Some(Ok(Message::Text(t)))) => break t,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            _ => return Err("expected hello".into()),
        }
    };
    match decode::<AgentMsg>(text.as_str()) {
        Ok(m @ AgentMsg::Hello { .. }) => Ok(m),
        Ok(_) => Err("expected hello".into()),
        Err(_) => {
            // a hello from another protocol generation may not decode; still name the fix
            let v: serde_json::Value = serde_json::from_str(text.as_str()).unwrap_or_default();
            match (v["t"].as_str(), v["proto"].as_u64()) {
                (Some("hello"), Some(p)) if p != u64::from(PROTOCOL_VERSION) => {
                    Err(update_reason(p))
                }
                _ => Err("expected hello".into()),
            }
        }
    }
}

pub(super) async fn reject(mut ws: WebSocket, reason: String) {
    let _ = tokio::time::timeout(FLUSH_TIMEOUT, async {
        let _ = ws
            .send(Message::Text(encode(&ServerMsg::Reject { reason }).into()))
            .await;
        let _ = ws.close().await;
    })
    .await;
}

/// Token bucket over every frame an agent sends (text, binary, ping, pong).
pub(super) struct FrameBudget {
    tokens: f64,
    at: Instant,
}

impl FrameBudget {
    pub(super) fn new() -> Self {
        FrameBudget {
            tokens: AGENT_MSG_BURST,
            at: Instant::now(),
        }
    }

    /// Charges one frame; false when the agent is over its rate.
    pub(super) fn charge(&mut self) -> bool {
        let now = Instant::now();
        let refill = now.duration_since(self.at).as_secs_f64() * AGENT_MSG_RATE;
        self.tokens = (self.tokens + refill).min(AGENT_MSG_BURST) - 1.0;
        self.at = now;
        self.tokens >= 0.0
    }
}

/// Sends queued messages until `kill`, a send error, a Reject, or a frame that cannot be
/// written within [`SEND_TIMEOUT`]. Every send races `kill`, so a peer that stopped
/// reading never pins this task; on its own failure it cancels `kill` so the reader ends too.
pub(super) async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<ServerMsg>,
    kill: CancellationToken,
) {
    loop {
        let m = tokio::select! {
            _ = kill.cancelled() => break,
            m = rx.recv() => m,
        };
        let Some(m) = m else { break };
        if matches!(m, ServerMsg::Reject { .. }) {
            // The reader queues a Reject and then ends the socket, which fires `kill` at once.
            // Racing `kill` here dropped the half-sent Reject (the agent saw a bare close), so
            // a Reject is always delivered, bounded by FLUSH_TIMEOUT instead.
            let frame = Message::Text(encode(&m).into());
            let _ = tokio::time::timeout(FLUSH_TIMEOUT, sink.send(frame)).await;
            break;
        }
        let sent = tokio::select! {
            _ = kill.cancelled() => false,
            r = tokio::time::timeout(SEND_TIMEOUT, sink.send(Message::Text(encode(&m).into()))) => {
                matches!(r, Ok(Ok(())))
            }
        };
        if !sent {
            break;
        }
    }
    kill.cancel();
    // flush anything already queued (e.g. a Reject) and say goodbye, but never hang on it
    let _ = tokio::time::timeout(FLUSH_TIMEOUT, async {
        while let Ok(m) = rx.try_recv() {
            if sink.send(Message::Text(encode(&m).into())).await.is_err() {
                return;
            }
        }
        let _ = sink.close().await;
    })
    .await;
}
