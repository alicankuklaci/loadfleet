//! loadfleet protocol: versioned server-agent messages (JSON) and clock offset.
mod clock;
mod guard;

pub use clock::*;
pub use guard::*;
pub use loadfleet_health::now_ms;

use loadfleet_health::{HealthSample, HostInfo};
use loadfleet_metrics::Snapshot;
use loadfleet_plan::Plan;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

/// Largest single protocol frame `decode` will parse.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum AgentMsg {
    /// Every field but `proto` and `name` defaults, so a hello from another protocol
    /// generation still decodes far enough for the server to answer "update the agent".
    Hello {
        proto: u32,
        #[serde(default)]
        version: String,
        name: String,
        #[serde(default)]
        host: HostInfo,
        #[serde(default)]
        max_conns: u64,
        #[serde(default)]
        public_ip: Option<String>,
    },
    Pong {
        id: u32,
        server_ms: i64,
        agent_ms: i64,
    },
    Heartbeat {
        agent_ms: i64,
    },
    Ready {
        run_id: String,
    },
    /// `rel_ms`: end of the 1 s window, ms since the run's start_at (agent clock).
    /// `seq`: per-run, per-agent counter starting at 1 and strictly increasing; together
    /// with a strictly increasing `snap.at_ms` it lets the receiver drop replays
    /// (see [`SeqGuard`]) before the aggregator's (agent, at_ms) duplicate check.
    Metrics {
        run_id: String,
        seq: u64,
        rel_ms: i64,
        snap: Snapshot,
        health: HealthSample,
    },
    Done {
        run_id: String,
        reason: String,
    },
    Failed {
        run_id: String,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub run_id: String,
    pub plan: Plan,
    /// The server's allow-list; the agent builds `AllowGuard::layered(plan, server, --allow)`.
    /// Empty permits nothing, so the server must always send its effective list.
    pub allow: Vec<String>,
    /// This agent's target per stage (closed: VUs, open: ops/s).
    pub stage_targets: Vec<u64>,
    pub vu_offset: u64,
    pub agent_index: u32,
    pub max_vus: u64,
    pub seed: u64,
    pub start_at_server_ms: i64,
    /// Server's estimate of (agent clock − server clock).
    pub offset_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerMsg {
    Welcome { agent_id: String, server_ms: i64 },
    Reject { reason: String },
    Ping { id: u32, server_ms: i64 },
    Heartbeat { server_ms: i64 },
    Assign(Box<Assignment>),
    Stop { run_id: String, reason: String },
}

/// Serialize, surfacing the error (e.g. a non-finite float in a payload).
pub fn try_encode<T: Serialize>(m: &T) -> Result<String, String> {
    serde_json::to_string(m).map_err(|e| e.to_string())
}

/// Infallible form required by the plan; yields `""` on error, which `decode` rejects.
/// Senders that care use [`try_encode`].
pub fn encode<T: Serialize>(m: &T) -> String {
    try_encode(m).unwrap_or_default()
}

/// Whether a peer speaking `proto` can talk to us.
pub fn compatible(proto: u32) -> bool {
    proto == PROTOCOL_VERSION
}

pub fn decode<T: DeserializeOwned>(s: &str) -> Result<T, String> {
    if s.len() > MAX_FRAME_BYTES {
        return Err(format!("frame too large: {} bytes", s.len()));
    }
    serde_json::from_str(s).map_err(|e| e.to_string())
}
