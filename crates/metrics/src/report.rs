use crate::{SeriesPoint, StageSummary};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentMeta {
    pub name: String,
    pub os: String,
    pub arch: String,
    pub cores: u32,
    pub ip: String,
    pub offset_ms: f64,
    pub stage_targets: Vec<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunMeta {
    pub id: String,
    pub name: String,
    pub plan_yaml: String,
    pub version: String,
    pub seed: Option<u64>,
    /// "closed" | "open" | "" (unknown, e.g. reports saved before this field existed).
    #[serde(default)]
    pub model: String,
    pub started_ms: i64,
    pub ended_ms: Option<i64>,
    /// pending | running | completed | stopped | failed
    pub status: String,
    pub verdict: String,
    pub agents: Vec<AgentMeta>,
    pub missing_agents: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    pub meta: RunMeta,
    pub stages: Vec<StageSummary>,
    pub series: Vec<SeriesPoint>,
}
