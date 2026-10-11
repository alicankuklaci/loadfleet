use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One agent's metrics for one ~1 s window. `at_ms` is the window end on the sender's clock.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub at_ms: i64,
    #[serde(default)]
    pub ops: BTreeMap<String, u64>,
    #[serde(default)]
    pub counters: BTreeMap<String, u64>,
    #[serde(default)]
    pub errors: BTreeMap<String, u64>,
    /// metric name -> base64(V2+DEFLATE histogram, µs)
    #[serde(default)]
    pub hists: BTreeMap<String, String>,
    #[serde(default)]
    pub gauges: BTreeMap<String, i64>,
}

impl Snapshot {
    pub fn total_ops(&self) -> u64 {
        self.ops.values().sum()
    }
    pub fn total_errors(&self) -> u64 {
        self.errors.values().sum()
    }
}
