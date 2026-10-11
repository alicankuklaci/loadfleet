use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub name: String,
    pub target: Target,
    pub scenario: Scenario,
    pub load: Load,
    #[serde(default)]
    pub stop: StopRules,
    #[serde(default)]
    pub agents: AgentsSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// Stage indexes left out of the verdict (warm-up), e.g. `[0]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_stages: Vec<usize>,
}

impl Plan {
    pub fn total_duration(&self) -> Duration {
        self.load.stages.iter().fold(Duration::ZERO, |a, s| {
            a.saturating_add(s.ramp).saturating_add(s.hold)
        })
    }
    pub fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[non_exhaustive]
pub enum Scenario {
    Http { http: HttpSpec },
    Tcp { tcp: TcpSpec },
    Ws { ws: WsSpec },
}

impl Scenario {
    pub fn kind(&self) -> &'static str {
        match self {
            Scenario::Http { .. } => "http",
            Scenario::Tcp { .. } => "tcp",
            Scenario::Ws { .. } => "ws",
        }
    }
    /// Every endpoint (URL or host:port) this scenario connects to, for allow-list checks.
    pub fn endpoints(&self) -> Vec<(String, String)> {
        match self {
            Scenario::Http { http } => vec![("scenario.http.url".into(), http.url.clone())],
            Scenario::Tcp { tcp } => tcp
                .addrs
                .list()
                .into_iter()
                .enumerate()
                .map(|(i, a)| (format!("scenario.tcp.addr[{i}]"), a))
                .collect(),
            Scenario::Ws { ws } => vec![("scenario.ws.url".into(), ws.url.clone())],
        }
    }
}

/// `5` or `[min, max]` milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MsRange {
    Fixed(u64),
    Between([u64; 2]),
}

impl MsRange {
    pub fn bounds(&self) -> (u64, u64) {
        match *self {
            MsRange::Fixed(v) => (v, v),
            MsRange::Between([a, b]) => (a, b),
        }
    }
}

fn default_method() -> String {
    "GET".into()
}
fn d10s() -> Duration {
    Duration::from_secs(10)
}
fn d2h() -> Duration {
    Duration::from_secs(7200)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpSpec {
    #[serde(default = "default_method")]
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Accepted statuses; empty means any 2xx.
    #[serde(default)]
    pub expect_status: Vec<u16>,
    #[serde(default = "d10s", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub think_ms: Option<MsRange>,
    #[serde(default)]
    pub http2: bool,
    /// Open model: keep-alive connections in the pool (default: max_vus).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections: Option<u64>,
    /// Metric name for latency (default "http").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Addrs {
    One(String),
    Many(Vec<String>),
}

impl Addrs {
    pub fn list(&self) -> Vec<String> {
        match self {
            Addrs::One(a) => vec![a.clone()],
            Addrs::Many(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Payload {
    Text(String),
    Hex(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    #[serde(default = "d10s", with = "humantime_serde")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSpec {
    /// `host:port` or a list; VU i uses entry i % n (spreads the 65k port limit).
    #[serde(alias = "addr")]
    pub addrs: Addrs,
    #[serde(default)]
    pub tls: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send: Option<Payload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<Expect>,
    /// Keep the connection this long after the exchange; 0 = until the stage ends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// New connections per second per agent (all VUs together).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conn_rate: Option<f64>,
    #[serde(default = "d10s", with = "humantime_serde")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WsStep {
    Send(String),
    Expect(Expect),
    SleepMs(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsSpec {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub on_open: Vec<WsStep>,
    #[serde(default)]
    pub steps: Vec<WsStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub think_ms: Option<MsRange>,
    #[serde(default = "d10s", with = "humantime_serde")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Model {
    #[default]
    Closed,
    Open,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Load {
    #[serde(default)]
    pub model: Model,
    pub stages: Vec<Stage>,
    /// Open model: cap on concurrently in-flight operations per agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_vus: Option<u64>,
}

/// Closed model: `target` = concurrent VUs. Open model: `target` = operations per second.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    pub target: u64,
    #[serde(with = "humantime_serde")]
    pub ramp: Duration,
    #[serde(with = "humantime_serde")]
    pub hold: Duration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopRules {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_rate: Option<f64>,
    #[serde(default)]
    pub p95_ms: BTreeMap<String, f64>,
    #[serde(default)]
    pub stop_on_agent_loss: bool,
    #[serde(default = "d2h", with = "humantime_serde")]
    pub max_duration: Duration,
}

impl Default for StopRules {
    fn default() -> Self {
        StopRules {
            error_rate: None,
            p95_ms: BTreeMap::new(),
            stop_on_agent_loss: false,
            max_duration: d2h(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Weights {
    Keyword(WeightKeyword),
    Manual(BTreeMap<String, f64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WeightKeyword {
    Auto,
    Equal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentsSpec {
    #[serde(default = "one")]
    pub min: usize,
    #[serde(default = "auto")]
    pub weights: Weights,
}

fn one() -> usize {
    1
}
fn auto() -> Weights {
    Weights::Keyword(WeightKeyword::Auto)
}

impl Default for AgentsSpec {
    fn default() -> Self {
        AgentsSpec {
            min: 1,
            weights: auto(),
        }
    }
}
