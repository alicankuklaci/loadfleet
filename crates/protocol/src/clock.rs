//! NTP-style clock offset estimation between the server and an agent.

pub const CLOCK_ROUNDS: u32 = 8;
pub const WARN_OFFSET_MS: f64 = 50.0;
pub const MAX_OFFSET_MS: f64 = 1000.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockSample {
    pub sent_ms: i64,
    pub agent_ms: i64,
    pub recv_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClockEstimate {
    /// agent clock − server clock
    pub offset_ms: f64,
    pub rtt_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Ready,
    Warn,
    NotReady,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// NTP-style: offset_i = agent_i − (sent_i + rtt_i / 2). Rounds are ranked by RTT and the
/// median offset/RTT of the lower-RTT half is returned, so stalled or asymmetric rounds
/// (which bias the midpoint assumption) are discarded.
pub fn estimate(samples: &[ClockSample]) -> Option<ClockEstimate> {
    if samples.is_empty() {
        return None;
    }
    let mut pairs: Vec<(f64, f64)> = samples
        .iter()
        // a wall-clock step between send and receive gives a negative RTT: unusable
        .filter(|s| s.recv_ms >= s.sent_ms)
        .map(|s| {
            let rtt = (s.recv_ms - s.sent_ms) as f64;
            (rtt, s.agent_ms as f64 - (s.sent_ms as f64 + rtt / 2.0))
        })
        .collect();
    pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
    if pairs.is_empty() {
        return None;
    }
    let keep = pairs.len().div_ceil(2).max(1);
    pairs.truncate(keep);
    Some(ClockEstimate {
        offset_ms: median(pairs.iter().map(|p| p.1).collect()),
        rtt_ms: median(pairs.iter().map(|p| p.0).collect()),
    })
}

pub fn readiness(offset_ms: f64) -> Readiness {
    let a = offset_ms.abs();
    if a > MAX_OFFSET_MS {
        Readiness::NotReady
    } else if a > WARN_OFFSET_MS {
        Readiness::Warn
    } else {
        Readiness::Ready
    }
}

pub fn server_to_agent_ms(server_ms: i64, offset_ms: f64) -> i64 {
    server_ms.saturating_add(offset_ms.round() as i64)
}
