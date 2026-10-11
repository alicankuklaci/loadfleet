use crate::{decode_hist, new_hist, LatSummary, MetricsError, Snapshot};
use hdrhistogram::Histogram;
use loadfleet_health::HealthSample;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Most distinct metric names (ops, counters, errors, gauges and histograms together) one run
/// accepts. Every new name costs memory for the rest of the run (and a histogram name a full
/// HDR histogram per stage), so an agent that keeps inventing names is refused, not merged.
pub const MAX_RUN_SERIES: usize = 512;
/// Most distinct metric names one agent may contribute to a run.
pub const MAX_AGENT_SERIES: usize = 256;
/// Most distinct histogram names one run accepts (each is ~200 KB per stage).
pub const MAX_RUN_HISTS: usize = 64;

/// Distinct metric names seen in a run, run-wide and per agent.
#[derive(Default)]
struct Names {
    run: BTreeSet<(u8, String)>,
    hists: usize,
    agents: BTreeMap<String, BTreeSet<(u8, String)>>,
}

impl Names {
    fn of(s: &Snapshot) -> impl Iterator<Item = (u8, &String)> {
        let ops = s.ops.keys().map(|n| (0, n));
        let counters = s.counters.keys().map(|n| (1, n));
        let errors = s.errors.keys().map(|n| (2, n));
        let gauges = s.gauges.keys().map(|n| (3, n));
        let hists = s.hists.keys().map(|n| (4, n));
        ops.chain(counters).chain(errors).chain(gauges).chain(hists)
    }

    /// Records the names of `s` for `agent`, or refuses (recording nothing) when that would
    /// take the run or the agent past its cap.
    fn admit(&mut self, agent: &str, s: &Snapshot) -> Result<(), MetricsError> {
        let mine = self.agents.get(agent);
        let mut new_run = Vec::new();
        let mut new_agent = 0usize;
        let mut new_hists = 0usize;
        for (k, n) in Self::of(s) {
            let key = (k, n.clone());
            if !mine.is_some_and(|m| m.contains(&key)) {
                new_agent += 1;
            }
            if !self.run.contains(&key) {
                new_hists += usize::from(k == 4);
                new_run.push(key);
            }
        }
        let agent_total = mine.map_or(0, BTreeSet::len) + new_agent;
        if agent_total > MAX_AGENT_SERIES {
            return Err(MetricsError::TooManySeries(format!(
                "agent {agent} sent {agent_total} distinct metric names (max {MAX_AGENT_SERIES} per agent per run)"
            )));
        }
        if self.run.len() + new_run.len() > MAX_RUN_SERIES {
            return Err(MetricsError::TooManySeries(format!(
                "the run would hold {} distinct metric names (max {MAX_RUN_SERIES} per run)",
                self.run.len() + new_run.len()
            )));
        }
        if self.hists + new_hists > MAX_RUN_HISTS {
            return Err(MetricsError::TooManySeries(format!(
                "the run would hold {} histograms (max {MAX_RUN_HISTS} per run)",
                self.hists + new_hists
            )));
        }
        self.hists += new_hists;
        self.run.extend(new_run);
        let mine = self.agents.entry(agent.to_string()).or_default();
        mine.extend(Self::of(s).map(|(k, n)| (k, n.clone())));
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageBounds {
    pub index: usize,
    pub target: u64,
    pub start_ms: i64,
    pub hold_from_ms: i64,
    pub end_ms: i64,
}

#[derive(Default)]
struct Acc {
    ops: BTreeMap<String, u64>,
    counters: BTreeMap<String, u64>,
    errors: BTreeMap<String, u64>,
    hists: BTreeMap<String, Histogram<u64>>,
}

impl Acc {
    /// Decodes every histogram of `s`; nothing is mutated, so a bad snapshot is rejected whole.
    fn decode(s: &Snapshot) -> Result<Vec<(String, Histogram<u64>)>, MetricsError> {
        s.hists
            .iter()
            .map(|(k, enc)| Ok((k.clone(), decode_hist(enc)?)))
            .collect()
    }
    fn add(&mut self, s: &Snapshot, hists: &[(String, Histogram<u64>)]) {
        for (k, v) in &s.ops {
            *self.ops.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &s.counters {
            *self.counters.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &s.errors {
            *self.errors.entry(k.clone()).or_default() += v;
        }
        for (k, h) in hists {
            // decode_hist yields new_hist() bounds, so merging cannot fail.
            let _ = self.hists.entry(k.clone()).or_insert_with(new_hist).add(h);
        }
    }
    fn total_ops(&self) -> u64 {
        self.ops.values().sum()
    }
    fn total_errors(&self) -> u64 {
        self.errors.values().sum()
    }
    fn latency(&self) -> BTreeMap<String, LatSummary> {
        self.hists
            .iter()
            .filter_map(|(k, h)| LatSummary::of(h).map(|l| (k.clone(), l)))
            .collect()
    }
}

#[derive(Default)]
struct Second {
    acc: Acc,
    gauges: BTreeMap<String, BTreeMap<String, i64>>, // agent -> gauges
    health: BTreeMap<String, HealthSample>,
}

#[derive(Default)]
struct HealthAcc {
    cpu_sum: f64,
    n: u64,
    lag_p95s: Vec<f64>,
    ports_pct_max: f64,
    client_errors: u64,
}

#[derive(Default)]
struct StageAcc {
    acc: Acc,
    hold_gauges: Vec<BTreeMap<String, i64>>,
    health: BTreeMap<String, HealthAcc>,
    first_ms: Option<i64>,
    last_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesPoint {
    pub t_s: i64,
    pub ops: u64,
    pub errors: u64,
    pub gauges: BTreeMap<String, i64>,
    pub latency: BTreeMap<String, LatSummary>,
    pub cpu_max: f64,
    pub lag_p95_max: f64,
    pub agents: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentStageHealth {
    pub cpu_avg: f64,
    pub lag_p95_ms: f64,
    pub ports_pct_max: f64,
    pub client_errors: u64,
    pub saturated: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StageSummary {
    pub index: usize,
    pub target: u64,
    pub duration_s: f64,
    pub ops: BTreeMap<String, u64>,
    pub counters: BTreeMap<String, u64>,
    pub total_ops: u64,
    pub errors: BTreeMap<String, u64>,
    /// (errors + `dropped`) / (total_ops + `dropped`): the single source for exports,
    /// stop rules and the UI.
    pub error_rate: f64,
    /// Open model: planned ops that were never started (`open.dropped` counter).
    pub dropped: u64,
    /// True when `dropped > 0`: the target rate was not achieved.
    pub rate_not_achieved: bool,
    pub ops_per_s: f64,
    pub latency: BTreeMap<String, LatSummary>,
    pub gauges_avg: BTreeMap<String, f64>,
    pub gauges_min: BTreeMap<String, i64>,
    pub gauges_max: BTreeMap<String, i64>,
    pub agents: BTreeMap<String, AgentStageHealth>,
    pub client_limited: bool,
    pub client_notes: Vec<String>,
    pub stop_reasons: Vec<String>,
}

pub struct RunAggregate {
    bounds: Vec<StageBounds>,
    open: BTreeMap<i64, Second>,
    stages: Vec<StageAcc>,
    /// Highest second already emitted as a series point.
    closed_through: i64,
    /// (agent, snapshot stamp) pairs already ingested, to drop exact duplicates.
    seen: BTreeSet<(String, i64)>,
    names: Names,
}

impl RunAggregate {
    pub fn new(bounds: Vec<StageBounds>) -> Self {
        let stages = bounds.iter().map(|_| StageAcc::default()).collect();
        RunAggregate {
            bounds,
            open: BTreeMap::new(),
            stages,
            closed_through: i64::MIN,
            seen: BTreeSet::new(),
            names: Names::default(),
        }
    }

    pub fn bounds(&self) -> &[StageBounds] {
        &self.bounds
    }

    /// Stage whose window contains the instant just before `rel_ms` (snapshots are stamped at window end).
    fn stage_of(&self, rel_ms: i64) -> Option<usize> {
        let t = rel_ms - 1;
        self.bounds
            .iter()
            .position(|b| t >= b.start_ms && t < b.end_ms)
    }

    pub fn ingest(
        &mut self,
        agent: &str,
        rel_ms: i64,
        snap: &Snapshot,
        health: &HealthSample,
    ) -> Result<(), MetricsError> {
        // The final flush is stamped past the last stage; fold it into that stage's last window.
        let tail = self.bounds.last().map(|b| b.end_ms).filter(|&e| rel_ms > e);
        let rel_ms = tail.unwrap_or(rel_ms);
        let sec = (rel_ms - 1).div_euclid(1000) + 1;
        let hists = Acc::decode(snap)?;
        if self.seen.contains(&(agent.to_string(), snap.at_ms)) {
            return Ok(());
        }
        // a snapshot over the name caps is refused whole (`MetricsError::TooManySeries`)
        self.names.admit(agent, snap)?;
        if !self.seen.insert((agent.to_string(), snap.at_ms)) {
            return Ok(());
        }
        // A snapshot for an already-closed second still counts toward its stage totals,
        // but never reopens the series or the gauge statistics.
        if sec > self.closed_through {
            let slot = self.open.entry(sec).or_default();
            slot.acc.add(snap, &hists);
            if tail.is_none() || !slot.gauges.contains_key(agent) {
                slot.gauges.insert(agent.to_string(), snap.gauges.clone());
                slot.health.insert(agent.to_string(), health.clone());
            }
        }
        if let Some(i) = self.stage_of(rel_ms) {
            let st = &mut self.stages[i];
            st.acc.add(snap, &hists);
            st.first_ms = Some(st.first_ms.map_or(rel_ms - 1000, |f| f.min(rel_ms - 1000)));
            st.last_ms = Some(st.last_ms.map_or(rel_ms, |l| l.max(rel_ms)));
            let h = st.health.entry(agent.to_string()).or_default();
            h.cpu_sum += health.cpu_pct;
            h.n += 1;
            h.lag_p95s.push(health.lag_p95_ms);
            h.ports_pct_max = h.ports_pct_max.max(health.ports_pct);
            h.client_errors += health.client_errors;
        }
        Ok(())
    }

    /// Closes every second that ended at or before `rel_ms` and returns them as series points.
    pub fn close_until(&mut self, rel_ms: i64) -> Vec<SeriesPoint> {
        let last = rel_ms.div_euclid(1000);
        self.closed_through = self.closed_through.max(last);
        let keys: Vec<i64> = self.open.range(..=last).map(|(k, _)| *k).collect();
        let mut out = Vec::new();
        for k in keys {
            let Some(slot) = self.open.remove(&k) else {
                continue;
            };
            let mut gauges: BTreeMap<String, i64> = BTreeMap::new();
            for g in slot.gauges.values() {
                for (name, v) in g {
                    *gauges.entry(name.clone()).or_default() += v;
                }
            }
            if let Some(i) = self.stage_of(k * 1000) {
                if k * 1000 > self.bounds[i].hold_from_ms {
                    self.stages[i].hold_gauges.push(gauges.clone());
                }
            }
            out.push(SeriesPoint {
                t_s: k,
                ops: slot.acc.total_ops(),
                errors: slot.acc.total_errors(),
                latency: slot.acc.latency(),
                cpu_max: slot.health.values().map(|h| h.cpu_pct).fold(0.0, f64::max),
                lag_p95_max: slot
                    .health
                    .values()
                    .map(|h| h.lag_p95_ms)
                    .fold(0.0, f64::max),
                agents: slot.gauges.len() as u32,
                gauges,
            });
        }
        out
    }

    pub fn stage_summary(&self, idx: usize) -> StageSummary {
        let b = &self.bounds[idx];
        let st = &self.stages[idx];
        let duration_s = match (st.first_ms, st.last_ms) {
            (Some(f), Some(l)) => ((l - f.max(b.start_ms)) as f64 / 1000.0).max(1.0),
            _ => 0.0,
        };
        let total_ops = st.acc.total_ops();
        let total_errors = st.acc.total_errors();
        let dropped = st.acc.counters.get("open.dropped").copied().unwrap_or(0);
        let mut s = StageSummary {
            index: idx,
            target: b.target,
            duration_s,
            ops: st.acc.ops.clone(),
            counters: st.acc.counters.clone(),
            total_ops,
            errors: st.acc.errors.clone(),
            error_rate: if total_ops + dropped > 0 {
                (total_errors + dropped) as f64 / (total_ops + dropped) as f64
            } else {
                0.0
            },
            dropped,
            rate_not_achieved: dropped > 0,
            ops_per_s: if duration_s > 0.0 {
                total_ops as f64 / duration_s
            } else {
                0.0
            },
            latency: st.acc.latency(),
            ..Default::default()
        };
        for g in &st.hold_gauges {
            for (k, v) in g {
                *s.gauges_avg.entry(k.clone()).or_default() += *v as f64;
                let mn = s.gauges_min.entry(k.clone()).or_insert(*v);
                *mn = (*mn).min(*v);
                let mx = s.gauges_max.entry(k.clone()).or_insert(*v);
                *mx = (*mx).max(*v);
            }
        }
        let n = st.hold_gauges.len().max(1) as f64;
        s.gauges_avg.values_mut().for_each(|v| *v /= n);
        for (agent, h) in &st.health {
            let lag = p95(&h.lag_p95s);
            let sample = HealthSample {
                cpu_pct: h.cpu_sum / h.n.max(1) as f64,
                lag_p95_ms: lag,
                ports_pct: h.ports_pct_max,
                client_errors: h.client_errors,
                ..Default::default()
            };
            let reasons = sample.reasons();
            if !reasons.is_empty() {
                s.client_notes
                    .push(format!("{agent}: {}", reasons.join(", ")));
            }
            s.agents.insert(
                agent.clone(),
                AgentStageHealth {
                    cpu_avg: (sample.cpu_pct * 10.0).round() / 10.0,
                    lag_p95_ms: lag,
                    ports_pct_max: h.ports_pct_max,
                    client_errors: h.client_errors,
                    saturated: !reasons.is_empty(),
                },
            );
        }
        s.client_limited = s.agents.values().any(|a| a.saturated);
        s
    }
}

fn p95(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() as f64 * 0.95).ceil() as usize).clamp(1, v.len()) - 1]
}
