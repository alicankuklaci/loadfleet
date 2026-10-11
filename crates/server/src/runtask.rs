//! One task per run: merges agent metrics, persists series and stage summaries, applies stop
//! rules, tells agents to stop and writes the final status.
use crate::coordinator::ActiveGuard;
use crate::{Hub, ServerConfig, Store};
use loadfleet_engine::{Schedule, StageWatch};
use loadfleet_health::HealthSample;
use loadfleet_metrics::{MetricsError, RunAggregate, RunMeta, Snapshot, StageSummary};
use loadfleet_plan::Plan;
use loadfleet_protocol::{check_snapshot, now_ms, SeqGuard, ServerMsg};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Metrics may arrive this late (network + 1 s batching) before a second/stage is closed.
const LATENESS_MS: i64 = 2000;
/// After Stop, how long agents get to confirm before the run is closed anyway.
const STOP_DRAIN: Duration = Duration::from_secs(3);
/// Agent clocks are synchronised to the server's: a window stamped further than this past
/// the server's own run clock is bogus and would open aggregation slots far ahead.
const FUTURE_SLACK_MS: i64 = 10_000;

/// `Metrics` is much larger than the other variants; events are a few per agent per second.
#[allow(clippy::large_enum_variant)]
pub enum RunEvent {
    Metrics {
        agent: String,
        seq: u64,
        rel_ms: i64,
        snap: Snapshot,
        health: HealthSample,
    },
    Done {
        agent: String,
        reason: String,
    },
    Failed {
        agent: String,
        error: String,
    },
    AgentLeft(String),
    /// An agent (re)registered; `connected_ms` identifies its socket.
    Joined {
        agent: String,
        connected_ms: i64,
    },
    Stop(String),
}

pub struct RunCtx {
    pub plan: Plan,
    pub meta: RunMeta,
    /// (agent id, `connected_ms` of the socket that received the Assign)
    pub agents: Vec<(String, i64)>,
    pub start_at_ms: i64,
    pub store: Store,
    pub hub: Hub,
    pub cfg: Arc<ServerConfig>,
    pub active: ActiveGuard,
}

enum StopKind {
    Rule,
    User,
    AgentLoss,
    Timeout,
}

/// Agent bookkeeping for one run.
struct Roster {
    ids: Vec<(String, i64)>,
    done: BTreeSet<String>,
    /// failed or lost: no longer part of the run
    gone: BTreeSet<String>,
    failures: Vec<String>,
    /// agents removed from the run by the server (metric name caps), with the reason
    dropped: Vec<String>,
    guards: HashMap<String, SeqGuard>,
}

impl Roster {
    fn member(&self, a: &str) -> bool {
        self.ids.iter().any(|(id, _)| id == a)
    }
    fn active(&self, a: &str) -> bool {
        self.member(a) && !self.gone.contains(a)
    }
    fn finished(&self, a: &str) -> bool {
        self.done.contains(a) || self.gone.contains(a)
    }
    fn all_finished(&self) -> bool {
        self.ids.iter().all(|(a, _)| self.finished(a))
    }
    fn all_gone(&self) -> bool {
        self.ids.iter().all(|(a, _)| self.gone.contains(a))
    }
    /// Marks `a` failed/lost; true if it was still running its share.
    fn lose(&mut self, a: &str, why: String, meta: &mut RunMeta) -> bool {
        if !self.member(a) || self.finished(a) {
            return false;
        }
        self.gone.insert(a.to_string());
        self.failures.push(format!("{a}: {why}"));
        if !meta.missing_agents.iter().any(|m| m == a) {
            meta.missing_agents.push(a.to_string());
        }
        true
    }
}

pub async fn run_task(mut c: RunCtx, mut rx: mpsc::UnboundedReceiver<RunEvent>) {
    let schedule = Schedule::new(&c.plan.load.stages);
    let bounds = schedule.bounds();
    let last_end = bounds.last().map_or(0, |b| b.end_ms);
    let mut agg = RunAggregate::new(bounds.clone());
    let mut watch = StageWatch::new(&bounds, c.plan.stop.clone(), c.plan.exclude_stages.clone());
    let total_ms = schedule.total().min(c.cfg.max_duration).as_millis() as i64;
    // never give up on agents before the last stage's stop rules could be evaluated
    let grace_ms = (c.cfg.finish_grace.as_millis() as i64).max(LATENESS_MS + 1000);
    let mut r = Roster {
        ids: std::mem::take(&mut c.agents),
        done: BTreeSet::new(),
        gone: BTreeSet::new(),
        failures: Vec::new(),
        dropped: Vec::new(),
        guards: HashMap::new(),
    };
    let mut stop: Option<(String, StopKind)> = None;
    let mut stop_at: Option<tokio::time::Instant> = None;
    // run-relative time the stop was decided: stages not reached by then never started
    let mut stop_rel: Option<i64> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let loss_stops = c.plan.stop.stop_on_agent_loss;
    while !r.all_finished() {
        tokio::select! {
            ev = rx.recv() => match ev {
                None => break,
                Some(ev) => {
                    let lost = handle(&mut c, &mut r, &mut agg, ev, &mut stop);
                    if let Some(what) = lost {
                        if loss_stops && stop.is_none() {
                            stop = Some((format!("stopped: agent {what} (stop_on_agent_loss)"), StopKind::AgentLoss));
                        }
                    }
                }
            },
            _ = tick.tick() => {}
        }
        let rel = now_ms() - c.start_at_ms;
        // the last stage's final second stays open for the agents' final flush
        let closed = agg.close_until((rel - LATENESS_MS).min(last_end - 1));
        if !closed.is_empty() {
            if let Err(e) = c.store.put_series(&c.meta.id, &closed).await {
                tracing::warn!(run = %c.meta.id, error = %e, "storing series failed");
            }
        }
        if stop.is_none() {
            // Stages before the last are final once LATENESS_MS has passed. The last one is
            // only finalized in `finish`, after every agent is done/gone (or the deadline),
            // so a slow agent's final flush is never left out of its summary.
            let (summaries, v) = watch.poll(&agg, (rel - LATENESS_MS).min(last_end - 1), false);
            for s in &summaries {
                persist_stage(&c, s).await;
            }
            stop = v.map(|v| (v, StopKind::Rule));
        }
        if stop.is_none() && rel - LATENESS_MS >= last_end {
            // still apply the last stage's stop rules to what has arrived, without finalizing
            let (_, v) = watch.clone().poll(&agg, rel - LATENESS_MS, false);
            stop = v.map(|v| (v, StopKind::Rule));
        }
        if rel > total_ms + grace_ms && stop.is_none() {
            stop = Some((
                "stopped: agents did not finish in time".into(),
                StopKind::Timeout,
            ));
            let late: Vec<String> = r
                .ids
                .iter()
                .map(|(a, _)| a.clone())
                .filter(|a| !r.finished(a))
                .collect();
            for a in late {
                r.lose(&a, "did not finish in time".into(), &mut c.meta);
            }
        }
        if let (Some((reason, _)), None) = (&stop, stop_at) {
            // agents given up on by the timeout may still be loading: tell everyone not done
            for (id, _) in r.ids.iter().filter(|(a, _)| !r.done.contains(a)) {
                c.hub.send(
                    id,
                    ServerMsg::Stop {
                        run_id: c.meta.id.clone(),
                        reason: reason.clone(),
                    },
                );
            }
            stop_at = Some(tokio::time::Instant::now() + STOP_DRAIN);
            stop_rel = Some(rel);
        }
        if stop_at.is_some_and(|t| tokio::time::Instant::now() >= t) {
            break;
        }
    }
    let ended_rel = stop_rel
        .unwrap_or_else(|| now_ms() - c.start_at_ms)
        .min(last_end);
    finish(c, r, agg, watch, stop, ended_rel).await;
}

/// Applies one event. Returns `Some(description)` when it cost the run an agent.
fn handle(
    c: &mut RunCtx,
    r: &mut Roster,
    agg: &mut RunAggregate,
    ev: RunEvent,
    stop: &mut Option<(String, StopKind)>,
) -> Option<String> {
    let run = c.meta.id.clone();
    match ev {
        RunEvent::Metrics {
            agent,
            seq,
            rel_ms,
            snap,
            health,
        } => {
            if !r.active(&agent) {
                return None;
            }
            if let Err(e) = check_snapshot(&snap) {
                tracing::warn!(%run, %agent, error = %e, "dropping oversized metrics");
                return None;
            }
            // replays and out-of-order frames are dropped before aggregation
            if !r
                .guards
                .entry(agent.clone())
                .or_default()
                .accept(seq, snap.at_ms)
            {
                return None;
            }
            if rel_ms > now_ms() - c.start_at_ms + FUTURE_SLACK_MS {
                tracing::warn!(%run, %agent, rel_ms, "dropping metrics stamped in the future");
                return None;
            }
            match agg.ingest(&agent, rel_ms, &snap, &health) {
                Ok(()) => None,
                Err(e @ MetricsError::TooManySeries(_)) => {
                    // an agent inventing metric names is removed from the run and told to stop
                    tracing::error!(%run, %agent, error = %e, "dropping agent from the run");
                    c.hub.send(
                        &agent,
                        ServerMsg::Stop {
                            run_id: run.clone(),
                            reason: format!("dropped by the server: {e}"),
                        },
                    );
                    r.dropped.push(format!("{agent}: {e}"));
                    r.lose(&agent, format!("dropped by the server: {e}"), &mut c.meta)
                        .then(|| format!("{agent} dropped ({e})"))
                }
                Err(e) => {
                    tracing::warn!(%run, %agent, error = %e, "dropping undecodable metrics");
                    None
                }
            }
        }
        RunEvent::Done { agent, reason } => {
            if r.active(&agent) {
                tracing::debug!(%run, %agent, %reason, "agent done");
                r.done.insert(agent);
            }
            None
        }
        RunEvent::Failed { agent, error } => {
            tracing::warn!(%run, %agent, %error, "agent failed its share");
            r.lose(&agent, error.clone(), &mut c.meta)
                .then(|| format!("{agent} failed: {error}"))
        }
        RunEvent::AgentLeft(agent) => r
            .lose(&agent, "connection lost".into(), &mut c.meta)
            .then(|| format!("{agent} lost")),
        RunEvent::Joined {
            agent,
            connected_ms,
        } => {
            // a reconnect replaces the socket that held the assignment
            let replaced = r
                .ids
                .iter()
                .any(|(a, conn)| *a == agent && *conn != connected_ms);
            (replaced && r.lose(&agent, "reconnected mid-run".into(), &mut c.meta))
                .then(|| format!("{agent} lost"))
        }
        RunEvent::Stop(reason) => {
            if stop.is_none() {
                *stop = Some((format!("stopped by user: {reason}"), StopKind::User));
            }
            None
        }
    }
}

async fn finish(
    mut c: RunCtx,
    r: Roster,
    mut agg: RunAggregate,
    mut watch: StageWatch,
    stop: Option<(String, StopKind)>,
    ended_rel: i64,
) {
    // everything that arrived, including the agents' final flush
    let closed = agg.close_until(i64::MAX / 4);
    if let Err(e) = c.store.put_series(&c.meta.id, &closed).await {
        tracing::warn!(run = %c.meta.id, error = %e, "storing series failed");
    }
    let mut stop = stop;
    // Polled at the run-relative time the run ended (the stop decision, else now), clamped
    // to the last stage end: StageWatch then omits a stage that had not ended by then and
    // has no ops, together with every later one — never-started stages get no summary and
    // no stop evaluation (the report model has no "not started" marker; absence means it).
    // Every finish path persists what the loop left, including the last stage, which is
    // only finalized here; after a rule stop at an earlier stage `rest` is empty.
    let (rest, rule) = watch.poll(&agg, ended_rel, true);
    for s in &rest {
        persist_stage(&c, s).await;
    }
    if stop.is_none() {
        stop = rule.map(|v| (v, StopKind::Rule));
    }
    // nobody finished and nothing but the losses themselves stopped the run
    let failed = r.all_gone() && matches!(stop, None | Some((_, StopKind::AgentLoss)));
    let mut verdict = if failed {
        c.meta.status = "failed".into();
        if r.failures.is_empty() {
            "failed: no agent received its assignment".to_string()
        } else {
            format!(
                "failed: no agent finished its share ({})",
                r.failures.join("; ")
            )
        }
    } else if let Some((v, _)) = stop {
        c.meta.status = "stopped".into();
        v
    } else {
        c.meta.status = "completed".into();
        watch.verdict_completed()
    };
    if !c.meta.missing_agents.is_empty() {
        verdict.push_str(&format!(
            " (missing agents: {})",
            c.meta.missing_agents.join(", ")
        ));
    }
    if !r.dropped.is_empty() {
        verdict.push_str(&format!(" (dropped agents: {})", r.dropped.join("; ")));
    }
    c.meta.verdict = verdict;
    c.meta.ended_ms = Some(now_ms());
    // free the agents and the active slot before the final write
    for (id, _) in &r.ids {
        c.hub.mark_busy(id, None);
    }
    let RunCtx {
        meta,
        store,
        active,
        ..
    } = c;
    drop(active);
    if let Err(e) = store.update_run(&meta).await {
        tracing::error!(run = %meta.id, error = %e, "storing the final run status failed");
    }
}

async fn persist_stage(c: &RunCtx, s: &StageSummary) {
    if let Err(e) = c.store.put_stage(&c.meta.id, s).await {
        tracing::warn!(run = %c.meta.id, error = %e, "storing stage summary failed");
    }
    for (agent, h) in &s.agents {
        if !h.saturated {
            continue;
        }
        let share = c
            .meta
            .agents
            .iter()
            .find(|a| &a.name == agent)
            .and_then(|a| a.stage_targets.get(s.index));
        if let Some(share) = share {
            let _ = c.store.set_saturation(agent, *share).await;
        }
    }
}
