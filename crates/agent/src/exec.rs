//! Runs one assignment: allow-list check #2, late-join refusal, offset-corrected start,
//! per-second metrics, final flush, Done/Failed.
use loadfleet_engine::{
    execute, CancellationToken, ExecError, ExecOutcome, Scenario, Schedule, Work,
};
use loadfleet_health::{now_ms, HealthMonitor};
use loadfleet_metrics::Recorder;
use loadfleet_plan::{AllowList, Model};
use loadfleet_protocol::{server_to_agent_ms, AgentMsg, Assignment};
use loadfleet_scenario::{build, AllowGuard, BuildCtx};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::time::Instant;

/// An assignment whose start is further in the past than this is refused (late join).
const LATE_JOIN_MS: i64 = 1000;

/// Strictly increasing `(seq, at_ms)` stamps for this run's Metrics frames: the server's
/// SeqGuard drops anything that does not increase, so a wall-clock step back must not
/// produce a stamp at or below the previous one.
#[derive(Default)]
struct Stamps {
    seq: u64,
    at_ms: i64,
}

impl Stamps {
    fn next(&mut self) -> (u64, i64) {
        self.seq += 1;
        self.at_ms = now_ms().max(self.at_ms.saturating_add(1));
        (self.seq, self.at_ms)
    }
}

/// Everything `run_assignment` checks before it accepts the work. Err is the `Failed` text.
fn prepare(
    a: &Assignment,
    local_allow: Option<&AllowList>,
    max_conns: u64,
) -> Result<(Arc<dyn Scenario>, Instant), String> {
    // allow-list check #2: plan ∩ the server's list ∩ this agent's --allow, every endpoint
    let server = AllowList::parse(&a.allow).map_err(|e| format!("server allow-list: {e}"))?;
    let guard = AllowGuard::layered(&a.plan.target.allow, Some(&server), local_allow)?;
    for (field, endpoint) in a.plan.scenario.endpoints() {
        let host = AllowList::host_of(&endpoint).map_err(|e| format!("{field}: {e}"))?;
        if guard.check(&host).is_err() {
            return Err(format!("{field}: target {host} not allowed by this agent"));
        }
    }
    // same peak the server checked against: closed = VUs, open = in-flight cap
    let peak = match a.plan.load.model {
        Model::Closed => a.stage_targets.iter().copied().max().unwrap_or(0),
        Model::Open => a.max_vus,
    };
    if peak > max_conns {
        return Err(format!(
            "share {peak} exceeds this agent's --max-conns {max_conns}"
        ));
    }
    let ctx = BuildCtx {
        guard,
        run_id: a.run_id.clone(),
        agent: format!("a{}", a.agent_index),
        max_vus: a.max_vus,
    };
    let scenario = build(&a.plan.scenario, ctx)?;
    // the server's start_at on this agent's clock (offset = agent − server)
    let wait_ms = server_to_agent_ms(a.start_at_server_ms, a.offset_ms) - now_ms();
    if wait_ms < -LATE_JOIN_MS {
        return Err(format!(
            "assignment arrived {} ms after start_at (late join)",
            -wait_ms
        ));
    }
    Ok((
        scenario,
        Instant::now() + Duration::from_millis(wait_ms.max(0) as u64),
    ))
}

/// Runs one assignment; every outgoing message goes through `out`. Returns after the final
/// `Done` or `Failed` (or once `out` is closed).
pub async fn run_assignment(
    a: Assignment,
    local_allow: Option<AllowList>,
    max_conns: u64,
    out: mpsc::Sender<AgentMsg>,
    cancel: CancellationToken,
) {
    let run_id = a.run_id.clone();
    let fail = |error: String| AgentMsg::Failed {
        run_id: run_id.clone(),
        error,
    };
    let (scenario, start) = match prepare(&a, local_allow.as_ref(), max_conns) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(run = %run_id, error = %e, "refusing the assignment");
            let _ = out.send(fail(e)).await;
            return;
        }
    };
    if out
        .send(AgentMsg::Ready {
            run_id: run_id.clone(),
        })
        .await
        .is_err()
    {
        return;
    }
    let rec = Recorder::new();
    let mut monitor = HealthMonitor::start();
    let work = Work {
        schedule: Schedule::new(&a.plan.load.stages).with_targets(&a.stage_targets),
        model: a.plan.load.model,
        max_vus: a.max_vus,
        vu_offset: a.vu_offset,
        id_prefix: format!("run-{}-a{}", a.run_id, a.agent_index),
        seed: a.seed ^ u64::from(a.agent_index).wrapping_mul(0x5851_F42D_4C95_7F2D),
    };
    // `execute` owns the only handle to the scenario: its pooled connections close with it
    let mut exec = tokio::spawn(execute(work, scenario, rec.clone(), cancel.clone(), start));
    let mut tick = tokio::time::interval_at(start + Duration::from_secs(1), Duration::from_secs(1));
    let mut stamps = Stamps::default();
    let mut k: i64 = 0;
    let mut metrics = |k: i64, monitor: &mut HealthMonitor| {
        let (seq, at_ms) = stamps.next();
        AgentMsg::Metrics {
            run_id: run_id.clone(),
            seq,
            rel_ms: k * 1000,
            snap: rec.snapshot(at_ms),
            health: monitor.sample(),
        }
    };
    let result = loop {
        tokio::select! {
            r = &mut exec => break r,
            _ = tick.tick() => {
                k += 1;
                // never block the load on a slow control link
                let _ = out.try_send(metrics(k, &mut monitor));
            }
        }
    };
    let last = match outcome(result) {
        Ok(o) => {
            // final flush: whatever completed since the last tick (the server folds a stamp
            // past the last stage into that stage)
            let _ = out.send(metrics(k + 1, &mut monitor)).await;
            let reason = if o.cancelled {
                "cancelled"
            } else {
                "completed"
            };
            AgentMsg::Done {
                run_id: run_id.clone(),
                reason: reason.into(),
            }
        }
        // an engine refusal is a failed share, never "completed with 0 ops"
        Err(e) => fail(e),
    };
    let _ = out.send(last).await;
}

fn outcome(r: Result<Result<ExecOutcome, ExecError>, JoinError>) -> Result<ExecOutcome, String> {
    match r {
        Ok(Ok(o)) => Ok(o),
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("engine task ended abnormally: {e}")),
    }
}
