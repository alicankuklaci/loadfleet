use crate::{execute, ExecError, ExecOutcome, Scenario, Schedule, StageWatch, Work};
use loadfleet_health::{HealthMonitor, HostInfo};
use loadfleet_metrics::{AgentMeta, Recorder, RunAggregate, RunMeta, RunReport, SeriesPoint};
use loadfleet_plan::{Model, Plan};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Delay between planning a local run and its first VU starting.
const LEAD: Duration = Duration::from_millis(200);

/// Per-second progress callback of a local run.
pub type OnSecond = Box<dyn FnMut(&SeriesPoint) + Send>;

pub struct LocalOpts {
    pub run_id: String,
    pub agent: String,
    pub version: String,
    /// Called once per closed second with its series point (live progress output).
    pub on_second: Option<OnSecond>,
}

/// 6 lowercase hex chars.
pub fn short_id() -> String {
    format!("{:06x}", rand::random::<u32>() & 0xff_ffff)
}

/// Per-second collection: snapshot the recorder, aggregate it, emit closed seconds.
struct Collector {
    rec: Recorder,
    monitor: HealthMonitor,
    agg: RunAggregate,
    agent: String,
    on_second: Option<OnSecond>,
    series: Vec<SeriesPoint>,
}

impl Collector {
    fn collect(&mut self, rel_ms: i64) {
        let snap = self.rec.snapshot(rel_ms);
        let _ = self
            .agg
            .ingest(&self.agent, rel_ms, &snap, &self.monitor.sample());
        for p in self.agg.close_until(rel_ms) {
            if let Some(cb) = self.on_second.as_mut() {
                cb(&p);
            }
            self.series.push(p);
        }
    }
}

/// Runs a whole plan in this process (no server): the `loadfleet run` command.
///
/// Status: `completed`, `stopped` (a stop rule fired), `interrupted` (`cancel` fired) or `failed`
/// (the engine refused the work). `scenario` is dropped before this returns, so its
/// pooled connections are closed.
pub async fn run_local(
    plan: &Plan,
    scenario: Arc<dyn Scenario>,
    opts: LocalOpts,
    cancel: CancellationToken,
) -> RunReport {
    let schedule = Schedule::new(&plan.load.stages);
    let bounds = schedule.bounds();
    let targets = schedule.targets();
    let mut watch = StageWatch::new(&bounds, plan.stop.clone(), plan.exclude_stages.clone());
    let mut col = Collector {
        rec: Recorder::new(),
        monitor: HealthMonitor::start(),
        agg: RunAggregate::new(bounds),
        agent: opts.agent.clone(),
        on_second: opts.on_second,
        series: Vec::new(),
    };
    let start = Instant::now() + LEAD;
    let started_ms = loadfleet_health::now_ms() + LEAD.as_millis() as i64;
    let seed = plan.seed.unwrap_or_else(rand::random);
    let work = Work {
        schedule,
        model: plan.load.model,
        max_vus: plan.load.max_vus.unwrap_or(1000),
        vu_offset: 0,
        id_prefix: format!("run-{}-{}", opts.run_id, opts.agent),
        seed,
    };
    let exec_cancel = cancel.child_token();
    // `execute` owns the only other handle to `scenario`; it is dropped when the task ends.
    let mut exec = tokio::spawn(execute(
        work,
        scenario,
        col.rec.clone(),
        exec_cancel.clone(),
        start,
    ));
    let mut tick = tokio::time::interval_at(start + Duration::from_secs(1), Duration::from_secs(1));
    let mut stages = Vec::new();
    let mut k: i64 = 0;
    let (status, verdict) = loop {
        let finished = tokio::select! {
            _ = tick.tick() => None,
            r = &mut exec => Some(r),
        };
        if let Some(result) = finished {
            // The executor ended: flush the last partial second, then summarize what is left.
            if let Err(msg) = exec_failure(result) {
                break ("failed", msg);
            }
            let elapsed = Instant::now().saturating_duration_since(start).as_millis() as i64;
            let rel_ms = ((k + 1) * 1000).max((elapsed + 999) / 1000 * 1000);
            col.collect(rel_ms);
            let (done, stop) = watch.poll(&col.agg, rel_ms, true);
            stages.extend(done);
            if cancel.is_cancelled() {
                break ("interrupted", "interrupted".to_string());
            }
            break match stop {
                Some(v) => ("stopped", v),
                None => ("completed", watch.verdict_completed()),
            };
        }
        k += 1;
        let rel_ms = k * 1000;
        col.collect(rel_ms);
        let (done, stop) = watch.poll(&col.agg, rel_ms, false);
        stages.extend(done);
        if let Some(v) = stop {
            exec_cancel.cancel();
            let _ = (&mut exec).await;
            break ("stopped", v);
        }
    };
    drop(exec);
    RunReport {
        meta: RunMeta {
            id: opts.run_id,
            name: plan.name.clone(),
            plan_yaml: plan.to_yaml(),
            version: opts.version,
            seed: Some(seed),
            model: match plan.load.model {
                Model::Closed => "closed",
                Model::Open => "open",
            }
            .into(),
            started_ms,
            ended_ms: Some(loadfleet_health::now_ms()),
            status: status.into(),
            verdict,
            agents: vec![local_agent(&opts.agent, targets)],
            missing_agents: Vec::new(),
        },
        stages,
        series: col.series,
    }
}

fn exec_failure(r: Result<Result<ExecOutcome, ExecError>, JoinError>) -> Result<(), String> {
    match r {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("failed: {e}")),
        Err(e) => Err(format!("failed: engine task ended abnormally: {e}")),
    }
}

fn local_agent(name: &str, stage_targets: Vec<u64>) -> AgentMeta {
    let host = HostInfo::detect();
    AgentMeta {
        name: name.into(),
        os: host.os,
        arch: host.arch,
        cores: host.cores,
        ip: String::new(),
        offset_ms: 0.0,
        stage_targets,
    }
}
