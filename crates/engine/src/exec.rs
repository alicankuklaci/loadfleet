use crate::{closed, open, Scenario, Schedule};
use loadfleet_metrics::Recorder;
use loadfleet_plan::Model;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How long in-flight work may finish after the schedule ends or Stop fires before it is aborted.
pub(crate) const DRAIN: Duration = Duration::from_millis(800);

#[derive(Debug, Clone)]
pub struct Work {
    pub schedule: Schedule,
    pub model: Model,
    /// Open model: max in-flight operations on this agent.
    pub max_vus: u64,
    /// Global index of this agent's first VU (shares are contiguous ranges).
    pub vu_offset: u64,
    /// `run-<short id>-<agent>`; VU id = `<id_prefix>-<vu>`.
    pub id_prefix: String,
    pub seed: u64,
}

/// Work the engine refuses before starting (plan validation should already have caught it).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecError {
    #[error("scenario '{0}' does not support the open model")]
    OpenUnsupported(&'static str),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecOutcome {
    pub vus_started: u64,
    pub ops_started: u64,
    pub cancelled: bool,
}

pub(crate) fn vu_seed(seed: u64, vu: u64) -> u64 {
    seed ^ vu.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Runs `work` from `start` until the schedule ends or `cancel` fires.
/// Fails at once (before waiting for `start`) if the open model is asked of a scenario without
/// `supports_open()`.
pub async fn execute(
    work: Work,
    scenario: Arc<dyn Scenario>,
    rec: Recorder,
    cancel: CancellationToken,
    start: Instant,
) -> Result<ExecOutcome, ExecError> {
    if work.model == Model::Open && !scenario.supports_open() {
        return Err(ExecError::OpenUnsupported(scenario.kind()));
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            scenario.stop();
            return Ok(ExecOutcome { cancelled: true, ..Default::default() });
        }
        _ = tokio::time::sleep_until(start) => {}
    }
    let hook = scenario.clone();
    let run = async {
        match work.model {
            Model::Closed => closed::run(&work, scenario, &rec, &cancel, start).await,
            Model::Open => open::run(&work, scenario, &rec, &cancel, start).await,
        }
    };
    tokio::pin!(run);
    let out = tokio::select! {
        out = &mut run => out,
        _ = cancel.cancelled() => {
            hook.stop();
            run.await
        }
    };
    hook.stop();
    Ok(out)
}
