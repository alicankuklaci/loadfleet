use crate::exec::{vu_seed, ExecOutcome, Work, DRAIN};
use crate::{Scenario, VuCtx};
use loadfleet_metrics::Recorder;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const TICK: Duration = Duration::from_millis(50);

/// Closed model: every 50 ms the running VU count is moved to `floor(target_at(t))`.
/// New VUs get the next global index; surplus VUs (highest index first) get their token cancelled.
pub(crate) async fn run(
    w: &Work,
    scenario: Arc<dyn Scenario>,
    rec: &Recorder,
    cancel: &CancellationToken,
    start: Instant,
) -> ExecOutcome {
    let gauge = rec.gauge("vus");
    let mut running: Vec<(CancellationToken, JoinHandle<()>)> = Vec::new();
    let mut stopping: Vec<JoinHandle<()>> = Vec::new();
    let mut out = ExecOutcome::default();
    let total = w.schedule.total();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => { out.cancelled = true; break; }
            _ = tick.tick() => {}
        }
        let t = start.elapsed();
        if t >= total {
            break;
        }
        let desired = (w.schedule.target_at(t) + 1e-9).floor() as usize;
        while running.len() < desired {
            let vu = w.vu_offset + running.len() as u64;
            let token = cancel.child_token();
            let ctx = VuCtx {
                vu,
                vu_id: format!("{}-{vu}", w.id_prefix),
                rec: rec.vu(vu),
                cancel: token.clone(),
                seed: vu_seed(w.seed, vu),
            };
            let s = scenario.clone();
            running.push((token, tokio::spawn(async move { s.run_vu(ctx).await })));
            out.vus_started = out.vus_started.max(running.len() as u64);
        }
        while running.len() > desired {
            if let Some((token, handle)) = running.pop() {
                token.cancel();
                stopping.push(handle);
            }
        }
        stopping.retain(|h| !h.is_finished());
        gauge.set(running.len() as i64);
    }
    for (token, handle) in running.drain(..) {
        token.cancel();
        stopping.push(handle);
    }
    let deadline = Instant::now() + DRAIN;
    for h in stopping {
        let abort = h.abort_handle();
        if tokio::time::timeout_at(deadline, h).await.is_err() {
            abort.abort();
        }
    }
    gauge.set(0);
    out
}
