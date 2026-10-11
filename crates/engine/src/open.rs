use crate::exec::{ExecOutcome, Work, DRAIN};
use crate::{OpCtx, Scenario};
use loadfleet_metrics::{Gauge, Recorder};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// An op that starts more than this after its planned time counts as `open.late`.
const LATE: Duration = Duration::from_millis(5);
/// How long a planned op may wait for a free slot (all `max_vus` busy) before it is dropped.
const MAX_WAIT: Duration = Duration::from_secs(1);
/// Ops are spread over this many shards (op k uses shard k % 64), each with its own child of the
/// run's op token and its own `JoinSet`. A `CancellationToken` guards its node with a mutex taken
/// on clone, drop and every `cancelled()` poll, and a `JoinSet` takes its mutex whenever one of its
/// tasks completes: one of each shared by every op were the hottest locks at 40k ops/s
/// (`cargo xtask bench http-rps`). A child token per op was no better: creating and dropping it
/// locks the parent.
const SHARDS: usize = 64;

struct Shard {
    cancel: CancellationToken,
    ops: JoinSet<()>,
}

/// In-flight op count. Op tasks only decrement it (an atomic, where a semaphore permit release
/// would take the semaphore's waiter lock on every op); the dispatcher, the only incrementer,
/// waits on `freed` when it is full.
struct InFlight {
    n: AtomicU64,
    max: u64,
    freed: Notify,
}

/// Held by each op task: frees the in-flight slot and the gauge even if the task is aborted.
struct Slot {
    gauge: Gauge,
    inflight: Arc<InFlight>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.gauge.dec();
        if self.inflight.n.fetch_sub(1, Ordering::AcqRel) == self.inflight.max {
            // the dispatcher may be waiting; a stored permit only costs it one extra check
            self.inflight.freed.notify_one();
        }
    }
}

/// Joins every shard's ops concurrently (completions are reaped as they happen in any shard, so a
/// long op in one shard does not leave the others unjoined when the drain times out).
async fn join_all(shards: &mut [Shard]) {
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for s in shards.iter_mut() {
            loop {
                match s.ops.poll_join_next(cx) {
                    Poll::Ready(Some(_)) => continue,
                    Poll::Ready(None) => break,
                    Poll::Pending => {
                        pending = true;
                        break;
                    }
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await
}

enum Wait {
    Free,
    Expired,
    Cancelled,
}

/// Waits until fewer than `max` ops are in flight, `deadline` passes, or Stop.
async fn wait_for_slot(
    inflight: &InFlight,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Wait {
    loop {
        if inflight.n.load(Ordering::Acquire) < inflight.max {
            return Wait::Free;
        }
        let expire = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Wait::Cancelled,
            _ = inflight.freed.notified() => {}
            _ = expire => return Wait::Expired,
        }
    }
}

/// Open model: operation k is planned at the time the integral of the rate curve reaches k,
/// independent of responses, and its latency is measured from that planned time
/// (coordinated-omission correction).
///
/// At most `max_vus` ops are in flight. A planned op that finds every slot busy waits for one, but
/// never past `MAX_WAIT` after its planned time nor past the schedule end: such ops are not
/// started and are counted as `open.dropped` (k6's dropped iterations), as are all planned ops
/// left when the schedule ends. Started ops that waited more than `LATE` count as `open.late`.
/// So a stalled target never stretches the run past `start + total`, and the backlog never
/// bursts above the cap. Dropped ops get no latency sample: the executor does not know the
/// scenario's metric names, and a lower-bound sample would mix with real ones; the counter is the
/// saturation signal.
///
/// The run lasts until `start + total` (also through trailing zero-rate stages). In-flight ops then
/// get `DRAIN` to finish; on Stop they are told at once through `OpCtx::cancel` and get the same
/// drain. Ops still running after it are aborted and counted as `open.aborted`.
pub(crate) async fn run(
    w: &Work,
    scenario: Arc<dyn Scenario>,
    rec: &Recorder,
    cancel: &CancellationToken,
    start: Instant,
) -> ExecOutcome {
    let inflight = Arc::new(InFlight {
        n: AtomicU64::new(0),
        max: w.max_vus.max(1),
        freed: Notify::new(),
    });
    let max = inflight.max;
    let gauge = rec.gauge("vus");
    let meta = rec.vu(0);
    let end = start.checked_add(w.schedule.total());
    let ops_cancel = cancel.child_token();
    let mut shards: Vec<Shard> = (0..SHARDS)
        .map(|_| Shard {
            cancel: ops_cancel.child_token(),
            ops: JoinSet::new(),
        })
        .collect();
    let mut out = ExecOutcome::default();
    let mut dropped: u64 = 0;
    let mut iter: u64 = 0;
    while let Some(offset) = w.schedule.op_time(iter as f64) {
        let Some(next) = start.checked_add(offset) else {
            break;
        };
        // Ops already due start without a timer round-trip (a past-deadline `Sleep` still
        // registers with the timer driver and waits for its next turn).
        if next > Instant::now() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => { out.cancelled = true; break; }
                _ = tokio::time::sleep_until(next) => {}
            }
        } else if cancel.is_cancelled() {
            out.cancelled = true;
            break;
        } else {
            // A dispatcher that is behind would otherwise never yield: let the worker run the
            // ops it just spawned (and its IO/timer drivers) once the coop budget is spent.
            tokio::task::consume_budget().await;
        }
        let deadline = match (next.checked_add(MAX_WAIT), end) {
            (Some(l), Some(e)) => Some(l.min(e)),
            (l, e) => l.or(e),
        };
        let free = match wait_for_slot(&inflight, cancel, deadline).await {
            Wait::Cancelled => {
                out.cancelled = true;
                break;
            }
            Wait::Free => true,
            Wait::Expired => false,
        };
        let now = Instant::now();
        if end.is_some_and(|e| now >= e) {
            break; // never start an op once the schedule is over; the rest are counted below
        }
        if !free {
            dropped += 1;
            iter += 1;
            continue;
        }
        if now.duration_since(next) > LATE {
            meta.count("open.late", 1);
        }
        let vu = w.vu_offset + iter % max;
        let shard = &mut shards[(iter % SHARDS as u64) as usize];
        let ctx = OpCtx {
            vu,
            vu_id: format!("{}-{vu}", w.id_prefix),
            iter,
            scheduled: next,
            rec: rec.vu(vu),
            cancel: shard.cancel.clone(),
        };
        inflight.n.fetch_add(1, Ordering::AcqRel);
        gauge.inc();
        let slot = Slot {
            gauge: gauge.clone(),
            inflight: inflight.clone(),
        };
        let s = scenario.clone();
        shard.ops.spawn(async move {
            let _slot = slot;
            s.op(ctx).await;
        });
        while shard.ops.try_join_next().is_some() {}
        out.ops_started += 1;
        iter += 1;
    }
    if !out.cancelled {
        dropped += w.schedule.planned_ops().saturating_sub(iter);
        if let Some(end) = end {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => out.cancelled = true,
                _ = tokio::time::sleep_until(end) => {}
            }
        }
    }
    if dropped > 0 {
        meta.count("open.dropped", dropped);
    }
    if out.cancelled {
        ops_cancel.cancel();
    }
    let _ = tokio::time::timeout(DRAIN, join_all(&mut shards)).await;
    // Count before cancelling: an op cut short by the cancel below was still running at the
    // drain deadline. `len()` also counts finished tasks not joined yet, so reap those first.
    for s in shards.iter_mut() {
        while s.ops.try_join_next().is_some() {}
    }
    let aborted: usize = shards.iter().map(|s| s.ops.len()).sum();
    ops_cancel.cancel();
    if aborted > 0 {
        meta.count("open.aborted", aborted as u64);
        for s in shards.iter_mut() {
            s.ops.abort_all();
        }
    }
    for s in shards.iter_mut() {
        while s.ops.join_next().await.is_some() {}
    }
    gauge.set(0);
    out
}
