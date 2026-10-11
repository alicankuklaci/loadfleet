//! `Scenario::stop` fires on Stop while VUs may still be finishing, and again when `execute`
//! returns (a scenario cancels its background work, e.g. worker threads, there).
use async_trait::async_trait;
use loadfleet_engine::*;
use loadfleet_metrics::Recorder;
use loadfleet_plan::{Model, Stage};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Default)]
struct Watch {
    stops: AtomicU64,
    stopped_before_vu_exit: AtomicBool,
}

#[async_trait]
impl Scenario for Watch {
    fn kind(&self) -> &'static str {
        "watch"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        ctx.cancel.cancelled().await;
        tokio::time::sleep(Duration::from_millis(50)).await; // closing sockets etc.
        if self.stops.load(Ordering::SeqCst) > 0 {
            self.stopped_before_vu_exit.store(true, Ordering::SeqCst);
        }
    }
    fn stop(&self) {
        self.stops.fetch_add(1, Ordering::SeqCst);
    }
}

fn work(hold_ms: u64) -> Work {
    let stages = [Stage {
        target: 2,
        ramp: Duration::ZERO,
        hold: Duration::from_millis(hold_ms),
    }];
    Work {
        schedule: Schedule::new(&stages),
        model: Model::Closed,
        max_vus: 2,
        vu_offset: 0,
        id_prefix: "run-t-a".into(),
        seed: 1,
    }
}

#[tokio::test(start_paused = true)]
async fn stop_hook_fires_on_cancel_and_at_the_end() {
    let s = Arc::new(Watch::default());
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        c2.cancel();
    });
    execute(
        work(60_000),
        s.clone(),
        Recorder::new(),
        cancel,
        Instant::now(),
    )
    .await
    .unwrap();
    assert!(s.stopped_before_vu_exit.load(Ordering::SeqCst));
    assert_eq!(s.stops.load(Ordering::SeqCst), 2);

    let s = Arc::new(Watch::default());
    execute(
        work(500),
        s.clone(),
        Recorder::new(),
        CancellationToken::new(),
        Instant::now(),
    )
    .await
    .unwrap();
    assert_eq!(s.stops.load(Ordering::SeqCst), 1, "natural end: once");
}
