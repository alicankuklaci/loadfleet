use async_trait::async_trait;
use loadfleet_engine::*;
use loadfleet_metrics::Recorder;
use loadfleet_plan::{Model, Stage};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

/// Op 0 outlives the drain; every other op is short.
struct OneSlow;

#[async_trait]
impl Scenario for OneSlow {
    fn kind(&self) -> &'static str {
        "one-slow"
    }
    async fn run_vu(&self, _ctx: VuCtx) {}
    fn supports_open(&self) -> bool {
        true
    }
    async fn op(&self, ctx: OpCtx) {
        ctx.rec.op("x");
        let ms = if ctx.iter == 0 { 5_000 } else { 300 };
        sleep(Duration::from_millis(ms)).await;
    }
}

/// Ops are spread over several JoinSets; a long op in the first one must not leave the finished
/// ops of the others counted as aborted when the drain times out.
#[tokio::test(start_paused = true)]
async fn drain_counts_only_unfinished_ops_as_aborted() {
    let rec = Recorder::new();
    let w = Work {
        schedule: Schedule::new(&[Stage {
            target: 10,
            ramp: Duration::ZERO,
            hold: Duration::from_millis(1000),
        }]),
        model: Model::Open,
        max_vus: 20,
        vu_offset: 0,
        id_prefix: "run-t-a".into(),
        seed: 7,
    };
    let t0 = Instant::now();
    let out = execute(
        w,
        Arc::new(OneSlow),
        rec.clone(),
        CancellationToken::new(),
        t0,
    )
    .await
    .unwrap();
    assert_eq!(out.ops_started, 10);
    assert!(
        t0.elapsed() <= Duration::from_millis(1900),
        "{:?}",
        t0.elapsed()
    );
    let snap = rec.snapshot(0);
    assert_eq!(snap.counters.get("open.aborted"), Some(&1));
    assert_eq!(snap.gauges["vus"], 0);
}
