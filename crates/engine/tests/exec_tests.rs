use async_trait::async_trait;
use loadfleet_engine::*;
use loadfleet_metrics::{decode_hist, LatSummary, Recorder};
use loadfleet_plan::{Model, Stage};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[derive(Default)]
struct Fake {
    active: AtomicI64,
    ops: AtomicU64,
    op_sleep_ms: u64,
}

#[async_trait]
impl Scenario for Fake {
    fn kind(&self) -> &'static str {
        "fake"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        self.active.fetch_add(1, Ordering::SeqCst);
        ctx.cancel.cancelled().await;
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
    fn supports_open(&self) -> bool {
        true
    }
    async fn op(&self, ctx: OpCtx) {
        self.ops.fetch_add(1, Ordering::SeqCst);
        ctx.rec.op("fake");
        sleep(Duration::from_millis(self.op_sleep_ms)).await;
        ctx.rec.latency("fake", ctx.scheduled.elapsed());
    }
}

fn st(target: u64, ramp_ms: u64, hold_ms: u64) -> Stage {
    Stage {
        target,
        ramp: Duration::from_millis(ramp_ms),
        hold: Duration::from_millis(hold_ms),
    }
}

fn work(model: Model, stages: &[Stage], max_vus: u64) -> Work {
    Work {
        schedule: Schedule::new(stages),
        model,
        max_vus,
        vu_offset: 0,
        id_prefix: "run-t-a".into(),
        seed: 7,
    }
}

#[tokio::test(start_paused = true)]
async fn closed_model_follows_ramp_and_hold() {
    let fake = Arc::new(Fake::default());
    let rec = Recorder::new();
    let start = Instant::now();
    let w = work(Model::Closed, &[st(10, 1000, 1000), st(4, 0, 1000)], 0);
    let h = tokio::spawn(execute(
        w,
        fake.clone(),
        rec.clone(),
        CancellationToken::new(),
        start,
    ));
    sleep(Duration::from_millis(520)).await;
    let a = fake.active.load(Ordering::SeqCst);
    assert!((4..=6).contains(&a), "ramp midpoint active={a}");
    sleep(Duration::from_millis(1000)).await; // t=1.52 s
    assert_eq!(fake.active.load(Ordering::SeqCst), 10);
    assert_eq!(rec.snapshot(0).gauges["vus"], 10);
    sleep(Duration::from_millis(1000)).await; // t=2.52 s
    assert_eq!(fake.active.load(Ordering::SeqCst), 4);
    let out = h.await.unwrap().unwrap();
    assert_eq!(fake.active.load(Ordering::SeqCst), 0);
    assert_eq!(out.vus_started, 10);
    assert!(!out.cancelled);
}

#[tokio::test(start_paused = true)]
async fn cancel_stops_vus_quickly() {
    let fake = Arc::new(Fake::default());
    let cancel = CancellationToken::new();
    let w = work(Model::Closed, &[st(50, 0, 60_000)], 0);
    let h = tokio::spawn(execute(
        w,
        fake.clone(),
        Recorder::new(),
        cancel.clone(),
        Instant::now(),
    ));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.active.load(Ordering::SeqCst), 50);
    let t0 = Instant::now();
    cancel.cancel();
    let out = h.await.unwrap().unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1));
    assert!(out.cancelled);
    assert_eq!(fake.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn open_model_hits_the_rate() {
    let fake = Arc::new(Fake::default());
    let w = work(Model::Open, &[st(100, 0, 1000)], 50);
    execute(
        w,
        fake.clone(),
        Recorder::new(),
        CancellationToken::new(),
        Instant::now(),
    )
    .await
    .unwrap();
    let n = fake.ops.load(Ordering::SeqCst);
    assert!((98..=102).contains(&n), "ops={n}");
}

#[tokio::test(start_paused = true)]
async fn open_model_corrects_coordinated_omission() {
    // 50 ops/s for 1 s, each takes 100 ms, only 1 in flight: the server "stalls" the client.
    // Op k (planned at 20k ms) can only start at 100k ms, so ops 0..=9 start (lag up to 720 ms)
    // and the 40 planned ops that find no slot before the schedule ends are dropped.
    let fake = Arc::new(Fake {
        op_sleep_ms: 100,
        ..Default::default()
    });
    let rec = Recorder::new();
    let w = work(Model::Open, &[st(50, 0, 1000)], 1);
    let t0 = Instant::now();
    let out = execute(w, fake.clone(), rec.clone(), CancellationToken::new(), t0)
        .await
        .unwrap();
    // bounded by the schedule (1 s) plus at most the drain, never the ~5 s of a catch-up backlog
    assert!(
        t0.elapsed() <= Duration::from_millis(1800),
        "{:?}",
        t0.elapsed()
    );
    let snap = rec.snapshot(0);
    let lat = LatSummary::of(&decode_hist(&snap.hists["fake"]).unwrap()).unwrap();
    assert_eq!(lat.n, 10);
    assert_eq!(out.ops_started, 10);
    // naive measurement would say ~100 ms; corrected latency includes the queueing behind the stall
    assert!(lat.p99_ms > 750.0, "p99 {}", lat.p99_ms);
    assert_eq!(snap.counters["open.late"], 9);
    assert_eq!(snap.counters["open.dropped"], 40);
}

#[tokio::test(start_paused = true)]
async fn open_model_drops_ops_that_wait_too_long_and_aborts_the_tail() {
    // 10 ops/s for 5 s, one slot, each op takes 2.95 s. Op 0 holds the slot until 2.95 s, so ops
    // planned at 0.1..=1.9 s pass their 1 s wait limit and are dropped; op 20 (planned 2.0 s) gets
    // the slot at 2.95 s and holds it past the end (5.9 s): every later op is dropped, and op 20 is
    // aborted when the 800 ms drain expires.
    let fake = Arc::new(Fake {
        op_sleep_ms: 2950,
        ..Default::default()
    });
    let rec = Recorder::new();
    let w = work(Model::Open, &[st(10, 0, 5000)], 1);
    let t0 = Instant::now();
    let out = execute(w, fake.clone(), rec.clone(), CancellationToken::new(), t0)
        .await
        .unwrap();
    assert!(
        t0.elapsed() <= Duration::from_millis(5800),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(out.ops_started, 2);
    let snap = rec.snapshot(0);
    assert_eq!(snap.counters["open.dropped"], 48);
    assert_eq!(snap.counters["open.aborted"], 1);
    assert_eq!(snap.gauges["vus"], 0);
}

#[tokio::test(start_paused = true)]
async fn open_model_waits_for_the_schedule_end() {
    // a trailing zero-rate stage still counts as run time, like the closed model
    let fake = Arc::new(Fake::default());
    let w = work(Model::Open, &[st(10, 0, 500), st(0, 0, 1000)], 5);
    let t0 = Instant::now();
    execute(
        w,
        fake.clone(),
        Recorder::new(),
        CancellationToken::new(),
        t0,
    )
    .await
    .unwrap();
    assert_eq!(fake.ops.load(Ordering::SeqCst), 5);
    assert!(
        t0.elapsed() >= Duration::from_millis(1500),
        "{:?}",
        t0.elapsed()
    );
}

struct ClosedOnly;

#[async_trait]
impl Scenario for ClosedOnly {
    fn kind(&self) -> &'static str {
        "closed-only"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        ctx.cancel.cancelled().await;
    }
}

#[tokio::test(start_paused = true)]
async fn open_model_rejects_a_scenario_without_open_support() {
    let w = work(Model::Open, &[st(10, 0, 1000)], 5);
    let t0 = Instant::now();
    let later = t0 + Duration::from_secs(10);
    let rec = Recorder::new();
    let err = execute(
        w,
        Arc::new(ClosedOnly),
        rec,
        CancellationToken::new(),
        later,
    )
    .await
    .unwrap_err();
    assert_eq!(err, ExecError::OpenUnsupported("closed-only"));
    assert!(err.to_string().contains("closed-only"));
    assert_eq!(
        t0.elapsed(),
        Duration::ZERO,
        "rejected up front, not at the start time"
    );
    let w = work(Model::Closed, &[st(2, 0, 100)], 0);
    let out = execute(
        w,
        Arc::new(ClosedOnly),
        Recorder::new(),
        CancellationToken::new(),
        t0,
    )
    .await
    .unwrap();
    assert_eq!(out.vus_started, 2);
}

#[tokio::test(start_paused = true)]
async fn open_model_ramp_from_zero_follows_the_rate_integral() {
    // 0 -> 100 ops/s over 1 s plans ∫ rate dt = 50 ops; stepping by 1/rate would plan ~1.
    let fake = Arc::new(Fake::default());
    let w = work(Model::Open, &[st(100, 1000, 0)], 50);
    let start = Instant::now();
    execute(
        w,
        fake.clone(),
        Recorder::new(),
        CancellationToken::new(),
        start,
    )
    .await
    .unwrap();
    let n = fake.ops.load(Ordering::SeqCst);
    assert!((48..=52).contains(&n), "ops={n}");
}

/// Ops that never finish on their own: one honours `ctx.cancel`, the other ignores it.
#[derive(Default)]
struct Stuck {
    in_flight: Arc<AtomicI64>,
    saw_cancel: AtomicU64,
}

struct Live(Arc<AtomicI64>);
impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Scenario for Stuck {
    fn kind(&self) -> &'static str {
        "stuck"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        ctx.cancel.cancelled().await;
    }
    fn supports_open(&self) -> bool {
        true
    }
    async fn op(&self, ctx: OpCtx) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let _live = Live(self.in_flight.clone());
        if ctx.iter % 2 == 0 {
            ctx.cancel.cancelled().await;
            self.saw_cancel.fetch_add(1, Ordering::SeqCst);
        } else {
            sleep(Duration::from_secs(3600)).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn open_cancel_reaches_in_flight_ops_within_a_second() {
    let stuck = Arc::new(Stuck::default());
    let rec = Recorder::new();
    let cancel = CancellationToken::new();
    let w = work(Model::Open, &[st(100, 0, 60_000)], 10);
    let h = tokio::spawn(execute(
        w,
        stuck.clone(),
        rec.clone(),
        cancel.clone(),
        Instant::now(),
    ));
    sleep(Duration::from_millis(500)).await;
    assert_eq!(stuck.in_flight.load(Ordering::SeqCst), 10);
    assert_eq!(rec.snapshot(0).gauges["vus"], 10);
    let t0 = Instant::now();
    cancel.cancel();
    let out = h.await.unwrap().unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
    assert!(out.cancelled);
    assert_eq!(out.ops_started, 10);
    // cooperative ops observed ctx.cancel; the rest were aborted (their futures dropped)
    assert_eq!(stuck.saw_cancel.load(Ordering::SeqCst), 5);
    assert_eq!(stuck.in_flight.load(Ordering::SeqCst), 0);
    assert_eq!(rec.snapshot(0).gauges["vus"], 0);
}

#[tokio::test(start_paused = true)]
async fn open_end_of_schedule_does_not_leave_ops_running() {
    let stuck = Arc::new(Stuck::default());
    let rec = Recorder::new();
    let w = work(Model::Open, &[st(10, 0, 1000)], 100);
    let t0 = Instant::now();
    let out = execute(w, stuck.clone(), rec.clone(), CancellationToken::new(), t0)
        .await
        .unwrap();
    assert!(!out.cancelled);
    assert_eq!(out.ops_started, 10);
    assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
    assert_eq!(stuck.in_flight.load(Ordering::SeqCst), 0);
    assert_eq!(rec.snapshot(0).gauges["vus"], 0);
}

#[tokio::test(start_paused = true)]
async fn open_model_ramp_down_and_zero_stage() {
    // 0 for 300 ms, 100/s for 500 ms (50 ops), then 100 -> 0 over 1 s (50 ops).
    let fake = Arc::new(Fake::default());
    let w = work(
        Model::Open,
        &[st(0, 0, 300), st(100, 0, 500), st(0, 1000, 0)],
        50,
    );
    let start = Instant::now();
    execute(
        w,
        fake.clone(),
        Recorder::new(),
        CancellationToken::new(),
        start,
    )
    .await
    .unwrap();
    let n = fake.ops.load(Ordering::SeqCst);
    assert!((98..=102).contains(&n), "ops={n}");
}
