use async_trait::async_trait;
use loadfleet_engine::*;
use loadfleet_plan::parse;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Flaky; // every op fails: stage 0 trips the error-rate rule

#[async_trait]
impl Scenario for Flaky {
    fn kind(&self) -> &'static str {
        "flaky"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        while !ctx.cancel.is_cancelled() {
            ctx.rec.op("x");
            ctx.rec.latency("x", Duration::from_millis(1));
            ctx.rec.error("x:fail");
            tokio::select! { _ = ctx.cancel.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(50)) => {} }
        }
    }
}

/// Succeeds on every op; closed model only.
struct Fine;

#[async_trait]
impl Scenario for Fine {
    fn kind(&self) -> &'static str {
        "fine"
    }
    async fn run_vu(&self, ctx: VuCtx) {
        while !ctx.cancel.is_cancelled() {
            ctx.rec.op("x");
            ctx.rec.latency("x", Duration::from_millis(1));
            tokio::select! { _ = ctx.cancel.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(50)) => {} }
        }
    }
}

const PLAN: &str = r#"
name: flaky
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:1/" } }
load: { stages: [{target: 3, ramp: 0s, hold: 2s}, {target: 6, ramp: 0s, hold: 2s}] }
stop: { error_rate: 0.5 }
"#;

fn opts(id: &str) -> LocalOpts {
    LocalOpts {
        run_id: id.into(),
        agent: "local".into(),
        version: "0".into(),
        on_second: None,
    }
}

#[tokio::test]
async fn stop_rule_ends_the_run_after_the_failing_stage() {
    let p = parse(PLAN).unwrap();
    let t0 = std::time::Instant::now();
    let r = run_local(&p, Arc::new(Flaky), opts("t1"), CancellationToken::new()).await;
    assert_eq!(r.meta.status, "stopped");
    assert!(
        r.meta
            .verdict
            .starts_with("stopped at stage 0 (target 3): error rate 100.00% > 50.00%"),
        "{}",
        r.meta.verdict
    );
    assert_eq!(r.stages.len(), 1);
    assert!(r.stages[0].total_ops > 50);
    assert!(
        t0.elapsed() < Duration::from_millis(3800),
        "stage 1 must not run"
    );
    assert!(r.series.len() >= 2);
}

const SHORT: &str = r#"
name: short
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:1/" } }
load: { model: MODEL, stages: [{target: 2, ramp: 0s, hold: 1s}, {target: 4, ramp: 0s, hold: 1s}] }
"#;

#[tokio::test]
async fn completed_run_keeps_seed_model_and_series_callback_and_drops_scenario() {
    let p = parse(&SHORT.replace("MODEL", "closed")).unwrap();
    assert_eq!(p.seed, None);
    let seen = Arc::new(AtomicU64::new(0));
    let s2 = seen.clone();
    let mut o = opts("t2");
    o.on_second = Some(Box::new(move |_p| {
        s2.fetch_add(1, Ordering::SeqCst);
    }));
    let sc: Arc<dyn Scenario> = Arc::new(Fine);
    let r = run_local(&p, sc.clone(), o, CancellationToken::new()).await;
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    assert_eq!(r.meta.verdict, "completed all stages up to 4");
    assert_eq!(r.meta.model, "closed");
    assert!(r.meta.seed.is_some(), "the generated seed is stored");
    assert_eq!(r.stages.len(), 2);
    assert!(r.stages.iter().all(|s| s.total_ops > 0));
    assert_eq!(seen.load(Ordering::SeqCst) as usize, r.series.len());
    assert_eq!(
        Arc::strong_count(&sc),
        1,
        "run_local must drop the scenario"
    );
}

#[tokio::test]
async fn plan_seed_is_kept() {
    let p = parse(&format!("{}seed: 42\n", SHORT.replace("MODEL", "closed"))).unwrap();
    let r = run_local(&p, Arc::new(Fine), opts("t3"), CancellationToken::new()).await;
    assert_eq!(r.meta.seed, Some(42));
}

#[tokio::test]
async fn open_model_on_closed_only_scenario_fails_not_completes() {
    let p = parse(&SHORT.replace("MODEL", "open")).unwrap();
    let t0 = std::time::Instant::now();
    let r = run_local(&p, Arc::new(Fine), opts("t4"), CancellationToken::new()).await;
    assert_eq!(r.meta.status, "failed");
    assert_eq!(r.meta.model, "open");
    assert!(
        r.meta.verdict.contains("does not support the open model"),
        "{}",
        r.meta.verdict
    );
    assert!(r.stages.is_empty());
    assert!(t0.elapsed() < Duration::from_millis(1500));
}

#[tokio::test]
async fn cancel_interrupts_the_run() {
    let p = parse(&SHORT.replace("MODEL", "closed")).unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1300)).await;
        c.cancel();
    });
    let t0 = std::time::Instant::now();
    let r = run_local(&p, Arc::new(Fine), opts("t5"), cancel).await;
    assert_eq!(r.meta.status, "interrupted");
    assert_eq!(r.meta.verdict, "interrupted");
    assert!(t0.elapsed() < Duration::from_millis(2300));
    assert!(!r.stages.is_empty() && r.stages.len() <= 2);
}
