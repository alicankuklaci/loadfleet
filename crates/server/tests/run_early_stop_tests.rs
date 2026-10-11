//! Runs that end before their schedule does: only stages that started are stored, and
//! stages that never started get no summary and no stop evaluation.
mod support;

use loadfleet_metrics::RunReport;
use loadfleet_server::*;
use std::time::Duration;
use support::*;

/// Three stages ending at 1 s, 3 s and 5 s; the error-rate rule must never fire (no errors).
const PLAN: &str = r#"
name: c
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:9/" } }
load: { stages: [{target: 10, ramp: 0s, hold: 1s}, {target: 20, ramp: 0s, hold: 2s}, {target: 30, ramp: 0s, hold: 2s}] }
stop: { error_rate: 0.5, stop_on_agent_loss: true }
"#;

async fn start(names: &[&str]) -> (ServerHandle, Vec<FakeAgent>, String, i64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let h = serve(ServerConfig {
        db: dir.join("t.db"),
        finish_grace: Duration::from_secs(1),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap();
    let mut ag = Vec::new();
    for n in names {
        ag.push(FakeAgent::connect(h.addr, &h.token, n, 0).await);
    }
    let hub = h.state.hub.clone();
    let n = names.len();
    assert!(
        wait_until(
            || hub
                .agents()
                .iter()
                .filter(|a| a.readiness.is_some())
                .count()
                == n,
            5
        )
        .await
    );
    let run = h.state.coord.start_run(PLAN).await.unwrap();
    assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
    let start_at = ag[0].assignment().unwrap().start_at_server_ms;
    (h, ag, run, start_at)
}

async fn sleep_until_rel(start_at: i64, rel_ms: i64) {
    let wait = start_at + rel_ms - loadfleet_protocol::now_ms();
    tokio::time::sleep(Duration::from_millis(wait.max(0) as u64)).await;
}

async fn finished(h: &ServerHandle, run: &str, secs: u64) -> RunReport {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(Some(r)) = h.state.store.report(run).await {
            if r.meta.status != "running" {
                return r;
            }
        }
        assert!(tokio::time::Instant::now() < end, "run still running");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn stored(r: &RunReport) -> Vec<(usize, u64)> {
    for s in &r.stages {
        assert!(s.stop_reasons.is_empty(), "spurious stop reasons: {s:?}");
    }
    r.stages.iter().map(|s| (s.index, s.total_ops)).collect()
}

#[tokio::test]
async fn user_stop_in_the_first_stage_stores_only_it() {
    let (h, ag, run, start_at) = start(&["a"]).await;
    ag[0].send_metrics(&run, 1000, 10, 0);
    sleep_until_rel(start_at, 500).await;
    assert!(h.state.coord.stop_run(&run, "enough"));
    assert!(wait_until(|| ag[0].stopped(), 3).await);
    ag[0].send_done(&run);
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "stopped", "{}", r.meta.verdict);
    assert_eq!(stored(&r), vec![(0, 10)]);
}

#[tokio::test]
async fn agent_loss_in_the_second_stage_stores_the_first_two() {
    let (h, ag, run, start_at) = start(&["a", "b"]).await;
    for a in &ag {
        a.send_metrics(&run, 1000, 10, 0);
        a.send_metrics(&run, 2000, 20, 0);
    }
    sleep_until_rel(start_at, 1500).await;
    ag[1].send_failed(&run, "boom");
    assert!(wait_until(|| ag[0].stopped(), 3).await);
    ag[0].send_done(&run);
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "stopped", "{}", r.meta.verdict);
    assert_eq!(stored(&r), vec![(0, 20), (1, 40)]);
}

#[tokio::test]
async fn timeout_stores_every_elapsed_stage() {
    // A timeout only fires after the whole schedule has elapsed, so every stage started:
    // stages without data are stored with 0 ops (and no stop reasons), none are dropped.
    let (h, ag, run, _) = start(&["a"]).await;
    ag[0].send_metrics(&run, 1000, 10, 0);
    let r = finished(&h, &run, 15).await;
    assert!(
        r.meta
            .verdict
            .starts_with("stopped: agents did not finish in time"),
        "{}",
        r.meta.verdict
    );
    assert_eq!(stored(&r), vec![(0, 10), (1, 0), (2, 0)]);
}
