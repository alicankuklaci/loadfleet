//! Every way a run can end after its last stage still stores that stage's summary
//! (timeout, user stop, agent loss, rule, normal end).
mod support;

use loadfleet_metrics::RunReport;
use loadfleet_server::*;
use std::time::Duration;
use support::*;

fn plan(extra: &str) -> String {
    format!(
        r#"
name: c
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: http, http: {{ url: "http://127.0.0.1:9/" }} }}
load: {{ stages: [{{target: 10, ramp: 0s, hold: 1s}}] }}
{extra}
"#
    )
}

async fn server() -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    serve(ServerConfig {
        db: dir.join("t.db"),
        finish_grace: Duration::from_secs(1),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap()
}

/// Two ready agents and a started run whose only stage has ended (+ the flushes sent).
async fn run_past_end(h: &ServerHandle, extra: &str, errors: u64) -> (Vec<FakeAgent>, String) {
    let mut ag = Vec::new();
    for n in ["a", "b"] {
        ag.push(FakeAgent::connect(h.addr, &h.token, n, 0).await);
    }
    let hub = h.state.hub.clone();
    assert!(
        wait_until(
            || hub
                .agents()
                .iter()
                .filter(|a| a.readiness.is_some())
                .count()
                == 2,
            5
        )
        .await
    );
    let run = h.state.coord.start_run(&plan(extra)).await.unwrap();
    assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
    let start_at = ag[0].assignment().unwrap().start_at_server_ms;
    for a in &ag {
        a.send_metrics(&run, 1000, 10, errors);
        a.send_metrics(&run, 2000, 2, 0);
    }
    let wait = start_at + 1200 - loadfleet_protocol::now_ms();
    tokio::time::sleep(Duration::from_millis(wait.max(0) as u64)).await;
    (ag, run)
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

fn assert_last_stage(r: &RunReport, status: &str) {
    assert_eq!(r.meta.status, status, "{}", r.meta.verdict);
    assert_eq!(
        r.stages.len(),
        1,
        "last stage summary missing: {}",
        r.meta.verdict
    );
    assert_eq!(r.stages[0].total_ops, 24);
}

#[tokio::test]
async fn normal_end() {
    let h = server().await;
    let (ag, run) = run_past_end(&h, "", 0).await;
    ag.iter().for_each(|a| a.send_done(&run));
    assert_last_stage(&finished(&h, &run, 5).await, "completed");
}

#[tokio::test]
async fn rule_stop() {
    let h = server().await;
    let (ag, run) = run_past_end(&h, "stop: { error_rate: 0.2 }", 5).await;
    assert!(wait_until(|| ag.iter().all(|a| a.stopped()), 5).await);
    ag.iter().for_each(|a| a.send_done(&run));
    let r = finished(&h, &run, 5).await;
    assert_last_stage(&r, "stopped");
    assert!(
        r.meta.verdict.starts_with("stopped at stage 0"),
        "{}",
        r.meta.verdict
    );
    assert!(!r.stages[0].stop_reasons.is_empty());
}

#[tokio::test]
async fn user_stop() {
    let h = server().await;
    let (ag, run) = run_past_end(&h, "", 0).await;
    assert!(h.state.coord.stop_run(&run, "enough"));
    assert!(wait_until(|| ag.iter().all(|a| a.stopped()), 3).await);
    ag.iter().for_each(|a| a.send_done(&run));
    assert_last_stage(&finished(&h, &run, 5).await, "stopped");
}

#[tokio::test]
async fn agent_loss_stop() {
    let h = server().await;
    let (ag, run) = run_past_end(&h, "stop: { stop_on_agent_loss: true }", 0).await;
    ag[1].send_failed(&run, "boom");
    assert!(wait_until(|| ag[0].stopped(), 3).await);
    ag[0].send_done(&run);
    assert_last_stage(&finished(&h, &run, 5).await, "stopped");
}

#[tokio::test]
async fn timeout_stop() {
    let h = server().await;
    // nobody ever reports Done: the run is given up on after the grace (clamped to 3 s)
    let (ag, run) = run_past_end(&h, "", 0).await;
    let r = finished(&h, &run, 10).await;
    assert!(ag.iter().all(|a| a.stopped()));
    assert!(
        r.meta
            .verdict
            .starts_with("stopped: agents did not finish in time"),
        "{}",
        r.meta.verdict
    );
    assert_last_stage(&r, "stopped");
}
