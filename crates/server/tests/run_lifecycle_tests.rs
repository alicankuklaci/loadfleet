//! Run lifecycle edge cases: failed agents, start atomicity / busy, reconnect mid-run, the
//! final flush, shutdown closing agent sockets.
mod support;

use loadfleet_metrics::RunReport;
use loadfleet_server::*;
use std::time::Duration;
use support::*;

fn plan(target: u64, hold: &str, extra: &str) -> String {
    format!(
        r#"
name: c
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: http, http: {{ url: "http://127.0.0.1:9/" }} }}
load: {{ stages: [{{target: {target}, ramp: 0s, hold: {hold}}}] }}
{extra}
"#
    )
}

async fn server() -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    serve(ServerConfig {
        db: dir.join("t.db"),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap()
}

async fn agents(h: &ServerHandle, names: &[&str]) -> Vec<FakeAgent> {
    let mut v = Vec::new();
    for n in names {
        v.push(FakeAgent::connect(h.addr, &h.token, n, 0).await);
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
        .await,
        "agents not ready"
    );
    v
}

/// Waits until the stored run leaves `running` and returns its report.
async fn finished(h: &ServerHandle, run: &str, secs: u64) -> RunReport {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if let Ok(Some(r)) = h.state.store.report(run).await {
            if r.meta.status != "running" {
                return r;
            }
        }
        assert!(
            tokio::time::Instant::now() < end,
            "run {run} still running after {secs}s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn every_agent_failing_fails_the_run_promptly() {
    let h = server().await;
    let ag = agents(&h, &["a", "b"]).await;
    let run = h.state.coord.start_run(&plan(10, "60s", "")).await.unwrap();
    assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
    ag[0].send_failed(&run, "env FOO missing");
    ag[1].send_failed(&run, "bad regex");
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "failed", "{:?}", r.meta);
    assert!(
        r.meta.verdict.contains("env FOO missing"),
        "{}",
        r.meta.verdict
    );
    assert!(
        r.meta.verdict.ends_with("(missing agents: a, b)"),
        "{}",
        r.meta.verdict
    );
    assert!(wait_until(|| h.state.coord.active().is_none(), 2).await);
    assert!(h.state.hub.agents().iter().all(|a| a.busy_run.is_none()));
}

#[tokio::test]
async fn failed_agent_honours_stop_on_agent_loss() {
    let h = server().await;
    let ag = agents(&h, &["a", "b"]).await;
    let run = h
        .state
        .coord
        .start_run(&plan(10, "60s", "stop: { stop_on_agent_loss: true }"))
        .await
        .unwrap();
    assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
    ag[0].send_failed(&run, "boom");
    assert!(
        wait_until(|| ag[1].stopped(), 3).await,
        "survivor told to stop"
    );
    ag[1].send_done(&run);
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "stopped");
    assert!(r.meta.verdict.contains("agent a"), "{}", r.meta.verdict);
    assert_eq!(r.meta.missing_agents, vec!["a".to_string()]);
}

#[tokio::test]
async fn all_done_ends_the_run_and_keeps_the_final_flush() {
    let h = server().await;
    let ag = agents(&h, &["a", "b"]).await;
    let run = h.state.coord.start_run(&plan(10, "1s", "")).await.unwrap();
    let asg = {
        assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
        ag[0].assignment().unwrap()
    };
    assert_eq!(asg.allow, ServerConfig::for_tests().allow);
    for a in &ag {
        a.send_metrics(&run, 1000, 10, 0);
        // the agent's last flush is stamped past the stage end
        a.send_metrics(&run, 2000, 5, 0);
        a.send_done(&run);
    }
    let r = finished(&h, &run, 8).await;
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    assert_eq!(r.meta.model, "closed");
    assert!(r.meta.missing_agents.is_empty());
    assert_eq!(r.stages.len(), 1);
    assert_eq!(
        r.stages[0].total_ops, 30,
        "final flush folded into the last stage"
    );
    assert_eq!(r.series.iter().map(|p| p.ops).sum::<u64>(), 30);
}

#[tokio::test]
async fn replayed_metrics_are_dropped() {
    let h = server().await;
    let ag = agents(&h, &["a"]).await;
    let run = h.state.coord.start_run(&plan(10, "1s", "")).await.unwrap();
    assert!(wait_until(|| ag[0].assignment().is_some(), 3).await);
    ag[0].send_metrics(&run, 1000, 10, 0);
    // same window again under a new seq, then an old seq: both must be ignored
    ag[0].send_metrics(&run, 1000, 10, 0);
    let _ = ag[0].out.send(loadfleet_protocol::AgentMsg::Metrics {
        run_id: run.clone(),
        seq: 1,
        rel_ms: 1500,
        snap: Default::default(),
        health: Default::default(),
    });
    ag[0].send_done(&run);
    let r = finished(&h, &run, 8).await;
    assert_eq!(r.stages[0].total_ops, 10);
}

#[tokio::test]
async fn reconnect_mid_run_counts_as_a_lost_agent() {
    let h = server().await;
    let ag = agents(&h, &["a"]).await;
    let run = h.state.coord.start_run(&plan(10, "60s", "")).await.unwrap();
    assert!(wait_until(|| ag[0].assignment().is_some(), 3).await);
    // same name, new socket: the old one is replaced
    let _again = FakeAgent::connect(h.addr, &h.token, "a", 0).await;
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "failed", "{}", r.meta.verdict);
    assert_eq!(r.meta.missing_agents, vec!["a".to_string()]);
}

#[tokio::test]
async fn busy_while_active_and_free_after_the_run() {
    let h = server().await;
    let ag = agents(&h, &["a"]).await;
    let run = h.state.coord.start_run(&plan(10, "60s", "")).await.unwrap();
    assert_eq!(h.state.coord.active(), Some(run.clone()));
    match h.state.coord.start_run(&plan(10, "1s", "")).await {
        Err(StartError::Busy(id)) => assert_eq!(id, run),
        other => panic!("{other:?}"),
    }
    assert!(!h.state.coord.stop_run("nope", "x"));
    assert!(h.state.coord.stop_run(&run, "enough"));
    assert!(wait_until(|| ag[0].stopped(), 3).await);
    ag[0].send_done(&run);
    let r = finished(&h, &run, 5).await;
    assert_eq!(r.meta.status, "stopped");
    assert!(
        r.meta.verdict.starts_with("stopped by user: enough"),
        "{}",
        r.meta.verdict
    );
    assert!(wait_until(|| h.state.coord.active().is_none(), 2).await);
    let hub = h.state.hub.clone();
    assert!(wait_until(|| hub.agents()[0].busy_run.is_none(), 2).await);
    assert!(h.state.coord.start_run(&plan(10, "1s", "")).await.is_ok());
}

#[tokio::test]
async fn kill_closes_agent_sockets() {
    let h = server().await;
    let ag = agents(&h, &["a", "b"]).await;
    h.kill();
    assert!(
        wait_until(|| ag.iter().all(|a| a.task.is_finished()), 3).await,
        "agent sockets still open after kill"
    );
}

#[tokio::test]
async fn slow_agents_final_flush_lands_in_the_last_stage() {
    let h = server().await;
    let ag = agents(&h, &["a", "b"]).await;
    let run = h.state.coord.start_run(&plan(10, "1s", "")).await.unwrap();
    assert!(wait_until(|| ag.iter().all(|a| a.assignment().is_some()), 3).await);
    let start_at = ag[0].assignment().unwrap().start_at_server_ms;
    for a in &ag {
        a.send_metrics(&run, 1000, 10, 0);
    }
    ag[0].send_metrics(&run, 2000, 5, 0);
    ag[0].send_done(&run);
    // b is a saturated generator: its final flush comes well past end + LATENESS (2 s)
    let late = start_at + 1000 + 3500 - loadfleet_protocol::now_ms();
    tokio::time::sleep(Duration::from_millis(late.max(0) as u64)).await;
    ag[1].send_metrics(&run, 2000, 7, 0);
    ag[1].send_done(&run);
    let r = finished(&h, &run, 8).await;
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    let series: u64 = r.series.iter().map(|p| p.ops).sum();
    assert_eq!(series, 32);
    assert_eq!(
        r.stages[0].total_ops, series,
        "late flush missing from the stage"
    );
}
