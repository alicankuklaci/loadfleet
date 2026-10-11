//! Real server + real agents in one process.
mod support;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_agents_share_and_merge() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let _a: Vec<_> = ["a1", "a2", "a3"]
        .iter()
        .map(|n| agent_with(&h, n, |_| {}))
        .collect();
    ready(&h, 3).await;
    let run = h
        .state
        .coord
        .start_run(&http_plan(addr, 30, "3s", ""))
        .await
        .unwrap();
    let r = finished(&h, &run, 20).await;
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    assert_eq!(
        r.meta
            .agents
            .iter()
            .map(|a| a.stage_targets[0])
            .collect::<Vec<_>>(),
        vec![10, 10, 10]
    );
    let ops = r.stages[0].total_ops;
    let served = hits.load(Ordering::SeqCst);
    // requests the target served but whose response was cut off by the end of the run are
    // the only difference (a percentage, not an absolute count)
    assert!(
        ops > 100 && ops.abs_diff(served) <= 30.max(served / 50),
        "ops {ops} served {served}"
    );
    assert!(r.series.iter().any(|p| p.agents == 3));
    assert!(r.meta.missing_agents.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_closes_target_connections_within_a_second() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let t = Target::spawn().await;
    let _a = [agent_with(&h, "a1", |_| {}), agent_with(&h, "a2", |_| {})];
    ready(&h, 2).await;
    let run = h
        .state
        .coord
        .start_run(&tcp_plan(t.addr, 200, "60s", ""))
        .await
        .unwrap();
    t.until_open(200, 10).await;
    let t0 = Instant::now();
    assert!(h.state.coord.stop_run(&run, "panel"));
    t.until_open(0, 3).await;
    assert!(
        t0.elapsed() < Duration::from_millis(1500),
        "connections closed after {:?}",
        t0.elapsed()
    );
    let r = finished(&h, &run, 10).await;
    assert!(
        r.meta.verdict.starts_with("stopped by user: panel"),
        "{}",
        r.meta.verdict
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_loss_marks_run_and_late_agent_stays_idle() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let (addr, _) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let a1 = agent_with(&h, "a1", |_| {});
    let _a2 = agent_with(&h, "a2", |_| {});
    ready(&h, 2).await;
    let run = h
        .state
        .coord
        .start_run(&http_plan(addr, 10, "4s", ""))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await; // past start_at (2 s)
    let _late = agent_with(&h, "late", |_| {});
    a1.cancel(); // agent a1 process exits mid-run
    let r = finished(&h, &run, 20).await;
    assert_eq!(r.meta.agents.len(), 2, "late agent was not added");
    assert_eq!(r.meta.missing_agents, vec!["a1".to_string()]);
    assert!(
        r.meta.verdict.ends_with("(missing agents: a1)"),
        "{}",
        r.meta.verdict
    );
    assert!(h
        .state
        .hub
        .agents()
        .iter()
        .any(|a| a.name == "late" && a.busy_run.is_none()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_stops_when_server_dies() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let t = Target::spawn().await;
    // the default silence is 15 s; 2 s keeps the test short while exercising the same path
    let _a = agent_with(&h, "a1", |c| c.silence = Duration::from_secs(2));
    ready(&h, 1).await;
    h.state
        .coord
        .start_run(&tcp_plan(t.addr, 50, "60s", ""))
        .await
        .unwrap();
    t.until_open(50, 10).await;
    let t0 = Instant::now();
    h.kill();
    t.until_open(0, 10).await;
    assert!(
        t0.elapsed() < Duration::from_secs(4),
        "stopped after {:?}",
        t0.elapsed()
    );
}
