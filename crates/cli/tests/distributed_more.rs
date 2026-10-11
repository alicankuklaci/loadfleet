//! More real server + agent scenarios: start alignment across agents,
//! stop_on_agent_loss, an agent whose --allow refuses the plan, restart without runaway load.
mod support;

use loadfleet_plan::AllowList;
use loadfleet_server::{serve, ServerConfig};
use std::time::Duration;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agents_start_together_at_start_at() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let t = Target::spawn().await;
    let _a: Vec<_> = ["a1", "a2", "a3"]
        .iter()
        .map(|n| agent_with(&h, n, |_| {}))
        .collect();
    ready(&h, 3).await;
    let run = h
        .state
        .coord
        .start_run(&tcp_plan(t.addr, 30, "1s", ""))
        .await
        .unwrap();
    let r = finished(&h, &run, 15).await;
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    let accepted = t.accepted.lock().unwrap().clone();
    assert_eq!(accepted.len(), 30);
    // every agent's VUs start within ±200 ms of the server's start_at
    for at in &accepted {
        assert!(
            at.abs_diff(r.meta.started_ms) <= 200,
            "connect at {} ms from start_at",
            at - r.meta.started_ms
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_an_agent_stops_the_run_when_asked() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let t = Target::spawn().await;
    let a1 = agent_with(&h, "a1", |_| {});
    let _a2 = agent_with(&h, "a2", |_| {});
    ready(&h, 2).await;
    let p = tcp_plan(t.addr, 20, "60s", "stop: { stop_on_agent_loss: true }");
    let run = h.state.coord.start_run(&p).await.unwrap();
    t.until_open(20, 10).await;
    a1.cancel();
    let r = finished(&h, &run, 10).await;
    assert_eq!(r.meta.status, "stopped", "{}", r.meta.verdict);
    assert!(
        r.meta.verdict.contains("stop_on_agent_loss"),
        "{}",
        r.meta.verdict
    );
    assert_eq!(r.meta.missing_agents, vec!["a1".to_string()]);
    t.until_open(0, 3).await; // a2 was told to stop too
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_allow_list_refuses_the_plan() {
    let _s = SERIAL.lock().await;
    let h = server_with(|_| {}).await;
    let t = Target::spawn().await;
    let _a = agent_with(&h, "a1", |c| {
        c.local_allow = Some(AllowList::parse(&["10.0.0.0/8".to_string()]).unwrap())
    });
    ready(&h, 1).await;
    let run = h
        .state
        .coord
        .start_run(&tcp_plan(t.addr, 5, "30s", ""))
        .await
        .unwrap();
    let r = finished(&h, &run, 8).await;
    assert_eq!(r.meta.status, "failed", "{}", r.meta.verdict);
    assert!(
        r.meta.verdict.contains("not allowed by this agent"),
        "{}",
        r.meta.verdict
    );
    assert!(
        t.accepted.lock().unwrap().is_empty(),
        "the refused target was contacted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_restart_does_not_resume_the_old_load() {
    let _s = SERIAL.lock().await;
    let db = temp_db();
    let d = db.clone();
    let h = server_with(move |c| c.db = d).await;
    let t = Target::spawn().await;
    let _a = agent_with(&h, "a1", |_| {});
    ready(&h, 1).await;
    h.state
        .coord
        .start_run(&tcp_plan(t.addr, 10, "60s", ""))
        .await
        .unwrap();
    t.until_open(10, 10).await;
    h.kill();
    // same address and database: the agent reconnects to the new server within its backoff
    let mut h2 = None;
    for _ in 0..50 {
        let cfg = ServerConfig {
            db: db.clone(),
            listen: h.addr,
            ..ServerConfig::for_tests()
        };
        if let Ok(s) = serve(cfg).await {
            h2 = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let h2 = h2.expect("could not rebind the server address");
    ready(&h2, 1).await;
    assert_eq!(t.open(), 0, "old run still loading after reconnect");
    assert!(h2.state.hub.agents().iter().all(|a| a.busy_run.is_none()));
}
