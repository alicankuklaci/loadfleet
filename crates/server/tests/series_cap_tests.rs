//! An agent that keeps inventing metric names (each new histogram name costs server memory for
//! the rest of the run) is dropped from the run, told to stop, and named in the verdict; the
//! other agents' run carries on.
mod support;

use loadfleet_metrics::{Recorder, MAX_RUN_HISTS};
use loadfleet_server::*;
use std::time::Duration;
use support::*;

const PLAN: &str = r#"
name: c
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:9/" } }
load: { stages: [{target: 10, ramp: 0s, hold: 2s}] }
"#;

#[tokio::test]
async fn an_agent_rotating_histogram_names_is_dropped_from_the_run() {
    let dir = tempfile::tempdir().unwrap().keep();
    let h = serve(ServerConfig {
        db: dir.join("t.db"),
        finish_grace: Duration::from_secs(1),
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap();
    let good = FakeAgent::connect(h.addr, &h.token, "good", 0).await;
    let evil = FakeAgent::connect(h.addr, &h.token, "evil", 0).await;
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
    let run = h.state.coord.start_run(PLAN).await.unwrap();
    assert!(
        wait_until(
            || good.assignment().is_some() && evil.assignment().is_some(),
            3
        )
        .await
    );
    // fresh histogram names in every frame, seq/at_ms incremented so the replay guard passes
    let frames = MAX_RUN_HISTS / 8 + 2;
    for f in 0..frames {
        let rec = Recorder::new();
        let vu = rec.vu(1);
        for i in 0..8 {
            let name = format!("rot{f}-{i}");
            vu.op(&name);
            vu.latency(&name, Duration::from_millis(2));
        }
        // window stamps 1, 2, 3 ...: all in the first second
        evil.send_snapshot(&run, 1000, rec.snapshot(1 + f as i64));
    }
    assert!(
        wait_until(|| evil.stopped(), 5).await,
        "evil agent was not told to stop"
    );
    assert!(!good.stopped(), "the run itself must carry on");
    good.send_metrics(&run, 1000, 4, 0);
    good.send_metrics(&run, 2000, 4, 0);
    good.send_done(&run);
    let end = tokio::time::Instant::now() + Duration::from_secs(10);
    let r = loop {
        if let Ok(Some(r)) = h.state.store.report(&run).await {
            if r.meta.status != "running" {
                break r;
            }
        }
        assert!(tokio::time::Instant::now() < end, "run still running");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(r.meta.status, "completed", "{}", r.meta.verdict);
    assert!(
        r.meta.verdict.contains("dropped agents: evil"),
        "{}",
        r.meta.verdict
    );
    assert!(
        r.meta.verdict.contains("too many metric names"),
        "{}",
        r.meta.verdict
    );
    assert!(r.meta.missing_agents.contains(&"evil".to_string()));
    let hists: usize = r.stages.iter().map(|s| s.latency.len()).max().unwrap_or(0);
    assert!(hists <= MAX_RUN_HISTS, "{hists}");
}
