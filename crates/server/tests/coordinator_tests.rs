mod support;

use loadfleet_plan::{WeightKeyword, Weights};
use loadfleet_server::*;
use std::collections::BTreeMap;
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

async fn server(max_conns: u64) -> ServerHandle {
    let dir = tempfile::tempdir().unwrap().keep();
    serve(ServerConfig {
        db: dir.join("t.db"),
        max_conns_per_agent: max_conns,
        ..ServerConfig::for_tests()
    })
    .await
    .unwrap()
}

async fn ready(h: &ServerHandle, n: usize) {
    let hub = h.state.hub.clone();
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
}

#[test]
fn share_split_and_weights() {
    assert_eq!(split_share(10, &[1.0, 1.0, 1.0]), vec![4, 3, 3]);
    assert_eq!(split_share(5000, &[2.0, 1.0]), vec![3334, 1666]);
    assert_eq!(split_share(0, &[1.0, 3.0]), vec![0, 0]);
    assert_eq!(split_share(1, &[1.0, 1.0, 1.0]), vec![1, 0, 0]);
    assert_eq!(auto_weight(8, None), 24_000.0);
    assert_eq!(auto_weight(8, Some(20_000)), 18_000.0);
    let agents = vec![
        ("m5".to_string(), 12, None),
        ("s1".to_string(), 8, Some(10_000)),
    ];
    assert_eq!(
        weights_for(&Weights::Keyword(WeightKeyword::Equal), &agents).unwrap(),
        vec![1.0, 1.0]
    );
    assert_eq!(
        weights_for(&Weights::Keyword(WeightKeyword::Auto), &agents).unwrap(),
        vec![36_000.0, 9_000.0]
    );
    let mut m = BTreeMap::new();
    m.insert("m5".to_string(), 2.0);
    assert!(weights_for(&Weights::Manual(m), &agents)
        .unwrap_err()
        .contains("s1"));
}

#[tokio::test]
async fn invalid_and_disallowed_plans_are_refused() {
    let h = server(1000).await;
    let bad = plan(10, "1s", "")
        .replace("127.0.0.1:9", "10.1.1.1:9")
        .replace("[\"127.0.0.1\"]", "[\"10.1.1.1\"]");
    match h.state.coord.start_run(&bad).await {
        Err(StartError::Invalid(e)) => assert!(
            e.iter().any(|m| m.contains("not allowed by the server")),
            "{e:?}"
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn skewed_agent_not_ready() {
    let h = server(1000).await;
    let _a = FakeAgent::connect(h.addr, &h.token, "skewed", 2_000).await;
    let hub = h.state.hub.clone();
    assert!(
        wait_until(
            || hub.agents().first().and_then(|a| a.readiness)
                == Some(loadfleet_protocol::Readiness::NotReady),
            5
        )
        .await
    );
    match h.state.coord.start_run(&plan(10, "1s", "")).await {
        Err(StartError::NotEnoughAgents {
            need: 1,
            ready: 0,
            not_ready,
        }) => assert_eq!(not_ready, vec!["skewed".to_string()]),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn share_over_conn_limit_rejected() {
    let h = server(100).await;
    let _a = FakeAgent::connect(h.addr, &h.token, "a", 0).await;
    ready(&h, 1).await;
    match h.state.coord.start_run(&plan(500, "1s", "")).await {
        Err(StartError::ConnLimit {
            agent,
            share: 500,
            limit: 100,
        }) => assert_eq!(agent, "a"),
        other => panic!("{other:?}"),
    }
    // a refused start leaves nothing reserved
    assert_eq!(h.state.coord.active(), None);
}

#[tokio::test]
async fn late_agent_gets_no_assignment() {
    let h = server(1000).await;
    let a = FakeAgent::connect(h.addr, &h.token, "a", 0).await;
    ready(&h, 1).await;
    let run = h.state.coord.start_run(&plan(10, "30s", "")).await.unwrap();
    assert!(wait_until(|| a.assignment().is_some(), 3).await);
    let asg = a.assignment().unwrap();
    assert_eq!(
        (
            asg.run_id.as_str(),
            asg.stage_targets.as_slice(),
            asg.vu_offset
        ),
        (run.as_str(), &[10u64][..], 0)
    );
    let b = FakeAgent::connect(h.addr, &h.token, "b", 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert!(b.assignment().is_none());
    assert!(h.state.coord.stop_run(&run, "test over"));
    assert!(wait_until(|| a.stopped(), 3).await);
}

#[tokio::test]
async fn stop_rule_on_merged_agent_metrics() {
    let h = server(1000).await;
    let a = FakeAgent::connect(h.addr, &h.token, "a", 0).await;
    let b = FakeAgent::connect(h.addr, &h.token, "b", 0).await;
    ready(&h, 2).await;
    let run = h
        .state
        .coord
        .start_run(&plan(20, "1s", "stop: { error_rate: 0.2 }"))
        .await
        .unwrap();
    assert!(wait_until(|| a.assignment().is_some() && b.assignment().is_some(), 3).await);
    assert_eq!(
        a.assignment().unwrap().stage_targets[0] + b.assignment().unwrap().stage_targets[0],
        20
    );
    // a is healthy, b fails half its ops: merged error rate 25% > 20%
    a.send_metrics(&run, 1000, 100, 0);
    b.send_metrics(&run, 1000, 100, 50);
    assert!(
        wait_until(|| a.stopped() && b.stopped(), 8).await,
        "both agents told to stop"
    );
    let mut verdict = String::new();
    for _ in 0..50 {
        if let Ok(Some(r)) = h.state.store.report(&run).await {
            verdict = r.meta.verdict;
            if verdict.starts_with("stopped") {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        verdict.starts_with("stopped at stage 0 (target 20): error rate 25.00% > 20.00%"),
        "{verdict}"
    );
}
