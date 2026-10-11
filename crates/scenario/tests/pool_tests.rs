use loadfleet_engine::{CancellationToken, OpCtx, Scenario};
use loadfleet_metrics::Recorder;
use loadfleet_plan::parse;
use loadfleet_scenario::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn scenario(url: &str, connections: u64) -> Arc<dyn Scenario> {
    let yaml = format!(
        r#"
name: t
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: http, http: {{ url: "{url}", connections: {connections} }} }}
load: {{ model: open, max_vus: 20, stages: [{{target: 5, ramp: 0s, hold: 1s}}] }}
"#
    );
    let p = parse(&yaml).unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    build(
        &p.scenario,
        BuildCtx {
            guard,
            run_id: "r".into(),
            agent: "a".into(),
            max_vus: 20,
        },
    )
    .unwrap()
}

fn ctx(rec: &Recorder, iter: u64, cancel: &CancellationToken) -> OpCtx {
    OpCtx {
        vu: iter,
        vu_id: format!("x-{iter}"),
        iter,
        scheduled: Instant::now(),
        rec: rec.vu(iter),
        cancel: cancel.clone(),
    }
}

/// A slow request must not hold up ops that could use another idle pooled connection
/// (the old pool tied op k to slot k % size and queued behind whatever that slot was doing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_request_does_not_block_other_slots() {
    let (addr, _) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    // iter 600 -> /delay/600 (slow), iter 0 -> /delay/0
    let s = scenario(&format!("http://{addr}/delay/{{{{iter}}}}"), 2);
    let rec = Recorder::new();
    let cancel = CancellationToken::new();
    let slow = {
        let (s, c) = (s.clone(), ctx(&rec, 600, &cancel));
        tokio::spawn(async move { s.op(c).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t0 = std::time::Instant::now();
    for _ in 0..4 {
        s.op(ctx(&rec, 0, &cancel)).await;
    }
    let fast = t0.elapsed();
    slow.await.unwrap();
    let snap = rec.snapshot(0);
    assert!(snap.errors.is_empty(), "{:?}", snap.errors);
    assert!(fast < Duration::from_millis(300), "fast ops took {fast:?}");
    assert_eq!(snap.ops.get("http"), Some(&5));
}

/// Every slot busy, several ops waiting: they must get whichever connection frees first, not
/// queue behind the slow request that holds "their" shard (each op starts at shard iter % n).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiters_take_any_freed_slot() {
    let (addr, _) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    // iter 800 -> /delay/800 (slow); even iters 0..=10 -> at most 10 ms; all start at shard 0
    let s = scenario(&format!("http://{addr}/delay/{{{{iter}}}}"), 2);
    let rec = Recorder::new();
    let cancel = CancellationToken::new();
    let slow = {
        let (s, c) = (s.clone(), ctx(&rec, 800, &cancel));
        tokio::spawn(async move { s.op(c).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t0 = std::time::Instant::now();
    let fast: Vec<_> = (0..6)
        .map(|k| {
            let (s, c) = (s.clone(), ctx(&rec, 2 * k, &cancel));
            tokio::spawn(async move { s.op(c).await })
        })
        .collect();
    for f in fast {
        f.await.unwrap();
    }
    let took = t0.elapsed();
    slow.await.unwrap();
    assert!(rec.snapshot(0).errors.is_empty());
    assert!(
        took < Duration::from_millis(400),
        "waiting ops took {took:?}"
    );
}

/// Stop while every slot is busy: waiting ops give up at once and the slots stay usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_a_slot_observes_cancel() {
    let (addr, _) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let s = scenario(&format!("http://{addr}/delay/{{{{iter}}}}"), 1);
    let rec = Recorder::new();
    let cancel = CancellationToken::new();
    let busy = {
        let (s, c) = (s.clone(), ctx(&rec, 2000, &cancel));
        tokio::spawn(async move { s.op(c).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waiting = {
        let (s, c) = (s.clone(), ctx(&rec, 0, &cancel));
        tokio::spawn(async move { s.op(c).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t0 = std::time::Instant::now();
    cancel.cancel();
    waiting.await.unwrap();
    busy.await.unwrap();
    assert!(
        t0.elapsed() < Duration::from_millis(300),
        "{:?}",
        t0.elapsed()
    );
    // the slot came back: a fresh op still completes
    s.op(ctx(&rec, 0, &CancellationToken::new())).await;
    assert!(rec.snapshot(0).errors.is_empty());
}
