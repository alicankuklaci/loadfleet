use loadfleet_engine::*;
use loadfleet_metrics::{LatSummary, StageSummary};
use loadfleet_plan::{Stage, StopRules};
use std::time::Duration;

fn st(target: u64, ramp: u64, hold: u64) -> Stage {
    Stage {
        target,
        ramp: Duration::from_secs(ramp),
        hold: Duration::from_secs(hold),
    }
}
fn s(x: f64) -> Duration {
    Duration::from_secs_f64(x)
}

#[test]
fn linear_ramps_then_holds() {
    let sc = Schedule::new(&[st(100, 10, 5), st(300, 10, 5), st(0, 5, 0)]);
    assert_eq!(sc.total(), s(35.0));
    assert_eq!(sc.target_at(s(0.0)), 0.0);
    assert_eq!(sc.target_at(s(5.0)), 50.0);
    assert_eq!(sc.target_at(s(12.0)), 100.0);
    assert_eq!(sc.target_at(s(20.0)), 200.0);
    assert_eq!(sc.target_at(s(27.0)), 300.0);
    assert_eq!(sc.target_at(s(32.5)), 150.0);
    assert_eq!(sc.target_at(s(36.0)), 0.0);
    assert_eq!(sc.position(s(12.0)), Some((0, Phase::Hold)));
    assert_eq!(sc.position(s(15.0)), Some((1, Phase::Ramp)));
    assert_eq!(sc.position(s(35.0)), None);
}

#[test]
fn zero_ramp_jumps() {
    let sc = Schedule::new(&[st(10, 0, 2)]);
    assert_eq!(sc.target_at(s(0.0)), 10.0);
}

#[test]
fn bounds_and_shares_keep_timing() {
    let sc = Schedule::new(&[st(100, 10, 5), st(300, 10, 5)]);
    let b = sc.bounds();
    assert_eq!(
        (b[1].start_ms, b[1].hold_from_ms, b[1].end_ms, b[1].target),
        (15_000, 25_000, 30_000, 300)
    );
    let share = sc.with_targets(&[34, 100]);
    assert_eq!(share.total(), sc.total());
    assert_eq!(share.targets(), vec![34, 100]);
    assert_eq!(share.target_at(s(20.0)), 67.0);
}

#[test]
fn stop_rules() {
    let mut sum = StageSummary {
        error_rate: 0.025,
        ..Default::default()
    };
    sum.latency.insert(
        "move.ack".into(),
        LatSummary {
            n: 10,
            p50_ms: 80.0,
            p95_ms: 612.0,
            p99_ms: 700.0,
            max_ms: 900.0,
        },
    );
    let mut rules = StopRules {
        error_rate: Some(0.02),
        ..Default::default()
    };
    rules.p95_ms.insert("move.ack".into(), 500.0);
    rules.p95_ms.insert("queue->found".into(), 12000.0); // absent metric: ignored
    let r = evaluate_stop(&rules, &sum);
    assert_eq!(
        r,
        vec![
            "error rate 2.50% > 2.00%".to_string(),
            "move.ack p95 612 ms > 500 ms".to_string()
        ]
    );
    assert!(evaluate_stop(&StopRules::default(), &sum).is_empty());
}

#[test]
fn huge_durations_saturate_instead_of_panicking() {
    let big = Stage {
        target: 5,
        ramp: Duration::MAX,
        hold: Duration::MAX,
    };
    let sc = Schedule::new(&[big.clone(), big]);
    assert_eq!(sc.total(), Duration::MAX);
    let b = sc.bounds();
    assert_eq!(b[1].end_ms, i64::MAX);
    assert!(b[1].start_ms <= b[1].hold_from_ms && b[1].hold_from_ms <= b[1].end_ms);
    let _ = sc.target_at(s(1.0));
    let _ = sc.position(Duration::MAX);
}

#[test]
#[cfg(not(debug_assertions))]
fn shares_shorter_than_stages_get_zero_for_uncovered() {
    let sc = Schedule::new(&[st(100, 1, 1), st(200, 1, 1)]);
    assert_eq!(sc.with_targets(&[7]).targets(), vec![7, 0]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "share/stage length mismatch")]
fn shares_length_mismatch_asserts_in_debug() {
    Schedule::new(&[st(100, 1, 1), st(200, 1, 1)]).with_targets(&[7]);
}

#[test]
fn exact_boundaries_and_ramp_down_shares() {
    let sc = Schedule::new(&[st(100, 10, 5), st(300, 10, 5), st(0, 5, 0)]);
    assert_eq!(sc.target_at(s(10.0)), 100.0);
    assert_eq!(sc.target_at(s(15.0)), 100.0);
    assert_eq!(sc.target_at(s(30.0)), 300.0);
    assert_eq!(sc.position(s(10.0)), Some((0, Phase::Hold)));
    assert_eq!(sc.with_targets(&[10, 30, 0]).target_at(s(32.5)), 15.0);
}

#[test]
fn stage_with_dropped_ops_trips_error_rate_rule() {
    // as computed by stage_summary: 0 errors, 100 ops, 100 dropped -> 50 %
    let sum = StageSummary {
        error_rate: 0.5,
        dropped: 100,
        rate_not_achieved: true,
        total_ops: 100,
        ..Default::default()
    };
    let rules = StopRules {
        error_rate: Some(0.02),
        ..Default::default()
    };
    assert_eq!(
        evaluate_stop(&rules, &sum),
        vec!["error rate 50.00% > 2.00%".to_string()]
    );
}
