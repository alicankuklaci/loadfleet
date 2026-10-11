//! A run accepts a bounded number of distinct metric names: an agent rotating fresh names
//! (each new histogram name costs a full HDR histogram per stage) is refused, not merged.
use loadfleet_health::HealthSample;
use loadfleet_metrics::*;
use std::time::Duration;

fn bounds() -> Vec<StageBounds> {
    vec![StageBounds {
        index: 0,
        target: 10,
        start_ms: 0,
        hold_from_ms: 0,
        end_ms: 3_600_000,
    }]
}

/// One snapshot carrying `n` fresh op names (and, with `hists`, a histogram per name).
fn fresh(prefix: &str, n: usize, hists: bool, at: i64) -> Snapshot {
    let rec = Recorder::new();
    let r = rec.vu(1);
    for i in 0..n {
        let name = format!("{prefix}{i}");
        r.op(&name);
        if hists {
            r.latency(&name, Duration::from_millis(3));
        }
    }
    rec.snapshot(at)
}

#[test]
fn an_agent_rotating_histogram_names_is_refused() {
    let mut agg = RunAggregate::new(bounds());
    let ok = HealthSample::default();
    let mut refused = None;
    for f in 0..100i64 {
        let at = (f + 1) * 1000;
        let s = fresh(&format!("f{f}-"), 8, true, at);
        if let Err(e) = agg.ingest("evil", at, &s, &ok) {
            refused = Some((f, e));
            break;
        }
    }
    let (f, e) = refused.expect("rotating names must hit a cap");
    assert!(matches!(e, MetricsError::TooManySeries(_)), "{e}");
    // 8 histograms per frame: the run cap of MAX_RUN_HISTS is reached first
    assert_eq!(f as usize, MAX_RUN_HISTS / 8, "{e}");
    assert!(agg.stage_summary(0).latency.len() <= MAX_RUN_HISTS);
}

#[test]
fn a_refused_snapshot_is_not_merged_at_all() {
    let mut agg = RunAggregate::new(bounds());
    let ok = HealthSample::default();
    agg.ingest("a", 1000, &fresh("x", 3, false, 1000), &ok)
        .unwrap();
    let big = fresh("y", MAX_AGENT_SERIES, false, 2000);
    assert!(agg.ingest("a", 2000, &big, &ok).is_err());
    let s = agg.stage_summary(0);
    assert_eq!(s.total_ops, 3);
    assert_eq!(s.ops.len(), 3);
}

#[test]
fn known_names_never_count_twice() {
    let mut agg = RunAggregate::new(bounds());
    let ok = HealthSample::default();
    for f in 0..40i64 {
        let at = (f + 1) * 1000;
        agg.ingest("a", at, &fresh("op", 20, true, at), &ok)
            .unwrap();
        agg.ingest("b", at, &fresh("op", 20, true, at), &ok)
            .unwrap();
    }
    assert_eq!(agg.stage_summary(0).total_ops, 40 * 40);
}

#[test]
fn per_agent_and_per_run_caps() {
    let ok = HealthSample::default();
    // one agent: MAX_AGENT_SERIES names are fine, one more is not
    let mut agg = RunAggregate::new(bounds());
    agg.ingest("a", 1000, &fresh("n", MAX_AGENT_SERIES, false, 1000), &ok)
        .unwrap();
    let e = agg
        .ingest("a", 2000, &fresh("m", 1, false, 2000), &ok)
        .unwrap_err();
    assert!(e.to_string().contains("per agent"), "{e}");
    // agents within their own cap still cannot take the run past MAX_RUN_SERIES
    let mut agg = RunAggregate::new(bounds());
    agg.ingest("a", 1000, &fresh("a", MAX_AGENT_SERIES, false, 1000), &ok)
        .unwrap();
    agg.ingest("b", 1000, &fresh("b", MAX_AGENT_SERIES, false, 1000), &ok)
        .unwrap();
    let e = agg
        .ingest("c", 1000, &fresh("c", 1, false, 1000), &ok)
        .unwrap_err();
    assert!(e.to_string().contains("per run"), "{e}");
    // the agents that stayed within the caps keep reporting
    agg.ingest("a", 2000, &fresh("a", 5, false, 2000), &ok)
        .unwrap();
}
