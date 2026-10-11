use loadfleet_health::HealthSample;
use loadfleet_metrics::*;
use std::time::Duration;

fn bounds() -> Vec<StageBounds> {
    vec![
        StageBounds {
            index: 0,
            target: 10,
            start_ms: 0,
            hold_from_ms: 2000,
            end_ms: 5000,
        },
        StageBounds {
            index: 1,
            target: 20,
            start_ms: 5000,
            hold_from_ms: 6000,
            end_ms: 10000,
        },
    ]
}

fn snap(rec: &Recorder, vu: u64, ms: u64, ops: u64, errs: u64, at: i64) -> Snapshot {
    let r = rec.vu(vu);
    for _ in 0..ops {
        r.op("http");
        r.latency("http", Duration::from_millis(ms));
    }
    for _ in 0..errs {
        r.error("http:status:500");
    }
    rec.snapshot(at)
}

#[test]
fn merges_agents_per_second_and_per_stage() {
    let mut agg = RunAggregate::new(bounds());
    let (ra, rb) = (Recorder::new(), Recorder::new());
    ra.gauge("vus").set(5);
    rb.gauge("vus").set(5);
    let ok = HealthSample::default();
    for sec in 1..=5i64 {
        agg.ingest("a", sec * 1000, &snap(&ra, 1, 10, 100, 0, sec * 1000), &ok)
            .unwrap();
        agg.ingest("b", sec * 1000, &snap(&rb, 2, 100, 100, 2, sec * 1000), &ok)
            .unwrap();
    }
    let pts = agg.close_until(10_000);
    assert_eq!(pts.len(), 5);
    assert_eq!(pts[0].ops, 200);
    assert_eq!(pts[0].gauges["vus"], 10);
    assert_eq!(pts[0].agents, 2);
    let s = agg.stage_summary(0);
    assert_eq!(s.total_ops, 1000);
    assert_eq!(s.errors["http:status:500"], 10);
    assert!((s.error_rate - 0.01).abs() < 1e-9);
    let l = s.latency["http"];
    assert_eq!(l.n, 1000);
    assert!(l.p50_ms <= 10.1 && l.p95_ms >= 99.0);
    assert_eq!(s.gauges_avg["vus"], 10.0); // hold seconds only (t=2..5)
    assert!(!s.client_limited);
    assert_eq!(agg.stage_summary(1).total_ops, 0);
}

#[test]
fn client_errors_mark_stage_client_limited() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    let hot = HealthSample {
        cpu_pct: 95.0,
        ..Default::default()
    };
    let errs = HealthSample {
        client_errors: 3,
        ..Default::default()
    };
    agg.ingest("a", 1000, &snap(&rec, 1, 5, 10, 0, 1000), &hot)
        .unwrap();
    agg.ingest("b", 1000, &snap(&rec, 1, 5, 10, 0, 1000), &errs)
        .unwrap();
    let s = agg.stage_summary(0);
    assert!(s.client_limited);
    assert!(s.agents["a"].saturated && s.agents["b"].saturated);
    assert!(s.client_notes.iter().any(|n| n.contains("a:")));
}

#[test]
fn snapshot_at_boundary_goes_to_the_window_it_covers() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    // the snapshot stamped exactly 5000 covers 4000..5000, i.e. stage 0
    agg.ingest(
        "a",
        5000,
        &snap(&rec, 1, 5, 7, 0, 5000),
        &HealthSample::default(),
    )
    .unwrap();
    assert_eq!(agg.stage_summary(0).total_ops, 7);
    assert_eq!(agg.stage_summary(1).total_ops, 0);
}

#[test]
fn final_flush_past_last_stage_is_clamped_into_it() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    let ok = HealthSample::default();
    agg.ingest("a", 10_000, &snap(&rec, 1, 5, 10, 0, 10_000), &ok)
        .unwrap();
    agg.ingest("a", 10_800, &snap(&rec, 1, 5, 3, 1, 10_800), &ok)
        .unwrap();
    let s = agg.stage_summary(1);
    assert_eq!(s.total_ops, 13);
    assert_eq!(s.errors["http:status:500"], 1);
    let pts = agg.close_until(11_000);
    assert_eq!(pts.len(), 1);
    assert_eq!(pts[0].t_s, 10);
    assert_eq!(pts[0].ops, 13);
}

#[test]
fn late_snapshot_counts_in_stage_but_not_in_closed_series() {
    let mut agg = RunAggregate::new(bounds());
    let (ra, rb) = (Recorder::new(), Recorder::new());
    ra.gauge("vus").set(5);
    rb.gauge("vus").set(5);
    let ok = HealthSample::default();
    for sec in 1..=4i64 {
        agg.ingest("a", sec * 1000, &snap(&ra, 1, 5, 10, 0, sec * 1000), &ok)
            .unwrap();
    }
    for sec in 1..=3i64 {
        agg.ingest("b", sec * 1000, &snap(&rb, 2, 5, 10, 0, sec * 1000), &ok)
            .unwrap();
    }
    assert_eq!(agg.close_until(4000).len(), 4);
    // b's second 4 arrives after the series closed it
    agg.ingest("b", 4000, &snap(&rb, 2, 5, 10, 0, 4000), &ok)
        .unwrap();
    assert!(agg.close_until(5000).is_empty());
    let s = agg.stage_summary(0);
    assert_eq!(s.total_ops, 80);
    // hold seconds 3 and 4 only: second 3 has both agents (10), second 4 only a (5)
    assert_eq!(s.gauges_min["vus"], 5);
    assert_eq!(s.gauges_max["vus"], 10);
    assert_eq!(s.gauges_avg["vus"], 7.5);
}

#[test]
fn duplicate_and_out_of_order_snapshots() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    let ok = HealthSample::default();
    let s2 = snap(&rec, 1, 5, 4, 0, 2000);
    let s1 = snap(&rec, 1, 5, 3, 0, 1000);
    agg.ingest("a", 2000, &s2, &ok).unwrap();
    agg.ingest("a", 2000, &s2, &ok).unwrap(); // exact duplicate dropped
    agg.ingest("a", 1000, &s1, &ok).unwrap(); // out of order but still open
    let pts = agg.close_until(2000);
    assert_eq!(
        pts.iter().map(|p| (p.t_s, p.ops)).collect::<Vec<_>>(),
        vec![(1, 3), (2, 4)]
    );
    assert_eq!(agg.stage_summary(0).total_ops, 7);
}

#[test]
fn corrupt_histogram_leaves_state_unchanged() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    let ok = HealthSample::default();
    let mut bad = snap(&rec, 1, 5, 9, 1, 1000);
    bad.hists.insert("http".into(), "not-a-histogram".into());
    assert!(agg.ingest("a", 1000, &bad, &ok).is_err());
    assert_eq!(agg.stage_summary(0).total_ops, 0);
    assert!(agg.stage_summary(0).agents.is_empty());
    assert!(agg.close_until(5000).is_empty());
    // the same stamp is not poisoned by the failed attempt
    let good = snap(&rec, 1, 5, 2, 0, 1000);
    agg.ingest("a", 1000, &good, &ok).unwrap();
    assert_eq!(agg.stage_summary(0).total_ops, 2);
}

#[test]
fn dropped_ops_count_into_error_rate() {
    let mut agg = RunAggregate::new(bounds());
    let rec = Recorder::new();
    let ok = HealthSample::default();
    let mut sn = snap(&rec, 1, 5, 10, 0, 1_000);
    sn.counters.insert("open.dropped".into(), 10);
    agg.ingest("a", 1_000, &sn, &ok).unwrap();
    let s = agg.stage_summary(0);
    assert_eq!(s.dropped, 10);
    assert!(s.rate_not_achieved);
    assert!((s.error_rate - 0.5).abs() < 1e-9, "{}", s.error_rate);
    let clean = {
        let mut agg = RunAggregate::new(bounds());
        agg.ingest("a", 1_000, &snap(&Recorder::new(), 1, 5, 10, 1, 1_000), &ok)
            .unwrap();
        agg.stage_summary(0)
    };
    assert!(!clean.rate_not_achieved && (clean.error_rate - 0.1).abs() < 1e-9);
}
