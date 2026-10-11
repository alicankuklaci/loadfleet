use loadfleet_metrics::*;
use std::time::Duration;

#[test]
fn hist_roundtrip_v2_deflate() {
    let mut h = new_hist();
    for i in 1..=10_000u64 {
        record(&mut h, Duration::from_micros(i * 100));
    }
    let enc = encode_hist(&h);
    assert!(enc.len() < 20_000, "compressed size {}", enc.len());
    let back = decode_hist(&enc).unwrap();
    assert_eq!(back.len(), h.len());
    assert_eq!(back.value_at_quantile(0.99), h.value_at_quantile(0.99));
}

#[test]
fn merged_percentiles_equal_single_histogram() {
    // two agents with different distributions: merge must equal recording everything in one histogram
    let (mut a, mut b, mut all) = (new_hist(), new_hist(), new_hist());
    for i in 1..=1000u64 {
        record(&mut a, Duration::from_millis(i));
        record(&mut all, Duration::from_millis(i));
    }
    for i in 1..=9000u64 {
        record(&mut b, Duration::from_micros(i / 10 + 1));
        record(&mut all, Duration::from_micros(i / 10 + 1));
    }
    let mut merged = decode_hist(&encode_hist(&a)).unwrap();
    merged.add(decode_hist(&encode_hist(&b)).unwrap()).unwrap();
    let (m, s) = (
        LatSummary::of(&merged).unwrap(),
        LatSummary::of(&all).unwrap(),
    );
    assert_eq!(m, s);
    assert!(m.p50_ms < 1.0 && m.p99_ms > 800.0);
}

#[test]
fn recorder_snapshot_drains_and_keeps_gauges() {
    let rec = Recorder::new();
    let vus = rec.gauge("vus");
    vus.add(3);
    let handles: Vec<_> = (0..8u64)
        .map(|i| {
            let r = rec.vu(i);
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    r.op("http");
                    r.latency("http", Duration::from_millis(2));
                }
                r.error("http:status:503");
                r.count("late_move", 2);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let s = rec.snapshot(1000);
    assert_eq!(s.total_ops(), 8000);
    assert_eq!(s.ops["http"], 8000);
    assert_eq!(s.errors["http:status:503"], 8);
    assert_eq!(s.total_errors(), 8);
    assert_eq!(s.counters["late_move"], 16);
    assert_eq!(decode_hist(&s.hists["http"]).unwrap().len(), 8000);
    assert_eq!(s.gauges["vus"], 3);
    let s2 = rec.snapshot(2000);
    assert_eq!(s2.total_ops(), 0);
    assert!(s2.hists.is_empty());
    assert_eq!(s2.gauges["vus"], 3);
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(serde_json::from_str::<Snapshot>(&json).unwrap(), s);
}
