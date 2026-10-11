use loadfleet_metrics::*;
use std::collections::BTreeMap;

fn lat(p95: f64) -> LatSummary {
    LatSummary {
        n: 100,
        p50_ms: p95 / 2.0,
        p95_ms: p95,
        p99_ms: p95 * 1.5,
        max_ms: p95 * 2.0,
    }
}

fn report(ops_per_s: f64, p95: f64, limited: bool) -> RunReport {
    let mut s = StageSummary {
        index: 0,
        target: 100,
        ops_per_s,
        total_ops: 1000,
        error_rate: 0.001,
        client_limited: limited,
        ..Default::default()
    };
    s.latency.insert("http".into(), lat(p95));
    s.gauges_avg.insert("vus".into(), 99.5);
    if limited {
        s.client_notes.push("m5: cpu 91% > 85%".into());
    }
    let mut g = BTreeMap::new();
    g.insert("vus".to_string(), 100i64);
    let mut l = BTreeMap::new();
    l.insert("http".to_string(), lat(p95));
    let series = (1..=3)
        .map(|t| SeriesPoint {
            t_s: t,
            ops: 100,
            errors: 0,
            gauges: g.clone(),
            latency: l.clone(),
            cpu_max: 10.0,
            lag_p95_max: 1.0,
            agents: 2,
        })
        .collect();
    RunReport {
        meta: RunMeta {
            id: "abc123".into(),
            name: "demo".into(),
            status: "completed".into(),
            verdict: "completed all stages up to 100".into(),
            ..Default::default()
        },
        stages: vec![s],
        series,
    }
}

#[test]
fn csv_has_header_and_one_row_per_second() {
    let csv = to_csv(&report(100.0, 10.0, false));
    let lines: Vec<_> = csv.lines().collect();
    assert_eq!(
        lines[0],
        "t_s,agents,ops,errors,cpu_max,lag_p95_max,vus,http.p50,http.p95,http.p99"
    );
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[1], "1,2,100,0,10.0,1.0,100,5.0,10.0,15.0");
}

#[test]
fn markdown_table_and_client_note() {
    let md = to_markdown(&report(100.0, 10.0, true), &[]);
    assert!(md.contains("# demo"));
    assert!(md.contains("**verdict:** completed all stages up to 100"));
    assert!(
        md.contains("| stage | target | ops/s | err rate | http p50/p95/p99 | vus avg | client |")
    );
    assert!(md.contains("| 0 | 100 | 100.0 | 0.10% | 5/10/15 | 99.5 | LIMITED |"));
    assert!(md.contains("m5: cpu 91% > 85%"));
    let back: RunReport = serde_json::from_str(&to_json(&report(1.0, 1.0, false))).unwrap();
    assert_eq!(back.meta.id, "abc123");
}

#[test]
fn compare_shows_deltas() {
    let md = compare_markdown(&report(100.0, 10.0, false), &report(110.0, 12.0, false));
    assert!(md.contains("| 0 | 100 | 100.0 → 110.0 (+10.0%) |"), "{md}");
    assert!(md.contains("10 → 12 (+20.0%)"), "{md}");
}

#[test]
fn closed_model_gets_coordinated_omission_note_open_does_not() {
    let mut r = report(100.0, 10.0, false);
    r.meta.model = "closed".into();
    assert!(to_markdown(&r, &[]).contains("Coordinated omission"));
    r.meta.model = String::new(); // unknown, no open.* counters
    assert!(to_markdown(&r, &[]).contains("Coordinated omission"));
    r.meta.model = "open".into();
    assert!(!to_markdown(&r, &[]).contains("Coordinated omission"));
    r.meta.model = "closed".into(); // explicit field wins over counters
    r.stages[0].counters.insert("open.late".into(), 3);
    assert!(to_markdown(&r, &[]).contains("Coordinated omission"));
    r.meta.model = String::new();
    assert!(!to_markdown(&r, &[]).contains("Coordinated omission"));
}

#[test]
fn dropped_flag_is_read_from_the_stage_summary() {
    let mut r = report(100.0, 10.0, false);
    r.stages[0].dropped = 1000;
    r.stages[0].rate_not_achieved = true;
    r.stages[0].error_rate = 0.5; // computed once by stage_summary
    let md = to_markdown(&r, &[]);
    assert!(md.contains("| 50.00% |"), "{md}");
    assert!(md.contains("rate not achieved"), "{md}");
    let cmp = compare_markdown(&report(100.0, 10.0, false), &r);
    assert!(
        cmp.contains("(b: rate not achieved)") && cmp.contains("0.10% → 50.00%"),
        "{cmp}"
    );
    let back: RunReport = serde_json::from_str(&to_json(&r)).unwrap();
    assert!(back.stages[0].rate_not_achieved && back.stages[0].dropped == 1000);
}

#[test]
fn csv_and_markdown_escape_names() {
    let mut r = report(100.0, 10.0, false);
    r.meta.name = "a\nb".into();
    let lat = r.stages[0].latency.remove("http").unwrap();
    r.stages[0].latency.insert("x|y".into(), lat);
    for p in &mut r.series {
        let l = p.latency.remove("http").unwrap();
        p.latency.insert("m,\"q\"".into(), l);
    }
    let csv = to_csv(&r);
    assert!(
        csv.lines().next().unwrap().contains("\"m,\"\"q\"\".p50\""),
        "{csv}"
    );
    let md = to_markdown(&r, &[]);
    assert!(
        md.contains("# a b") && md.contains("x\\|y p50/p95/p99"),
        "{md}"
    );
}

#[test]
fn empty_report_exports_do_not_panic() {
    let r = RunReport::default();
    assert_eq!(to_csv(&r).lines().count(), 1);
    assert!(to_markdown(&r, &[]).contains("# "));
    assert!(compare_markdown(&r, &r).contains("vs"));
}
