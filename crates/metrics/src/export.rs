use crate::{RunReport, StageSummary};
use std::collections::BTreeSet;
use std::fmt::Write;

pub fn to_json(r: &RunReport) -> String {
    serde_json::to_string_pretty(r).unwrap_or_else(|_| "{}".into())
}

pub fn metric_names(r: &RunReport) -> Vec<String> {
    let set: BTreeSet<String> = r
        .stages
        .iter()
        .flat_map(|s| s.latency.keys().cloned())
        .collect();
    set.into_iter().collect()
}

fn gauge_names(r: &RunReport) -> Vec<String> {
    let set: BTreeSet<String> = r
        .series
        .iter()
        .flat_map(|p| p.gauges.keys().cloned())
        .collect();
    set.into_iter().collect()
}

pub fn to_csv(r: &RunReport) -> String {
    let metrics: BTreeSet<String> = r
        .series
        .iter()
        .flat_map(|p| p.latency.keys().cloned())
        .collect();
    let gauges = gauge_names(r);
    let mut out = String::from("t_s,agents,ops,errors,cpu_max,lag_p95_max");
    for g in &gauges {
        let _ = write!(out, ",{}", csv(g));
    }
    for m in &metrics {
        let _ = write!(
            out,
            ",{},{},{}",
            csv(&format!("{m}.p50")),
            csv(&format!("{m}.p95")),
            csv(&format!("{m}.p99"))
        );
    }
    out.push('\n');
    for p in &r.series {
        let _ = write!(
            out,
            "{},{},{},{},{:.1},{:.1}",
            p.t_s, p.agents, p.ops, p.errors, p.cpu_max, p.lag_p95_max
        );
        for g in &gauges {
            let _ = write!(out, ",{}", p.gauges.get(g).copied().unwrap_or(0));
        }
        for m in &metrics {
            match p.latency.get(m) {
                Some(l) => {
                    let _ = write!(out, ",{:.1},{:.1},{:.1}", l.p50_ms, l.p95_ms, l.p99_ms);
                }
                None => out.push_str(",,,"),
            }
        }
        out.push('\n');
    }
    out
}

/// Open model per the explicit `meta.model`; for unknown (empty) fall back to `open.*` counters.
fn is_open_model(r: &RunReport) -> bool {
    match r.meta.model.as_str() {
        "open" => true,
        "closed" => false,
        _ => r
            .stages
            .iter()
            .any(|s| s.counters.keys().any(|k| k.starts_with("open."))),
    }
}

/// Markdown table cell / inline text: no pipes, no line breaks.
fn md(s: &str) -> String {
    s.replace('|', "\\|").replace(['\r', '\n'], " ")
}

/// RFC 4180 field quoting.
fn csv(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn triple(s: &StageSummary, m: &str) -> String {
    s.latency
        .get(m)
        .map(|l| format!("{:.0}/{:.0}/{:.0}", l.p50_ms, l.p95_ms, l.p99_ms))
        .unwrap_or_else(|| "–".into())
}

fn primary_gauge(r: &RunReport) -> Option<String> {
    ["vus", "connections"]
        .iter()
        .map(|s| s.to_string())
        .find(|g| r.stages.iter().any(|s| s.gauges_avg.contains_key(g)))
}

pub fn to_markdown(r: &RunReport, excluded: &[usize]) -> String {
    let m = &r.meta;
    let metrics = metric_names(r);
    let gauge = primary_gauge(r);
    let mut out = String::new();
    let _ = writeln!(out, "# {}\n", md(&m.name));
    let _ = writeln!(
        out,
        "- run `{}` · status {} · loadfleet {}",
        m.id, m.status, m.version
    );
    for a in &m.agents {
        let _ = writeln!(
            out,
            "- agent `{}` {} {}/{} cores, ip {}, clock offset {:.1} ms, share {:?}",
            md(&a.name),
            a.os,
            a.arch,
            a.cores,
            a.ip,
            a.offset_ms,
            a.stage_targets
        );
    }
    if !m.missing_agents.is_empty() {
        let _ = writeln!(
            out,
            "- **missing agents:** {}",
            md(&m.missing_agents.join(", "))
        );
    }
    let _ = writeln!(out, "- **verdict:** {}\n", md(&m.verdict));
    let mut head = String::from("| stage | target | ops/s | err rate |");
    let mut sep = String::from("|---|---|---|---|");
    for name in &metrics {
        let _ = write!(head, " {} p50/p95/p99 |", md(name));
        sep.push_str("---|");
    }
    if let Some(g) = &gauge {
        let _ = write!(head, " {} avg |", md(g));
        sep.push_str("---|");
    }
    head.push_str(" client |");
    sep.push_str("---|");
    let _ = writeln!(out, "{head}\n{sep}");
    for s in &r.stages {
        let mut row = format!(
            "| {}{} | {} | {:.1} | {:.2}% |",
            s.index,
            if excluded.contains(&s.index) {
                " (warm-up)"
            } else {
                ""
            },
            s.target,
            s.ops_per_s,
            s.error_rate * 100.0
        );
        for name in &metrics {
            let _ = write!(row, " {} |", triple(s, name));
        }
        if let Some(g) = &gauge {
            let _ = write!(
                row,
                " {} |",
                s.gauges_avg
                    .get(g)
                    .map(|v| format!("{v:.1}"))
                    .unwrap_or_else(|| "–".into())
            );
        }
        let cell = match (s.client_limited, s.rate_not_achieved) {
            (true, true) => " LIMITED, rate not achieved |",
            (true, false) => " LIMITED |",
            (false, true) => " rate not achieved |",
            (false, false) => " ok |",
        };
        row.push_str(cell);
        let _ = writeln!(out, "{row}");
    }
    let _ = writeln!(out, "\nLatencies in ms. \"LIMITED\": a load agent saturated (CPU > 85 %, runtime lag p95 > 50 ms, ephemeral ports ≥ 80 % or client socket errors) — that stage measures the client, not only the target.\n");
    if !is_open_model(r) {
        let _ = writeln!(out, "**Coordinated omission:** this run used the closed model (a fixed number of concurrent VUs, each waiting for its previous response). When the server slows down the clients send less, so the slowest moments are under-sampled and the latency percentiles above can understate what an independent arrival stream would see. Use the open model for latency-under-load claims.\n");
    }
    for s in &r.stages {
        let _ = writeln!(out, "## stage {} (target {})", s.index, s.target);
        let _ = writeln!(out, "- ops: {:?}", s.ops);
        if !s.counters.is_empty() {
            let _ = writeln!(out, "- counters: {:?}", s.counters);
        }
        let _ = writeln!(
            out,
            "- errors: {}",
            if s.errors.is_empty() {
                "none".to_string()
            } else {
                format!("{:?}", s.errors)
            }
        );
        if !s.stop_reasons.is_empty() {
            let _ = writeln!(out, "- **stop:** {}", md(&s.stop_reasons.join("; ")));
        }
        if s.rate_not_achieved {
            let _ = writeln!(out, "- **rate not achieved:** {} planned ops were dropped (`open.dropped`) because all VU slots were busy; they count as errors in the err rate", s.dropped);
        }
        for n in &s.client_notes {
            let _ = writeln!(out, "- **client:** {}", md(n));
        }
        out.push('\n');
    }
    out
}

fn delta(a: f64, b: f64) -> String {
    if a == 0.0 {
        return format!("{a:.1} → {b:.1}");
    }
    format!("{a:.1} → {b:.1} ({:+.1}%)", (b - a) / a * 100.0)
}

pub fn compare_markdown(a: &RunReport, b: &RunReport) -> String {
    let mut metrics = metric_names(a);
    for m in metric_names(b) {
        if !metrics.contains(&m) {
            metrics.push(m);
        }
    }
    let mut out = format!(
        "# {} vs {}\n\n| stage | target | ops/s | err rate |",
        a.meta.id, b.meta.id
    );
    for m in &metrics {
        let _ = write!(out, " {} p95 |", md(m));
    }
    out.push_str("\n|---|---|---|---|");
    for _ in &metrics {
        out.push_str("---|");
    }
    out.push('\n');
    for (sa, sb) in a.stages.iter().zip(b.stages.iter()) {
        let target = if sa.target == sb.target {
            sa.target.to_string()
        } else {
            format!("{}/{}", sa.target, sb.target)
        };
        let _ = write!(
            out,
            "| {} | {} | {} | {:.2}% → {:.2}%{} |",
            sa.index,
            target,
            delta(sa.ops_per_s, sb.ops_per_s),
            sa.error_rate * 100.0,
            sb.error_rate * 100.0,
            match (sa.rate_not_achieved, sb.rate_not_achieved) {
                (false, false) => "",
                (true, false) => " (a: rate not achieved)",
                (false, true) => " (b: rate not achieved)",
                (true, true) => " (a, b: rate not achieved)",
            }
        );
        for m in &metrics {
            match (sa.latency.get(m), sb.latency.get(m)) {
                (Some(x), Some(y)) => {
                    let pct = if x.p95_ms > 0.0 {
                        format!(" ({:+.1}%)", (y.p95_ms - x.p95_ms) / x.p95_ms * 100.0)
                    } else {
                        String::new()
                    };
                    let _ = write!(out, " {:.0} → {:.0}{pct} |", x.p95_ms, y.p95_ms);
                }
                _ => out.push_str(" – |"),
            }
        }
        out.push('\n');
    }
    if a.stages.len() != b.stages.len() {
        let _ = writeln!(
            out,
            "\nStage counts differ ({} vs {}); only the common prefix is compared.",
            a.stages.len(),
            b.stages.len()
        );
    }
    out
}
