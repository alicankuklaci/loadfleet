use loadfleet_metrics::StageSummary;
use loadfleet_plan::StopRules;

/// Stop reasons for a finished stage (empty = continue). Metrics absent from the stage are ignored.
pub fn evaluate_stop(rules: &StopRules, s: &StageSummary) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(limit) = rules.error_rate {
        if s.error_rate > limit {
            out.push(format!(
                "error rate {:.2}% > {:.2}%",
                s.error_rate * 100.0,
                limit * 100.0
            ));
        }
    }
    for (name, limit) in &rules.p95_ms {
        if let Some(l) = s.latency.get(name) {
            if l.p95_ms > *limit {
                out.push(format!("{name} p95 {:.0} ms > {:.0} ms", l.p95_ms, limit));
            }
        }
    }
    out
}
