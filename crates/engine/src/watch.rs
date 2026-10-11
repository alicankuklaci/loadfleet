use crate::evaluate_stop;
use loadfleet_metrics::{RunAggregate, StageBounds, StageSummary};
use loadfleet_plan::StopRules;

/// Decides when stages are complete, summarizes them and applies stop rules (local runs and the server).
#[derive(Clone)]
pub struct StageWatch {
    ends: Vec<(i64, u64)>,
    done: usize,
    rules: StopRules,
    excluded: Vec<usize>,
}

impl StageWatch {
    pub fn new(bounds: &[StageBounds], rules: StopRules, excluded: Vec<usize>) -> StageWatch {
        StageWatch {
            ends: bounds.iter().map(|b| (b.end_ms, b.target)).collect(),
            done: 0,
            rules,
            excluded,
        }
    }

    pub fn all_done(&self) -> bool {
        self.done == self.ends.len()
    }

    /// Summaries for stages whose end has passed (`finished` flushes the rest). The second value is a
    /// "stopped at …" verdict when a non-excluded stage broke a stop rule. Stop rules read
    /// `StageSummary::error_rate`, which already counts open-model drops.
    pub fn poll(
        &mut self,
        agg: &RunAggregate,
        rel_ms: i64,
        finished: bool,
    ) -> (Vec<StageSummary>, Option<String>) {
        let mut out = Vec::new();
        while self.done < self.ends.len() && (finished || rel_ms >= self.ends[self.done].0) {
            let i = self.done;
            let mut s = agg.stage_summary(i);
            self.done += 1;
            if s.total_ops == 0 && finished && rel_ms < self.ends[i].0 {
                // a stage that never started (run ended early): leave it and the rest out
                self.done = self.ends.len();
                break;
            }
            if !self.excluded.contains(&i) {
                s.stop_reasons = evaluate_stop(&self.rules, &s);
            }
            let stop = (!s.stop_reasons.is_empty()).then(|| {
                format!(
                    "stopped at stage {i} (target {}): {}{}",
                    self.ends[i].1,
                    s.stop_reasons.join("; "),
                    if s.client_limited {
                        " (client was saturated)"
                    } else {
                        ""
                    }
                )
            });
            out.push(s);
            if stop.is_some() {
                self.done = self.ends.len();
                return (out, stop);
            }
        }
        (out, None)
    }

    pub fn verdict_completed(&self) -> String {
        format!(
            "completed all stages up to {}",
            self.ends.last().map(|e| e.1).unwrap_or(0)
        )
    }
}
