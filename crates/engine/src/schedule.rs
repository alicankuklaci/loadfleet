use loadfleet_metrics::StageBounds;
use loadfleet_plan::Stage;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
struct Timing {
    target: u64,
    ramp: Duration,
    hold: Duration,
    start: Duration,
    /// `start + ramp`, saturating.
    hold_from: Duration,
    /// `start + ramp + hold`, saturating.
    end: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Ramp,
    Hold,
}

/// The stage timeline, identical on every agent; only targets differ per agent share.
#[derive(Debug, Clone, PartialEq)]
pub struct Schedule {
    stages: Vec<Timing>,
}

impl Schedule {
    pub fn new(stages: &[Stage]) -> Schedule {
        let mut start = Duration::ZERO;
        let stages = stages
            .iter()
            .map(|s| {
                let hold_from = start.saturating_add(s.ramp);
                let end = hold_from.saturating_add(s.hold);
                let t = Timing {
                    target: s.target,
                    ramp: s.ramp,
                    hold: s.hold,
                    start,
                    hold_from,
                    end,
                };
                start = end;
                t
            })
            .collect();
        Schedule { stages }
    }

    /// Same timeline with per-agent targets. Stages the share list does not cover
    /// get target 0 (never the full-plan target); lengths must match.
    pub fn with_targets(&self, targets: &[u64]) -> Schedule {
        debug_assert_eq!(
            targets.len(),
            self.stages.len(),
            "share/stage length mismatch"
        );
        let stages = self
            .stages
            .iter()
            .enumerate()
            .map(|(i, s)| Timing {
                target: targets.get(i).copied().unwrap_or(0),
                ..s.clone()
            })
            .collect();
        Schedule { stages }
    }

    pub fn targets(&self) -> Vec<u64> {
        self.stages.iter().map(|s| s.target).collect()
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn total(&self) -> Duration {
        self.stages.last().map(|s| s.end).unwrap_or_default()
    }

    /// Desired VUs (closed) or ops/s (open) at `t` since the run start.
    pub fn target_at(&self, t: Duration) -> f64 {
        let mut prev = 0.0;
        for s in &self.stages {
            let target = s.target as f64;
            if t < s.hold_from {
                let frac = if s.ramp.is_zero() {
                    1.0
                } else {
                    (t - s.start).as_secs_f64() / s.ramp.as_secs_f64()
                };
                return prev + (target - prev) * frac;
            }
            if t < s.end {
                return target;
            }
            prev = target;
        }
        0.0
    }

    pub fn position(&self, t: Duration) -> Option<(usize, Phase)> {
        self.stages.iter().enumerate().find_map(|(i, s)| {
            if t >= s.start && t < s.hold_from {
                Some((i, Phase::Ramp))
            } else if t >= s.hold_from && t < s.end {
                Some((i, Phase::Hold))
            } else {
                None
            }
        })
    }

    pub fn bounds(&self) -> Vec<StageBounds> {
        let ms = |d: Duration| i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.stages
            .iter()
            .enumerate()
            .map(|(i, s)| StageBounds {
                index: i,
                target: s.target,
                start_ms: ms(s.start),
                hold_from_ms: ms(s.hold_from),
                end_ms: ms(s.end),
            })
            .collect()
    }
    /// Open model: number of ops the rate curve plans in total (`op_time(k)` is `Some` for exactly
    /// `k < planned_ops()`).
    pub(crate) fn planned_ops(&self) -> u64 {
        let mut area = 0.0;
        let mut prev = 0.0;
        for s in &self.stages {
            let target = s.target as f64;
            area += (prev + target) / 2.0 * (s.hold_from - s.start).as_secs_f64();
            area += target * (s.end - s.hold_from).as_secs_f64();
            prev = target;
        }
        area.ceil() as u64
    }

    /// Open model: offset of operation `k` (0-based), i.e. the time at which the integral of the
    /// rate curve reaches `k`. `None` once the schedule cannot fit operation `k`. Ramps are linear,
    /// so each ramp solves a quadratic; ops therefore follow the curve even when ramping from 0.
    pub(crate) fn op_time(&self, k: f64) -> Option<Duration> {
        let at = |base: Duration, secs: f64, max: Duration| {
            let off = Duration::try_from_secs_f64(secs.max(0.0))
                .unwrap_or(max)
                .min(max);
            base.checked_add(off)
        };
        let mut done = 0.0; // ops planned before the current segment
        let mut prev = 0.0;
        for s in &self.stages {
            let target = s.target as f64;
            let ramp = (s.hold_from - s.start).as_secs_f64();
            let area = (prev + target) / 2.0 * ramp;
            if k < done + area {
                // prev*u + (target-prev)/(2*ramp)*u^2 = x, solved in a cancellation-free form
                let x = k - done;
                let a = (target - prev) / (2.0 * ramp);
                let root = (prev * prev + 4.0 * a * x).max(0.0).sqrt();
                let u = if x <= 0.0 {
                    0.0
                } else {
                    2.0 * x / (prev + root)
                };
                return at(s.start, u, s.hold_from - s.start);
            }
            done += area;
            let area = target * (s.end - s.hold_from).as_secs_f64();
            if k < done + area {
                return at(s.hold_from, (k - done) / target, s.end - s.hold_from);
            }
            done += area;
            prev = target;
        }
        None
    }
}
