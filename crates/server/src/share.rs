//! How a stage target is split between agents.
use loadfleet_plan::{WeightKeyword, Weights};

/// Splits `target` by weight: floor of the proportional share, remainder +1 to the first agents.
/// All-zero (or non-finite) weights split equally rather than dropping the load.
pub fn split_share(target: u64, weights: &[f64]) -> Vec<u64> {
    if weights.is_empty() {
        return Vec::new();
    }
    let total: f64 = weights.iter().sum();
    let equal = !(total > 0.0 && total.is_finite());
    let mut out: Vec<u64> = weights
        .iter()
        .map(|w| {
            let frac = if equal {
                1.0 / weights.len() as f64
            } else {
                w / total
            };
            ((target as f64) * frac).floor() as u64
        })
        .collect();
    // float rounding must never hand out more than `target`
    while out.iter().sum::<u64>() > target {
        if let Some(v) = out.iter_mut().rev().find(|v| **v > 0) {
            *v -= 1;
        }
    }
    let mut rest = target - out.iter().sum::<u64>();
    while rest > 0 {
        for v in out.iter_mut() {
            if rest == 0 {
                break;
            }
            *v += 1;
            rest -= 1;
        }
    }
    out
}

/// Capacity estimate in VUs: 90 % of the last measured saturation point, else 3000 per core.
pub fn auto_weight(cores: u32, last_saturation_vus: Option<u64>) -> f64 {
    match last_saturation_vus {
        Some(s) => s as f64 * 0.9,
        None => cores.max(1) as f64 * 3000.0,
    }
}

/// agents: (name, cores, last saturation VUs)
pub fn weights_for(
    spec: &Weights,
    agents: &[(String, u32, Option<u64>)],
) -> Result<Vec<f64>, String> {
    match spec {
        Weights::Keyword(WeightKeyword::Equal) => Ok(vec![1.0; agents.len()]),
        Weights::Keyword(WeightKeyword::Auto) => {
            Ok(agents.iter().map(|(_, c, s)| auto_weight(*c, *s)).collect())
        }
        Weights::Manual(m) => agents
            .iter()
            .map(|(n, _, _)| {
                m.get(n)
                    .copied()
                    .filter(|w| w.is_finite() && *w >= 0.0)
                    .ok_or_else(|| format!("agents.weights: no weight for agent {n}"))
            })
            .collect(),
    }
}
