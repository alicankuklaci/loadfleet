//! Receive-side checks for agent-supplied metrics frames.
use loadfleet_metrics::{Snapshot, MAX_HIST_B64_BYTES};

/// Most histogram series one snapshot may carry.
/// Most entries any one snapshot map may carry, and the longest series name.
pub const MAX_SERIES_PER_MAP: usize = 512;
pub const MAX_NAME_BYTES: usize = 128;

pub const MAX_HISTS_PER_SNAPSHOT: usize = 256;

/// Reject snapshots whose encoded histograms exceed the size caps (before any decoding).
pub fn check_snapshot(snap: &Snapshot) -> Result<(), String> {
    let maps: [(&str, usize, Box<dyn Iterator<Item = &String> + '_>); 5] = [
        ("ops", snap.ops.len(), Box::new(snap.ops.keys())),
        (
            "counters",
            snap.counters.len(),
            Box::new(snap.counters.keys()),
        ),
        ("errors", snap.errors.len(), Box::new(snap.errors.keys())),
        ("gauges", snap.gauges.len(), Box::new(snap.gauges.keys())),
        ("hists", snap.hists.len(), Box::new(snap.hists.keys())),
    ];
    for (what, n, keys) in maps {
        if n > MAX_SERIES_PER_MAP {
            return Err(format!("too many {what}: {n}"));
        }
        if keys.into_iter().any(|k| k.len() > MAX_NAME_BYTES) {
            return Err(format!("{what} name too long"));
        }
    }
    if snap.hists.len() > MAX_HISTS_PER_SNAPSHOT {
        return Err(format!("too many histograms: {}", snap.hists.len()));
    }
    for (name, enc) in &snap.hists {
        if enc.len() > MAX_HIST_B64_BYTES {
            return Err(format!("histogram {name} too large: {} bytes", enc.len()));
        }
    }
    Ok(())
}

/// Per-agent, per-run replay guard: accepts a metrics frame only when both `seq` and
/// `snap.at_ms` are strictly greater than the last accepted one. What passes is therefore
/// unique per (agent, at_ms), which the aggregator's duplicate guard relies on.
#[derive(Debug, Default, Clone)]
pub struct SeqGuard {
    last: Option<(u64, i64)>,
}

impl SeqGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// True (and recorded) if the frame is new; false if it is a replay or out of order.
    pub fn accept(&mut self, seq: u64, at_ms: i64) -> bool {
        match self.last {
            Some((s, t)) if seq <= s || at_ms <= t => false,
            _ => {
                self.last = Some((seq, at_ms));
                true
            }
        }
    }
}
