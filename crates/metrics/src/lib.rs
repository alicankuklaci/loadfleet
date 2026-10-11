//! loadfleet metrics: mergeable HDR histograms, counters, gauges, 1 s snapshots, aggregation and exports.
mod aggregate;
mod export;
mod hist;
mod recorder;
mod report;
mod snapshot;

pub use aggregate::*;
pub use export::*;
pub use hdrhistogram::Histogram;
pub use hist::{decode_hist, encode_hist, new_hist, record, LatSummary, MAX_HIST_B64_BYTES};
pub use recorder::{Gauge, Recorder, VuRecorder};
pub use report::*;
pub use snapshot::Snapshot;

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("histogram decode: {0}")]
    Decode(String),
    #[error("histogram merge: {0}")]
    Merge(String),
    /// The snapshot would take its run past a distinct-metric-name cap
    /// (`MAX_RUN_SERIES`, `MAX_AGENT_SERIES`, `MAX_RUN_HISTS`).
    #[error("too many metric names: {0}")]
    TooManySeries(String),
}
