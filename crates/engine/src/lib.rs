//! loadfleet engine: stages, closed (VU) and open (rate) models, scheduler, stop rules.
mod closed;
mod exec;
mod local;
mod open;
mod scenario;
mod schedule;
mod stop;
mod watch;

pub use exec::{execute, ExecError, ExecOutcome, Work};
pub use local::{run_local, short_id, LocalOpts, OnSecond};
pub use scenario::{OpCtx, Scenario, VuCtx};
pub use schedule::{Phase, Schedule};
pub use stop::evaluate_stop;
pub use tokio_util::sync::CancellationToken;
pub use watch::StageWatch;
