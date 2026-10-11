//! loadfleet agent: dials the server, receives a share of a plan, runs it, streams metrics,
//! stops itself when the server goes silent or the control connection is lost.
mod conn;
mod exec;
mod session;
mod watchdog;

pub use exec::run_assignment;
pub use session::{run_agent, AgentConfig};
pub use watchdog::Watchdog;
