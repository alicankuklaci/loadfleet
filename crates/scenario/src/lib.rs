//! loadfleet scenario: http, tcp and ws implementations of `engine::Scenario`, plus the plugin
//! registry other scenario types (compiled in by a binary, e.g. a private protocol) hook into.
mod guard;
mod http;
mod io;
mod net;
mod plugin;
mod pool;
mod prepare;
mod registry;
mod retry;
mod tcp;
mod template;
mod tls;
mod ws;

pub use guard::AllowGuard;
pub use plugin::{plugin_templates, register, ScenarioPlugin};
pub use prepare::{check_text, prepare_plan};
pub use registry::{build, check_plan, BuildCtx};
pub use template::{Template, Vars};
pub use tls::tls_config;

/// Guarded dialing for plugin scenarios: the same allow-list re-check after DNS, connection
/// tracking and rustls setup the built-in scenarios use.
pub mod transport {
    pub use crate::io::{dial, BoxIo, Io};
    pub use crate::net::connect_tcp;
    pub use crate::tls::{default_config, wrap};
}
