mod agent_cmd;
mod run;
mod server_cmd;
mod signal;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "loadfleet",
    about = "Distributed load testing: server, agents and a panel"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print the version.
    Version,
    /// Run a plan on this machine only (no server).
    Run {
        plan: PathBuf,
        /// Report directory (default: reports/<name>-<timestamp>).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Extra local allow-list entries; the plan's list is narrowed to these.
        #[arg(long)]
        allow: Vec<String>,
        /// No per-second progress lines.
        #[arg(long)]
        quiet: bool,
    },
    /// Run the control server (panel, API, agent hub).
    Server(server_cmd::ServerArgs),
    /// Run a load agent that dials the server.
    Agent(agent_cmd::AgentArgs),
}

fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let code = match Cli::parse().cmd {
        Cmd::Version => {
            println!("loadfleet {}", env!("CARGO_PKG_VERSION"));
            0
        }
        Cmd::Run {
            plan,
            out,
            allow,
            quiet,
        } => runtime()
            .block_on(run::run(run::RunArgs {
                plan,
                out,
                allow,
                quiet,
            }))
            .unwrap_or_else(report_error),
        Cmd::Server(a) => runtime()
            .block_on(server_cmd::run(a))
            .unwrap_or_else(report_error),
        Cmd::Agent(a) => runtime()
            .block_on(agent_cmd::run(a))
            .unwrap_or_else(report_error),
    };
    std::process::exit(code);
}

fn report_error(e: anyhow::Error) -> i32 {
    eprintln!("error: {e:#}");
    1
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}
