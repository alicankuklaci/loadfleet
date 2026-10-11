mod bench;
mod hostload;

use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Self-check of the load generator on this machine.
    Bench {
        #[command(subcommand)]
        which: bench::Which,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Bench { which } => bench::run(which),
    }
}
