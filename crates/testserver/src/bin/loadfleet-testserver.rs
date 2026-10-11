use clap::Parser;

#[derive(Parser)]
#[command(about = "Local echo targets for loadfleet tests and benches")]
struct Args {
    #[arg(long)]
    http: Vec<String>,
    #[arg(long)]
    tcp: Vec<String>,
    #[arg(long)]
    ws: Vec<String>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _ = rlimit::increase_nofile_limit(u64::MAX);
    let a = Args::parse();
    for addr in &a.http {
        println!("http {}", loadfleet_testserver::spawn_http(addr).await?.0);
    }
    for addr in &a.tcp {
        println!(
            "tcp {}",
            loadfleet_testserver::spawn_tcp_echo(addr).await?.0
        );
    }
    for addr in &a.ws {
        println!("ws {}", loadfleet_testserver::spawn_ws_echo(addr).await?.0);
    }
    tokio::signal::ctrl_c().await
}
