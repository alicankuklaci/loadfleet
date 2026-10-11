use anyhow::Result;
use clap::Args;
use loadfleet_agent::{run_agent, AgentConfig};
use loadfleet_engine::CancellationToken;
use loadfleet_plan::AllowList;

#[derive(Args)]
pub struct AgentArgs {
    /// Server base URL, e.g. wss://loadfleet.example.com (ws:// on a private network).
    #[arg(long, env = "LOADFLEET_SERVER")]
    server: String,
    /// Agent token. Prefer the LOADFLEET_TOKEN env var: a flag value is visible to every
    /// local user in `ps`.
    #[arg(long, env = "LOADFLEET_TOKEN", hide_env_values = true)]
    token: String,
    /// Agent name shown in the panel (default: the hostname).
    #[arg(long, env = "LOADFLEET_NAME")]
    name: Option<String>,
    #[arg(long, default_value_t = 100_000, env = "LOADFLEET_MAX_CONNS")]
    max_conns: u64,
    /// Optional local allow-list; the plan's list is narrowed to it.
    #[arg(long, value_delimiter = ',', env = "LOADFLEET_AGENT_ALLOW")]
    allow: Vec<String>,
    /// The IP the target will see, if the server sees another one: shown in the panel as
    /// "reported" next to the observed address (never put into the Cloudflare expression).
    #[arg(long, env = "LOADFLEET_PUBLIC_IP")]
    public_ip: Option<String>,
}

pub async fn run(a: AgentArgs) -> Result<i32> {
    let fds = loadfleet_health::raise_fd_limit();
    let name = a
        .name
        .unwrap_or_else(|| loadfleet_health::HostInfo::detect().hostname);
    eprintln!("loadfleet agent {name} -> {} (fd limit {fds})", a.server);
    let mut cfg = AgentConfig::new(a.server, a.token, name);
    cfg.max_conns = a.max_conns;
    cfg.public_ip = a.public_ip;
    if !a.allow.is_empty() {
        cfg.local_allow = Some(AllowList::parse(&a.allow).map_err(anyhow::Error::msg)?);
    }
    let stop = CancellationToken::new();
    let s = stop.clone();
    let shutdown = crate::signal::shutdown();
    tokio::spawn(async move {
        shutdown.await;
        s.cancel();
    });
    run_agent(cfg, stop).await?;
    Ok(0)
}
