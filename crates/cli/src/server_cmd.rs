use anyhow::Result;
use clap::Args;
use loadfleet_server::{serve, ServerConfig};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Args)]
pub struct ServerArgs {
    #[arg(long, default_value = "0.0.0.0:8080", env = "LOADFLEET_LISTEN")]
    listen: std::net::SocketAddr,
    #[arg(long, default_value = "loadfleet.db", env = "LOADFLEET_DB")]
    db: PathBuf,
    /// Hosts, *.domains, IPs or CIDRs plans may target (repeat or comma-separate). Required.
    #[arg(long, env = "LOADFLEET_ALLOW", value_delimiter = ',', required = true)]
    allow: Vec<String>,
    /// Admin password. Prefer the LOADFLEET_ADMIN_PASSWORD env var: a flag value is visible
    /// to every local user in `ps`.
    #[arg(long, env = "LOADFLEET_ADMIN_PASSWORD", hide_env_values = true)]
    admin_password: Option<String>,
    /// Reverse proxies (IPs or CIDRs, repeat or comma-separate) whose X-Forwarded-For /
    /// X-Forwarded-Proto headers are believed, e.g. 127.0.0.1 for a local nginx or
    /// Cloudflare's ranges. Without it those headers are ignored.
    #[arg(long, env = "LOADFLEET_TRUST_PROXY", value_delimiter = ',', value_parser = loadfleet_server::parse_proxy)]
    trust_proxy: Vec<loadfleet_server::IpNet>,
    /// Believe CF-Connecting-IP from those proxies. Only when they are Cloudflare's ranges:
    /// any other proxy passes a client-sent CF-Connecting-IP through.
    #[arg(long, env = "LOADFLEET_TRUST_CF_HEADER")]
    trust_cf_header: bool,
    #[arg(long, env = "LOADFLEET_SECURE_COOKIES")]
    secure_cookies: bool,
    #[arg(long, default_value_t = 100_000)]
    max_conns_per_agent: u64,
    #[arg(long, default_value = "2h", value_parser = humantime::parse_duration)]
    max_duration: Duration,
}

pub async fn run(a: ServerArgs) -> Result<i32> {
    let cfg = ServerConfig {
        listen: a.listen,
        db: a.db,
        allow: a.allow,
        admin_password: a.admin_password,
        trusted_proxies: a.trust_proxy,
        trust_cf_header: a.trust_cf_header,
        secure_cookies: a.secure_cookies,
        max_conns_per_agent: a.max_conns_per_agent,
        max_duration: a.max_duration,
        ..ServerConfig::default()
    };
    let shutdown = crate::signal::shutdown();
    let h = serve(cfg).await?;
    eprintln!("loadfleet server on http://{}", h.addr);
    // printed once for copy/paste (and the deploy scripts); it also lands in service logs
    eprintln!("agent token: {}", h.token);
    shutdown.await;
    h.kill();
    Ok(0)
}
