use ipnet::IpNet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub db: PathBuf,
    /// Hosts/IPs/CIDRs any plan may target (mandatory; empty = every plan is refused).
    pub allow: Vec<String>,
    /// Admin password; when None the server generates one at first start and prints it once.
    pub admin_password: Option<String>,
    pub max_duration: Duration,
    pub max_conns_per_agent: u64,
    /// Reverse proxies (Cloudflare ranges, a local nginx, ...) whose CF-Connecting-IP /
    /// X-Forwarded-For / X-Forwarded-Proto headers are believed. Empty (the default): headers
    /// are ignored and the TCP peer is the client.
    pub trusted_proxies: Vec<IpNet>,
    /// Believe `CF-Connecting-IP` from a trusted proxy. Only for deployments whose
    /// `trusted_proxies` are Cloudflare's ranges: any other proxy forwards a client-sent value.
    pub trust_cf_header: bool,
    pub secure_cookies: bool,
    pub start_delay: Duration,
    pub heartbeat: Duration,
    pub agent_silence: Duration,
    /// How long past a run's planned end agents may take to report Done before the run is
    /// stopped as "agents did not finish in time".
    pub finish_grace: Duration,
    /// Failed admin logins (all clients together) per rolling hour before logins are
    /// throttled server-wide to one attempt per 10 s (429 otherwise); issued sessions keep
    /// working.
    pub login_failure_budget: usize,
    /// Serves unauthenticated test-only endpoints (`/healthz/agents`). Never on in production.
    pub test_endpoints: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen: "0.0.0.0:8080".parse().expect("valid addr"),
            db: PathBuf::from("loadfleet.db"),
            allow: Vec::new(),
            admin_password: None,
            max_duration: Duration::from_secs(7200),
            max_conns_per_agent: 100_000,
            trusted_proxies: Vec::new(),
            trust_cf_header: false,
            secure_cookies: false,
            start_delay: Duration::from_secs(10),
            heartbeat: Duration::from_secs(5),
            agent_silence: Duration::from_secs(15),
            finish_grace: Duration::from_secs(30),
            login_failure_budget: crate::auth::GLOBAL_FAILURE_BUDGET,
            test_endpoints: false,
        }
    }
}

impl ServerConfig {
    /// The server's data directory: the db file's directory, made absolute. Plan data given
    /// as a relative path (e.g. a scenario plugin's input directory) resolves against it, not
    /// against the process's working directory.
    pub fn data_dir(&self) -> PathBuf {
        let db = std::path::absolute(&self.db).unwrap_or_else(|_| self.db.clone());
        db.parent().map(PathBuf::from).unwrap_or_default()
    }

    pub fn for_tests() -> ServerConfig {
        ServerConfig {
            listen: "127.0.0.1:0".parse().expect("valid addr"),
            allow: vec!["127.0.0.1".into(), "localhost".into()],
            admin_password: Some("test-password".into()),
            start_delay: Duration::from_secs(2),
            heartbeat: Duration::from_secs(1),
            test_endpoints: true,
            ..Default::default()
        }
    }
}
