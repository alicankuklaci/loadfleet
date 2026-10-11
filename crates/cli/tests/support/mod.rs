//! Shared helpers of the distributed (real server + real agents) tests.
#![allow(dead_code)] // each test binary uses a different subset

use loadfleet_agent::{run_agent, AgentConfig};
use loadfleet_engine::CancellationToken;
use loadfleet_health::now_ms;
use loadfleet_server::{serve, ServerConfig, ServerHandle};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// These tests time real load; running them one at a time keeps them from starving each other.
pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn temp_db() -> PathBuf {
    tempfile::tempdir().unwrap().keep().join("d.db")
}

/// `ServerConfig::for_tests()` on a fresh database, adjusted by `f`.
pub async fn server_with(f: impl FnOnce(&mut ServerConfig)) -> ServerHandle {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut cfg = ServerConfig {
        db: temp_db(),
        ..ServerConfig::for_tests()
    };
    f(&mut cfg);
    serve(cfg).await.unwrap()
}

/// Starts an agent against `h`; cancel the token to make the agent process "exit".
pub fn agent_with(
    h: &ServerHandle,
    name: &str,
    f: impl FnOnce(&mut AgentConfig),
) -> CancellationToken {
    let stop = CancellationToken::new();
    let mut cfg = AgentConfig::new(format!("ws://{}", h.addr), h.token.clone(), name.into());
    f(&mut cfg);
    let s = stop.clone();
    tokio::spawn(async move {
        let _ = run_agent(cfg, s).await;
    });
    stop
}

pub async fn ready(h: &ServerHandle, n: usize) {
    for _ in 0..200 {
        let ready = h
            .state
            .hub
            .agents()
            .iter()
            .filter(|a| a.readiness.is_some())
            .count();
        if ready >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agents not ready");
}

/// Waits until the stored run leaves `running` and returns its report.
pub async fn finished(h: &ServerHandle, run: &str, secs: u64) -> loadfleet_metrics::RunReport {
    for _ in 0..secs * 10 {
        if let Ok(Some(r)) = h.state.store.report(run).await {
            if r.meta.status != "running" {
                return r;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("run {run} did not finish");
}

pub fn plan(scenario: &str, target: u64, hold: &str, extra: &str) -> String {
    format!("name: d\ntarget: {{ allow: [\"127.0.0.1\"] }}\nscenario: {scenario}\nload: {{ stages: [{{target: {target}, ramp: 0s, hold: {hold}}}] }}\nagents: {{ min: 1, weights: equal }}\n{extra}\n")
}

pub fn tcp_plan(addr: SocketAddr, target: u64, hold: &str, extra: &str) -> String {
    let scenario = format!("{{ type: tcp, tcp: {{ addr: \"{addr}\", hold_ms: 0 }} }}");
    plan(&scenario, target, hold, extra)
}

pub fn http_plan(addr: SocketAddr, target: u64, hold: &str, extra: &str) -> String {
    let scenario = format!("{{ type: http, http: {{ url: \"http://{addr}/\" }} }}");
    plan(&scenario, target, hold, extra)
}

/// TCP target recording the wall-clock time of every accept; `open` counts live sockets.
pub struct Target {
    pub addr: SocketAddr,
    pub accepted: Arc<Mutex<Vec<i64>>>,
    pub open: Arc<AtomicU64>,
}

impl Target {
    pub async fn spawn() -> Target {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let open = Arc::new(AtomicU64::new(0));
        let (s, o) = (accepted.clone(), open.clone());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = l.accept().await {
                s.lock().unwrap().push(now_ms());
                o.fetch_add(1, Ordering::SeqCst);
                let o = o.clone();
                tokio::spawn(async move {
                    let mut b = [0u8; 256];
                    while matches!(sock.read(&mut b).await, Ok(n) if n > 0) {}
                    o.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        Target {
            addr,
            accepted,
            open,
        }
    }

    pub fn open(&self) -> u64 {
        self.open.load(Ordering::SeqCst)
    }

    /// Polls until `open() == want` or `secs` pass; returns the time it took.
    pub async fn until_open(&self, want: u64, secs: u64) -> Duration {
        let t0 = std::time::Instant::now();
        while self.open() != want && t0.elapsed() < Duration::from_secs(secs) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(self.open(), want, "after {:?}", t0.elapsed());
        t0.elapsed()
    }
}
