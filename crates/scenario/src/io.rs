//! Byte-stream dialing shared by the tcp and ws scenarios (and plugins via `transport`).
use crate::net::connect_tcp;
use crate::{tls, AllowGuard};
use loadfleet_health::ConnGuard;
use loadfleet_metrics::Gauge;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
pub type BoxIo = Box<dyn Io>;

/// Guarded TCP connect (+ TLS with the HTTP/1.1 ALPN config when `use_tls`, as WebSocket
/// upgrades need). The `ConnGuard` must live as long as the stream.
pub async fn dial(
    host: &str,
    port: u16,
    use_tls: bool,
    guard: &AllowGuard,
    rr: &AtomicUsize,
    timeout: Duration,
) -> Result<(BoxIo, ConnGuard), String> {
    let cfg = use_tls.then(tls::default_config);
    dial_with(host, port, cfg, guard, rr, timeout).await
}

/// [`dial`] with an explicit TLS config (`None` = plain TCP).
pub(crate) async fn dial_with(
    host: &str,
    port: u16,
    tls_cfg: Option<Arc<rustls::ClientConfig>>,
    guard: &AllowGuard,
    rr: &AtomicUsize,
    timeout: Duration,
) -> Result<(BoxIo, ConnGuard), String> {
    let (tcp, tracked) = connect_tcp(host, port, guard, rr, timeout).await?;
    match tls_cfg {
        Some(cfg) => {
            let s = tokio::time::timeout(timeout, tls::wrap(tcp, host, cfg))
                .await
                .map_err(|_| "tls:timeout".to_string())??;
            Ok((Box::new(s), tracked))
        }
        None => Ok((Box::new(tcp), tracked)),
    }
}

/// Decrements a gauge when dropped (connection counting that survives early returns and
/// cancellation).
pub(crate) struct Held(Gauge);

impl Held {
    pub(crate) fn new(g: Gauge) -> Held {
        g.inc();
        Held(g)
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.dec();
    }
}
