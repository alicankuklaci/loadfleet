//! loadfleet testserver: local HTTP/TCP/WS echo servers for tests and benches.
mod http;
mod tcp;
mod ws;

pub use http::{spawn_http, spawn_http_stats, HttpStats};
pub use tcp::spawn_tcp_echo;
pub use ws::spawn_ws_echo;

/// Pause after a failed `accept` (e.g. EMFILE in a bench) instead of spinning.
pub(crate) async fn accept_backoff() {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
}
