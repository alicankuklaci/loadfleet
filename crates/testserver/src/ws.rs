use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket echo (served on any path, `/ws` by convention); the counter is the number of open sockets.
pub async fn spawn_ws_echo(addr: &str) -> std::io::Result<(SocketAddr, Arc<AtomicU64>)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let open = Arc::new(AtomicU64::new(0));
    let o = open.clone();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                crate::accept_backoff().await;
                continue;
            };
            let o = o.clone();
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(s).await else {
                    return;
                };
                o.fetch_add(1, Ordering::Relaxed);
                while let Some(Ok(msg)) = ws.next().await {
                    match msg {
                        Message::Text(_) | Message::Binary(_) => {
                            if ws.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
                o.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
    Ok((local, open))
}
