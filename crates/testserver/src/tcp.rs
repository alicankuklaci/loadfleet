use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Echo server; the counter is the number of currently open connections.
pub async fn spawn_tcp_echo(addr: &str) -> std::io::Result<(SocketAddr, Arc<AtomicU64>)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let open = Arc::new(AtomicU64::new(0));
    let o = open.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                crate::accept_backoff().await;
                continue;
            };
            let o = o.clone();
            o.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                o.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
    Ok((local, open))
}
