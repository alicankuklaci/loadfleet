use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

/// Counters of a test HTTP server.
#[derive(Clone, Debug, Default)]
pub struct HttpStats {
    /// Requests served.
    pub requests: Arc<AtomicU64>,
    /// Currently open client connections.
    pub open: Arc<AtomicU64>,
}

/// HTTP/1.1 + h2c server: `/` → `ok`, `POST /echo` → body, `/status/{code}`, `/delay/{ms}`,
/// `/close` → `ok` with `Connection: close`. The counter is the number of requests served.
pub async fn spawn_http(addr: &str) -> std::io::Result<(SocketAddr, Arc<AtomicU64>)> {
    let (local, stats) = spawn_http_stats(addr).await?;
    Ok((local, stats.requests))
}

/// [`spawn_http`] that also counts open connections.
pub async fn spawn_http_stats(addr: &str) -> std::io::Result<(SocketAddr, HttpStats)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let stats = HttpStats::default();
    let st = stats.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                crate::accept_backoff().await;
                continue;
            };
            let _ = stream.set_nodelay(true);
            let st = st.clone();
            st.open.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let h = st.requests.clone();
                let svc = service_fn(move |req| handle(req, h.clone()));
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
                st.open.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
    Ok((local, stats))
}

async fn handle(
    req: Request<Incoming>,
    hits: Arc<AtomicU64>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    hits.fetch_add(1, Ordering::Relaxed);
    let path = req.uri().path().to_string();
    if let Some(code) = path.strip_prefix("/status/") {
        let status = code
            .parse::<u16>()
            .ok()
            .and_then(|c| StatusCode::from_u16(c).ok())
            .unwrap_or(StatusCode::BAD_REQUEST);
        let mut r = Response::new(Full::new(Bytes::from_static(b"status")));
        *r.status_mut() = status;
        return Ok(r);
    }
    if let Some(ms) = path.strip_prefix("/delay/") {
        tokio::time::sleep(std::time::Duration::from_millis(ms.parse().unwrap_or(0))).await;
    }
    if path == "/close" {
        let mut r = Response::new(Full::new(Bytes::from_static(b"ok")));
        r.headers_mut()
            .insert(hyper::header::CONNECTION, "close".parse().expect("static"));
        return Ok(r);
    }
    if path == "/echo" {
        let body = req
            .into_body()
            .collect()
            .await
            .map(|b| b.to_bytes())
            .unwrap_or_default();
        return Ok(Response::new(Full::new(body)));
    }
    Ok(Response::new(Full::new(Bytes::from_static(b"ok"))))
}
