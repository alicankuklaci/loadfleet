//! The control connection: one WebSocket to `<server>/agent`, bearer-token authenticated.
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest, Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

pub(crate) type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type Sink = SplitSink<Ws, Message>;
pub(crate) type Stream = SplitStream<Ws>;

#[derive(Debug)]
pub(crate) enum DialError {
    /// Retrying cannot help (bad URL, refused token).
    Fatal(String),
    Retry(String),
}

/// `server` may be `wss://host`, `ws://host:port` or already end in `/agent`.
pub(crate) fn agent_url(server: &str) -> String {
    let s = server.trim_end_matches('/');
    if s.ends_with("/agent") {
        s.to_string()
    } else {
        format!("{s}/agent")
    }
}

/// TCP connect + TLS (for `wss://`) + WebSocket handshake. Has no timeout of its own: the
/// caller bounds it.
pub(crate) async fn dial(server: &str, token: &str) -> Result<(Sink, Stream), DialError> {
    let mut req = agent_url(server)
        .into_client_request()
        .map_err(|e| DialError::Fatal(format!("bad --server: {e}")))?;
    let auth = format!("Bearer {token}")
        .parse()
        .map_err(|_| DialError::Fatal("the agent token is not a valid header value".into()))?;
    req.headers_mut().insert("authorization", auth);
    // the same rustls setup (ring, Mozilla roots) the scenarios use
    let tls = Connector::Rustls(loadfleet_scenario::tls_config(&[b"http/1.1"]));
    match tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(tls)).await {
        Ok((ws, _)) => Ok(ws.split()),
        Err(tungstenite::Error::Http(r)) if r.status().as_u16() == 401 => Err(DialError::Fatal(
            "server refused the agent token (401)".into(),
        )),
        Err(e @ tungstenite::Error::Url(_)) => Err(DialError::Fatal(format!("bad --server: {e}"))),
        Err(e) => Err(DialError::Retry(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::agent_url;

    #[test]
    fn agent_path_is_appended_once() {
        assert_eq!(
            agent_url("wss://lf.example.com"),
            "wss://lf.example.com/agent"
        );
        assert_eq!(agent_url("ws://10.0.0.1:8080/"), "ws://10.0.0.1:8080/agent");
        assert_eq!(agent_url("wss://h/agent"), "wss://h/agent");
    }
}
