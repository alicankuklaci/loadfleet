use crate::AllowGuard;
use loadfleet_health::{classify_connect_error, conns, ConnGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

/// Checks the host, resolves it, re-checks the resolved addresses (see
/// [`AllowGuard::check_resolved`]), spreads connections over every permitted address (Cloudflare
/// returns several edge IPs) and tracks the socket for the ephemeral-port gauge.
pub async fn connect_tcp(
    host: &str,
    port: u16,
    guard: &AllowGuard,
    rr: &AtomicUsize,
    timeout: Duration,
) -> Result<(TcpStream, ConnGuard), String> {
    guard.check(host)?;
    let resolved: Vec<_> =
        match tokio::time::timeout(timeout, tokio::net::lookup_host((host, port))).await {
            Err(_) | Ok(Err(_)) => return Err("net:dns".into()),
            Ok(Ok(a)) => a.collect(),
        };
    if resolved.is_empty() {
        return Err("net:dns".into());
    }
    let addrs = guard.check_resolved(host, &resolved)?;
    let addr = addrs[rr.fetch_add(1, Ordering::Relaxed) % addrs.len()];
    let stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
        Err(_) => return Err("net:connect_timeout".into()),
        Ok(Err(e)) => return Err(classify_connect_error(&e).to_string()),
        Ok(Ok(s)) => s,
    };
    let _ = stream.set_nodelay(true);
    Ok((stream, conns().open(addr)))
}

/// `host:port` or `[v6]:port` -> (host without brackets, port).
pub(crate) fn split_host_port(addr: &str) -> Result<(String, u16), String> {
    let bad = || format!("scenario.tcp.addr: {addr:?} is not host:port");
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        let (h, p) = rest.split_once("]:").ok_or_else(bad)?;
        (h, p)
    } else {
        let (h, p) = addr.rsplit_once(':').ok_or_else(bad)?;
        if h.contains(':') {
            return Err(format!(
                "scenario.tcp.addr: {addr:?}: write IPv6 addresses as [addr]:port"
            ));
        }
        (h, p)
    };
    let port: u16 = port.parse().map_err(|_| bad())?;
    if host.is_empty() || port == 0 {
        return Err(bad());
    }
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::split_host_port;

    #[test]
    fn splits_host_and_port() {
        assert_eq!(
            split_host_port("example.com:443").unwrap(),
            ("example.com".into(), 443)
        );
        assert_eq!(split_host_port("[::1]:9000").unwrap(), ("::1".into(), 9000));
        for bad in [
            "example.com",
            ":80",
            "h:0",
            "h:70000",
            "h:x",
            "::1:80",
            "[::1]80",
            "[]:80",
        ] {
            assert!(split_host_port(bad).is_err(), "{bad}");
        }
    }
}
