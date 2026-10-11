use std::sync::{Arc, OnceLock};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

/// rustls client config with the ring provider and Mozilla roots (no OpenSSL, no system store
/// needed). Shared with the agent's control connection.
pub fn tls_config(alpn: &[&[u8]]) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports the default protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(cfg)
}

/// HTTP/1.1 ALPN config, built once.
pub fn default_config() -> Arc<rustls::ClientConfig> {
    static CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| tls_config(&[b"http/1.1"])).clone()
}

/// No-ALPN config for raw TCP-over-TLS, built once.
pub(crate) fn raw_config() -> Arc<rustls::ClientConfig> {
    static CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| tls_config(&[])).clone()
}

/// h2 ALPN config, built once.
pub(crate) fn h2_config() -> Arc<rustls::ClientConfig> {
    static CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| tls_config(&[b"h2"])).clone()
}

pub async fn wrap(
    stream: TcpStream,
    host: &str,
    cfg: Arc<rustls::ClientConfig>,
) -> Result<TlsStream<TcpStream>, String> {
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| "tls:bad_name".to_string())?;
    TlsConnector::from(cfg)
        .connect(name, stream)
        .await
        .map_err(|e| tls_error_code(&e))
}

/// Protocol/certificate failures are `tls:handshake`; socket failures during the handshake go
/// through `classify_io_error` so `client:*` exhaustion is still counted as such.
fn tls_error_code(e: &std::io::Error) -> String {
    let is_tls = e
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some());
    if is_tls {
        "tls:handshake".into()
    } else {
        loadfleet_health::classify_io_error(e).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    #[test]
    fn tls_errors_map_to_codes() {
        let proto = Error::new(ErrorKind::InvalidData, rustls::Error::DecryptError);
        assert_eq!(tls_error_code(&proto), "tls:handshake");
        assert_eq!(
            tls_error_code(&Error::from(ErrorKind::ConnectionReset)),
            "net:reset"
        );
        assert_eq!(
            tls_error_code(&Error::from_raw_os_error(24)),
            "client:too_many_files"
        );
    }
}
