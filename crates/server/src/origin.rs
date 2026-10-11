//! Same-origin check for `POST /login`: a cross-site page must not be able to
//! log a browser into this panel (login CSRF) nor burn the admin's failure budget.
//!
//! The request's own origin is `scheme://host`: the scheme is `https` when a trusted proxy
//! says so (`X-Forwarded-Proto`) or with `--secure-cookies`, else `http`; the host is the
//! trusted proxy's `X-Forwarded-Host`, else `Host` (or the HTTP/2 authority).
use crate::ip::{forwarded_https, peer_trusted};
use axum::http::{header, HeaderMap, Uri};
use ipnet::IpNet;
use std::net::SocketAddr;

/// What the check needs to know about the deployment.
pub(crate) struct Expect<'a> {
    pub peer: SocketAddr,
    pub trusted_proxies: &'a [IpNet],
    pub secure_cookies: bool,
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok()).map(str::trim)
}

/// `host[:port]` lowercased, with the scheme's default port dropped.
fn norm_host(scheme: &str, host: &str) -> String {
    let h = host.trim().to_ascii_lowercase();
    let default = match scheme {
        "https" => ":443",
        _ => ":80",
    };
    h.strip_suffix(default).map(str::to_string).unwrap_or(h)
}

/// `scheme://host[:port]` of an `Origin` value or a `Referer` URL; None when it does not parse.
fn origin_of(s: &str) -> Option<(String, String)> {
    let (scheme, rest) = s.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let host = norm_host(&scheme, authority);
    Some((scheme, host))
}

/// The origin this server is reached at, as the browser sees it.
fn own_origin(h: &HeaderMap, uri: &Uri, e: &Expect) -> Option<(String, String)> {
    let https = e.secure_cookies || forwarded_https(h, e.peer, e.trusted_proxies);
    let scheme = if https { "https" } else { "http" };
    let forwarded = peer_trusted(e.peer, e.trusted_proxies)
        .then(|| header_str(h, "x-forwarded-host"))
        .flatten()
        .and_then(|v| v.split(',').next())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let host = forwarded
        .or_else(|| header_str(h, header::HOST.as_str()))
        .or_else(|| uri.authority().map(|a| a.as_str()))?;
    Some((scheme.to_string(), norm_host(scheme, host)))
}

/// True when the login post may proceed: `Origin` (else `Referer`) names this server's own
/// scheme and host. With neither header only a non-browser client (no `Sec-Fetch-Site`) or a
/// same-origin / user-initiated (`none`) navigation passes.
pub(crate) fn login_allowed(h: &HeaderMap, uri: &Uri, e: &Expect) -> bool {
    let claimed = header_str(h, header::ORIGIN.as_str())
        .map(origin_of)
        .or_else(|| header_str(h, header::REFERER.as_str()).map(origin_of));
    match claimed {
        Some(Some(claimed)) => own_origin(h, uri, e).is_some_and(|own| own == claimed),
        // `Origin: null`, an opaque or unparsable value
        Some(None) => false,
        None => matches!(
            header_str(h, "sec-fetch-site"),
            None | Some("same-origin") | Some("none")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_parse_and_normalise() {
        assert_eq!(
            origin_of("https://LF.example.com:443/login?x"),
            Some(("https".into(), "lf.example.com".into()))
        );
        assert_eq!(
            origin_of("http://[::1]:8080"),
            Some(("http".into(), "[::1]:8080".into()))
        );
        assert_eq!(origin_of("null"), None);
        assert_eq!(origin_of("file:///x"), None);
        assert_eq!(origin_of("http://user@evil/"), None);
    }
}
