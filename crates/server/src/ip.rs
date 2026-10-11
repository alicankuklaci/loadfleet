use axum::http::HeaderMap;
use ipnet::IpNet;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

/// One trusted-proxy entry: a CIDR, or a bare IP (a single-address network).
pub fn parse_proxy(s: &str) -> Result<IpNet, String> {
    let s = s.trim();
    s.parse::<IpNet>()
        .or_else(|_| s.parse::<IpAddr>().map(IpNet::from))
        .map_err(|_| format!("trusted proxy {s:?}: expected an IP or a CIDR"))
}

/// IPv4-mapped IPv6 (`::ffff:a.b.c.d`, how dual-stack sockets report IPv4 peers) as IPv4.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(a) => a.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

fn trusted(ip: IpAddr, proxies: &[IpNet]) -> bool {
    proxies.iter().any(|n| n.contains(&ip))
}

/// The client's IP, the one helper the hub and the API share.
///
/// Proxy headers count only when the TCP peer is inside `trusted_proxies`; otherwise — and
/// with the default empty list — it is the peer itself, so nobody picks their own address.
/// From a trusted peer:
/// - `CF-Connecting-IP`, but only with `trust_cf_header` (the deployment is behind Cloudflare
///   and the trusted ranges are Cloudflare's): other proxies pass a client-sent CF header on
///   untouched, so by default it is ignored;
/// - else the rightmost `X-Forwarded-For` hop that is not itself a trusted proxy (the hops our
///   proxies appended are read from the right, a client can only prepend);
/// - else (no chain, every hop trusted, or a hop that does not parse) the peer.
pub fn client_ip(
    h: &HeaderMap,
    peer: SocketAddr,
    trusted_proxies: &[IpNet],
    trust_cf_header: bool,
) -> IpAddr {
    let peer_ip = canonical(peer.ip());
    if !trusted(peer_ip, trusted_proxies) {
        return peer_ip;
    }
    if trust_cf_header {
        let cf = h.get("cf-connecting-ip").and_then(|v| v.to_str().ok());
        if let Some(ip) = cf.and_then(|v| v.trim().parse::<IpAddr>().ok()) {
            return canonical(ip);
        }
    }
    let hops: Vec<&str> = h
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect();
    for hop in hops.iter().rev() {
        let Ok(ip) = hop.parse::<IpAddr>().map(canonical) else {
            return peer_ip;
        };
        if !trusted(ip, trusted_proxies) {
            return ip;
        }
    }
    peer_ip
}

/// True when the TCP peer is one of the trusted reverse proxies.
pub(crate) fn peer_trusted(peer: SocketAddr, trusted_proxies: &[IpNet]) -> bool {
    trusted(canonical(peer.ip()), trusted_proxies)
}

/// True when a trusted proxy says the client spoke HTTPS to it (TLS ends at the proxy).
pub fn forwarded_https(h: &HeaderMap, peer: SocketAddr, trusted_proxies: &[IpNet]) -> bool {
    peer_trusted(peer, trusted_proxies)
        && h.get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("https"))
}

/// One allow-list entry: an IPv4 address as is, an IPv6 address widened to its /64
/// (hosts rotate privacy addresses inside their prefix), IPv4-mapped IPv6 as IPv4.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Entry {
    V4(std::net::Ipv4Addr),
    V6Net(Ipv6Addr),
}

fn entry(s: &str) -> Option<Entry> {
    match s.trim().parse::<IpAddr>().ok()? {
        IpAddr::V4(a) => Some(Entry::V4(a)),
        IpAddr::V6(a) => match a.to_ipv4_mapped() {
            Some(v4) => Some(Entry::V4(v4)),
            None => Some(Entry::V6Net(Ipv6Addr::from(
                u128::from(a) & !((1u128 << 64) - 1),
            ))),
        },
    }
}

/// Cloudflare WAF expression that matches every agent IP (paste into a custom/skip rule):
/// `(ip.src in {a.b.c.d 2a00:x:y:z::/64})`. IPv4 first in numeric order, then IPv6 /64s;
/// duplicates collapse and unparsable entries are skipped. Empty string when nothing is
/// left, since Cloudflare rejects an empty set.
pub fn cloudflare_expression(ips: &[String]) -> String {
    let set: BTreeSet<Entry> = ips.iter().filter_map(|s| entry(s)).collect();
    if set.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = set
        .iter()
        .map(|e| match e {
            Entry::V4(a) => a.to_string(),
            Entry::V6Net(n) => format!("{n}/64"),
        })
        .collect();
    format!("(ip.src in {{{}}})", parts.join(" "))
}
