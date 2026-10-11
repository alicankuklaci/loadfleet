//! Proxy headers are trusted only when the direct peer is a configured proxy;
//! CF-Connecting-IP additionally needs the explicit `trust_cf_header` switch.
use axum::http::HeaderMap;
use loadfleet_server::{client_ip, parse_proxy, IpNet};
use std::net::SocketAddr;

fn nets(v: &[&str]) -> Vec<IpNet> {
    v.iter().map(|s| parse_proxy(s).unwrap()).collect()
}

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(*k, v.parse().unwrap());
    }
    h
}

fn both() -> HeaderMap {
    headers(&[
        ("cf-connecting-ip", "203.0.113.7"),
        ("x-forwarded-for", "203.0.113.8"),
    ])
}

#[test]
fn headers_are_ignored_without_trusted_proxies() {
    let peer: SocketAddr = "198.51.100.9:4000".parse().unwrap();
    for cf in [false, true] {
        assert_eq!(
            client_ip(&both(), peer, &[], cf).to_string(),
            "198.51.100.9"
        );
    }
}

#[test]
fn headers_are_ignored_when_the_peer_is_not_a_trusted_proxy() {
    let peer: SocketAddr = "198.51.100.9:4000".parse().unwrap();
    let trusted = nets(&["10.0.0.0/8"]);
    for cf in [false, true] {
        assert_eq!(
            client_ip(&both(), peer, &trusted, cf).to_string(),
            "198.51.100.9"
        );
    }
}

#[test]
fn trusted_peer_cf_connecting_ip_wins_only_when_switched_on() {
    let peer: SocketAddr = "10.1.2.3:4000".parse().unwrap();
    let trusted = nets(&["10.0.0.0/8"]);
    assert_eq!(
        client_ip(&both(), peer, &trusted, true).to_string(),
        "203.0.113.7"
    );
    // switched off (the default): the forwarded chain decides
    assert_eq!(
        client_ip(&both(), peer, &trusted, false).to_string(),
        "203.0.113.8"
    );
}

#[test]
fn spoofed_cf_header_behind_a_local_proxy_is_ignored() {
    // nginx on 127.0.0.1 passes a client-sent CF-Connecting-IP through untouched
    let peer: SocketAddr = "127.0.0.1:4000".parse().unwrap();
    let trusted = nets(&["127.0.0.1"]);
    for fake in ["1.2.3.4", "5.6.7.8"] {
        let h = headers(&[
            ("cf-connecting-ip", fake),
            ("x-forwarded-for", "203.0.113.5"),
        ]);
        assert_eq!(
            client_ip(&h, peer, &trusted, false).to_string(),
            "203.0.113.5"
        );
    }
    // no forwarded chain at all: the peer, never the CF header
    let h = headers(&[("cf-connecting-ip", "1.2.3.4")]);
    assert_eq!(
        client_ip(&h, peer, &trusted, false).to_string(),
        "127.0.0.1"
    );
    // switched on but unparsable: fall back to the chain
    let h = headers(&[
        ("cf-connecting-ip", "junk"),
        ("x-forwarded-for", "203.0.113.5"),
    ]);
    assert_eq!(
        client_ip(&h, peer, &trusted, true).to_string(),
        "203.0.113.5"
    );
}

#[test]
fn forwarded_for_takes_the_rightmost_untrusted_hop() {
    let peer: SocketAddr = "10.1.2.3:4000".parse().unwrap();
    let trusted = nets(&["10.0.0.0/8"]);
    // the client prepended a forged entry; the proxy chain appended the real one
    let h = headers(&[("x-forwarded-for", "1.1.1.1, 203.0.113.5, 10.0.0.1")]);
    assert_eq!(
        client_ip(&h, peer, &trusted, false).to_string(),
        "203.0.113.5"
    );
    // several headers are one list, in order
    let mut h = HeaderMap::new();
    h.append("x-forwarded-for", "1.1.1.1".parse().unwrap());
    h.append("x-forwarded-for", "203.0.113.6".parse().unwrap());
    assert_eq!(
        client_ip(&h, peer, &trusted, false).to_string(),
        "203.0.113.6"
    );
    // garbage in the chain stops the walk: fall back to the peer
    let h = headers(&[("x-forwarded-for", "203.0.113.5, not-an-ip")]);
    assert_eq!(client_ip(&h, peer, &trusted, false).to_string(), "10.1.2.3");
    // every hop trusted: the socket peer
    let h = headers(&[("x-forwarded-for", "10.9.9.9, 10.0.0.1")]);
    assert_eq!(client_ip(&h, peer, &trusted, false).to_string(), "10.1.2.3");
}

#[test]
fn ipv4_mapped_peer_matches_ipv4_proxy_ranges() {
    let peer: SocketAddr = "[::ffff:10.1.2.3]:4000".parse().unwrap();
    let h = headers(&[("cf-connecting-ip", "2001:db8::7")]);
    assert_eq!(
        client_ip(&h, peer, &nets(&["10.0.0.0/8"]), true).to_string(),
        "2001:db8::7"
    );
    assert_eq!(
        client_ip(&HeaderMap::new(), peer, &[], false).to_string(),
        "10.1.2.3"
    );
}

#[test]
fn parse_proxy_accepts_ips_and_cidrs_only() {
    assert_eq!(parse_proxy("10.0.0.1").unwrap().to_string(), "10.0.0.1/32");
    assert_eq!(
        parse_proxy(" 2001:db8::/32 ").unwrap().to_string(),
        "2001:db8::/32"
    );
    assert_eq!(
        parse_proxy("173.245.48.0/20").unwrap().to_string(),
        "173.245.48.0/20"
    );
    assert!(parse_proxy("proxy.example.com").is_err());
    assert!(parse_proxy("10.0.0.0/33").is_err());
}
