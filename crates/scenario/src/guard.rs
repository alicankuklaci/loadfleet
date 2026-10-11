use loadfleet_plan::AllowList;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// The agent-side allow-list check, applied on every connect (second check after the server's).
///
/// Two stages: [`check`](Self::check) judges the connect host as written (name or IP literal)
/// before DNS; [`check_resolved`](Self::check_resolved) judges the addresses DNS returned.
#[derive(Clone, Debug)]
pub struct AllowGuard {
    list: Arc<AllowList>,
}

impl AllowGuard {
    /// Plan list narrowed by the agent's own `--allow` (if any).
    pub fn new(plan_allow: &[String], local: Option<&AllowList>) -> Result<AllowGuard, String> {
        Self::layered(plan_allow, None, local)
    }

    /// plan ∩ server ∩ local: a plan entry survives only if every given list covers it.
    pub fn layered(
        plan_allow: &[String],
        server: Option<&AllowList>,
        local: Option<&AllowList>,
    ) -> Result<AllowGuard, String> {
        let mut list = AllowList::parse(plan_allow)?;
        for l in [server, local].into_iter().flatten() {
            list = list.intersect(l);
        }
        Ok(AllowGuard {
            list: Arc::new(list),
        })
    }

    pub fn check(&self, host: &str) -> Result<(), String> {
        if self.list.permits_host(host) {
            Ok(())
        } else {
            Err(blocked(host))
        }
    }

    /// True when a connect to `ip` (resolved from the already-permitted `host`) may proceed.
    ///
    /// Any address an IP/CIDR entry covers is fine. An address reached through a *name* entry is
    /// trusted to DNS, except loopback, link-local (incl. the 169.254.169.254 metadata endpoint),
    /// unspecified, multicast and broadcast addresses: those need an explicit IP/CIDR entry, so a
    /// permitted name that resolves (or is rebound) there is refused.
    pub fn permits_addr(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        if self.list.permits_host(&ip.to_string()) {
            return true;
        }
        !sensitive(ip)
    }

    /// The resolved addresses of `host` this guard permits; `blocked:<host>` when none is left.
    pub fn check_resolved(
        &self,
        host: &str,
        addrs: &[SocketAddr],
    ) -> Result<Vec<SocketAddr>, String> {
        let ok: Vec<SocketAddr> = addrs
            .iter()
            .filter(|a| self.permits_addr(a.ip()))
            .copied()
            .collect();
        if ok.is_empty() {
            Err(blocked(host))
        } else {
            Ok(ok)
        }
    }
}

fn blocked(host: &str) -> String {
    format!(
        "blocked:{}",
        host.trim_end_matches('.').to_ascii_lowercase()
    )
}

/// IPv4-mapped IPv6 (`::ffff:a.b.c.d`) is judged as the IPv4 address it reaches.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        v4 => v4,
    }
}

/// AWS instance metadata over IPv6 (`fd00:ec2::254`), the v6 twin of 169.254.169.254.
const AWS_IMDS_V6: std::net::Ipv6Addr =
    std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254);

/// Addresses a *name* entry may not reach without an explicit IP/CIDR entry. Private ranges
/// (10/8, 172.16/12, 192.168/16, fc00::/7) are deliberately not here: internal targets are a
/// normal use, so a permitted name rebound onto an internal host is still accepted.
fn sensitive(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_link_local()
                || v4.octets()[0] == 0 // 0.0.0.0/8 ("this network"; 0.0.0.0 reaches localhost)
                || v4.is_multicast()
                || v4.is_broadcast()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                || v6 == AWS_IMDS_V6
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(items: &[&str]) -> AllowGuard {
        let v: Vec<String> = items.iter().map(|s| s.to_string()).collect();
        AllowGuard::new(&v, None).unwrap()
    }

    fn sa(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn host_stage_is_case_and_dot_insensitive() {
        let guard = g(&["api.example.com"]);
        assert!(guard.check("API.example.com.").is_ok());
        assert_eq!(guard.check("Evil.com.").unwrap_err(), "blocked:evil.com");
    }

    #[test]
    fn names_may_not_resolve_to_sensitive_addresses() {
        let guard = g(&["api.example.com"]);
        let public = sa("93.184.216.34:443");
        for bad in [
            "127.0.0.1:443",
            "169.254.169.254:80",
            "0.0.0.0:80",
            "[::1]:443",
            "[::ffff:127.0.0.1]:443",
            "[fe80::1]:443",
            "224.0.0.1:80",
            "0.1.2.3:80",
            "[fd00:ec2::254]:80",
        ] {
            assert!(
                guard.check_resolved("api.example.com", &[sa(bad)]).is_err(),
                "{bad}"
            );
        }
        assert_eq!(
            guard.check_resolved("api.example.com", &[sa("127.0.0.1:1"), public]),
            Ok(vec![public])
        );
        // private ranges stay reachable by name (internal load tests)
        assert!(guard
            .check_resolved("api.example.com", &[sa("10.1.2.3:80")])
            .is_ok());
    }

    #[test]
    fn explicit_ip_entries_unlock_sensitive_addresses() {
        let guard = g(&["localhost", "127.0.0.0/8"]);
        assert!(guard
            .check_resolved("localhost", &[sa("127.0.0.1:80")])
            .is_ok());
        assert!(guard
            .check_resolved("localhost", &[sa("[::1]:80")])
            .is_err());
    }

    #[test]
    fn layers_intersect() {
        let server = AllowList::parse(&["*.example.com".into()]).unwrap();
        let local = AllowList::parse(&["api.example.com".into()]).unwrap();
        let plan = vec![
            "api.example.com".to_string(),
            "web.example.com".into(),
            "x.org".into(),
        ];
        let guard = AllowGuard::layered(&plan, Some(&server), Some(&local)).unwrap();
        assert!(guard.check("api.example.com").is_ok());
        assert!(guard.check("web.example.com").is_err());
        assert!(guard.check("x.org").is_err());
    }
}
