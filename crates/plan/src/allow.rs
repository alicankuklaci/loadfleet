use ipnet::IpNet;
use std::net::IpAddr;

#[derive(Debug, Clone, PartialEq)]
enum Entry {
    Host(String),
    /// `*.suffix` — strict subdomains of `suffix`.
    Wildcard(String),
    Net(IpNet),
}

/// Target allow-list: exact hosts, `*.domain` wildcards, IPs and CIDRs.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AllowList {
    entries: Vec<Entry>,
}

fn norm_host(h: &str) -> String {
    let h = h.trim();
    let h = h.strip_prefix('[').unwrap_or(h);
    let h = h.strip_suffix(']').unwrap_or(h);
    h.strip_suffix('.').unwrap_or(h).to_ascii_lowercase()
}

fn parse_entry(raw: &str) -> Result<Entry, String> {
    let s = norm_host(raw);
    if s.is_empty() {
        return Err("empty allow entry".into());
    }
    if let Ok(net) = s.parse::<IpNet>() {
        return Ok(Entry::Net(net));
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Entry::Net(IpNet::from(ip)));
    }
    if let Some(suffix) = s.strip_prefix("*.") {
        if suffix.contains('*') || !suffix.contains('.') {
            return Err(format!("wildcard too broad: {raw}"));
        }
        return Ok(Entry::Wildcard(suffix.to_string()));
    }
    if s.contains('*') || s.contains('/') || s.contains(':') || s.contains('@') {
        return Err(format!("not a host, wildcard, IP or CIDR: {raw}"));
    }
    // `127.1`, `2130706433`, `0x7f.1`: legacy numeric spellings that resolvers treat as IPv4
    // but that are not valid IP literals here; a name list entry must not be one of them.
    let last = s.rsplit('.').next().unwrap_or("");
    if last.bytes().all(|b| b.is_ascii_digit()) || last.starts_with("0x") {
        return Err(format!(
            "numeric host must be a full IP literal or CIDR: {raw}"
        ));
    }
    Ok(Entry::Host(s))
}

impl AllowList {
    pub fn parse(items: &[String]) -> Result<AllowList, String> {
        Ok(AllowList {
            entries: items
                .iter()
                .map(|i| parse_entry(i))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// True when `host` (a hostname or an IP literal) may receive load.
    pub fn permits_host(&self, host: &str) -> bool {
        let h = norm_host(host);
        if let Ok(ip) = h.parse::<IpAddr>() {
            return self
                .entries
                .iter()
                .any(|e| matches!(e, Entry::Net(n) if n.contains(&ip)));
        }
        self.entries.iter().any(|e| match e {
            Entry::Host(x) => *x == h,
            Entry::Wildcard(sfx) => {
                h.len() > sfx.len() + 1
                    && h.ends_with(sfx.as_str())
                    && h.as_bytes()[h.len() - sfx.len() - 1] == b'.'
            }
            Entry::Net(_) => false,
        })
    }

    /// The host a URL (`scheme://...`) or a `host:port` address will actually connect to.
    pub fn host_of(endpoint: &str) -> Result<String, String> {
        if endpoint.contains("://") {
            let u = url::Url::parse(endpoint).map_err(|e| format!("bad url {endpoint}: {e}"))?;
            return u
                .host_str()
                .map(norm_host)
                .ok_or_else(|| format!("url without host: {endpoint}"));
        }
        let (host, port) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| format!("expected host:port: {endpoint}"))?;
        port.parse::<u16>()
            .map_err(|_| format!("bad port in {endpoint}"))?;
        Ok(norm_host(host))
    }

    /// True when a plan entry (host, wildcard, IP, CIDR) lies inside this (server) list.
    pub fn covers_entry(&self, raw: &str) -> bool {
        match parse_entry(raw) {
            Ok(Entry::Host(h)) => self.permits_host(&h),
            Ok(Entry::Wildcard(sfx)) => self.entries.iter().any(|e| match e {
                Entry::Wildcard(s) => {
                    sfx == *s
                        || (sfx.ends_with(s.as_str())
                            && sfx.as_bytes().get(sfx.len().wrapping_sub(s.len() + 1))
                                == Some(&b'.'))
                }
                _ => false,
            }),
            Ok(Entry::Net(n)) => self
                .entries
                .iter()
                .any(|e| matches!(e, Entry::Net(s) if s.contains(&n))),
            Err(_) => false,
        }
    }

    /// Entries of `self` that `other` also covers (agent-side narrowing).
    pub fn intersect(&self, other: &AllowList) -> AllowList {
        let entries = self
            .entries
            .iter()
            .filter(|e| other.covers_entry(&entry_text(e)))
            .cloned()
            .collect();
        AllowList { entries }
    }
}

fn entry_text(e: &Entry) -> String {
    match e {
        Entry::Host(h) => h.clone(),
        Entry::Wildcard(s) => format!("*.{s}"),
        Entry::Net(n) => n.to_string(),
    }
}
