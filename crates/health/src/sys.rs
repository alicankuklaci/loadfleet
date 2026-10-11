use serde::{Deserialize, Serialize};

pub fn parse_port_range(text: &str) -> Option<(u16, u16)> {
    let mut it = text.split_whitespace().map(|t| t.parse::<u16>());
    match (it.next(), it.next()) {
        (Some(Ok(a)), Some(Ok(b))) if a < b => Some((a, b)),
        _ => None,
    }
}

/// The OS ephemeral port range (Linux procfs, macOS sysctl); falls back to the IANA range.
pub fn ephemeral_port_range() -> (u16, u16) {
    if let Ok(t) = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range") {
        if let Some(r) = parse_port_range(&t) {
            return r;
        }
    }
    if let Ok(out) = std::process::Command::new("sysctl")
        .args([
            "-n",
            "net.inet.ip.portrange.first",
            "net.inet.ip.portrange.last",
        ])
        .output()
    {
        if let Some(r) = parse_port_range(&String::from_utf8_lossy(&out.stdout)) {
            return r;
        }
    }
    (49152, 65535)
}

pub fn fd_count() -> Option<u64> {
    std::fs::read_dir("/dev/fd")
        .ok()
        .map(|d| d.count().saturating_sub(1) as u64)
}

pub fn fd_limit() -> Option<u64> {
    rlimit::getrlimit(rlimit::Resource::NOFILE)
        .ok()
        .map(|(soft, _)| soft)
}

/// Raises the soft fd limit to the hard limit (like `ulimit -n`); returns the new soft limit.
pub fn raise_fd_limit() -> u64 {
    rlimit::increase_nofile_limit(u64::MAX).unwrap_or_else(|_| fd_limit().unwrap_or(0))
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HostInfo {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub cores: u32,
    pub mem_mb: u64,
}

impl HostInfo {
    pub fn detect() -> Self {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        HostInfo {
            hostname: sysinfo::System::host_name().unwrap_or_else(|| "unknown".into()),
            os: sysinfo::System::long_os_version().unwrap_or_else(|| std::env::consts::OS.into()),
            arch: std::env::consts::ARCH.into(),
            cores: std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1),
            mem_mb: sys.total_memory() / 1024 / 1024,
        }
    }
}
