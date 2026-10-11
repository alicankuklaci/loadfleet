//! `cargo xtask bench`: self-checks of the load generator on this machine (spec success
//! criterion 2: 50k held TCP connections, 50k HTTP req/s from one agent).
use crate::hostload;
use anyhow::{bail, Context, Result};
use clap::Subcommand;
use loadfleet_metrics::{RunReport, StageSummary};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Ramp of the first stage (the hold stage follows it).
const TCP_RAMP_S: u64 = 20;
const HTTP_RAMP_S: u64 = 10;

/// First testserver port (tcp-hold listeners count up from here; http-rps uses +10).
const PORT_BASE: u16 = 19001;

#[derive(Subcommand)]
pub enum Which {
    /// Open and hold `--conns` TCP connections against echo targets.
    TcpHold {
        #[arg(long, default_value_t = 50_000)]
        conns: u64,
        #[arg(long, default_value = "60s")]
        hold: String,
        /// Remote echo targets (comma-separated host:port); default: a local testserver.
        #[arg(long)]
        target: Option<String>,
        /// Local addresses the testserver binds (comma-separated, two ports each). Default:
        /// 127.0.0.1 and ::1 on macOS, 127.0.0.1 (four ports) elsewhere.
        #[arg(long)]
        hosts: Option<String>,
    },
    /// Open-model HTTP GET at `--rate` requests per second.
    HttpRps {
        #[arg(long, default_value_t = 50_000)]
        rate: u64,
        #[arg(long, default_value = "60s")]
        hold: String,
        #[arg(long)]
        target: Option<String>,
        /// Keep-alive connections in the client pool.
        #[arg(long, default_value_t = 512)]
        connections: u64,
    },
}

pub struct Verdict {
    pub pass: bool,
    pub lines: Vec<String>,
}

fn no_stage() -> Verdict {
    Verdict {
        pass: false,
        lines: vec!["no stage finished".into()],
    }
}

fn saturation_lines(s: &StageSummary) -> Vec<String> {
    s.client_notes
        .iter()
        .map(|n| format!("client saturated: {n}"))
        .collect()
}

pub fn evaluate_tcp(r: &RunReport, conns: u64) -> Verdict {
    let Some(s) = r.stages.last() else {
        return no_stage();
    };
    let held = s.gauges_min.get("connections").copied().unwrap_or(0).max(0) as u64;
    let peak = s.gauges_max.get("connections").copied().unwrap_or(0).max(0);
    let mut lines = vec![
        format!("held connections (min over hold): {held} / {conns} (max {peak})"),
        format!("errors: {:?}", s.errors),
    ];
    if let Some(l) = s.latency.get("tcp.connect") {
        lines.push(format!(
            "connect p50/p99 {:.1}/{:.1} ms",
            l.p50_ms, l.p99_ms
        ));
    }
    lines.extend(health_lines(s));
    lines.extend(saturation_lines(s));
    Verdict {
        pass: held >= conns && !s.client_limited,
        lines,
    }
}

pub fn evaluate_http(r: &RunReport, rate: u64) -> Verdict {
    let Some(s) = r.stages.last() else {
        return no_stage();
    };
    let mut lines = vec![format!(
        "achieved {:.0} req/s of {rate} (error rate {:.3}%)",
        s.ops_per_s,
        s.error_rate * 100.0
    )];
    if let Some(l) = s.latency.get("http") {
        lines.push(format!(
            "latency p50/p95/p99 {:.2}/{:.2}/{:.2} ms",
            l.p50_ms, l.p95_ms, l.p99_ms
        ));
    }
    if let Some(late) = s.counters.get("open.late") {
        lines.push(format!("sends started late: {late}"));
    }
    if s.dropped > 0 {
        lines.push(format!("planned sends dropped: {}", s.dropped));
    }
    lines.extend(health_lines(s));
    lines.extend(saturation_lines(s));
    Verdict {
        pass: s.ops_per_s >= rate as f64 * 0.95 && s.error_rate < 0.001 && !s.client_limited,
        lines,
    }
}

/// Per-agent CPU / runtime lag of the stage, so a pass also shows the headroom left.
fn health_lines(s: &StageSummary) -> Vec<String> {
    s.agents
        .iter()
        .map(|(a, h)| {
            format!(
                "agent {a}: cpu avg {:.0}%, runtime lag p95 {:.1} ms, ports max {:.0}%",
                h.cpu_avg, h.lag_p95_ms, h.ports_pct_max
            )
        })
        .collect()
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn preflight(fds: u64) {
    let soft = rlimit::increase_nofile_limit(u64::MAX).unwrap_or(0);
    if soft < fds + 2_000 {
        eprintln!("fd limit {soft} is below {}: raise it first:", fds + 2_000);
        eprintln!("  macOS: sudo sysctl -w kern.maxfiles=2097152 kern.maxfilesperproc=1048576 && ulimit -n 1048576");
        eprintln!("  Linux: ulimit -n 1048576 (and fs.file-max)");
    }
}

/// Starts the release testserver and waits until it printed one line per listener.
fn testserver(args: &[String]) -> Result<Child> {
    let bin = root().join("target/release/loadfleet-testserver");
    let mut child = Command::new(bin)
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .context("start loadfleet-testserver")?;
    let listeners = args.len() / 2;
    let out = child.stdout.take().context("testserver stdout")?;
    let mut lines = BufReader::new(out).lines();
    for _ in 0..listeners {
        match lines.next() {
            Some(Ok(l)) => eprintln!("testserver: {l}"),
            _ => {
                let _ = child.kill();
                bail!("testserver exited before listening on {args:?}");
            }
        }
    }
    // keep draining: a later write into a closed pipe would make the testserver panic (EPIPE)
    std::thread::spawn(move || lines.for_each(drop));
    Ok(child)
}

fn stop(child: Option<Child>) {
    if let Some(mut c) = child {
        let _ = c.kill();
        let _ = c.wait();
    }
}

/// `[v6]` when the address is IPv6, as `host:port` needs.
fn bracket(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

/// `host:port`, `[v6]:port`, `host` or `[v6]` -> host without brackets.
fn host_of(addr: &str) -> String {
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest).to_string();
    }
    addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_string()
}

/// macOS/BSD: one ephemeral-port pool per address family, so IPv4 + IPv6 loopback doubles it
/// (more IPv4 aliases would not help: connect() wants a port free on every IPv4 address).
/// Linux: the pool is per destination ip:port, so four ports on 127.0.0.1 are enough.
fn default_hosts() -> Vec<String> {
    if cfg!(target_os = "macos") {
        vec!["127.0.0.1".into(), "::1".into()]
    } else {
        vec!["127.0.0.1".into()]
    }
}

/// Listener addresses: two ports per host, four when there is only one host.
pub fn local_targets(hosts: &[String]) -> Vec<String> {
    let per = if hosts.len() == 1 { 4 } else { 2 };
    let mut port = PORT_BASE;
    let mut out = Vec::new();
    for h in hosts {
        for _ in 0..per {
            out.push(format!("{}:{port}", bracket(h)));
            port += 1;
        }
    }
    out
}

fn macos_port_hint(hosts: &[String], conns: u64) {
    let (lo, hi) = loadfleet_health::ephemeral_port_range();
    let per = u64::from(hi - lo) + 1;
    let families = 1 + u64::from(hosts.iter().any(|h| h.contains(':')))
        - u64::from(hosts.iter().all(|h| h.contains(':')));
    let cap = per * families;
    eprintln!("macOS: {per} ephemeral ports ({lo}-{hi}) per address family, {families} in use -> at most ~{cap} connections");
    if cap < conns {
        eprintln!("  below --conns {conns}; widen the range: sudo sysctl -w net.inet.ip.portrange.first=10240 (or run the bench on Linux)");
    }
}

/// Runs the plan while sampling the host CPU; returns the report and the host busy share
/// during the hold.
fn run_measured(kind: &str, yaml: &str, ramp_s: u64) -> (Result<RunReport>, Option<f32>) {
    let sampler = hostload::Sampler::start();
    let r = run_plan(kind, yaml);
    (r, sampler.finish(Duration::from_secs(ramp_s + 2)))
}

fn run_plan(kind: &str, yaml: &str) -> Result<RunReport> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let out = root().join(format!("reports/bench-{kind}-{ts}"));
    std::fs::create_dir_all(&out)?;
    let plan = out.join("plan.yaml");
    std::fs::write(&plan, yaml)?;
    let status = Command::new(root().join("target/release/loadfleet"))
        .arg("run")
        .arg(&plan)
        .arg("--out")
        .arg(&out)
        .status()?;
    eprintln!(
        "loadfleet run exited with {status}; report: {}",
        out.join("report.md").display()
    );
    read_report(&out.join("report.json"))
}

fn read_report(p: &Path) -> Result<RunReport> {
    let text = std::fs::read_to_string(p).with_context(|| format!("read {}", p.display()))?;
    Ok(serde_json::from_str(&text)?)
}

pub fn tcp_plan(addrs: &[String], conns: u64, hold: &str) -> String {
    let mut hosts: Vec<String> = addrs.iter().map(|a| host_of(a)).collect();
    hosts.dedup();
    format!(
        "name: bench-tcp-hold\ntarget: {{ allow: {hosts:?} }}\nscenario: {{ type: tcp, tcp: {{ addr: {addrs:?}, hold_ms: 0, conn_rate: 5000 }} }}\nload: {{ stages: [{{target: {conns}, ramp: {TCP_RAMP_S}s, hold: 0s}}, {{target: {conns}, ramp: 0s, hold: {hold}}}] }}\n"
    )
}

pub fn http_plan(url: &str, rate: u64, hold: &str, connections: u64) -> String {
    let host = url
        .split("://")
        .nth(1)
        .and_then(|r| r.split(['/']).next())
        .map(host_of)
        .unwrap_or_else(|| "127.0.0.1".into());
    format!(
        "name: bench-http-rps\ntarget: {{ allow: [\"{host}\"] }}\nscenario: {{ type: http, http: {{ url: \"{url}\", connections: {connections} }} }}\nload: {{ model: open, max_vus: 4096, stages: [{{target: {rate}, ramp: {HTTP_RAMP_S}s, hold: 0s}}, {{target: {rate}, ramp: 0s, hold: {hold}}}] }}\n"
    )
}

fn tcp_hold(
    conns: u64,
    hold: &str,
    target: Option<String>,
    hosts: Option<String>,
) -> Result<Verdict> {
    preflight(conns);
    let before = hostload::busy_over(Duration::from_secs(3));
    let (addrs, child) = match target {
        Some(t) => (t.split(',').map(str::to_string).collect::<Vec<_>>(), None),
        None => {
            let hosts: Vec<String> = match hosts {
                Some(h) => h.split(',').map(|s| s.trim().to_string()).collect(),
                None => default_hosts(),
            };
            if cfg!(target_os = "macos") {
                macos_port_hint(&hosts, conns);
            }
            let addrs = local_targets(&hosts);
            let args: Vec<String> = addrs
                .iter()
                .flat_map(|a| ["--tcp".to_string(), a.clone()])
                .collect();
            (addrs, Some(testserver(&args)?))
        }
    };
    let (r, during) = run_measured("tcp-hold", &tcp_plan(&addrs, conns, hold), TCP_RAMP_S);
    stop(child);
    let mut v = evaluate_tcp(&r?, conns);
    v.lines.extend(hostload::lines(before, during, false));
    Ok(v)
}

fn http_rps(rate: u64, hold: &str, target: Option<String>, connections: u64) -> Result<Verdict> {
    preflight(10_000);
    let before = hostload::busy_over(Duration::from_secs(3));
    let (url, child) = match target {
        Some(t) => (t, None),
        None => {
            let addr = format!("127.0.0.1:{}", PORT_BASE + 10);
            let child = testserver(&["--http".into(), addr.clone()])?;
            (format!("http://{addr}/"), Some(child))
        }
    };
    let plan = http_plan(&url, rate, hold, connections);
    let (r, during) = run_measured("http-rps", &plan, HTTP_RAMP_S);
    stop(child);
    let mut v = evaluate_http(&r?, rate);
    v.lines.extend(hostload::lines(before, during, !v.pass));
    Ok(v)
}

pub fn run(w: Which) -> Result<()> {
    let ok = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "loadfleet-cli",
            "-p",
            "loadfleet-testserver",
        ])
        .current_dir(root())
        .status()?;
    if !ok.success() {
        bail!("build failed");
    }
    let verdict = match w {
        Which::TcpHold {
            conns,
            hold,
            target,
            hosts,
        } => tcp_hold(conns, &hold, target, hosts)?,
        Which::HttpRps {
            rate,
            hold,
            target,
            connections,
        } => http_rps(rate, &hold, target, connections)?,
    };
    for l in &verdict.lines {
        println!("  {l}");
    }
    println!(
        "{}",
        if verdict.pass {
            "BENCH PASS"
        } else {
            "BENCH FAIL"
        }
    );
    if verdict.pass {
        Ok(())
    } else {
        bail!("bench failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadfleet_metrics::{RunReport, StageSummary};

    fn report(conns_min: i64, ops_per_s: f64, limited: bool) -> RunReport {
        let mut s = StageSummary {
            index: 1,
            ops_per_s,
            total_ops: 1,
            client_limited: limited,
            ..Default::default()
        };
        s.gauges_min.insert("connections".into(), conns_min);
        if limited {
            s.client_notes.push("local: cpu 97% > 85%".into());
        }
        RunReport {
            stages: vec![StageSummary::default(), s],
            ..Default::default()
        }
    }

    #[test]
    fn tcp_verdicts() {
        assert!(evaluate_tcp(&report(50_000, 0.0, false), 50_000).pass);
        let v = evaluate_tcp(&report(41_000, 0.0, true), 50_000);
        assert!(!v.pass);
        assert!(v
            .lines
            .iter()
            .any(|l| l.contains("client saturated: local: cpu 97%")));
    }

    #[test]
    fn http_verdicts() {
        assert!(evaluate_http(&report(0, 49_000.0, false), 50_000).pass);
        assert!(!evaluate_http(&report(0, 30_000.0, false), 50_000).pass);
    }

    #[test]
    fn local_targets_spread_hosts_and_ports() {
        assert_eq!(
            local_targets(&["127.0.0.1".into()]),
            [
                "127.0.0.1:19001",
                "127.0.0.1:19002",
                "127.0.0.1:19003",
                "127.0.0.1:19004"
            ]
        );
        assert_eq!(
            local_targets(&["127.0.0.1".into(), "::1".into()]),
            [
                "127.0.0.1:19001",
                "127.0.0.1:19002",
                "[::1]:19003",
                "[::1]:19004"
            ]
        );
    }

    #[test]
    fn host_of_strips_port_and_brackets() {
        assert_eq!(host_of("127.0.0.1:19001"), "127.0.0.1");
        assert_eq!(host_of("[::1]:19003"), "::1");
        assert_eq!(host_of("[::1]"), "::1");
        assert_eq!(host_of("example.com"), "example.com");
        assert_eq!(
            http_plan("http://[::1]/", 1, "1s", 1).lines().nth(1),
            Some("target: { allow: [\"::1\"] }")
        );
    }

    #[test]
    fn generated_plans_validate() {
        let limits = loadfleet_plan::Limits::default();
        let addrs = local_targets(&["127.0.0.1".into(), "::1".into()]);
        let p =
            loadfleet_plan::parse_and_validate(&tcp_plan(&addrs, 50_000, "60s"), &limits).unwrap();
        assert_eq!(p.scenario.endpoints().len(), 4);
        assert_eq!(p.load.stages[1].target, 50_000);
        let p = loadfleet_plan::parse_and_validate(
            &http_plan("http://[::1]:19011/", 50_000, "60s", 512),
            &limits,
        )
        .unwrap();
        assert_eq!(p.target.allow, ["::1"]);
    }
}
