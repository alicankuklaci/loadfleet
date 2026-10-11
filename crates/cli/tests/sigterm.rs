//! `docker stop` and systemd send SIGTERM: the server, the agent and `loadfleet run` shut down
//! the same graceful way as on Ctrl-C (exit code 0 / a written report) instead of being killed.
#![cfg(unix)]
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn bin() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin("loadfleet"))
}

/// Waits until a stderr line contains `ready`, then sends SIGTERM and returns the exit status.
fn term_after(mut child: Child, ready: &str) -> std::process::ExitStatus {
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let mut seen = Vec::new();
    loop {
        match lines.next() {
            Some(Ok(l)) if l.contains(ready) => break,
            Some(Ok(l)) => seen.push(l),
            _ => {
                let _ = child.kill();
                panic!("no {ready:?} line: {seen:?}");
            }
        }
    }
    // keep draining stderr so the child never blocks on a full pipe
    std::thread::spawn(move || lines.for_each(drop));
    let ok = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(ok.success());
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(st) = child.try_wait().unwrap() {
            return st;
        }
        if Instant::now() > end {
            let _ = child.kill();
            panic!("still running 10 s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn server_exits_cleanly_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let child = bin()
        .args(["server", "--listen", "127.0.0.1:0", "--allow", "127.0.0.1"])
        .arg("--db")
        .arg(dir.path().join("s.db"))
        .env("LOADFLEET_ADMIN_PASSWORD", "sigterm-test-password")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let st = term_after(child, "agent token:");
    assert_eq!(st.code(), Some(0), "{st:?}");
}

#[test]
fn agent_exits_cleanly_on_sigterm() {
    let child = bin()
        .args([
            "agent",
            "--server",
            "ws://127.0.0.1:9",
            "--token",
            "t",
            "--name",
            "a1",
        ])
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    // the first failed dial is logged; the agent then keeps retrying until stopped
    let st = term_after(child, "a1");
    assert_eq!(st.code(), Some(0), "{st:?}");
}

#[test]
fn run_writes_its_report_on_sigterm() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _) = rt
        .block_on(loadfleet_testserver::spawn_http("127.0.0.1:0"))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("plan.yaml");
    std::fs::write(
        &plan,
        format!(
            r#"
name: long
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: http, http: {{ url: "http://{addr}/" }} }}
load: {{ model: closed, stages: [{{target: 2, ramp: 0s, hold: 60s}}] }}
"#
        ),
    )
    .unwrap();
    let out = dir.path().join("out");
    let child = bin()
        .arg("run")
        .arg(&plan)
        .arg("--out")
        .arg(&out)
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    // the first per-second progress line: the run is under way
    let st = term_after(child, "vus=");
    assert_eq!(st.code(), Some(130), "interrupted, like Ctrl-C: {st:?}");
    assert!(out.join("report.json").exists());
}
