use assert_cmd::Command;

fn write_plan(dir: &std::path::Path, url: &str, stop: &str) -> std::path::PathBuf {
    let path = dir.join("plan.yaml");
    std::fs::write(
        &path,
        format!(
            r#"
name: slice
target: {{ allow: ["127.0.0.1"] }}
scenario: {{ type: http, http: {{ url: "{url}" }} }}
load: {{ model: closed, stages: [{{target: 5, ramp: 0s, hold: 2s}}, {{target: 10, ramp: 1s, hold: 1s}}] }}
{stop}
"#
        ),
    )
    .unwrap();
    path
}

#[test]
fn run_writes_reports_and_exits_zero() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _) = rt
        .block_on(loadfleet_testserver::spawn_http("127.0.0.1:0"))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plan = write_plan(dir.path(), &format!("http://{addr}/"), "");
    let out = dir.path().join("out");
    Command::cargo_bin("loadfleet")
        .unwrap()
        .args([
            "run",
            plan.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--quiet",
        ])
        .assert()
        .code(0);
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["stages"].as_array().unwrap().len(), 2);
    assert!(report["stages"][1]["total_ops"].as_u64().unwrap() > 0);
    assert_eq!(report["stages"][0]["error_rate"].as_f64().unwrap(), 0.0);
    assert_eq!(report["meta"]["model"], "closed");
    assert!(report["meta"]["seed"].as_u64().is_some());
    assert!(std::fs::read_to_string(out.join("report.md"))
        .unwrap()
        .contains("**verdict:** completed all stages up to 10"));
    assert!(
        std::fs::read_to_string(out.join("series.csv"))
            .unwrap()
            .lines()
            .count()
            >= 4
    );
}

#[test]
fn stop_rule_exit_code_two_and_invalid_plan_exit_one() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, _) = rt
        .block_on(loadfleet_testserver::spawn_http("127.0.0.1:0"))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plan = write_plan(
        dir.path(),
        &format!("http://{addr}/status/500"),
        "stop: { error_rate: 0.01 }",
    );
    Command::cargo_bin("loadfleet")
        .unwrap()
        .args([
            "run",
            plan.to_str().unwrap(),
            "--out",
            dir.path().join("o").to_str().unwrap(),
            "--quiet",
        ])
        .assert()
        .code(2);
    let bad = write_plan(dir.path(), "http://10.9.9.9/", "");
    let assert = Command::cargo_bin("loadfleet")
        .unwrap()
        .args(["run", bad.to_str().unwrap()])
        .assert()
        .code(1);
    assert!(String::from_utf8_lossy(&assert.get_output().stderr).contains("not in target.allow"));
}

#[test]
fn local_allow_that_excludes_the_target_refuses_the_run() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (addr, hits) = rt
        .block_on(loadfleet_testserver::spawn_http("127.0.0.1:0"))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plan = write_plan(dir.path(), &format!("http://{addr}/"), "");
    let assert = Command::cargo_bin("loadfleet")
        .unwrap()
        .args([
            "run",
            plan.to_str().unwrap(),
            "--out",
            dir.path().join("o").to_str().unwrap(),
            "--allow",
            "example.com",
            "--quiet",
        ])
        .assert()
        .code(1);
    assert!(
        String::from_utf8_lossy(&assert.get_output().stderr).contains("not permitted by --allow")
    );
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!dir.path().join("o").exists());
}
