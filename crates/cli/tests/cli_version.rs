use assert_cmd::Command;

#[test]
fn version_subcommand_prints_name_and_version() {
    let out = Command::cargo_bin("loadfleet")
        .unwrap()
        .arg("version")
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert_eq!(
        stdout.trim(),
        format!("loadfleet {}", env!("CARGO_PKG_VERSION"))
    );
}
