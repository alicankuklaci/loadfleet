//! Runs the node unit tests of the panel's series feed (`web/series.js`) when node is installed.
use std::process::Command;

#[test]
fn series_feed_js_tests() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let Ok(out) = Command::new("node")
        .arg("scripts/panel-js-test.mjs")
        .current_dir(root)
        .output()
    else {
        eprintln!("node not found: skipping scripts/panel-js-test.mjs");
        return;
    };
    assert!(
        out.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
