use loadfleet_metrics::{RunMeta, SeriesPoint, StageSummary};
use loadfleet_server::Store;
use std::collections::BTreeMap;

fn meta(id: &str, started: i64, status: &str) -> RunMeta {
    RunMeta {
        id: id.into(),
        name: "n".into(),
        plan_yaml: "name: n".into(),
        status: status.into(),
        started_ms: started,
        ..Default::default()
    }
}

fn pt(t: i64) -> SeriesPoint {
    SeriesPoint {
        t_s: t,
        ops: 10,
        errors: 1,
        gauges: BTreeMap::new(),
        latency: BTreeMap::new(),
        cpu_max: t as f64,
        lag_p95_max: 0.0,
        agents: 1,
    }
}

#[tokio::test]
async fn runs_stages_series_roundtrip() {
    let s = Store::memory().unwrap();
    s.create_run(&meta("a", 1000, "running")).await.unwrap();
    s.put_stage(
        "a",
        &StageSummary {
            index: 0,
            target: 5,
            total_ops: 50,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    s.put_series("a", &(1..=5).map(pt).collect::<Vec<_>>())
        .await
        .unwrap();
    let mut m = meta("a", 1000, "completed");
    m.verdict = "completed all stages up to 5".into();
    s.update_run(&m).await.unwrap();
    let r = s.report("a").await.unwrap().unwrap();
    assert_eq!(r.meta.status, "completed");
    assert_eq!(r.stages[0].total_ops, 50);
    assert_eq!(r.series.len(), 5);
    assert_eq!(s.series_since("a", 3).await.unwrap().len(), 2);
    assert_eq!(s.list_runs(10).await.unwrap()[0].id, "a");
    assert!(s.report("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn settings_agents_and_restart_recovery() {
    let s = Store::memory().unwrap();
    assert!(s.setting("agent_token").await.unwrap().is_none());
    s.set_setting("agent_token", "t1").await.unwrap();
    s.set_setting("agent_token", "t2").await.unwrap();
    assert_eq!(
        s.setting("agent_token").await.unwrap().as_deref(),
        Some("t2")
    );
    s.agent_seen("m5", "{}", 5).await.unwrap();
    s.set_saturation("m5", 21000).await.unwrap();
    assert_eq!(s.saturation("m5").await.unwrap(), Some(21000));
    s.create_run(&meta("r", 1, "running")).await.unwrap();
    assert_eq!(s.fail_unfinished_runs().await.unwrap(), 1);
    let m = s.report("r").await.unwrap().unwrap().meta;
    assert_eq!(m.status, "failed");
    assert_eq!(m.verdict, "server restarted during the run");
}

#[tokio::test]
async fn old_series_are_thinned_to_10s_buckets() {
    let s = Store::memory().unwrap();
    s.create_run(&meta("old", 1_000, "completed"))
        .await
        .unwrap();
    s.create_run(&meta("new", 9_000_000, "completed"))
        .await
        .unwrap();
    s.put_series("old", &(1..=25).map(pt).collect::<Vec<_>>())
        .await
        .unwrap();
    s.put_series("new", &(1..=25).map(pt).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(s.thin_series(5_000_000).await.unwrap(), 25);
    let old = s.report("old").await.unwrap().unwrap().series;
    assert_eq!(old.len(), 3);
    assert_eq!(old.iter().map(|p| p.ops).sum::<u64>(), 250);
    assert_eq!(old[0].cpu_max, 9.0);
    assert_eq!(s.report("new").await.unwrap().unwrap().series.len(), 25);
    // thinning twice is a no-op
    assert_eq!(s.thin_series(5_000_000).await.unwrap(), 0);
}

#[tokio::test]
async fn open_persists_and_unfinished_runs_are_not_thinned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lf.db");
    {
        let s = Store::open(&path).unwrap();
        s.create_run(&meta("live", 1_000, "running")).await.unwrap();
        s.put_series("live", &(1..=12).map(pt).collect::<Vec<_>>())
            .await
            .unwrap();
        assert_eq!(s.thin_series(5_000_000).await.unwrap(), 0);
    }
    let s = Store::open(&path).unwrap();
    assert_eq!(s.report("live").await.unwrap().unwrap().series.len(), 12);
}

fn corrupt(path: &std::path::Path, sql: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

#[tokio::test]
async fn corrupt_rows_are_never_hidden_or_destroyed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lf.db");
    let s = Store::open(&path).unwrap();
    s.create_run(&meta("bad", 1_000, "completed"))
        .await
        .unwrap();
    s.create_run(&meta("good", 2_000, "completed"))
        .await
        .unwrap();
    s.create_run(&meta("stuck", 3_000, "running"))
        .await
        .unwrap();
    for id in ["bad", "good"] {
        s.put_series(id, &(1..=12).map(pt).collect::<Vec<_>>())
            .await
            .unwrap();
    }
    corrupt(
        &path,
        "UPDATE series SET point_json = 'garbage' WHERE run_id = 'bad' AND t_s = 5;",
    );
    corrupt(&path, "UPDATE runs SET meta_json = '{' WHERE id = 'stuck';");

    // listed as a placeholder, not hidden
    let runs = s.list_runs(10).await.unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0].id, "stuck");
    assert_eq!(runs[0].verdict, "unreadable");
    assert_eq!(
        s.report("stuck").await.unwrap().unwrap().meta.verdict,
        "unreadable"
    );

    // thinning leaves the run with a corrupt point untouched, and still thins the good one
    assert_eq!(s.thin_series(5_000_000).await.unwrap(), 12);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM series WHERE run_id = 'bad' AND width_s = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 12);
    assert_eq!(s.report("good").await.unwrap().unwrap().series.len(), 2);

    // an unreadable running run is still failed on restart
    assert_eq!(s.fail_unfinished_runs().await.unwrap(), 1);
    let st: String = conn
        .query_row("SELECT status FROM runs WHERE id = 'stuck'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(st, "failed");
    assert_eq!(s.fail_unfinished_runs().await.unwrap(), 0);
}

#[tokio::test]
async fn update_unknown_run_is_an_error() {
    let s = Store::memory().unwrap();
    assert!(s.update_run(&meta("nope", 1, "failed")).await.is_err());
}

#[tokio::test]
async fn run_exists_reports_stored_ids() {
    let s = Store::memory().unwrap();
    assert!(!s.run_exists("abc123").await.unwrap());
    let m = loadfleet_metrics::RunMeta {
        id: "abc123".into(),
        status: "running".into(),
        ..Default::default()
    };
    s.create_run(&m).await.unwrap();
    assert!(s.run_exists("abc123").await.unwrap());
    // a second insert under the same id fails: the coordinator retries with a fresh id
    assert!(s.create_run(&m).await.is_err());
}

/// Plan-relative data (files a scenario plugin reads) resolves against the server's
/// data directory (the db file's directory), never the process's working directory.
#[test]
fn data_dir_is_the_absolute_directory_of_the_db() {
    let cfg = |db: &str| loadfleet_server::ServerConfig {
        db: db.into(),
        ..loadfleet_server::ServerConfig::default()
    };
    assert_eq!(
        cfg("/srv/loadfleet/lf.db").data_dir(),
        std::path::PathBuf::from("/srv/loadfleet")
    );
    let rel = cfg("loadfleet.db").data_dir();
    assert!(rel.is_absolute(), "{rel:?}");
    assert_eq!(rel, std::env::current_dir().unwrap());
    let nested = cfg("state/lf.db").data_dir();
    assert_eq!(nested, std::env::current_dir().unwrap().join("state"));
}
