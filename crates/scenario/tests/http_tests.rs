use loadfleet_engine::{execute, CancellationToken, Schedule, Work};
use loadfleet_metrics::Recorder;
use loadfleet_plan::{parse, Model, Stage};
use loadfleet_scenario::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

fn plan_yaml(url: &str, allow: &str, model: &str, extra: &str) -> String {
    format!(
        r#"
name: t
target: {{ allow: [{allow}] }}
scenario: {{ type: http, http: {{ url: "{url}" {extra} }} }}
load: {{ model: {model}, max_vus: 20, stages: [{{target: 5, ramp: 0s, hold: 1s}}] }}
"#
    )
}

async fn run(yaml: &str, stages: &[Stage], model: Model) -> loadfleet_metrics::Snapshot {
    let p = parse(yaml).unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    let s = build(
        &p.scenario,
        BuildCtx {
            guard,
            run_id: "r1".into(),
            agent: "a".into(),
            max_vus: 20,
        },
    )
    .unwrap();
    let rec = Recorder::new();
    let w = Work {
        schedule: Schedule::new(stages),
        model,
        max_vus: 20,
        vu_offset: 0,
        id_prefix: "run-r1-a".into(),
        seed: 1,
    };
    execute(w, s, rec.clone(), CancellationToken::new(), Instant::now())
        .await
        .unwrap();
    rec.snapshot(0)
}

fn hold(target: u64, ms: u64) -> Vec<Stage> {
    vec![Stage {
        target,
        ramp: Duration::ZERO,
        hold: Duration::from_millis(ms),
    }]
}

/// Runs a 1-VU closed plan for 300 ms against `url` with `guard`.
async fn run_guarded(url: &str, guard: AllowGuard) -> loadfleet_metrics::Snapshot {
    let p = parse(&plan_yaml(url, r#""127.0.0.1""#, "closed", "")).unwrap();
    let s = build(
        &p.scenario,
        BuildCtx {
            guard,
            run_id: "r".into(),
            agent: "a".into(),
            max_vus: 1,
        },
    )
    .unwrap();
    let rec = Recorder::new();
    let w = Work {
        schedule: Schedule::new(&hold(1, 300)),
        model: Model::Closed,
        max_vus: 1,
        vu_offset: 0,
        id_prefix: "x".into(),
        seed: 1,
    };
    execute(w, s, rec.clone(), CancellationToken::new(), Instant::now())
        .await
        .unwrap();
    rec.snapshot(0)
}

#[test]
fn templates_render_and_reject_unknown() {
    std::env::set_var("LOADFLEET_VAR_TEST_TOKEN", "s3cret");
    let t = Template::compile("{{vu}}/{{iter}}/{{run}}/{{agent}}/{{env.LOADFLEET_VAR_TEST_TOKEN}}")
        .unwrap()
        .with_env()
        .unwrap();
    let v = Vars {
        vu_id: "run-r1-a-3",
        iter: 9,
        run: "r1",
        agent: "a",
    };
    assert_eq!(t.render(&v), "run-r1-a-3/9/r1/a/s3cret");
    assert_eq!(Template::compile("{{uuid}}").unwrap().render(&v).len(), 36);
    assert!(Template::compile("{{env.LOADFLEET_VAR_MISSING_X}}")
        .unwrap()
        .with_env()
        .is_err());
}

#[test]
fn env_templates_only_reach_loadfleet_var_names() {
    // whatever the agent host has in its environment stays there unless it is meant for plans
    std::env::set_var("LF_TEST_HOST_SECRET", "do-not-send");
    for name in [
        "LF_TEST_HOST_SECRET",
        "AWS_SECRET_ACCESS_KEY",
        "LOADFLEET_TOKEN",
        "LOADFLEET_VAR_",
    ] {
        let e = Template::compile(&format!("{{{{env.{name}}}}}")).unwrap_err();
        assert!(e.contains("LOADFLEET_VAR_"), "{name}: {e}");
    }
}

#[test]
fn unknown_template_var_is_error() {
    let p = parse(&plan_yaml(
        "http://127.0.0.1:1/",
        r#""127.0.0.1""#,
        "closed",
        r#", body: "{{nope}}""#,
    ))
    .unwrap();
    let errs = check_plan(&p).unwrap_err();
    assert_eq!(errs[0].field, "scenario.http.body");
    let p = parse(&plan_yaml(
        "http://{{vu}}.example/",
        r#""*.example""#,
        "closed",
        "",
    ))
    .unwrap();
    assert_eq!(check_plan(&p).unwrap_err()[0].field, "scenario.http.url");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_closed_against_echo() {
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let snap = run(
        &plan_yaml(&format!("http://{addr}/"), r#""127.0.0.1""#, "closed", ""),
        &hold(5, 1000),
        Model::Closed,
    )
    .await;
    let served = hits.load(Ordering::SeqCst);
    assert!(served > 50, "served {served}");
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
    // a request cut by the end-of-stage cancel may be counted on one side only
    assert!(
        snap.ops["http"].abs_diff(served) <= 5,
        "ops {} vs served {served}",
        snap.ops["http"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_status_and_open_model() {
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let yaml = plan_yaml(
        &format!("http://{addr}/status/503"),
        r#""127.0.0.1""#,
        "open",
        "",
    );
    let snap = run(&yaml, &hold(200, 1000), Model::Open).await;
    let n = hits.load(Ordering::SeqCst);
    assert!((180..=210).contains(&n), "served {n}");
    assert!(
        snap.errors["http:status:503"] + 2 >= n,
        "{:?} vs {n}",
        snap.errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guard_rechecks_resolved_host() {
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("http://localhost:{}/", addr.port());
    // the plan names "localhost" but this agent only allows 127.0.0.1: every connect must be refused locally
    let local = loadfleet_plan::AllowList::parse(&["127.0.0.1".into()]).unwrap();
    let guard = AllowGuard::new(&["localhost".into()], Some(&local)).unwrap();
    let snap = run_guarded(&url, guard).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(
        snap.errors.contains_key("blocked:localhost"),
        "{:?}",
        snap.errors
    );

    // the name is allowed, but it resolves to loopback, which no IP/CIDR entry covers:
    // refused after DNS (a permitted name must not reach loopback/link-local/metadata addresses)
    let guard = AllowGuard::new(&["localhost".into()], None).unwrap();
    let snap = run_guarded(&url, guard).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(
        snap.errors.contains_key("blocked:localhost"),
        "{:?}",
        snap.errors
    );

    // name and address both allowed: load flows
    let guard = AllowGuard::new(&["localhost".into(), "127.0.0.1".into()], None).unwrap();
    let snap = run_guarded(&url, guard).await;
    assert!(hits.load(Ordering::SeqCst) > 0);
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guard_layers_plan_server_and_local() {
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    // the plan allows 127.0.0.1 but the server's list does not: the agent refuses it
    let server = loadfleet_plan::AllowList::parse(&["10.0.0.0/8".into()]).unwrap();
    let guard = AllowGuard::layered(&["127.0.0.1".into()], Some(&server), None).unwrap();
    let snap = run_guarded(&format!("http://{addr}/"), guard).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(
        snap.errors.contains_key("blocked:127.0.0.1"),
        "{:?}",
        snap.errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_stop_closes_pooled_connections_fast() {
    let (addr, stats) = loadfleet_testserver::spawn_http_stats("127.0.0.1:0")
        .await
        .unwrap();
    let p = parse(&plan_yaml(
        &format!("http://{addr}/delay/5000"),
        r#""127.0.0.1""#,
        "open",
        ", timeout: 10s",
    ))
    .unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    let s = build(
        &p.scenario,
        BuildCtx {
            guard,
            run_id: "r".into(),
            agent: "a".into(),
            max_vus: 20,
        },
    )
    .unwrap();
    let rec = Recorder::new();
    let w = Work {
        schedule: Schedule::new(&hold(50, 10_000)),
        model: Model::Open,
        max_vus: 20,
        vu_offset: 0,
        id_prefix: "x".into(),
        seed: 1,
    };
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        c.cancel();
    });
    let t0 = std::time::Instant::now();
    let out = execute(w, s, rec.clone(), cancel, Instant::now())
        .await
        .unwrap();
    assert!(out.cancelled);
    assert!(stats.requests.load(Ordering::SeqCst) > 0);
    // ops observe OpCtx::cancel and drop their pooled connection at once (no 800 ms abort)
    assert!(
        t0.elapsed() < Duration::from_millis(700),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(rec.snapshot(0).counters.get("open.aborted"), None);
    // and the target sees every socket closed within 1 s of Stop
    let deadline = std::time::Instant::now() + Duration::from_millis(600);
    while stats.open.load(Ordering::SeqCst) > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(stats.open.load(Ordering::SeqCst), 0);
}

/// A server closing keep-alive connections normally must cost no errors and no extra ops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_close_is_not_an_error() {
    let (addr, hits) = loadfleet_testserver::spawn_http("127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("http://{addr}/close");
    for model in [Model::Closed, Model::Open] {
        let before = hits.load(Ordering::SeqCst);
        let m = if model == Model::Closed {
            "closed"
        } else {
            "open"
        };
        let (target, ms) = if model == Model::Closed {
            (2, 700)
        } else {
            (200, 700)
        };
        let snap = run(
            &plan_yaml(&url, r#""127.0.0.1""#, m, ""),
            &hold(target, ms),
            model,
        )
        .await;
        let served = hits.load(Ordering::SeqCst) - before;
        assert!(served > 20, "{m}: served {served}");
        assert_eq!(snap.total_errors(), 0, "{m}: {:?}", snap.errors);
        assert!(
            snap.ops["http"].abs_diff(served) <= 2,
            "{m}: ops {} vs served {served}",
            snap.ops["http"]
        );
    }
}

/// Keep-alive response, then the server closes the idle socket (no `Connection: close`):
/// the client must notice at checkout and reconnect silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_close_is_not_an_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let h = hits.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let h = h.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                h.fetch_add(1, Ordering::SeqCst);
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                    .await;
                // idle close right after the response, as an LB idle timeout would
            });
        }
    });
    let snap = run(
        &plan_yaml(
            &format!("http://{addr}/"),
            r#""127.0.0.1""#,
            "closed",
            ", think_ms: 20",
        ),
        &hold(2, 700),
        Model::Closed,
    )
    .await;
    let served = hits.load(Ordering::SeqCst);
    assert!(served > 20, "served {served}");
    assert_eq!(snap.total_errors(), 0, "{:?}", snap.errors);
    assert!(
        snap.ops["http"].abs_diff(served) <= 2,
        "ops {} vs served {served}",
        snap.ops["http"]
    );
}

#[test]
fn ws_template_errors_name_their_own_list() {
    let p = parse(
        r#"
name: t
target: { allow: ["127.0.0.1"] }
scenario: { type: ws, ws: { url: "ws://127.0.0.1:1/ws", on_open: [{send: "a"}, {send: "b"}], steps: [{send: "{{nope}}"}] } }
load: { model: closed, stages: [{target: 1, ramp: 0s, hold: 1s}] }
"#,
    )
    .unwrap();
    assert_eq!(check_plan(&p).unwrap_err()[0].field, "scenario.ws.steps[0]");
}

#[test]
fn bad_header_is_a_build_error() {
    let p = parse(&plan_yaml(
        "http://127.0.0.1:1/",
        r#""127.0.0.1""#,
        "closed",
        r#", headers: { "bad name": "x" }"#,
    ))
    .unwrap();
    let guard = AllowGuard::new(&p.target.allow, None).unwrap();
    let ctx = BuildCtx {
        guard,
        run_id: "r".into(),
        agent: "a".into(),
        max_vus: 1,
    };
    let err = build(&p.scenario, ctx).err().expect("must fail");
    assert!(err.contains("scenario.http.headers.bad name"), "{err}");
}
