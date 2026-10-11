use loadfleet_plan::*;
use std::time::Duration;

/// A typical plan's shape: closed model, two stages, stop rules, weights.
const CLOSED: &str = r#"
name: closed-ws
target: { allow: ["ws.example.com"] }
scenario: { type: ws, ws: { url: "wss://ws.example.com/ws" } }
load:
  model: closed
  stages: [{target: 2000, ramp: 40s, hold: 90s}, {target: 6000, ramp: 80s, hold: 90s}]
stop:  { error_rate: 0.02, p95_ms: { "move.ack": 500, "queue->found": 12000 } }
agents: { min: 2, weights: auto }
"#;

const HTTP: &str = r#"
name: get
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:8080/", think_ms: [0, 10] } }
load: { model: open, max_vus: 200, stages: [{target: 500, ramp: 5s, hold: 10s}] }
"#;

#[test]
fn parses_the_spec_example_shape() {
    let p = parse(CLOSED).unwrap();
    assert_eq!(p.scenario.kind(), "ws");
    assert_eq!(p.load.stages[1].ramp, Duration::from_secs(80));
    assert_eq!(p.stop.p95_ms["queue->found"], 12000.0);
    assert_eq!(p.agents.weights, Weights::Keyword(WeightKeyword::Auto));
    assert_eq!(p.total_duration(), Duration::from_secs(300));
    validate(&p, &Limits::default()).unwrap();
}

#[test]
fn json_and_yaml_roundtrip() {
    let p = parse(HTTP).unwrap();
    let again = parse(&serde_json::to_string(&p).unwrap()).unwrap();
    assert_eq!(p, again);
    assert_eq!(parse(&p.to_yaml()).unwrap(), p);
}

#[test]
fn rejects_unknown_fields() {
    let bad = HTTP.replace("max_vus", "max_vuz");
    assert!(parse(&bad).is_err());
}

#[test]
fn rejects_degenerate_plans() {
    let limits = Limits::default();
    let cases = [
        (
            HTTP.replace("stages: [{target: 500, ramp: 5s, hold: 10s}]", "stages: []"),
            "load.stages",
        ),
        (HTTP.replace("target: 500", "target: 0"), "load.stages"),
        (
            HTTP.replace("ramp: 5s, hold: 10s", "ramp: 0s, hold: 0s"),
            "load.stages[0]",
        ),
        (HTTP.replace("hold: 10s", "hold: 3h"), "load.stages"),
        (
            HTTP.replace("allow: [\"127.0.0.1\"]", "allow: []"),
            "target.allow",
        ),
        (
            HTTP.replace("allow: [\"127.0.0.1\"]", "allow: [\"10.0.0.1\"]"),
            "scenario",
        ),
        (
            HTTP.replace("http://127.0.0.1", "ftp://127.0.0.1"),
            "scenario.http.url",
        ),
        (CLOSED.replace("model: closed", "model: open"), "load.model"),
        (
            CLOSED.replace("error_rate: 0.02", "error_rate: 1.5"),
            "stop.error_rate",
        ),
    ];
    for (text, field) in cases {
        let errs = parse(&text)
            .map_err(|e| e.to_string())
            .and_then(|p| validate(&p, &limits).map_err(|v| format!("{v:?}")));
        let msg = errs.expect_err(&format!("expected error for {field}"));
        assert!(msg.contains(field), "error {msg} should mention {field}");
    }
}

#[test]
fn server_allow_list_bounds_the_plan() {
    let p = parse(HTTP).unwrap();
    let limits = Limits {
        server_allow: Some(AllowList::parse(&["example.com".into()]).unwrap()),
        ..Limits::default()
    };
    let errs = validate(&p, &limits).unwrap_err();
    assert!(errs.iter().any(|e| e.field == "target.allow[0]"));
}

#[test]
fn allow_rejects_bypass_spellings() {
    let a = AllowList::parse(&[
        "allowed.example".into(),
        "*.svc.example".into(),
        "10.0.0.0/8".into(),
    ])
    .unwrap();
    assert!(a.permits_host("ALLOWED.example."));
    assert!(a.permits_host("api.svc.example"));
    assert!(!a.permits_host("svc.example"));
    assert!(!a.permits_host("evilsvc.example"));
    assert!(!a.permits_host("allowed.example.evil.com"));
    assert!(a.permits_host("10.1.2.3"));
    assert!(!a.permits_host("11.1.2.3"));
    assert_eq!(
        AllowList::host_of("http://allowed.example@evil.com/x").unwrap(),
        "evil.com"
    );
    assert_eq!(AllowList::host_of("[::1]:9000").unwrap(), "::1");
    assert_eq!(
        AllowList::host_of("allowed.example:443").unwrap(),
        "allowed.example"
    );
    assert!(!a.permits_host("::1"));
    let server = AllowList::parse(&["*.svc.example".into()]).unwrap();
    assert!(server.covers_entry("a.svc.example"));
    assert!(server.covers_entry("*.b.svc.example"));
    assert!(!server.covers_entry("*.example"));
}
#[test]
fn bundled_examples_parse_and_validate() {
    for f in ["http-get", "http-post-json", "tcp-hold", "ws-echo"] {
        let text = std::fs::read_to_string(format!(
            "{}/../../examples/{f}.yaml",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let p = parse(&text).unwrap_or_else(|e| panic!("{f}: {e}"));
        validate(&p, &Limits::default()).unwrap_or_else(|e| panic!("{f}: {e:?}"));
    }
}

fn first_error(text: &str) -> String {
    match parse_and_validate(text, &Limits::default()) {
        Ok(_) => String::new(),
        Err(v) => format!("{v:?}"),
    }
}

const TCP: &str = r#"
name: t
target: { allow: ["127.0.0.1"] }
scenario: { type: tcp, tcp: { addr: "127.0.0.1:9", expect: { hex: "0a0b" } } }
load: { stages: [{target: 1, ramp: 1s, hold: 1s}] }
"#;
const WS: &str = r#"
name: w
target: { allow: ["127.0.0.1"] }
scenario: { type: ws, ws: { url: "ws://127.0.0.1/ws", steps: [{expect: {regex: "^ok"}}] } }
load: { stages: [{target: 1, ramp: 1s, hold: 1s}] }
"#;

#[test]
fn rejects_bad_regex_hex_method_and_headers() {
    assert_eq!(first_error(TCP), "");
    assert_eq!(first_error(WS), "");
    let cases = [
        (
            TCP.replace("hex: \"0a0b\"", "hex: \"\""),
            "scenario.tcp.expect.hex",
        ),
        (TCP.replace("0a0b", "0a0"), "scenario.tcp.expect.hex"),
        (TCP.replace("0a0b", "zz"), "scenario.tcp.expect.hex"),
        (
            TCP.replace("hex: \"0a0b\"", "regex: \"(\""),
            "scenario.tcp.expect.regex",
        ),
        (
            TCP.replace("expect:", "send: {hex: \"abc\"}, expect:"),
            "scenario.tcp.send",
        ),
        (
            WS.replace("regex: \"^ok\"", "regex: \"(\""),
            "scenario.ws.steps[0].expect.regex",
        ),
        (
            WS.replace("regex: \"^ok\"", "hex: \"00\""),
            "scenario.ws.steps[0].expect.hex",
        ),
        (
            WS.replace("regex: \"^ok\"", "hex: \"\""),
            "scenario.ws.steps[0].expect.hex",
        ),
        (
            HTTP.replace("url:", "method: \"GE T\", url:"),
            "scenario.http.method",
        ),
        (
            HTTP.replace("url:", "method: \"\", url:"),
            "scenario.http.method",
        ),
        (
            HTTP.replace("url:", "headers: {\"bad name\": x}, url:"),
            "scenario.http.headers",
        ),
        (
            HTTP.replace("url:", "headers: {x: \"a\\r\\nb: c\"}, url:"),
            "scenario.http.headers.x",
        ),
        (
            WS.replace("steps:", "headers: {\"a:b\": x}, steps:"),
            "scenario.ws.headers",
        ),
    ];
    for (text, field) in cases {
        let msg = first_error(&text);
        assert!(msg.contains(field), "{msg:?} should mention {field}");
    }
}

#[test]
fn parse_and_validate_reports_parse_errors_as_field_errors() {
    let errs = parse_and_validate("name: [", &Limits::default()).unwrap_err();
    assert_eq!(errs[0].field, "plan");
    assert!(parse_and_validate(HTTP, &Limits::default()).is_ok());
}

#[test]
fn intersect_keeps_only_what_both_lists_cover() {
    let plan = AllowList::parse(&[
        "a.svc.example".into(),
        "*.svc.example".into(),
        "other.example".into(),
        "10.1.0.0/16".into(),
    ])
    .unwrap();
    let server = AllowList::parse(&["*.svc.example".into(), "10.0.0.0/8".into()]).unwrap();
    let both = plan.intersect(&server);
    assert!(both.permits_host("a.svc.example"));
    assert!(both.permits_host("b.svc.example"));
    assert!(both.permits_host("10.1.2.3"));
    assert!(!both.permits_host("10.2.0.1"));
    assert!(!both.permits_host("other.example"));
    assert!(AllowList::default().intersect(&server).is_empty());
}

#[test]
fn allow_rejects_legacy_numeric_host_spellings() {
    for bad in ["2130706433", "127.1", "0x7f.1", "1"] {
        assert!(AllowList::parse(&[bad.into()]).is_err(), "{bad}");
    }
    assert!(
        AllowList::parse(&["127.0.0.1".into(), "10.0.0.0/8".into(), "a1.example".into()]).is_ok()
    );
}

#[test]
fn rejects_overflow_empty_lists_and_non_finite_numbers() {
    let huge = HTTP.replace(
        "ramp: 5s, hold: 10s",
        "ramp: 400000000000y, hold: 400000000000y",
    );
    let msg = first_error(&huge);
    assert!(msg.contains("load.stages"), "{msg}");
    let cases = [
        (
            TCP.replace("addr: \"127.0.0.1:9\"", "addr: []"),
            "scenario.tcp.addr",
        ),
        (
            TCP.replace("expect:", "conn_rate: .nan, expect:"),
            "scenario.tcp.conn_rate",
        ),
        (
            TCP.replace("expect:", "conn_rate: 0, expect:"),
            "scenario.tcp.conn_rate",
        ),
        (
            CLOSED.replace("\"move.ack\": 500", "\"move.ack\": .nan"),
            "stop.p95_ms",
        ),
        (
            CLOSED.replace("error_rate: 0.02", "error_rate: .nan"),
            "stop.error_rate",
        ),
        (
            CLOSED.replace("weights: auto", "weights: {a: -1}"),
            "agents.weights",
        ),
        (
            CLOSED.replace("weights: auto", "weights: {a: .nan}"),
            "agents.weights",
        ),
        (
            CLOSED.replace("weights: auto", "weights: {a: 0}"),
            "agents.weights",
        ),
        (
            CLOSED.replace("weights: auto", "weights: {}"),
            "agents.weights",
        ),
    ];
    for (text, field) in cases {
        let msg = first_error(&text);
        assert!(msg.contains(field), "{msg:?} should mention {field}");
    }
    assert_eq!(
        first_error(&CLOSED.replace("weights: auto", "weights: {a: 1, b: 2}")),
        ""
    );
}

#[test]
fn hostile_endpoints_are_rejected_through_validate() {
    let cases = [
        HTTP.replace("http://127.0.0.1:8080/", "http://127.0.0.1@evil.com/"),
        HTTP.replace("127.0.0.1:8080", "2130706433.evil.com"),
        HTTP.replace("127.0.0.1:8080", "[::1]:8080"),
        HTTP.replace("http://127.0.0.1:8080/", "http://127.0.0.1.evil.com./"),
        HTTP.replace("http://127.0.0.1:8080/", "http://EVIL.com./"),
        WS.replace("ws://127.0.0.1/ws", "ws://127.0.0.1@evil.com/ws"),
        TCP.replace("127.0.0.1:9", "evil.com:9"),
        TCP.replace("127.0.0.1:9", "[::1]:9"),
    ];
    for text in cases {
        let msg = first_error(&text);
        assert!(
            msg.contains("scenario"),
            "should be rejected: {text}\n{msg}"
        );
    }
    // legacy numeric spelling in a URL is normalised to the real host, and so is allowed only if listed
    assert_eq!(
        first_error(&HTTP.replace("127.0.0.1:8080", "2130706433:8080")),
        ""
    );
}
