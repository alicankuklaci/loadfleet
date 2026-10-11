use loadfleet_protocol::*;

fn plan() -> loadfleet_plan::Plan {
    loadfleet_plan::parse(
        r#"
name: p
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:1/" } }
load: { stages: [{target: 1, ramp: 0s, hold: 1s}] }
"#,
    )
    .unwrap()
}

#[test]
fn messages_roundtrip_with_type_tag() {
    let m = ServerMsg::Ping {
        id: 3,
        server_ms: 1000,
    };
    let s = encode(&m);
    assert_eq!(s, r#"{"t":"ping","id":3,"server_ms":1000}"#);
    assert_eq!(decode::<ServerMsg>(&s).unwrap(), m);
    let a = ServerMsg::Assign(Box::new(Assignment {
        run_id: "r".into(),
        plan: plan(),
        allow: vec!["127.0.0.0/8".into()],
        stage_targets: vec![1],
        vu_offset: 0,
        agent_index: 0,
        max_vus: 10,
        seed: 1,
        start_at_server_ms: 5,
        offset_ms: 1.5,
    }));
    let back = decode::<ServerMsg>(&encode(&a)).unwrap();
    assert_eq!(back, a);
    match back {
        ServerMsg::Assign(x) => assert_eq!(x.allow, vec!["127.0.0.0/8".to_string()]),
        _ => panic!(),
    }
    assert!(decode::<AgentMsg>(r#"{"t":"teleport"}"#).is_err());
}

#[test]
fn metrics_frame_carries_seq() {
    let m = AgentMsg::Metrics {
        run_id: "r".into(),
        seq: 7,
        rel_ms: 1000,
        snap: loadfleet_metrics::Snapshot::default(),
        health: Default::default(),
    };
    assert!(encode(&m).contains(r#""seq":7"#));
    assert_eq!(decode::<AgentMsg>(&encode(&m)).unwrap(), m);
}

#[test]
fn clock_offset_is_the_median() {
    let mut samples: Vec<ClockSample> = (0..7)
        .map(|i| {
            let sent = 10_000 + i * 100;
            ClockSample {
                sent_ms: sent,
                agent_ms: sent + 20 + 120,
                recv_ms: sent + 40,
            }
        })
        .collect();
    samples.push(ClockSample {
        sent_ms: 20_000,
        agent_ms: 20_140,
        recv_ms: 20_940,
    });
    let e = estimate(&samples).unwrap();
    assert!((e.offset_ms - 120.0).abs() <= 1.0, "{e:?}");
    assert!((e.rtt_ms - 40.0).abs() <= 1.0);
    assert_eq!(readiness(e.offset_ms), Readiness::Warn);
    assert_eq!(readiness(10.0), Readiness::Ready);
    assert_eq!(readiness(-1500.0), Readiness::NotReady);
    assert_eq!(server_to_agent_ms(50_000, 120.0), 50_120);
    assert!(estimate(&[]).is_none());
}

#[test]
fn estimate_uses_lower_rtt_half_of_eight_rounds() {
    // 4 clean rounds (offset 100, rtt 20); 4 stalled rounds with a skewed midpoint
    let mut v = vec![];
    for i in 0..4 {
        let s = 1000 + i * 100;
        v.push(ClockSample {
            sent_ms: s,
            agent_ms: s + 10 + 100,
            recv_ms: s + 20,
        });
    }
    for i in 0..4 {
        let s = 5000 + i * 100;
        v.push(ClockSample {
            sent_ms: s,
            agent_ms: s + 10 + 100,
            recv_ms: s + 800,
        });
    }
    let e = estimate(&v).unwrap();
    assert_eq!(e.offset_ms, 100.0);
    assert_eq!(e.rtt_ms, 20.0);
    assert_eq!(CLOCK_ROUNDS, 8);
}

#[test]
fn seq_guard_rejects_replays_and_reordering() {
    let mut g = SeqGuard::new();
    assert!(g.accept(1, 1000));
    assert!(!g.accept(1, 1000));
    assert!(!g.accept(2, 1000)); // same at_ms
    assert!(!g.accept(1, 2000)); // seq not increasing
    assert!(g.accept(2, 2000));
    assert!(!g.accept(3, 1500)); // time went backwards
}

#[test]
fn oversized_histograms_and_frames_are_refused() {
    let mut snap = loadfleet_metrics::Snapshot::default();
    snap.hists.insert(
        "lat".into(),
        "A".repeat(loadfleet_metrics::MAX_HIST_B64_BYTES + 1),
    );
    assert!(check_snapshot(&snap).is_err());
    assert!(loadfleet_metrics::decode_hist(&snap.hists["lat"]).is_err());
    snap.hists.clear();
    let h = loadfleet_metrics::new_hist();
    snap.hists
        .insert("lat".into(), loadfleet_metrics::encode_hist(&h));
    assert!(check_snapshot(&snap).is_ok());
    for i in 0..=MAX_HISTS_PER_SNAPSHOT {
        snap.hists.insert(format!("h{i}"), String::new());
    }
    assert!(check_snapshot(&snap).is_err());
    let big = " ".repeat(MAX_FRAME_BYTES + 1);
    assert!(decode::<AgentMsg>(&big).unwrap_err().contains("too large"));
}

#[test]
fn clock_edge_cases() {
    // negative RTT (wall clock stepped back) is discarded, not preferred
    let v = [
        ClockSample {
            sent_ms: 1000,
            agent_ms: 1110,
            recv_ms: 900,
        },
        ClockSample {
            sent_ms: 2000,
            agent_ms: 2110,
            recv_ms: 2020,
        },
    ];
    let e = estimate(&v).unwrap();
    assert_eq!((e.offset_ms, e.rtt_ms), (100.0, 20.0));
    assert!(estimate(&v[..1]).is_none());
    assert_eq!(server_to_agent_ms(i64::MAX, 1e18), i64::MAX);
    assert!(compatible(PROTOCOL_VERSION) && !compatible(PROTOCOL_VERSION + 1));
    assert!(try_encode(&AgentMsg::Heartbeat { agent_ms: 1 }).is_ok());
}

#[test]
fn snapshot_map_caps() {
    let mut snap = loadfleet_metrics::Snapshot::default();
    snap.ops.insert("x".repeat(MAX_NAME_BYTES + 1), 1);
    assert!(check_snapshot(&snap).is_err());
    snap.ops.clear();
    for i in 0..=MAX_SERIES_PER_MAP {
        snap.errors.insert(format!("e{i}"), 1);
    }
    assert!(check_snapshot(&snap).is_err());
}
