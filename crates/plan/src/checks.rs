//! Static scenario checks that need no network: HTTP method / header syntax, regex and hex
//! literals. Running them here means a bad plan fails at validation, with a field path, instead
//! of failing every agent when it builds the scenario.
use crate::{Expect, Payload, Scenario, WsStep};
use std::collections::BTreeMap;

type Err<'a> = &'a mut dyn FnMut(&str, String);

pub(crate) fn check_scenario(s: &Scenario, err: Err<'_>) {
    match s {
        Scenario::Http { http } => {
            if !is_token(&http.method) {
                err(
                    "scenario.http.method",
                    format!("{:?} is not a valid HTTP method", http.method),
                );
            }
            headers("scenario.http.headers", &http.headers, err);
        }
        Scenario::Tcp { tcp } => {
            if let Some(Payload::Hex(h)) = &tcp.send {
                if let Err(e) = check_hex(h) {
                    err("scenario.tcp.send", e);
                }
            }
            if let Some(e) = &tcp.expect {
                expect("scenario.tcp.expect", e, true, err);
            }
        }
        Scenario::Ws { ws } => {
            headers("scenario.ws.headers", &ws.headers, err);
            for (name, list) in [("on_open", &ws.on_open), ("steps", &ws.steps)] {
                for (i, step) in list.iter().enumerate() {
                    if let WsStep::Expect(e) = step {
                        expect(&format!("scenario.ws.{name}[{i}].expect"), e, false, err);
                    }
                }
            }
        }
    }
}

/// `bytes` selects `regex::bytes` (tcp) over `regex` (ws), as the scenarios compile them.
fn expect(field: &str, e: &Expect, bytes: bool, err: Err<'_>) {
    if let Some(h) = &e.hex {
        if !bytes {
            err(
                &format!("{field}.hex"),
                "hex is not supported for websocket messages; use regex".into(),
            );
        } else if let Err(m) = check_hex(h) {
            err(&format!("{field}.hex"), m);
        }
    }
    if let Some(r) = &e.regex {
        let res = if bytes {
            regex::bytes::Regex::new(r).map(|_| ())
        } else {
            regex::Regex::new(r).map(|_| ())
        };
        if let Err(m) = res {
            err(&format!("{field}.regex"), m.to_string());
        }
    }
}

/// Non-empty, even number of hex digits (what `hex::decode` accepts, minus the empty string,
/// which would match everywhere or nowhere).
fn check_hex(h: &str) -> Result<(), String> {
    if h.is_empty() {
        return Err("hex must not be empty".into());
    }
    if h.len() % 2 != 0 {
        return Err("hex must have an even number of digits".into());
    }
    if !h.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("hex may contain only 0-9 and a-f".into());
    }
    Ok(())
}

fn headers(field: &str, map: &BTreeMap<String, String>, err: Err<'_>) {
    for (k, v) in map {
        if !is_token(k) {
            err(&format!("{field}.{k}"), "invalid header name".into());
        }
        if v.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f) {
            err(
                &format!("{field}.{k}"),
                "header value must not contain control characters".into(),
            );
        }
    }
}

/// RFC 9110 token: valid for methods and header names.
fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}
