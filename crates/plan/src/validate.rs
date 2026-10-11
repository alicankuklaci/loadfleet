use crate::{AllowList, Model, Plan, Scenario, Weights};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for FieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_duration: Duration,
    pub server_allow: Option<AllowList>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_duration: Duration::from_secs(7200),
            server_allow: None,
        }
    }
}

pub fn validate(p: &Plan, limits: &Limits) -> Result<(), Vec<FieldError>> {
    let mut errs = Vec::new();
    let mut err = |field: &str, message: String| {
        errs.push(FieldError {
            field: field.into(),
            message,
        })
    };

    if p.name.is_empty()
        || p.name.len() > 80
        || !p
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    {
        err("name", "1-80 chars of [A-Za-z0-9._-]".into());
    }

    // allow-list
    let allow = match AllowList::parse(&p.target.allow) {
        Ok(a) if !a.is_empty() => Some(a),
        Ok(_) => {
            err("target.allow", "must list at least one host/IP".into());
            None
        }
        Err(e) => {
            err("target.allow", e);
            None
        }
    };
    if let Some(server) = &limits.server_allow {
        for (i, e) in p.target.allow.iter().enumerate() {
            if !server.covers_entry(e) {
                err(
                    &format!("target.allow[{i}]"),
                    format!("{e} is not allowed by the server config"),
                );
            }
        }
    }
    for (field, endpoint) in p.scenario.endpoints() {
        match AllowList::host_of(&endpoint) {
            Err(e) => err(&field, e),
            Ok(host) => {
                if let Some(a) = &allow {
                    if !a.permits_host(&host) {
                        err(
                            "scenario",
                            format!("{field} host {host} is not in target.allow"),
                        );
                    }
                }
                if let Some(s) = &limits.server_allow {
                    if !s.permits_host(&host) {
                        err(
                            "scenario",
                            format!("{field} host {host} is not allowed by the server"),
                        );
                    }
                }
            }
        }
    }
    check_schemes(p, &mut err);
    if let Scenario::Tcp { tcp } = &p.scenario {
        if tcp.addrs.list().is_empty() {
            err("scenario.tcp.addr", "at least one host:port".into());
        }
        if let Some(r) = tcp.conn_rate {
            if !(r > 0.0 && r.is_finite()) {
                err(
                    "scenario.tcp.conn_rate",
                    "must be a finite number > 0".into(),
                );
            }
        }
    }
    if let Weights::Manual(m) = &p.agents.weights {
        if m.is_empty()
            || m.values().any(|w| !(*w >= 0.0 && w.is_finite()))
            || m.values().sum::<f64>() <= 0.0
        {
            err(
                "agents.weights",
                "manual weights must be finite, >= 0 and not all zero".into(),
            );
        }
    }
    crate::checks::check_scenario(&p.scenario, &mut err);

    // stages
    if p.load.stages.is_empty() {
        err("load.stages", "at least one stage".into());
    }
    if !p.load.stages.is_empty() && p.load.stages.iter().all(|s| s.target == 0) {
        err("load.stages", "every stage has target 0".into());
    }
    for (i, s) in p.load.stages.iter().enumerate() {
        if s.ramp.is_zero() && s.hold.is_zero() {
            err(
                &format!("load.stages[{i}]"),
                "ramp + hold must be > 0".into(),
            );
        }
    }
    let max = limits.max_duration.min(p.stop.max_duration);
    if p.total_duration() > max {
        err(
            "load.stages",
            format!("total {:?} exceeds the {:?} limit", p.total_duration(), max),
        );
    }
    if p.load.model == Model::Open {
        // the open model needs one-shot operations; only the http scenario has them
        if !matches!(p.scenario, Scenario::Http { .. }) {
            err(
                "load.model",
                format!(
                    "the {} scenario runs closed only (open model is http-only)",
                    p.scenario.kind()
                ),
            );
        }
        if p.load.max_vus == Some(0) {
            err("load.max_vus", "must be > 0".into());
        }
    }

    // stop rules
    if let Some(r) = p.stop.error_rate {
        if !(r > 0.0 && r <= 1.0) {
            err("stop.error_rate", "must be in (0, 1]".into());
        }
    }
    for (k, v) in &p.stop.p95_ms {
        if k.is_empty() || !(*v > 0.0 && v.is_finite()) {
            err("stop.p95_ms", format!("{k}: limit must be > 0"));
        }
    }
    if p.agents.min == 0 {
        err("agents.min", "must be >= 1".into());
    }
    for i in &p.exclude_stages {
        if *i >= p.load.stages.len() {
            err("exclude_stages", format!("stage {i} does not exist"));
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

fn check_schemes(p: &Plan, err: &mut impl FnMut(&str, String)) {
    let scheme = |u: &str| u.split("://").next().unwrap_or("").to_ascii_lowercase();
    match &p.scenario {
        Scenario::Http { http } if !matches!(scheme(&http.url).as_str(), "http" | "https") => {
            err("scenario.http.url", "scheme must be http or https".into())
        }
        Scenario::Ws { ws } if !matches!(scheme(&ws.url).as_str(), "ws" | "wss") => {
            err("scenario.ws.url", "scheme must be ws or wss".into())
        }
        _ => {}
    }
}
