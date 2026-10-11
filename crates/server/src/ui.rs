//! HTML fragments for the panel's htmx calls. Every value that came from an agent, a plan, a
//! stored run or an error is passed through `esc`; plan texts are never rendered.
use crate::api::{plan_errors, start_error_status};
use crate::{cloudflare_expression, AppState, StartError};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use loadfleet_metrics::{metric_names, RunReport};
use loadfleet_protocol::Readiness;
use serde::Deserialize;
use std::fmt::Write;
use std::net::IpAddr;

/// HTML-escapes text for element content and quoted attribute values.
pub fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/ui/agents", get(agents))
        .route("/ui/cloudflare", get(cloudflare))
        .route("/ui/runs", get(runs))
        .route("/ui/runs/{id}/summary", get(summary))
        .route("/ui/runs/{id}/agents", get(run_agents))
        .route("/ui/compare", get(compare))
        .route("/ui/template", get(template))
        .route("/ui/template-select", get(template_select))
        .route("/ui/validate", post(validate))
        .route("/ui/start", post(start))
        .layer(axum::middleware::map_response(fragment_headers))
}

/// Fragments opened directly as a page get the same script lockdown as the shells.
async fn fragment_headers(mut r: Response) -> Response {
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(crate::panel::CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    r
}

fn dash() -> String {
    "–".into()
}

fn ip_family(ip: &str) -> &'static str {
    match ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => "IPv4",
        Ok(IpAddr::V6(_)) => "IPv6",
        Err(_) => "",
    }
}

async fn agents(State(st): State<AppState>) -> Html<String> {
    let list = st.hub.agents();
    if list.is_empty() {
        return Html(
            "<p class=\"muted\">No agent connected. Start one with the command below.</p>".into(),
        );
    }
    let mut h = String::from(
        "<table><tr><th>name</th><th>status</th><th>version</th><th>OS / cores</th>\
         <th>public IP</th><th>clock offset</th><th>last saturation</th></tr>",
    );
    for a in list {
        let (cls, label) = match a.readiness {
            Some(Readiness::Ready) => ("ok", "ready"),
            Some(Readiness::Warn) => ("warn", "ready (clock &gt; 50 ms)"),
            Some(Readiness::NotReady) => ("bad", "not ready (clock &gt; 1 s)"),
            None => ("warn", "syncing clock"),
        };
        let busy = a
            .busy_run
            .as_deref()
            .map(|r| format!(" · run <a href=\"/live/{0}\">{0}</a>", esc(r)))
            .unwrap_or_default();
        let sat = st
            .store
            .saturation(&a.name)
            .await
            .ok()
            .flatten()
            .map(|v| format!("{v} VUs"))
            .unwrap_or_else(dash);
        let reported = a
            .reported_ip
            .as_deref()
            .map(|r| format!(" <span class=\"muted\">(reported {})</span>", esc(r)))
            .unwrap_or_default();
        let offset = a
            .offset_ms
            .map(|o| format!("{o:.1} ms (rtt {:.0} ms)", a.rtt_ms.unwrap_or(0.0)))
            .unwrap_or_else(dash);
        let _ = write!(
            h,
            "<tr><td>{}</td><td><span class=\"{cls}\">{label}</span>{busy}</td><td>{}</td>\
             <td>{} {} / {}</td><td><span class=\"mono\">{}</span> <span class=\"muted\">{}</span>{reported}</td>\
             <td>{offset}</td><td>{sat}</td></tr>",
            esc(&a.name),
            esc(&a.version),
            esc(&a.host.os),
            esc(&a.host.arch),
            a.host.cores,
            esc(&a.ip),
            ip_family(&a.ip),
        );
    }
    h.push_str("</table>");
    Html(h)
}

async fn cloudflare(State(st): State<AppState>) -> Html<String> {
    let ips: Vec<String> = st.hub.agents().into_iter().map(|a| a.ip).collect();
    let expr = cloudflare_expression(&ips);
    if expr.is_empty() {
        return Html(
            "<p class=\"muted\">Appears when an agent is connected (its address as the server sees it; an agent's own --public-ip is shown but never used here).</p>".into(),
        );
    }
    Html(format!(
        "<pre id=\"cfexpr\" class=\"copy\">{}</pre><button type=\"button\" data-copy=\"cfexpr\">Copy</button>",
        esc(&expr)
    ))
}

fn status_class(status: &str) -> &'static str {
    match status {
        "completed" => "ok",
        "running" | "pending" => "warn",
        _ => "bad",
    }
}

async fn runs(State(st): State<AppState>) -> Html<String> {
    let list = st.store.list_runs(200).await.unwrap_or_default();
    if list.is_empty() {
        return Html("<p class=\"muted\">No runs yet.</p>".into());
    }
    let mut h = String::from(
        "<table><tr><th>A</th><th>B</th><th>run</th><th>name</th><th>started</th>\
         <th>status</th><th>verdict</th></tr>",
    );
    for r in list {
        let id = esc(&r.id);
        let _ = write!(
            h,
            "<tr><td><input type=\"radio\" name=\"a\" value=\"{id}\" aria-label=\"A\"></td>\
             <td><input type=\"radio\" name=\"b\" value=\"{id}\" aria-label=\"B\"></td>\
             <td><a href=\"/runs/{id}\" class=\"mono\">{id}</a></td><td>{}</td><td data-ms=\"{}\"></td>\
             <td><span class=\"{}\">{}</span></td><td>{}</td></tr>",
            esc(&r.name),
            r.started_ms,
            status_class(&r.status),
            esc(&r.status),
            esc(&r.verdict)
        );
    }
    h.push_str("</table>");
    Html(h)
}

fn stage_table(r: &RunReport) -> String {
    if r.stages.is_empty() {
        return "<p class=\"muted\">No stage finished yet.</p>".into();
    }
    let metrics = metric_names(r);
    let mut h =
        String::from("<table><tr><th>stage</th><th>target</th><th>ops/s</th><th>err rate</th>");
    for m in &metrics {
        let _ = write!(h, "<th>{} p50/p95/p99 ms</th>", esc(m));
    }
    h.push_str("<th>client</th></tr>");
    for s in &r.stages {
        let rate = if s.rate_not_achieved {
            " <span class=\"warn\">rate not achieved</span>"
        } else {
            ""
        };
        let _ = write!(
            h,
            "<tr><td>{}</td><td>{}</td><td>{:.1}{rate}</td><td>{:.2}%</td>",
            s.index,
            s.target,
            s.ops_per_s,
            s.error_rate * 100.0
        );
        for m in &metrics {
            let cell = s
                .latency
                .get(m)
                .map(|l| format!("{:.0}/{:.0}/{:.0}", l.p50_ms, l.p95_ms, l.p99_ms))
                .unwrap_or_else(dash);
            let _ = write!(h, "<td>{cell}</td>");
        }
        let notes: Vec<String> = s.client_notes.iter().map(|n| esc(n)).collect();
        let client = if s.client_limited {
            format!(
                "<span class=\"bad\">LIMITED</span><br>{}",
                notes.join("<br>")
            )
        } else {
            "<span class=\"ok\">ok</span>".into()
        };
        let _ = write!(h, "<td>{client}</td></tr>");
    }
    h.push_str("</table>");
    h
}

fn unknown_run() -> Response {
    (
        StatusCode::NOT_FOUND,
        Html("<p class=\"bad\">unknown run</p>".to_string()),
    )
        .into_response()
}

async fn summary(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(Some(r)) = st.store.report(&id).await else {
        return unknown_run();
    };
    let m = &r.meta;
    let mut h = format!(
        "<p data-status=\"{}\"><b>{}</b> · <span class=\"mono\">{}</span> · <span class=\"{}\">{}</span></p><p>{}</p>",
        esc(&m.status),
        esc(&m.name),
        esc(&m.id),
        status_class(&m.status),
        esc(&m.status),
        esc(&m.verdict)
    );
    if !m.missing_agents.is_empty() {
        let _ = write!(
            h,
            "<p class=\"bad\">missing agents: {}</p>",
            esc(&m.missing_agents.join(", "))
        );
    }
    h.push_str(&stage_table(&r));
    let _ = write!(
        h,
        "<p>Export: <a href=\"/api/runs/{0}/export.json\">JSON</a> · \
         <a href=\"/api/runs/{0}/export.csv\">CSV</a> · \
         <a href=\"/api/runs/{0}/export.md\">Markdown</a></p>",
        esc(&m.id)
    );
    (done_status(&m.status), Html(h)).into_response()
}

/// htmx stops an `every 2s` poll when it gets 286; a finished run's fragments no longer change.
fn done_status(status: &str) -> StatusCode {
    if matches!(status, "running" | "pending") {
        StatusCode::OK
    } else {
        StatusCode::from_u16(286).unwrap_or(StatusCode::OK)
    }
}

fn pct(v: Option<f64>, unit: &str) -> String {
    v.map(|x| format!("{x:.0}{unit}")).unwrap_or_else(dash)
}

async fn run_agents(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(Some(r)) = st.store.report(&id).await else {
        return unknown_run();
    };
    let last = r.stages.last();
    let mut h = String::from(
        "<table><tr><th>agent</th><th>ip</th><th>share per stage</th><th>CPU avg</th>\
         <th>lag p95</th><th>ports</th><th>state</th></tr>",
    );
    for a in &r.meta.agents {
        let hs = last.and_then(|s| s.agents.get(&a.name));
        let state = if r.meta.missing_agents.contains(&a.name) {
            "<span class=\"bad\">lost</span>"
        } else if hs.is_some_and(|x| x.saturated) {
            "<span class=\"bad\">saturated</span>"
        } else {
            "<span class=\"ok\">ok</span>"
        };
        let share: Vec<String> = a.stage_targets.iter().map(u64::to_string).collect();
        let _ = write!(
            h,
            "<tr><td>{}</td><td class=\"mono\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{state}</td></tr>",
            esc(&a.name),
            esc(&a.ip),
            share.join(" · "),
            pct(hs.map(|x| x.cpu_avg), "%"),
            pct(hs.map(|x| x.lag_p95_ms), " ms"),
            pct(hs.map(|x| x.ports_pct_max), "%"),
        );
    }
    h.push_str("</table>");
    (done_status(&r.meta.status), Html(h)).into_response()
}

/// Both optional so a bare `/compare` shows a hint instead of an extractor error.
#[derive(Deserialize)]
struct MaybePair {
    a: Option<String>,
    b: Option<String>,
}

async fn compare(State(st): State<AppState>, Query(p): Query<MaybePair>) -> Html<String> {
    let hint = || {
        Html("<p>Pick two runs (A and B) on the <a href=\"/history\">History</a> page.</p>".into())
    };
    let (Some(a), Some(b)) = (p.a, p.b) else {
        return hint();
    };
    match (st.store.report(&a).await, st.store.report(&b).await) {
        (Ok(Some(a)), Ok(Some(b))) => Html(format!(
            "<h2>A · {} <span class=\"mono\">{}</span></h2>{}<h2>B · {} <span class=\"mono\">{}</span></h2>{}\
             <h2>Markdown</h2><pre id=\"cmpmd\" class=\"copy\">{}</pre>\
             <button type=\"button\" data-copy=\"cmpmd\">Copy</button>",
            esc(&a.meta.name),
            esc(&a.meta.id),
            stage_table(&a),
            esc(&b.meta.name),
            esc(&b.meta.id),
            stage_table(&b),
            esc(&loadfleet_metrics::compare_markdown(&a, &b))
        )),
        _ => hint(),
    }
}

#[derive(Deserialize)]
struct Name {
    #[serde(default)]
    name: String,
}

/// The New test page's template picker: the built-in templates, then those of the scenario
/// plugins this binary registered.
async fn template_select() -> Html<String> {
    let options: String = crate::api::templates_list()
        .iter()
        .map(|(name, _)| format!("<option>{}</option>", esc(name)))
        .collect();
    Html(format!(
        "<select name=\"name\" hx-get=\"/ui/template\" hx-target=\"#plan\" hx-swap=\"outerHTML\" \
         hx-trigger=\"change\"><option value=\"\">— choose —</option>{options}</select>"
    ))
}

async fn template(Query(n): Query<Name>) -> Html<String> {
    let yaml = crate::api::templates_list()
        .into_iter()
        .find(|(k, _)| *k == n.name)
        .map(|(_, y)| y)
        .unwrap_or("");
    Html(format!(
        "<textarea id=\"plan\" name=\"plan\" rows=\"28\" spellcheck=\"false\" autocapitalize=\"off\" \
         autocomplete=\"off\">{}</textarea>",
        esc(yaml)
    ))
}

#[derive(Deserialize)]
struct PlanForm {
    plan: String,
}

fn error_list(errors: &[String]) -> String {
    let items: String = errors
        .iter()
        .map(|e| format!("<li>{}</li>", esc(e)))
        .collect();
    format!("<ul class=\"errors\">{items}</ul>")
}

async fn validate(State(st): State<AppState>, Form(f): Form<PlanForm>) -> Html<String> {
    let errors = plan_errors(&st, &f.plan);
    Html(if errors.is_empty() {
        "<p class=\"ok\">plan is valid</p>".into()
    } else {
        error_list(&errors)
    })
}

async fn start(State(st): State<AppState>, Form(f): Form<PlanForm>) -> Response {
    match st.coord.start_run(&f.plan).await {
        Ok(id) => {
            let mut r = Html(String::new()).into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("/live/{id}")) {
                r.headers_mut().insert("hx-redirect", v);
            }
            r
        }
        Err(e) => {
            let status = start_error_status(&e);
            let errors = match e {
                StartError::Invalid(errs) => errs,
                other => vec![other.to_string()],
            };
            (status, Html(error_list(&errors))).into_response()
        }
    }
}
