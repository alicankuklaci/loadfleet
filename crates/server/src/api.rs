//! The JSON/HTTP API behind the panel: login, agents, token, runs, series, exports, compare.
use crate::auth::LoginError;
use crate::ip::forwarded_https;
use crate::{client_ip, cloudflare_expression, hub, AppState, StartError};
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use loadfleet_metrics::{compare_markdown, to_csv, to_json, to_markdown};
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;

const BUILTIN_TEMPLATES: [(&str, &str); 4] = [
    ("HTTP GET", include_str!("../../../examples/http-get.yaml")),
    (
        "HTTP POST JSON",
        include_str!("../../../examples/http-post-json.yaml"),
    ),
    (
        "TCP connect & hold",
        include_str!("../../../examples/tcp-hold.yaml"),
    ),
    ("WS echo", include_str!("../../../examples/ws-echo.yaml")),
];

/// The panel's plan templates: the built-in ones, then those of registered scenario plugins.
pub(crate) fn templates_list() -> Vec<(&'static str, &'static str)> {
    let mut t = BUILTIN_TEMPLATES.to_vec();
    t.extend(loadfleet_scenario::plugin_templates());
    t
}

/// The CSRF guard header every state-changing request must carry (a cross-site form or a
/// simple cross-origin fetch cannot set it).
pub const CSRF_HEADER: &str = "x-loadfleet";

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/agents", get(agents))
        .route("/api/token", get(token))
        .route("/api/token/rotate", post(rotate))
        .route("/api/runs/validate", post(validate_plan))
        .route("/api/runs", get(list_runs).post(start_run))
        .route("/api/runs/{id}", get(get_run))
        .route("/api/runs/{id}/series", get(series))
        .route("/api/runs/{id}/stop", post(stop_run))
        .route("/api/runs/{id}/{file}", get(export))
        .route("/api/compare", get(compare))
        .route("/api/templates", get(templates))
        .route("/api/active", get(active))
}

/// Session check for everything but the public paths; the CSRF header for every non-GET
/// (`/logout` is exempt so a plain HTML form can post it; the SameSite=Strict cookie already
/// keeps cross-site requests from carrying a session).
pub async fn guard(State(st): State<AppState>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    let public = path == "/login"
        || path == "/healthz"
        || path == "/agent"
        || path.starts_with("/static/")
        || (st.cfg.test_endpoints && path == "/healthz/agents");
    if public {
        return next.run(req).await;
    }
    if !st.auth.check(req.headers()) {
        return if path.starts_with("/api/") || path.starts_with("/ui/") {
            (StatusCode::UNAUTHORIZED, "login required").into_response()
        } else {
            Redirect::to("/login").into_response()
        };
    }
    let safe = matches!(*req.method(), Method::GET | Method::HEAD);
    let csrf_ok = req.headers().get(CSRF_HEADER).is_some_and(|v| v == "1");
    if !safe && !csrf_ok && path != "/logout" {
        return (StatusCode::FORBIDDEN, "missing x-loadfleet header").into_response();
    }
    next.run(req).await
}

#[derive(Deserialize)]
pub struct LoginForm {
    password: String,
}

/// A browser form post (it asks for HTML) gets redirected back to the form with a message
/// code; API clients get the plain status (401 / 429).
fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/html"))
}

pub async fn login(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    uri: Uri,
    headers: HeaderMap,
    Form(f): Form<LoginForm>,
) -> Response {
    let proxies = &st.cfg.trusted_proxies;
    let expect = crate::origin::Expect {
        peer,
        trusted_proxies: proxies,
        secure_cookies: st.cfg.secure_cookies,
    };
    if !crate::origin::login_allowed(&headers, &uri, &expect) {
        tracing::warn!(%peer, "admin login refused: cross-origin post");
        return (StatusCode::FORBIDDEN, "cross-origin login refused").into_response();
    }
    let ip = client_ip(&headers, peer, proxies, st.cfg.trust_cf_header);
    let tls = forwarded_https(&headers, peer, proxies);
    let (status, code, text) = match st.auth.login_gated(ip, f.password).await {
        Ok(s) => {
            tracing::info!(%ip, "admin login");
            return (
                StatusCode::SEE_OTHER,
                [
                    (header::SET_COOKIE, st.auth.cookie_with(&s, tls)),
                    (header::LOCATION, "/agents".to_string()),
                ],
            )
                .into_response();
        }
        Err(LoginError::Bad) => {
            tracing::warn!(%ip, "admin login failed: wrong password");
            (StatusCode::UNAUTHORIZED, "bad", "wrong password")
        }
        Err(LoginError::RateLimited) => {
            tracing::warn!(%ip, "admin login rate limited");
            (
                StatusCode::TOO_MANY_REQUESTS,
                "rate",
                "too many attempts, wait a minute",
            )
        }
        Err(LoginError::Throttled) => {
            tracing::error!(
                %ip,
                "admin login throttled: global failed-login budget spent (one attempt per 10 s)"
            );
            (
                StatusCode::TOO_MANY_REQUESTS,
                "throttled",
                "too many failed logins on this server: one attempt per 10 s, try again shortly",
            )
        }
        Err(LoginError::Busy) => {
            tracing::warn!(%ip, "admin login refused: verification gate full");
            (
                StatusCode::TOO_MANY_REQUESTS,
                "busy",
                "server busy, try again shortly",
            )
        }
    };
    if wants_html(&headers) {
        return Redirect::to(&format!("/login?e={code}")).into_response();
    }
    (status, text).into_response()
}

pub async fn logout(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    st.auth.logout(&headers);
    let tls = forwarded_https(&headers, peer, &st.cfg.trusted_proxies);
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, st.auth.clear_cookie(tls)),
            (header::LOCATION, "/login".to_string()),
        ],
    )
        .into_response()
}

async fn agents(State(st): State<AppState>) -> Json<serde_json::Value> {
    let agents = st.hub.agents();
    let ips: Vec<String> = agents.iter().map(|a| a.ip.clone()).collect();
    Json(json!({ "agents": agents, "cloudflare": cloudflare_expression(&ips), "ips": ips }))
}

async fn token(State(st): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "token": st.hub.token() }))
}

/// New agents need the new token; connected agents keep their sessions.
async fn rotate(State(st): State<AppState>) -> Response {
    let t = hub::new_token();
    if let Err(e) = st.store.set_setting("agent_token", &t).await {
        tracing::warn!(error = %e, "storing the rotated agent token failed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    st.hub.set_token(t.clone());
    tracing::info!("agent token rotated");
    Json(json!({ "token": t })).into_response()
}

/// The same parse -> validate (server limits, allow-list) -> scenario pipeline a start uses.
pub(crate) fn plan_errors(st: &AppState, text: &str) -> Vec<String> {
    match st.coord.check(text) {
        Ok(_) => Vec::new(),
        Err(StartError::Invalid(errs)) => errs,
        Err(e) => vec![e.to_string()],
    }
}

async fn validate_plan(State(st): State<AppState>, body: Bytes) -> Json<serde_json::Value> {
    let errors = plan_errors(&st, &String::from_utf8_lossy(&body));
    Json(json!({ "ok": errors.is_empty(), "errors": errors }))
}

pub(crate) fn start_error_status(e: &StartError) -> StatusCode {
    match e {
        StartError::Invalid(_) => StatusCode::BAD_REQUEST,
        StartError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        StartError::Busy(_) | StartError::NotEnoughAgents { .. } | StartError::ConnLimit { .. } => {
            StatusCode::CONFLICT
        }
    }
}

async fn start_run(State(st): State<AppState>, body: Bytes) -> Response {
    match st.coord.start_run(&String::from_utf8_lossy(&body)).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({ "id": id }))).into_response(),
        Err(StartError::Internal(msg)) => {
            // raw store/IO text stays in the log
            tracing::warn!(error = %msg, "starting a run failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal error (see the server log)" })),
            )
                .into_response()
        }
        Err(e) => (
            start_error_status(&e),
            Json(json!({ "error": e.to_string(), "detail": e })),
        )
            .into_response(),
    }
}

/// Run list without plan texts (they can be long and may hold literal header values);
/// a single run's report carries its plan.
async fn list_runs(State(st): State<AppState>) -> Response {
    match st.store.list_runs(200).await {
        Ok(mut runs) => {
            runs.iter_mut().for_each(|r| r.plan_yaml.clear());
            Json(runs).into_response()
        }
        Err(e) => internal(e),
    }
}

fn internal(e: anyhow::Error) -> Response {
    tracing::warn!(error = %e, "api store error");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

async fn get_run(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match st.store.report(&id).await {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct Since {
    since: Option<i64>,
}

async fn series(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<Since>,
) -> Response {
    match st.store.series_since(&id, q.since.unwrap_or(0)).await {
        Ok(p) => Json(p).into_response(),
        Err(e) => internal(e),
    }
}

async fn stop_run(State(st): State<AppState>, Path(id): Path<String>) -> StatusCode {
    if st.coord.stop_run(&id, "stopped from the panel") {
        StatusCode::ACCEPTED
    } else {
        StatusCode::NOT_FOUND
    }
}

/// One export format: the report and the excluded stage indexes in, the file body out.
type Render = fn(&loadfleet_metrics::RunReport, &[usize]) -> String;

async fn export(State(st): State<AppState>, Path((id, file)): Path<(String, String)>) -> Response {
    let (render, ctype): (Render, &str) = match file.as_str() {
        "export.json" => (|r, _| to_json(r), "application/json"),
        "export.csv" => (|r, _| to_csv(r), "text/csv; charset=utf-8"),
        "export.md" => (to_markdown, "text/markdown; charset=utf-8"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let r = match st.store.report(&id).await {
        Ok(Some(r)) => r,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal(e),
    };
    let excluded = loadfleet_plan::parse(&r.meta.plan_yaml)
        .map(|p| p.exclude_stages)
        .unwrap_or_default();
    // the stored id (generated by the server), never raw path text, goes into the header
    let disp = format!("attachment; filename=\"loadfleet-{}-{file}\"", r.meta.id);
    (
        [
            (header::CONTENT_TYPE, ctype.to_string()),
            (header::CONTENT_DISPOSITION, disp),
        ],
        render(&r, &excluded),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct Pair {
    pub a: String,
    pub b: String,
}

async fn compare(State(st): State<AppState>, Query(p): Query<Pair>) -> Response {
    match (st.store.report(&p.a).await, st.store.report(&p.b).await) {
        (Ok(Some(a)), Ok(Some(b))) => (
            [(header::CONTENT_TYPE, "text/markdown; charset=utf-8")],
            compare_markdown(&a, &b),
        )
            .into_response(),
        (Err(e), _) | (_, Err(e)) => internal(e),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn templates() -> Json<serde_json::Value> {
    Json(
        templates_list()
            .iter()
            .map(|(n, y)| json!({ "name": n, "yaml": y }))
            .collect(),
    )
}

async fn active(State(st): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({ "id": st.coord.active() }))
}
