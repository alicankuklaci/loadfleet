//! The panel's static pages and assets, embedded from `web/` (htmx + uPlot vendored, no CDN).
use axum::extract::Path;
use axum::http::{header, HeaderName, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../web/"]
struct Assets;

/// Pages load only our own scripts (no inline script, no eval), so an escaping slip in a
/// fragment cannot run code; `frame-ancestors` blocks click-jacking.
pub(crate) const CSP: &str =
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

fn file(path: &str) -> Response {
    // debug builds read `web/` from disk: never let a path step outside it
    let outside = path
        .split(['/', '\\'])
        .any(|seg| seg == ".." || seg.contains(':'));
    if outside || path.starts_with('/') {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(f) = Assets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let mut headers: Vec<(HeaderName, String)> = vec![
        (header::CONTENT_TYPE, mime.essence_str().to_string()),
        (header::CACHE_CONTROL, "no-cache".to_string()),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
    ];
    if path.ends_with(".html") {
        headers.push((header::CONTENT_SECURITY_POLICY, CSP.to_string()));
        // `same-origin`, not `no-referrer`: the latter makes browsers send `Origin: null` on
        // the login form post, which `POST /login` refuses
        headers.push((header::REFERRER_POLICY, "same-origin".to_string()));
    }
    let mut r = f.data.into_owned().into_response();
    for (k, v) in headers {
        if let Ok(v) = v.parse() {
            r.headers_mut().insert(k, v);
        }
    }
    r
}

/// One page shell by file stem (`agents` -> `web/agents.html`).
pub fn page(name: &str) -> Response {
    file(&format!("{name}.html"))
}

pub fn routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/", get(|| async { Redirect::to("/agents") }))
        .route("/login", get(|| async { page("login") }))
        .route("/agents", get(|| async { page("agents") }))
        .route("/new", get(|| async { page("new") }))
        .route("/history", get(|| async { page("history") }))
        .route("/compare", get(|| async { page("compare") }))
        .route("/live/{id}", get(|| async { page("live") }))
        .route("/runs/{id}", get(|| async { page("run") }))
        .route(
            "/static/{*path}",
            get(|Path(p): Path<String>| async move { file(&p) }),
        )
}
