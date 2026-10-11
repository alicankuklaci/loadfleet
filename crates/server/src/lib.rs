//! loadfleet server: control server: panel, HTTP API, agent hub, coordinator, aggregator, storage.
mod api;
mod auth;
mod auth_limits;
mod config;
mod coordinator;
mod hub;
mod ip;
mod origin;
mod panel;
mod runtask;
mod share;
mod state;
mod store;
mod ui;

pub use auth::{hash_password, Auth, LoginError, GLOBAL_FAILURE_BUDGET, MIN_PASSWORD_CHARS};
pub use config::ServerConfig;
pub use coordinator::{Coordinator, StartError};
pub use hub::{AgentView, Hub, HubEvent, SocketOpts};
pub use ip::{client_ip, cloudflare_expression, parse_proxy};
pub use ipnet::IpNet;
pub use share::{auto_weight, split_share, weights_for};
pub use state::AppState;
pub use store::Store;
pub use ui::esc;

use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use loadfleet_protocol::MAX_FRAME_BYTES;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Constant-time comparison (the length is not secret: tokens have a fixed size).
fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

async fn agent_ws(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let token = st.hub.token();
    // an empty configured token must never mean "anyone may connect"
    if token.is_empty() || !ct_eq(presented, &token) {
        return (StatusCode::UNAUTHORIZED, "bad agent token").into_response();
    }
    let ip = client_ip(
        &headers,
        peer,
        &st.cfg.trusted_proxies,
        st.cfg.trust_cf_header,
    )
    .to_string();
    let max_ms = i64::try_from(st.cfg.max_duration.as_millis()).unwrap_or(i64::MAX);
    let opts = SocketOpts {
        heartbeat: st.cfg.heartbeat,
        silence: st.cfg.agent_silence,
        max_rel_ms: max_ms.saturating_add(hub::REL_MS_SLACK),
        max_conns: st.cfg.max_conns_per_agent,
    };
    let hub = st.hub.clone();
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move { hub.serve_socket(socket, ip, opts).await })
}

pub fn router(state: AppState) -> Router {
    let mut r = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/agent", get(agent_ws))
        .route("/login", post(api::login))
        .route("/logout", post(api::logout))
        .merge(api::routes())
        .merge(ui::routes())
        .merge(panel::routes());
    // public only with `test_endpoints` (see `api::guard`); production configs never set it
    if state.cfg.test_endpoints {
        r = r.route(
            "/healthz/agents",
            get(|State(st): State<AppState>| async move { Json(st.hub.agents()) }),
        );
    }
    r.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        api::guard,
    ))
    .with_state(state)
}

/// The persisted agent token, minted on first start.
pub(crate) async fn agent_token(store: &Store) -> anyhow::Result<String> {
    if let Some(t) = store.setting("agent_token").await? {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    let t = hub::new_token();
    store.set_setting("agent_token", &t).await?;
    Ok(t)
}

/// The admin login: `--admin-password` / `LOADFLEET_ADMIN_PASSWORD` (its hash replaces the
/// stored one), else the stored argon2 hash (`settings.admin_hash`), else a generated password that is
/// printed once on stderr and stored only as its hash.
async fn make_auth(store: &Store, cfg: &ServerConfig) -> anyhow::Result<Auth> {
    let budget =
        |a: Auth| a.with_global_budget(cfg.login_failure_budget, crate::auth::GLOBAL_WINDOW);
    if let Some(p) = cfg.admin_password.as_deref().filter(|p| !p.is_empty()) {
        if p.chars().count() < MIN_PASSWORD_CHARS {
            anyhow::bail!(
                "the admin password (--admin-password / LOADFLEET_ADMIN_PASSWORD) must be at \
                 least {MIN_PASSWORD_CHARS} characters; unset it to have one generated"
            );
        }
        // replaces any stored hash, so dropping the flag later never revives an older password
        let h = hash_password(p);
        store.set_setting("admin_hash", &h).await?;
        return Ok(budget(Auth::from_hash(h, cfg.secure_cookies)));
    }
    if let Some(h) = store.setting("admin_hash").await? {
        if !h.is_empty() {
            return Ok(budget(Auth::from_hash(h, cfg.secure_cookies)));
        }
    }
    let p = hub::new_token()[..18].to_string();
    let h = hash_password(&p);
    store.set_setting("admin_hash", &h).await?;
    eprintln!("generated admin password (shown once, stored as a hash): {p}");
    Ok(budget(Auth::from_hash(h, cfg.secure_cookies)))
}

/// A running server. Cloning shares it; `kill` (or the token) shuts it down.
#[derive(Clone)]
pub struct ServerHandle {
    pub addr: SocketAddr,
    pub token: String,
    pub state: AppState,
    pub stop: CancellationToken,
}

impl ServerHandle {
    /// Drops every agent socket and stops listening (the server "vanishes").
    pub fn kill(&self) {
        self.stop.cancel();
    }
}

/// Series older than 30 days are thinned to 10 s buckets, checked every 6 h.
const THIN_AFTER_MS: i64 = 30 * 24 * 3600 * 1000;
const THIN_EVERY: std::time::Duration = std::time::Duration::from_secs(6 * 3600);

/// Opens the store, starts the hub, coordinator and series thinning, binds and serves in the
/// background until the handle's `stop` fires.
pub async fn serve(cfg: ServerConfig) -> anyhow::Result<ServerHandle> {
    let store = Store::open(&cfg.db)?;
    store.fail_unfinished_runs().await?;
    let token = agent_token(&store).await?;
    let auth = Arc::new(make_auth(&store, &cfg).await?);
    let stop = CancellationToken::new();
    let (hub, events) = Hub::new(token.clone(), stop.clone());
    let cfg = Arc::new(cfg);
    let coord = Coordinator::new(hub.clone(), store.clone(), cfg.clone());
    tokio::spawn(coord.clone().pump(events));
    let (thin, s) = (store.clone(), stop.clone());
    tokio::spawn(async move {
        loop {
            let cutoff = loadfleet_protocol::now_ms() - THIN_AFTER_MS;
            if let Err(e) = thin.thin_series(cutoff).await {
                tracing::warn!(error = %e, "series thinning failed");
            }
            tokio::select! {
                _ = s.cancelled() => break,
                _ = tokio::time::sleep(THIN_EVERY) => {}
            }
        }
    });
    let state = AppState {
        cfg: cfg.clone(),
        store,
        hub,
        coord,
        auth,
    };
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    let addr = listener.local_addr()?;
    let app = router(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    let s = stop.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { s.cancelled().await })
            .await;
    });
    Ok(ServerHandle {
        addr,
        token,
        state,
        stop,
    })
}

/// `serve` for tests that only need the address and agent token; `stop` shuts the server down.
pub async fn serve_for_tests(
    cfg: ServerConfig,
    stop: CancellationToken,
) -> anyhow::Result<(SocketAddr, String)> {
    let h = serve(cfg).await?;
    let own = h.stop.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = stop.cancelled() => own.cancel(),
            _ = own.cancelled() => {}
        }
    });
    Ok((h.addr, h.token))
}
