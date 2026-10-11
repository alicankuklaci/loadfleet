use crate::net::connect_tcp;
use crate::{tls, AllowGuard, Template, Vars};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::{http1, http2, TrySendError};
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use loadfleet_engine::{CancellationToken, OpCtx, Scenario, VuCtx};
use loadfleet_health::ConnGuard;
use loadfleet_plan::HttpSpec;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::time::{timeout, Instant};

/// One client connection; dropping it closes the socket (the hyper connection task ends once
/// every sender is gone) and releases the port-gauge entry.
pub(crate) enum Sender {
    H1 {
        tx: http1::SendRequest<Full<Bytes>>,
        _tracked: ConnGuard,
    },
    H2 {
        tx: http2::SendRequest<Full<Bytes>>,
        _tracked: ConnGuard,
    },
}

impl Sender {
    fn is_closed(&self) -> bool {
        match self {
            Sender::H1 { tx, .. } => tx.is_closed(),
            Sender::H2 { tx, .. } => tx.is_closed(),
        }
    }

    async fn ready(&mut self) -> Result<(), hyper::Error> {
        match self {
            Sender::H1 { tx, .. } => tx.ready().await,
            Sender::H2 { tx, .. } => tx.ready().await,
        }
    }

    // The error hands the unsent request back for a retry on a fresh connection, so it
    // cannot be boxed away; it is matched immediately by the caller.
    #[allow(clippy::result_large_err)]
    async fn try_send(&mut self, req: Req) -> Result<Response<Incoming>, TrySendError<Req>> {
        match self {
            Sender::H1 { tx, .. } => tx.try_send_request(req).await,
            Sender::H2 { tx, .. } => tx.try_send_request(req).await,
        }
    }
}

type Req = Request<Full<Bytes>>;

/// A header value fixed at build time, or a template rendered per request.
enum HeaderTpl {
    Static(HeaderValue),
    Dynamic(Template),
}

pub struct HttpScenario {
    pub(crate) spec: HttpSpec,
    method: Method,
    https: bool,
    host: String,
    port: u16,
    authority: String,
    /// `Host` header value (HTTP/1), validated once.
    host_header: HeaderValue,
    path: Template,
    /// The request URI pre-built when `path` has no placeholders (most benches and plain GETs).
    static_uri: Option<hyper::Uri>,
    headers: Vec<(HeaderName, HeaderTpl)>,
    body: Option<Template>,
    /// `body` pre-rendered when it has no placeholders.
    static_body: Option<Bytes>,
    metric: String,
    guard: AllowGuard,
    rr: AtomicUsize,
    run: String,
    agent: String,
    pub(crate) pool: crate::pool::Pool,
}

impl HttpScenario {
    pub fn new(
        spec: &HttpSpec,
        guard: AllowGuard,
        run: &str,
        agent: &str,
        pool_size: usize,
    ) -> Result<Self, String> {
        let url = url::Url::parse(&spec.url).map_err(|e| format!("scenario.http.url: {e}"))?;
        let host = url
            .host_str()
            .ok_or("scenario.http.url: no host")?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let https = match url.scheme() {
            "https" => true,
            "http" => false,
            s => return Err(format!("scenario.http.url: unsupported scheme {s}")),
        };
        let port = url
            .port_or_known_default()
            .ok_or("scenario.http.url: no port")?;
        let authority = match (url.host_str(), url.port()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            (Some(h), None) => h.to_string(),
            (None, _) => host.clone(),
        };
        let pq = raw_path_and_query(&spec.url);
        let path = Template::compile(if pq.is_empty() { "/" } else { pq })?.with_env()?;
        // (an unparsable static path keeps failing per request as `http:bad_request`)
        let static_uri = path
            .is_static()
            .then(|| request_uri(spec.http2, https, &authority, path.render(&NO_VARS)))
            .and_then(|u| hyper::Uri::try_from(u).ok());
        let host_header = HeaderValue::from_str(&authority)
            .map_err(|_| "scenario.http.url: invalid host".to_string())?;
        let body = spec
            .body
            .as_deref()
            .map(|b| Template::compile(b).and_then(Template::with_env))
            .transpose()?;
        Ok(HttpScenario {
            method: Method::from_bytes(spec.method.as_bytes())
                .map_err(|_| "scenario.http.method: invalid")?,
            path,
            static_uri,
            host_header,
            headers: spec
                .headers
                .iter()
                .map(|(k, v)| header(k, v))
                .collect::<Result<_, String>>()?,
            static_body: body
                .as_ref()
                .filter(|t| t.is_static())
                .map(|t| Bytes::from(t.render(&NO_VARS))),
            body,
            metric: spec.name.clone().unwrap_or_else(|| "http".into()),
            spec: spec.clone(),
            https,
            host,
            port,
            authority,
            guard,
            rr: AtomicUsize::new(0),
            run: run.into(),
            agent: agent.into(),
            pool: crate::pool::Pool::new(pool_size),
        })
    }

    pub(crate) async fn connect(&self) -> Result<Sender, String> {
        let (tcp, tracked) = connect_tcp(
            &self.host,
            self.port,
            &self.guard,
            &self.rr,
            self.spec.timeout,
        )
        .await?;
        macro_rules! handshake {
            ($io:expr) => {{
                if self.spec.http2 {
                    let (s, c) = http2::handshake(TokioExecutor::new(), TokioIo::new($io))
                        .await
                        .map_err(handshake_code)?;
                    tokio::spawn(c);
                    Sender::H2 {
                        tx: s,
                        _tracked: tracked,
                    }
                } else {
                    let (s, c) = http1::handshake(TokioIo::new($io))
                        .await
                        .map_err(handshake_code)?;
                    tokio::spawn(c);
                    Sender::H1 {
                        tx: s,
                        _tracked: tracked,
                    }
                }
            }};
        }
        let fut = async {
            Ok(if self.https {
                let cfg = if self.spec.http2 {
                    tls::h2_config()
                } else {
                    tls::default_config()
                };
                handshake!(tls::wrap(tcp, &self.host, cfg).await?)
            } else {
                handshake!(tcp)
            })
        };
        tokio::time::timeout(self.spec.timeout, fut)
            .await
            .map_err(|_| "net:connect_timeout".to_string())?
    }

    fn request(&self, vu_id: &str, iter: u64) -> Result<Request<Full<Bytes>>, String> {
        let v = Vars {
            vu_id,
            iter,
            run: &self.run,
            agent: &self.agent,
        };
        let uri = match &self.static_uri {
            Some(u) => u.clone(),
            None => request_uri(
                self.spec.http2,
                self.https,
                &self.authority,
                self.path.render(&v),
            )
            .try_into()
            .map_err(|_| "http:bad_request".to_string())?,
        };
        let mut b = Request::builder().method(self.method.clone()).uri(uri);
        if !self.spec.http2 {
            b = b.header(hyper::header::HOST, self.host_header.clone());
        }
        for (k, t) in &self.headers {
            b = match t {
                HeaderTpl::Static(hv) => b.header(k, hv.clone()),
                HeaderTpl::Dynamic(t) => b.header(k, t.render(&v)),
            };
        }
        let body = match (&self.static_body, &self.body) {
            (Some(s), _) => s.clone(),
            (None, Some(t)) => Bytes::from(t.render(&v)),
            (None, None) => Bytes::new(),
        };
        b.body(Full::new(body))
            .map_err(|_| "http:bad_request".to_string())
    }

    /// Makes `slot` hold a ready connection. A cached one the server has closed normally
    /// (`Connection: close`, keep-alive request limit, idle timeout) is replaced silently: that is
    /// connection recycling, not a target failure. `Err` only when a fresh connection fails.
    pub(crate) async fn checkout(&self, slot: &mut Option<Sender>) -> Result<(), String> {
        if let Some(c) = slot.as_mut() {
            if !c.is_closed() && matches!(timeout(READY_WAIT, c.ready()).await, Ok(Ok(()))) {
                return Ok(());
            }
            *slot = None;
        }
        let mut c = self.connect().await?;
        match timeout(self.spec.timeout, c.ready()).await {
            Err(_) => return Err("http:timeout".into()),
            Ok(Err(e)) => return Err(code(e)),
            Ok(Ok(())) => {}
        }
        *slot = Some(c);
        Ok(())
    }

    /// Sends one request on the checked-out connection in `slot`; `Ok(status)` or an error code.
    /// If the connection closed before the request was written (a keep-alive close racing the
    /// checkout), the request is re-sent once on a fresh connection; only a request that reached
    /// the wire, or a failed reconnect, yields an error. The slot is emptied on any error.
    pub(crate) async fn exchange(
        &self,
        slot: &mut Option<Sender>,
        req: Req,
    ) -> Result<u16, String> {
        let r = timeout(self.spec.timeout, self.exchange_inner(slot, req))
            .await
            .unwrap_or_else(|_| Err("http:timeout".into()));
        if r.is_err() {
            *slot = None;
        }
        r
    }

    async fn exchange_inner(&self, slot: &mut Option<Sender>, mut req: Req) -> Result<u16, String> {
        let mut retried = false;
        loop {
            if slot.is_none() {
                self.checkout(slot).await?;
            }
            let Some(conn) = slot.as_mut() else {
                return Err("net:closed".into());
            };
            match conn.try_send(req).await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    resp.into_body().collect().await.map_err(code)?;
                    return Ok(status);
                }
                Err(mut e) => {
                    let unsent = e.take_message();
                    let err = e.into_error();
                    match unsent {
                        Some(r) if !retried => {
                            *slot = None;
                            retried = true;
                            req = r;
                        }
                        _ => return Err(code(err)),
                    }
                }
            }
        }
    }

    pub(crate) fn status_ok(&self, s: u16) -> bool {
        if self.spec.expect_status.is_empty() {
            (200..300).contains(&s)
        } else {
            self.spec.expect_status.contains(&s)
        }
    }

    /// Records the outcome of one request.
    pub(crate) fn account(
        &self,
        rec: &loadfleet_metrics::VuRecorder,
        r: &Result<u16, String>,
        since: Instant,
    ) {
        match r {
            Ok(s) => {
                rec.latency(&self.metric, since.elapsed());
                if !self.status_ok(*s) {
                    rec.error(&format!("http:status:{s}"));
                }
            }
            Err(c) => rec.error(c),
        }
    }

    pub(crate) fn metric(&self) -> &str {
        &self.metric
    }

    pub(crate) fn build_request(
        &self,
        vu_id: &str,
        iter: u64,
    ) -> Result<Request<Full<Bytes>>, String> {
        self.request(vu_id, iter)
    }
}

/// HTTP/2 wants the absolute URI (scheme + authority), HTTP/1 only the path.
fn request_uri(http2: bool, https: bool, authority: &str, path: String) -> String {
    if http2 {
        let scheme = if https { "https" } else { "http" };
        format!("{scheme}://{authority}{path}")
    } else {
        path
    }
}

/// Path + query exactly as written (templates intact; `Url` would percent-encode the braces).
fn raw_path_and_query(url: &str) -> &str {
    let after = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    after.find(['/', '?']).map(|i| &after[i..]).unwrap_or("")
}

/// How long a cached connection may take to report ready before it is replaced.
const READY_WAIT: Duration = Duration::from_millis(50);

const NO_VARS: Vars<'static> = Vars {
    vu_id: "",
    iter: 0,
    run: "",
    agent: "",
};

/// Validates the header name (and a static value) once at build time.
fn header(k: &str, v: &str) -> Result<(HeaderName, HeaderTpl), String> {
    let name = HeaderName::from_bytes(k.as_bytes())
        .map_err(|_| format!("scenario.http.headers.{k}: invalid header name"))?;
    let t = Template::compile(v)?.with_env()?;
    let tpl = if t.is_static() {
        HeaderValue::from_str(&t.render(&NO_VARS))
            .map(HeaderTpl::Static)
            .map_err(|_| format!("scenario.http.headers.{k}: invalid header value"))?
    } else {
        HeaderTpl::Dynamic(t)
    };
    Ok((name, tpl))
}

fn handshake_code(e: hyper::Error) -> String {
    match code(e).as_str() {
        "http:proto" => "http:handshake".into(),
        other => other.into(),
    }
}

fn code(e: hyper::Error) -> String {
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            return loadfleet_health::classify_io_error(io).to_string();
        }
        src = s.source();
    }
    if e.is_closed() || e.is_incomplete_message() {
        "net:closed".into()
    } else {
        "http:proto".into()
    }
}

pub(crate) async fn think(
    spec: Option<loadfleet_plan::MsRange>,
    rng: &mut StdRng,
    cancel: &CancellationToken,
) {
    if let Some(r) = spec {
        let (a, b) = r.bounds();
        let ms = if b > a { rng.gen_range(a..=b) } else { a };
        tokio::select! {
            _ = cancel.cancelled() => {},
            _ = tokio::time::sleep(Duration::from_millis(ms)) => {},
        }
    }
}

#[async_trait]
impl Scenario for HttpScenario {
    fn kind(&self) -> &'static str {
        "http"
    }

    async fn run_vu(&self, ctx: VuCtx) {
        let mut rng = StdRng::seed_from_u64(ctx.seed);
        let mut conn: Option<Sender> = None;
        let mut iter = 0u64;
        while !ctx.cancel.is_cancelled() {
            let r = tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                r = self.checkout(&mut conn) => r,
            };
            if let Err(e) = r {
                ctx.rec.op(&self.metric);
                ctx.rec.error(&e);
                tokio::select! {
                    _ = ctx.cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                }
                continue;
            }
            let req = match self.request(&ctx.vu_id, iter) {
                Ok(r) => r,
                Err(e) => {
                    ctx.rec.op(&self.metric);
                    ctx.rec.error(&e);
                    break;
                }
            };
            ctx.rec.op(&self.metric);
            let t0 = Instant::now();
            let r = tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                r = self.exchange(&mut conn, req) => r,
            };
            self.account(&ctx.rec, &r, t0);
            iter += 1;
            think(self.spec.think_ms, &mut rng, &ctx.cancel).await;
        }
    }

    fn supports_open(&self) -> bool {
        true
    }

    async fn op(&self, ctx: OpCtx) {
        self.pool.op(self, ctx).await;
    }
}
