//! WebSocket scenario: connect, run `on_open` once, then loop `steps` (or hold when empty).
use crate::io::{dial, BoxIo, Held};
use crate::retry::{after_session, Backoff, Outcome};
use crate::{AllowGuard, Template, Vars};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use loadfleet_engine::{Scenario, VuCtx};
use loadfleet_health::ConnGuard;
use loadfleet_plan::{WsSpec, WsStep};
use rand::{rngs::StdRng, SeedableRng};
use regex::Regex;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderName;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

enum Step {
    Send(Template),
    Expect(Regex, Duration),
    Sleep(Duration),
}

pub struct WsScenario {
    spec: WsSpec,
    host: String,
    port: u16,
    tls: bool,
    headers: Vec<(HeaderName, Template)>,
    on_open: Vec<Step>,
    steps: Vec<Step>,
    guard: AllowGuard,
    rr: AtomicUsize,
    run: String,
    agent: String,
}

fn compile(field: &str, steps: &[WsStep], default_timeout: Duration) -> Result<Vec<Step>, String> {
    steps
        .iter()
        .enumerate()
        .map(|(i, s)| {
            Ok(match s {
                WsStep::Send(t) => Step::Send(Template::compile(t)?.with_env()?),
                WsStep::Expect(e) => {
                    if e.hex.is_some() {
                        return Err(format!(
                            "{field}[{i}].expect.hex: hex is not supported for websocket messages; use regex"
                        ));
                    }
                    let re = Regex::new(e.regex.as_deref().unwrap_or(""))
                        .map_err(|e| format!("{field}[{i}].expect.regex: {e}"))?;
                    let t = if e.timeout.is_zero() {
                        default_timeout
                    } else {
                        e.timeout
                    };
                    Step::Expect(re, t)
                }
                WsStep::SleepMs(ms) => Step::Sleep(Duration::from_millis(*ms)),
            })
        })
        .collect()
}

impl WsScenario {
    pub fn new(
        spec: &WsSpec,
        guard: AllowGuard,
        run: &str,
        agent: &str,
    ) -> Result<WsScenario, String> {
        let url = url::Url::parse(&spec.url).map_err(|e| format!("scenario.ws.url: {e}"))?;
        let tls = match url.scheme() {
            "ws" => false,
            "wss" => true,
            s => return Err(format!("scenario.ws.url: scheme {s:?} must be ws or wss")),
        };
        let host = url
            .host_str()
            .ok_or("scenario.ws.url: no host")?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let headers = spec
            .headers
            .iter()
            .map(|(k, v)| {
                let name: HeaderName = k
                    .parse()
                    .map_err(|_| format!("scenario.ws.headers.{k}: invalid header name"))?;
                Ok((name, Template::compile(v)?.with_env()?))
            })
            .collect::<Result<_, String>>()?;
        Ok(WsScenario {
            host,
            port: url
                .port_or_known_default()
                .ok_or("scenario.ws.url: no port")?,
            tls,
            headers,
            on_open: compile("scenario.ws.on_open", &spec.on_open, spec.timeout)?,
            steps: compile("scenario.ws.steps", &spec.steps, spec.timeout)?,
            spec: spec.clone(),
            guard,
            rr: AtomicUsize::new(0),
            run: run.into(),
            agent: agent.into(),
        })
    }

    /// Guarded dial + upgrade. The `ConnGuard` must live as long as the stream.
    async fn handshake(
        &self,
        ctx: &VuCtx,
        iter: u64,
    ) -> Result<(WebSocketStream<BoxIo>, ConnGuard), String> {
        let (io, tracked) = dial(
            &self.host,
            self.port,
            self.tls,
            &self.guard,
            &self.rr,
            self.spec.timeout,
        )
        .await?;
        let mut req = self
            .spec
            .url
            .as_str()
            .into_client_request()
            .map_err(|_| "ws:bad_url".to_string())?;
        let vars = self.vars(ctx, iter);
        for (name, t) in &self.headers {
            let v = t
                .render(&vars)
                .parse()
                .map_err(|_| "ws:bad_header".to_string())?;
            req.headers_mut().insert(name.clone(), v);
        }
        let (ws, _) = tokio::time::timeout(
            self.spec.timeout,
            tokio_tungstenite::client_async_with_config(req, io, Some(ws_config())),
        )
        .await
        .map_err(|_| "ws:handshake_timeout".to_string())?
        .map_err(|_| "ws:handshake".to_string())?;
        Ok((ws, tracked))
    }

    fn vars<'a>(&'a self, ctx: &'a VuCtx, iter: u64) -> Vars<'a> {
        Vars {
            vu_id: &ctx.vu_id,
            iter,
            run: &self.run,
            agent: &self.agent,
        }
    }

    /// Runs `steps` once. `last_send` carries across calls so `ws.rtt` is always measured from
    /// the most recent send to the matching message.
    async fn run_steps(
        &self,
        ctx: &VuCtx,
        ws: &mut WebSocketStream<BoxIo>,
        steps: &[Step],
        iter: u64,
        last_send: &mut Instant,
    ) -> Result<(), String> {
        for st in steps {
            if ctx.cancel.is_cancelled() {
                return Ok(());
            }
            match st {
                Step::Send(t) => {
                    *last_send = Instant::now();
                    ws.send(Message::text(t.render(&self.vars(ctx, iter))))
                        .await
                        .map_err(|_| "ws:closed".to_string())?;
                }
                Step::Expect(re, timeout) => {
                    ctx.rec.op("ws.rtt");
                    let deadline = Instant::now() + *timeout;
                    loop {
                        let msg = tokio::select! {
                            _ = ctx.cancel.cancelled() => return Ok(()),
                            m = tokio::time::timeout_at(deadline, ws.next()) => m,
                        };
                        match msg {
                            Err(_) => return Err("ws:expect_timeout".into()),
                            Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => {
                                return Err("ws:closed".into())
                            }
                            Ok(Some(Ok(Message::Text(t)))) if re.is_match(t.as_str()) => break,
                            Ok(Some(Ok(_))) => continue,
                        }
                    }
                    ctx.rec.latency("ws.rtt", last_send.elapsed());
                }
                Step::Sleep(d) => {
                    tokio::select! {
                        _ = ctx.cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(*d) => {},
                    }
                }
            }
        }
        Ok(())
    }

    async fn session(
        &self,
        ctx: &VuCtx,
        iter: &mut u64,
        rng: &mut StdRng,
        connected_at: &mut Option<Instant>,
    ) -> Result<(), String> {
        ctx.rec.op("ws.connect");
        let t0 = Instant::now();
        let (mut ws, _tracked) = self.handshake(ctx, *iter).await?;
        ctx.rec.latency("ws.connect", t0.elapsed());
        *connected_at = Some(Instant::now());
        let _held = Held::new(ctx.rec.gauge("connections"));
        let mut last_send = Instant::now();
        self.run_steps(ctx, &mut ws, &self.on_open, *iter, &mut last_send)
            .await?;
        if self.steps.is_empty() {
            // Connect-and-hold: read (answers pings) until the stage ends or the peer leaves.
            loop {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => break,
                    m = ws.next() => match m {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {
                            return Err("ws:closed".into())
                        }
                        Some(Ok(_)) => {}
                    },
                }
            }
        } else {
            while !ctx.cancel.is_cancelled() {
                self.run_steps(ctx, &mut ws, &self.steps, *iter, &mut last_send)
                    .await?;
                *iter += 1;
                crate::http::think(self.spec.think_ms, rng, &ctx.cancel).await;
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
        Ok(())
    }
}

#[async_trait]
impl Scenario for WsScenario {
    fn kind(&self) -> &'static str {
        "ws"
    }

    async fn run_vu(&self, ctx: VuCtx) {
        let mut rng = StdRng::seed_from_u64(ctx.seed);
        let mut backoff = Backoff::default();
        let mut iter = 0u64;
        while !ctx.cancel.is_cancelled() {
            let mut connected_at = None;
            // Dropping the session on cancel closes the socket at once (Stop within ~1 s).
            let result = tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                r = self.session(&ctx, &mut iter, &mut rng, &mut connected_at) => r,
            };
            if result.is_err() {
                iter += 1;
            }
            let out = Outcome {
                result,
                connected_at,
            };
            if !after_session(&ctx, out, &mut backoff, &mut rng).await {
                break;
            }
        }
    }
}

/// Per-connection buffers sized for 50k+ held sockets. tungstenite's default eagerly allocates a
/// 128 KiB read buffer per stream (~6.4 GB at 50k); load-test messages are small, and the read
/// buffer grows on demand for a larger frame. Message/frame caps bound what a hostile or broken
/// server can make an agent buffer.
pub(crate) fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(READ_BUFFER)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_MESSAGE)
        .max_message_size(Some(MAX_MESSAGE))
        .max_frame_size(Some(MAX_MESSAGE))
}

const READ_BUFFER: usize = 4096;
const MAX_MESSAGE: usize = 4 << 20;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_connection_buffers_are_small() {
        let c = ws_config();
        assert_eq!(c.read_buffer_size, 4096);
        assert_eq!(c.write_buffer_size, 0, "flush every message (rtt accuracy)");
        assert_eq!(c.max_message_size, Some(4 << 20));
        assert_eq!(c.max_frame_size, Some(4 << 20));
    }
}
