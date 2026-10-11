//! Raw TCP scenario: connect (optionally TLS), optional send/expect exchange, then hold.
use crate::io::{dial_with, BoxIo, Held};
use crate::net::split_host_port;
use crate::retry::{after_session, Backoff, Outcome};
use crate::{tls, AllowGuard, Template, Vars};
use async_trait::async_trait;
use loadfleet_engine::{Scenario, VuCtx};
use loadfleet_health::classify_io_error;
use loadfleet_plan::{Payload, TcpSpec};
use parking_lot::Mutex;
use rand::{rngs::StdRng, SeedableRng};
use regex::bytes::Regex;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

/// Bytes buffered while waiting for `expect` before the reply counts as a mismatch.
const MAX_EXPECT_BUF: usize = 65_536;
/// Heap read reservation while waiting for `expect` (freed after the exchange).
const READ_CHUNK: usize = 4096;
/// Stack buffer used to drain a held connection.
const DRAIN_BUF: usize = 64;

enum SendPayload {
    Text(Template),
    Bytes(Vec<u8>),
}

enum Want {
    Bytes(Vec<u8>),
    Regex(Regex),
}

pub struct TcpScenario {
    spec: TcpSpec,
    targets: Vec<(String, u16)>,
    guard: AllowGuard,
    rr: AtomicUsize,
    send: Option<SendPayload>,
    want: Option<Want>,
    expect_timeout: Duration,
    next_conn: Option<(Duration, Mutex<Instant>)>,
    run: String,
    agent: String,
}

/// Non-empty hex (an empty pattern would match everywhere; an empty send sends nothing).
fn decode_hex(field: &str, h: &str) -> Result<Vec<u8>, String> {
    let b = hex::decode(h).map_err(|e| format!("{field}: {e}"))?;
    if b.is_empty() {
        return Err(format!("{field}: hex must not be empty"));
    }
    Ok(b)
}

impl TcpScenario {
    pub fn new(
        spec: &TcpSpec,
        guard: AllowGuard,
        run: &str,
        agent: &str,
    ) -> Result<TcpScenario, String> {
        let send = match &spec.send {
            None => None,
            Some(Payload::Text(t)) => Some(SendPayload::Text(Template::compile(t)?.with_env()?)),
            Some(Payload::Hex(h)) => Some(SendPayload::Bytes(decode_hex("scenario.tcp.send", h)?)),
        };
        let want = match &spec.expect {
            None => None,
            Some(e) => match (&e.hex, &e.regex) {
                (Some(h), _) => Some(Want::Bytes(decode_hex("scenario.tcp.expect.hex", h)?)),
                (None, Some(r)) => Some(Want::Regex(
                    Regex::new(r).map_err(|e| format!("scenario.tcp.expect.regex: {e}"))?,
                )),
                (None, None) => None,
            },
        };
        let targets = spec
            .addrs
            .list()
            .iter()
            .map(|a| split_host_port(a))
            .collect::<Result<Vec<_>, _>>()?;
        if targets.is_empty() {
            return Err("scenario.tcp.addr: at least one host:port".into());
        }
        let next_conn = spec
            .conn_rate
            .filter(|r| r.is_finite() && *r > 0.0)
            .map(|r| (Duration::from_secs_f64(1.0 / r), Mutex::new(Instant::now())));
        Ok(TcpScenario {
            targets,
            next_conn,
            expect_timeout: spec.expect.as_ref().map_or(spec.timeout, |e| e.timeout),
            spec: spec.clone(),
            guard,
            rr: AtomicUsize::new(0),
            send,
            want,
            run: run.into(),
            agent: agent.into(),
        })
    }

    /// Per-agent connection-rate limit (`conn_rate`): each connect takes the next free slot.
    async fn pace(&self) {
        if let Some((gap, next)) = &self.next_conn {
            let at = {
                let mut n = next.lock();
                let at = (*n).max(Instant::now());
                *n = at + *gap;
                at
            };
            tokio::time::sleep_until(at).await;
        }
    }

    fn matches(&self, buf: &[u8]) -> bool {
        match &self.want {
            None => true,
            Some(Want::Bytes(b)) => buf.windows(b.len()).any(|w| w == b.as_slice()),
            Some(Want::Regex(r)) => r.is_match(buf),
        }
    }

    async fn session(
        &self,
        ctx: &VuCtx,
        iter: u64,
        connected_at: &mut Option<Instant>,
    ) -> Result<(), String> {
        let (host, port) = &self.targets[(ctx.vu as usize) % self.targets.len()];
        self.pace().await;
        ctx.rec.op("tcp.connect");
        let t0 = Instant::now();
        let cfg = self.spec.tls.then(tls::raw_config);
        let (mut io, _tracked) =
            dial_with(host, *port, cfg, &self.guard, &self.rr, self.spec.timeout).await?;
        ctx.rec.latency("tcp.connect", t0.elapsed());
        *connected_at = Some(Instant::now());
        let _held = Held::new(ctx.rec.gauge("connections"));
        if let Some(send) = &self.send {
            let bytes = match send {
                SendPayload::Text(t) => t
                    .render(&Vars {
                        vu_id: &ctx.vu_id,
                        iter,
                        run: &self.run,
                        agent: &self.agent,
                    })
                    .into_bytes(),
                SendPayload::Bytes(b) => b.clone(),
            };
            ctx.rec.op("tcp.rtt");
            let t1 = Instant::now();
            io.write_all(&bytes)
                .await
                .map_err(|e| classify_io_error(&e).to_string())?;
            if self.want.is_some() {
                self.expect_reply(&mut io, t1 + self.expect_timeout).await?;
            }
            ctx.rec.latency("tcp.rtt", t1.elapsed());
        }
        match self.spec.hold_ms {
            Some(0) => {
                // Hold until the stage ends, draining anything the peer sends; a peer close is
                // an error and the VU reconnects. The drain buffer is tiny on purpose: it lives
                // in every held connection's future.
                let mut drain = [0u8; DRAIN_BUF];
                loop {
                    tokio::select! {
                        _ = ctx.cancel.cancelled() => break,
                        r = io.read(&mut drain) => match r {
                            Ok(0) | Err(_) => return Err("tcp:closed_by_peer".into()),
                            Ok(_) => {}
                        },
                    }
                }
            }
            Some(ms) => {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {},
                    _ = tokio::time::sleep(Duration::from_millis(ms)) => {},
                }
            }
            None => {}
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), io.shutdown()).await;
        Ok(())
    }

    /// Reads until `expect` matches. The buffer is heap-allocated only for the exchange, and
    /// the whole buffer is re-matched after each read so a reply split across segments matches.
    /// Unmatched data followed by a close or timeout is `tcp:mismatch`.
    async fn expect_reply(&self, io: &mut BoxIo, deadline: Instant) -> Result<(), String> {
        let mut buf: Vec<u8> = Vec::new();
        let ended = |buf: &Vec<u8>, code: &str| {
            Err(if buf.is_empty() { code } else { "tcp:mismatch" }.to_string())
        };
        loop {
            buf.reserve(READ_CHUNK);
            let n = match tokio::time::timeout_at(deadline, io.read_buf(&mut buf)).await {
                Err(_) => return ended(&buf, "tcp:expect_timeout"),
                Ok(Err(e)) => return Err(classify_io_error(&e).to_string()),
                Ok(Ok(n)) => n,
            };
            if n == 0 {
                return ended(&buf, "tcp:closed_by_peer");
            }
            if self.matches(&buf) {
                return Ok(());
            }
            if buf.len() > MAX_EXPECT_BUF {
                return Err("tcp:mismatch".into());
            }
        }
    }
}

#[async_trait]
impl Scenario for TcpScenario {
    fn kind(&self) -> &'static str {
        "tcp"
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
                r = self.session(&ctx, iter, &mut connected_at) => r,
            };
            iter += 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use loadfleet_metrics::Recorder;
    use loadfleet_plan::Addrs;

    /// Per-connection memory at 50k held connections: the session future (which holds the
    /// socket, the drain buffer and all locals) must stay small.
    #[test]
    fn held_connection_footprint_is_small() {
        let spec = TcpSpec {
            addrs: Addrs::One("127.0.0.1:1".into()),
            tls: false,
            send: None,
            expect: None,
            hold_ms: Some(0),
            conn_rate: None,
            timeout: Duration::from_secs(1),
        };
        let guard = AllowGuard::new(&["127.0.0.1".to_string()], None).unwrap();
        let s = TcpScenario::new(&spec, guard, "r", "a").unwrap();
        let ctx = VuCtx {
            vu: 0,
            vu_id: "v".into(),
            rec: Recorder::new().vu(0),
            cancel: Default::default(),
            seed: 0,
        };
        let mut at = None;
        let fut = s.session(&ctx, 0, &mut at);
        let size = std::mem::size_of_val(&fut);
        assert!(size <= 2048, "tcp session future is {size} bytes");
        const { assert!(DRAIN_BUF <= 128 && READ_CHUNK <= 4096) };
    }
}
