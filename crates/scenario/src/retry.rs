//! Reconnect policy shared by the connection-oriented scenarios (tcp, ws).
//!
//! Exponential backoff with full jitter: delay = uniform(0, min(CAP, BASE * 2^n)). Jitter
//! de-synchronises VUs after a mass disconnect (LB idle timeout, target restart), and the
//! exponent keeps a dead target from being hammered at a fixed rate. A healthy connection
//! resets the attempt count. Permanent errors (allow-guard denials, invalid config) stop the
//! VU: retrying cannot change the outcome and would only flood the error counters.
use loadfleet_engine::VuCtx;
use rand::rngs::StdRng;
use rand::Rng;
use std::time::Duration;

pub(crate) const BASE: Duration = Duration::from_millis(500);
pub(crate) const CAP: Duration = Duration::from_secs(10);
/// A connection that stayed up this long counts as healthy and resets the backoff.
pub(crate) const HEALTHY_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
pub(crate) struct Backoff {
    attempt: u32,
}

impl Backoff {
    /// Upper bound of the next delay (before jitter).
    pub(crate) fn ceiling(&self) -> Duration {
        BASE.saturating_mul(1u32 << self.attempt.min(16)).min(CAP)
    }

    /// Draws the next delay and advances the exponent.
    pub(crate) fn next_delay(&mut self, rng: &mut StdRng) -> Duration {
        let ceil = self.ceiling();
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_millis(rng.gen_range(0..=ceil.as_millis() as u64))
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Errors that the same VU would hit again on every retry.
pub(crate) fn is_permanent(code: &str) -> bool {
    code.starts_with("blocked:") || matches!(code, "ws:bad_url" | "ws:bad_header" | "tls:bad_name")
}

/// How a session ended, as far as reconnecting is concerned.
pub(crate) struct Outcome {
    pub(crate) result: Result<(), String>,
    /// When the connection was established (None = never connected).
    pub(crate) connected_at: Option<tokio::time::Instant>,
}

/// Records the error (if any), updates the backoff and sleeps. Returns false when the VU
/// must stop (cancelled, or a permanent error).
pub(crate) async fn after_session(
    ctx: &VuCtx,
    out: Outcome,
    backoff: &mut Backoff,
    rng: &mut StdRng,
) -> bool {
    let healthy = out
        .connected_at
        .is_some_and(|t| out.result.is_ok() || t.elapsed() >= HEALTHY_AFTER);
    if healthy {
        backoff.reset();
    }
    let Err(code) = out.result else {
        return true;
    };
    ctx.rec.error(&code);
    if is_permanent(&code) {
        return false;
    }
    let d = backoff.next_delay(rng);
    tokio::select! {
        _ = ctx.cancel.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn backoff_grows_caps_and_resets() {
        let mut b = Backoff::default();
        let ceilings: Vec<_> = (0..8)
            .map(|_| {
                let c = b.ceiling();
                b.attempt += 1;
                c.as_millis()
            })
            .collect();
        assert_eq!(
            ceilings,
            [500, 1000, 2000, 4000, 8000, 10_000, 10_000, 10_000]
        );
        b.attempt = u32::MAX;
        assert_eq!(b.ceiling(), CAP);
        b.reset();
        assert_eq!(b.ceiling(), BASE);
    }

    #[test]
    fn delays_are_jittered_within_the_ceiling() {
        let mut rng = StdRng::seed_from_u64(7);
        // Many VUs failing at the same moment must not retry in lockstep.
        let first: Vec<_> = (0..100)
            .map(|_| Backoff::default().next_delay(&mut rng))
            .collect();
        assert!(first.iter().all(|d| *d <= BASE));
        let distinct: std::collections::BTreeSet<_> = first.iter().collect();
        assert!(distinct.len() > 50, "{}", distinct.len());
        // One VU: each delay stays under its doubling ceiling, and later ones trend larger.
        let mut b = Backoff::default();
        let mut sum_early = Duration::ZERO;
        let mut sum_late = Duration::ZERO;
        for i in 0..10 {
            let ceil = b.ceiling();
            let d = b.next_delay(&mut rng);
            assert!(d <= ceil);
            if i < 2 {
                sum_early += d;
            } else if i >= 8 {
                sum_late += d;
            }
        }
        assert!(sum_late > sum_early, "{sum_early:?} vs {sum_late:?}");
    }

    #[test]
    fn permanent_errors() {
        assert!(is_permanent("blocked:localhost"));
        assert!(is_permanent("ws:bad_header"));
        assert!(!is_permanent("net:refused"));
        assert!(!is_permanent("tcp:closed_by_peer"));
        assert!(!is_permanent("ws:handshake"));
    }
}
