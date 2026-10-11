//! Per-socket clock sync: ping rounds and the offset estimate.
use loadfleet_protocol::{estimate, now_ms, ClockEstimate, ClockSample, ServerMsg, CLOCK_ROUNDS};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

const RESYNC_EVERY: Duration = Duration::from_secs(30);

/// Ping rounds and offset estimate for one socket. Only send times the server recorded
/// itself are used, so an agent cannot forge `server_ms` to skew its offset.
#[derive(Default)]
pub(super) struct ClockSync {
    seq: u32,
    pings_left: u32,
    resync_at: Option<Instant>,
    outstanding: HashMap<u32, i64>,
    samples: Vec<ClockSample>,
}

impl ClockSync {
    /// The next ping of the current round, or None between rounds (re-sync every 30 s).
    pub(super) fn next_ping(&mut self) -> Option<ServerMsg> {
        if self.pings_left == 0 {
            if self.resync_at.is_some_and(|t| Instant::now() < t) {
                return None;
            }
            self.pings_left = CLOCK_ROUNDS;
            self.resync_at = Some(Instant::now() + RESYNC_EVERY);
            self.outstanding.clear();
        }
        self.pings_left -= 1;
        self.seq = self.seq.wrapping_add(1);
        let server_ms = now_ms();
        self.outstanding.insert(self.seq, server_ms);
        Some(ServerMsg::Ping {
            id: self.seq,
            server_ms,
        })
    }

    /// Records a pong; returns the new estimate and whether a full window backs it.
    pub(super) fn pong(&mut self, id: u32, agent_ms: i64) -> Option<(ClockEstimate, bool)> {
        let sent_ms = self.outstanding.remove(&id)?;
        self.samples.push(ClockSample {
            sent_ms,
            agent_ms,
            recv_ms: now_ms(),
        });
        if self.samples.len() > CLOCK_ROUNDS as usize {
            self.samples.remove(0);
        }
        let e = estimate(&self.samples)?;
        Some((e, self.samples.len() >= CLOCK_ROUNDS as usize))
    }
}
