use std::time::Duration;
use tokio::time::Instant;

/// Expires when the server has been silent for longer than `limit`.
pub struct Watchdog {
    last: Instant,
    limit: Duration,
}

impl Watchdog {
    pub fn new(limit: Duration) -> Watchdog {
        Watchdog {
            last: Instant::now(),
            limit,
        }
    }

    pub fn feed(&mut self) {
        self.last = Instant::now();
    }

    pub fn expired(&self) -> bool {
        self.last.elapsed() > self.limit
    }

    /// Time left before the watchdog expires (zero once expired).
    pub fn remaining(&self) -> Duration {
        self.limit.saturating_sub(self.last.elapsed())
    }
}
