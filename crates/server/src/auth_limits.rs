//! Login attempt bookkeeping: the per-client failure windows and the global failure budget
//! across all clients (a throttle once spent). Both are O(1) amortized per attempt.
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Failed (and in-flight) attempts of one client in the current window.
struct Window {
    attempts: u32,
    since: Instant,
}

/// Per-client windows, bounded at `cap` clients. Expired windows are swept at most once per
/// `PRUNE_EVERY` and only when the map is full; when it is still full the oldest window goes,
/// found through an insertion-order queue (stale queue entries are skipped lazily and the queue
/// is rebuilt once it grows past twice the map).
pub(crate) struct FailureMap {
    map: HashMap<IpAddr, Window>,
    order: VecDeque<(IpAddr, Instant)>,
    last_prune: Option<Instant>,
    pub(crate) cap: usize,
}

const PRUNE_EVERY: Duration = Duration::from_secs(1);

impl FailureMap {
    pub(crate) fn new(cap: usize) -> FailureMap {
        FailureMap {
            map: HashMap::new(),
            order: VecDeque::new(),
            last_prune: None,
            cap,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    fn make_room(&mut self, now: Instant, window: Duration) {
        let due = self
            .last_prune
            .map_or(true, |t| now.duration_since(t) >= PRUNE_EVERY);
        if due {
            self.map
                .retain(|_, w| now.duration_since(w.since) <= window);
            self.last_prune = Some(now);
        }
        while self.map.len() >= self.cap {
            let Some((k, since)) = self.order.pop_front() else {
                break;
            };
            if self.map.get(&k).is_some_and(|w| w.since == since) {
                self.map.remove(&k);
            }
        }
    }

    fn open(&mut self, key: IpAddr, now: Instant) {
        self.map.insert(
            key,
            Window {
                attempts: 0,
                since: now,
            },
        );
        self.order.push_back((key, now));
        if self.order.len() > 2 * self.cap + 16 {
            let mut live: Vec<(IpAddr, Instant)> =
                self.map.iter().map(|(k, w)| (*k, w.since)).collect();
            live.sort_by_key(|(_, s)| *s);
            self.order = live.into();
        }
    }

    /// Counts an attempt for `key`; false when the client already used up `max` in `window`.
    pub(crate) fn reserve(&mut self, key: IpAddr, max: u32, window: Duration) -> bool {
        let now = Instant::now();
        match self.map.get(&key) {
            None => {
                if self.map.len() >= self.cap {
                    self.make_room(now, window);
                }
                self.open(key, now);
            }
            Some(w) if now.duration_since(w.since) > window => self.open(key, now),
            Some(_) => {}
        }
        let w = self.map.get_mut(&key).expect("window just opened");
        if w.attempts >= max {
            return false;
        }
        w.attempts += 1;
        true
    }

    /// Takes back one attempt.
    pub(crate) fn refund(&mut self, key: IpAddr) {
        if let Some(w) = self.map.get_mut(&key) {
            w.attempts = w.attempts.saturating_sub(1);
            if w.attempts == 0 {
                self.map.remove(&key);
            }
        }
    }

    pub(crate) fn clear(&mut self, key: IpAddr) {
        self.map.remove(&key);
    }
}

/// The server-wide failed-login budget: up to `budget` failed (or in-flight) logins across
/// every client per rolling `window` go at full speed; past that, logins are throttled
/// server-wide to one attempt per `spacing` (the rest are refused) until enough failures age
/// out of the window. Never a lockout: a free slot always reaches the password check, so the
/// admin can still log in while a brute force is slowed to `window / spacing` guesses.
/// Sessions already issued are not affected. The queue holds at most `budget` timestamps plus
/// the throttled ones (one per `spacing`) of the current window.
pub(crate) struct GlobalBudget {
    times: VecDeque<Instant>,
    /// When the last throttled attempt was let through.
    last_slot: Option<Instant>,
    pub(crate) budget: usize,
    pub(crate) window: Duration,
    pub(crate) spacing: Duration,
}

impl GlobalBudget {
    pub(crate) fn new(budget: usize, window: Duration, spacing: Duration) -> GlobalBudget {
        GlobalBudget {
            times: VecDeque::new(),
            last_slot: None,
            budget,
            window,
            spacing,
        }
    }

    /// Reserves one attempt; false when the budget is spent and the throttled slot is taken.
    pub(crate) fn reserve(&mut self) -> bool {
        let now = Instant::now();
        while self
            .times
            .front()
            .is_some_and(|t| now.duration_since(*t) > self.window)
        {
            self.times.pop_front();
        }
        if self.times.len() >= self.budget {
            if self
                .last_slot
                .is_some_and(|t| now.duration_since(t) < self.spacing)
            {
                return false;
            }
            self.last_slot = Some(now);
        }
        self.times.push_back(now);
        true
    }

    /// Gives back a reserved attempt that did not fail (success, or never checked).
    pub(crate) fn refund(&mut self) {
        self.times.pop_back();
    }
}
