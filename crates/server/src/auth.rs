//! Admin login: one password (argon2 hash in memory and at rest), per-IP failure limit,
//! a global gate on argon2 work, a global failure budget that throttles logins once spent,
//! random server-side sessions carried in an HttpOnly SameSite=Strict cookie.
use crate::auth_limits::{FailureMap, GlobalBudget};
use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString,
};
use argon2::Argon2;
use axum::http::HeaderMap;
use parking_lot::Mutex;
use rand::RngCore;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const MAX_SESSIONS: usize = 1024;
const MAX_FAILURES: u32 = 5;
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
/// Most clients tracked at once; when full, expired windows go first, then the oldest.
const MAX_TRACKED: usize = 10_000;
/// Concurrent argon2 verifications (~19 MiB and tens of ms of CPU each) and how long an
/// attempt may queue for one before it is answered "busy".
const GATE_PERMITS: usize = 2;
const GATE_WAIT: Duration = Duration::from_secs(2);
/// Failed logins across all clients per rolling `GLOBAL_WINDOW` before logins are throttled
/// server-wide to one per `THROTTLE_SPACING`.
pub const GLOBAL_FAILURE_BUDGET: usize = 200;
pub const GLOBAL_WINDOW: Duration = Duration::from_secs(3600);
pub const THROTTLE_SPACING: Duration = Duration::from_secs(10);
/// Shortest admin password the server starts with.
pub const MIN_PASSWORD_CHARS: usize = 12;
/// Longer inputs are refused without hashing.
const MAX_PASSWORD_LEN: usize = 1024;
pub const COOKIE: &str = "lf_session";

#[derive(Debug, PartialEq, Eq)]
pub enum LoginError {
    Bad,
    RateLimited,
    /// The verification gate stayed full (HTTP 429); the attempt was not counted.
    Busy,
    /// The server-wide failure budget is spent and this throttled slot (one attempt per
    /// `THROTTLE_SPACING` server-wide) is taken (HTTP 429); the attempt was not counted.
    Throttled,
}

pub struct Auth {
    hash: String,
    secure: bool,
    sessions: Mutex<HashMap<String, Instant>>,
    failures: Mutex<FailureMap>,
    global: Mutex<GlobalBudget>,
    gate: Arc<Semaphore>,
    gate_wait: Duration,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

/// An argon2id PHC string (`$argon2id$...`) with a fresh random salt.
pub fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .unwrap_or_default()
}

/// A random 256-bit session id, hex encoded.
fn new_session() -> String {
    let mut b = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

/// Rate-limit key: IPv6 clients usually own a whole /64, so they are limited per /64.
fn limit_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(a) => match a.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(Ipv6Addr::from(u128::from(a) & !((1u128 << 64) - 1))),
        },
        v4 => v4,
    }
}

impl Auth {
    pub fn new(password: &str, secure: bool) -> Auth {
        // an empty password must never open the panel: keep an unverifiable hash instead
        let hash = if password.is_empty() {
            String::new()
        } else {
            hash_password(password)
        };
        Auth::from_hash(hash, secure)
    }

    pub fn from_hash(hash: String, secure: bool) -> Auth {
        Auth {
            hash,
            secure,
            sessions: Mutex::new(HashMap::new()),
            failures: Mutex::new(FailureMap::new(MAX_TRACKED)),
            global: Mutex::new(GlobalBudget::new(
                GLOBAL_FAILURE_BUDGET,
                GLOBAL_WINDOW,
                THROTTLE_SPACING,
            )),
            gate: Arc::new(Semaphore::new(GATE_PERMITS)),
            gate_wait: GATE_WAIT,
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// Overrides the verification gate (tests; the server keeps 2 permits, 2 s).
    pub fn with_gate(mut self, permits: usize, wait: Duration) -> Auth {
        self.gate = Arc::new(Semaphore::new(permits));
        self.gate_wait = wait;
        self
    }

    /// Overrides how many clients the failure map tracks (tests; the server keeps 10 000).
    pub fn with_failure_cap(self, cap: usize) -> Auth {
        self.failures.lock().cap = cap.max(1);
        self
    }

    /// Overrides the global failure budget (the server keeps 200 per rolling hour).
    pub fn with_global_budget(self, budget: usize, window: Duration) -> Auth {
        {
            let mut g = self.global.lock();
            *g = GlobalBudget::new(budget, window, g.spacing);
        }
        self
    }

    /// Overrides the throttle past the budget (the server keeps one attempt per 10 s).
    pub fn with_throttle_spacing(self, spacing: Duration) -> Auth {
        self.global.lock().spacing = spacing;
        self
    }

    /// Clients with attempts in the current window.
    pub fn tracked_clients(&self) -> usize {
        self.failures.lock().len()
    }

    /// The most argon2 verifications ever seen running at once.
    pub fn peak_verifications(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Counts an attempt *before* the slow check, so parallel guesses from one client cannot
    /// slip past the limit; then spends one unit of the global budget.
    fn reserve(&self, key: IpAddr) -> Result<(), LoginError> {
        if !self
            .failures
            .lock()
            .reserve(key, MAX_FAILURES, FAILURE_WINDOW)
        {
            return Err(LoginError::RateLimited);
        }
        if !self.global.lock().reserve() {
            self.failures.lock().refund(key);
            return Err(LoginError::Throttled);
        }
        Ok(())
    }

    /// Takes back an attempt that never reached the password check.
    fn refund(&self, key: IpAddr) {
        self.failures.lock().refund(key);
        self.global.lock().refund();
    }

    /// argon2 verification (constant-time digest comparison). Empty or absurdly long input
    /// is refused without hashing.
    fn verify(&self, password: &str) -> bool {
        if password.is_empty() || password.len() > MAX_PASSWORD_LEN {
            return false;
        }
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let ok = PasswordHash::new(&self.hash)
            .map(|h| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &h)
                    .is_ok()
            })
            .unwrap_or(false);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        ok
    }

    /// After a check: a failure keeps its counted attempt, a success clears the client's
    /// window and opens a session.
    fn finish(&self, key: IpAddr, ok: bool) -> Result<String, LoginError> {
        if !ok {
            return Err(LoginError::Bad);
        }
        self.failures.lock().clear(key);
        self.global.lock().refund();
        let token = new_session();
        let now = Instant::now();
        let mut s = self.sessions.lock();
        s.retain(|_, exp| *exp > now);
        if s.len() >= MAX_SESSIONS {
            // drop the session closest to expiry (the oldest login)
            if let Some(k) = s.iter().min_by_key(|(_, e)| **e).map(|(k, _)| k.clone()) {
                s.remove(&k);
            }
        }
        s.insert(token.clone(), now + SESSION_TTL);
        Ok(token)
    }

    /// The server's login: rate limit, then argon2 on a blocking thread behind the global
    /// gate. When no permit frees up within the wait the answer is `Busy` and the attempt is
    /// refunded, so a flood cannot lock out the admin's own budget.
    pub async fn login_gated(
        self: &Arc<Self>,
        ip: IpAddr,
        password: String,
    ) -> Result<String, LoginError> {
        let key = limit_key(ip);
        self.reserve(key)?;
        let permit =
            match tokio::time::timeout(self.gate_wait, self.gate.clone().acquire_owned()).await {
                Ok(Ok(p)) => p,
                _ => {
                    self.refund(key);
                    return Err(LoginError::Busy);
                }
            };
        let me = self.clone();
        let ok = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            me.verify(&password)
        })
        .await
        .unwrap_or(false);
        self.finish(key, ok)
    }

    /// Synchronous login without the gate (blocks for the argon2 check); the HTTP handler
    /// uses `login_gated`.
    pub fn login(&self, ip: IpAddr, password: &str) -> Result<String, LoginError> {
        let key = limit_key(ip);
        self.reserve(key)?;
        let ok = self.verify(password);
        self.finish(key, ok)
    }

    fn session_of(headers: &HeaderMap) -> Option<String> {
        headers
            .get_all("cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .find_map(|kv| {
                let (k, v) = kv.trim().split_once('=')?;
                (k == COOKIE).then(|| v.to_string())
            })
    }

    pub fn check(&self, headers: &HeaderMap) -> bool {
        let Some(t) = Self::session_of(headers) else {
            return false;
        };
        let mut s = self.sessions.lock();
        match s.get(&t) {
            Some(exp) if *exp > Instant::now() => true,
            Some(_) => {
                s.remove(&t);
                false
            }
            None => false,
        }
    }

    pub fn logout(&self, headers: &HeaderMap) {
        if let Some(t) = Self::session_of(headers) {
            self.sessions.lock().remove(&t);
        }
    }

    /// The `Set-Cookie` value for a new session; `Secure` with `--secure-cookies`.
    pub fn cookie(&self, session: &str) -> String {
        self.cookie_with(session, self.secure)
    }

    /// As `cookie`, also `Secure` when `tls` (a trusted proxy terminated HTTPS).
    pub fn cookie_with(&self, session: &str, tls: bool) -> String {
        format!(
            "{COOKIE}={session}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}{}",
            SESSION_TTL.as_secs(),
            if self.secure || tls { "; Secure" } else { "" }
        )
    }

    /// The `Set-Cookie` value that deletes the session cookie in the browser.
    pub fn clear_cookie(&self, tls: bool) -> String {
        format!(
            "{COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{}",
            if self.secure || tls { "; Secure" } else { "" }
        )
    }
}
