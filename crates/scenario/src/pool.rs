use crate::http::{HttpScenario, Sender};
use loadfleet_engine::OpCtx;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{fence, AtomicUsize, Ordering};
use tokio::sync::oneshot;

/// Upper bound on pool shards; each holds at least one slot.
const MAX_SHARDS: usize = 16;

/// Open-model keep-alive pool: `size` slots, each one connection with at most one request in
/// flight.
///
/// The slots are split over up to 16 shards. Each shard is a FIFO queue of free slots and an
/// atomic count of them. While nobody waits, an op scans the shards, starting at
/// `iter % shards`, and claims the first free slot without any pool-wide lock. Ops are not tied
/// to a slot, so a slow request only holds its own connection. The shard locks spread the
/// contention; the single semaphore they replaced was among the hottest locks at 40k ops/s
/// (`cargo xtask bench http-rps`).
///
/// When every slot is busy, an op joins one pool-wide FIFO of waiters, kept under a mutex. A
/// slot that frees up is handed straight to the oldest live waiter, with the claim moving over a
/// oneshot channel, so waiters are served in arrival order. While anyone waits, a new op joins
/// the back of the queue instead of claiming a slot itself, so no newcomer overtakes a waiter.
/// An op arrives when it reads the waiter count. An op that read "nobody waits" just before a
/// waiter queued arrived earlier and may still claim. A waiter's wait is therefore bounded by the
/// requests ahead of it.
///
/// Stop (`OpCtx::cancel`) is watched around the whole op. When it fires, the op gives up at
/// whatever it was waiting on (queue, connect, request), drops its connection and closes every
/// idle pooled connection, so Stop closes sockets at once instead of after the request timeout.
/// A waiter dropped by Stop gives back any slot handed to it that it had not taken yet.
pub(crate) struct Pool {
    shards: Box<[Shard]>,
    /// Live waiters: raised under `queue`'s lock when an op queues, lowered when its `Ticket`
    /// drops. Read without the lock to choose between the lock-free fast path and the queue, and
    /// by `publish` to decide whether to serve the queue.
    waiters: AtomicUsize,
    /// Waiting ops, oldest first; each receives the index of a shard whose slot it now owns.
    queue: Mutex<VecDeque<oneshot::Sender<usize>>>,
}

struct Shard {
    free: Mutex<VecDeque<Option<Sender>>>,
    /// Free slots in `free` that no op has claimed yet (never more than its length).
    avail: AtomicUsize,
}

impl Shard {
    fn try_claim(&self) -> bool {
        let mut a = self.avail.load(Ordering::Acquire);
        while a > 0 {
            match self
                .avail
                .compare_exchange_weak(a, a - 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(now) => a = now,
            }
        }
        false
    }
}

/// A slot taken from the pool; goes back on drop. Its connection goes back only when the op
/// finished its exchange (`keep`); an op that was stopped or aborted mid-way returns an empty
/// slot, so a connection with a request in flight is never handed to another op.
struct Lease<'a> {
    pool: &'a Pool,
    shard: usize,
    slot: Option<Sender>,
    keep: bool,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let slot = if self.keep { self.slot.take() } else { None };
        // queue first, then publish: a claimed count always finds a queued slot
        self.pool.shards[self.shard].free.lock().push_back(slot);
        self.pool.publish(self.shard);
    }
}

/// A waiter's place in the queue. Dropped untaken (Stop), it leaves the waiter count and gives
/// back a slot that was already handed to it.
struct Ticket<'a> {
    pool: &'a Pool,
    rx: oneshot::Receiver<usize>,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.rx.close(); // from here on the pump skips this waiter
        self.pool.waiters.fetch_sub(1, Ordering::SeqCst);
        if let Ok(shard) = self.rx.try_recv() {
            self.pool.publish(shard);
        }
    }
}

impl Pool {
    pub(crate) fn new(size: usize) -> Pool {
        let size = size.max(1);
        let n = size.min(MAX_SHARDS);
        let shards = (0..n)
            .map(|i| {
                let slots = size / n + usize::from(i < size % n);
                Shard {
                    free: Mutex::new((0..slots).map(|_| None).collect()),
                    avail: AtomicUsize::new(slots),
                }
            })
            .collect();
        Pool {
            shards,
            waiters: AtomicUsize::new(0),
            queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Claims a free slot in the first shard (from `home`) that has one.
    fn claim(&self, home: usize) -> Option<usize> {
        let n = self.shards.len();
        (0..n)
            .map(|i| (home + i) % n)
            .find(|&i| self.shards[i].try_claim())
    }

    fn take(&self, shard: usize) -> Lease<'_> {
        Lease {
            pool: self,
            shard,
            slot: self.shards[shard].free.lock().pop_front().flatten(),
            keep: false,
        }
    }

    /// Makes one claimed-but-unused slot of `shard` free again and serves the queue.
    fn publish(&self, shard: usize) {
        self.shards[shard].avail.fetch_add(1, Ordering::Release);
        // Pairs with the fence in `lease`: either that waiter's pump sees this slot, or this
        // sees the waiter and pumps.
        fence(Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) > 0 {
            self.pump(&mut self.queue.lock());
        }
    }

    /// Under the queue lock: hands free slots to the oldest live waiters.
    fn pump(&self, q: &mut VecDeque<oneshot::Sender<usize>>) {
        loop {
            while q.front().is_some_and(|tx| tx.is_closed()) {
                q.pop_front();
            }
            if q.is_empty() {
                return;
            }
            let Some(mut shard) = self.claim(0) else {
                return;
            };
            loop {
                let Some(tx) = q.pop_front() else {
                    // every remaining waiter left meanwhile: the slot is free again
                    self.shards[shard].avail.fetch_add(1, Ordering::Release);
                    return;
                };
                match tx.send(shard) {
                    Ok(()) => break,
                    Err(back) => shard = back,
                }
            }
        }
    }

    async fn lease(&self, home: usize) -> Lease<'_> {
        if self.waiters.load(Ordering::SeqCst) == 0 {
            if let Some(shard) = self.claim(home) {
                return self.take(shard);
            }
        }
        loop {
            let (tx, rx) = oneshot::channel();
            let mut ticket = {
                let mut q = self.queue.lock();
                self.waiters.fetch_add(1, Ordering::SeqCst);
                q.push_back(tx);
                let ticket = Ticket { pool: self, rx };
                fence(Ordering::SeqCst);
                self.pump(&mut q);
                ticket
            };
            // `Ok` moves the slot to the lease; the ticket then only leaves the count
            if let Ok(shard) = (&mut ticket.rx).await {
                return self.take(shard);
            }
            // sender dropped without a slot (cannot happen while the pool lives): queue again
        }
    }

    pub(crate) async fn op(&self, s: &HttpScenario, ctx: OpCtx) {
        tokio::select! {
            biased;
            // the op future (and with it any leased connection) is dropped before this runs
            _ = ctx.cancel.cancelled() => self.close_idle(),
            _ = self.exchange(s, &ctx) => {}
        }
    }

    async fn exchange(&self, s: &HttpScenario, ctx: &OpCtx) {
        let mut lease = self.lease(ctx.iter as usize).await;
        // a cached connection the server closed normally is replaced here without an op or error
        let r = s.checkout(&mut lease.slot).await;
        ctx.rec.op(s.metric());
        if let Err(e) = r {
            ctx.rec.error(&e);
            return;
        }
        let req = match s.build_request(&ctx.vu_id, ctx.iter) {
            Ok(r) => r,
            Err(e) => {
                ctx.rec.error(&e);
                lease.keep = true; // nothing was sent: the connection stays usable
                return;
            }
        };
        let r = s.exchange(&mut lease.slot, req).await;
        s.account(&ctx.rec, &r, ctx.scheduled); // from the planned time: coordinated-omission correction
        lease.keep = true;
    }

    /// Drops every connection not currently held by an op (outside the shard locks).
    fn close_idle(&self) {
        for shard in self.shards.iter() {
            let idle: Vec<Sender> = shard
                .free
                .lock()
                .iter_mut()
                .filter_map(Option::take)
                .collect();
            drop(idle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Sustained contention on a one-slot pool (so the recorded order is the hand-off order):
    /// the slot is busy, 20 ops queue one after another, then a stream of newcomers keeps
    /// arriving. Slots must go to the waiters in arrival order (no newcomer and
    /// no later waiter overtakes), and the k-th waiter waits about k slot hold times, not more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn waiters_are_served_in_arrival_order() {
        const WAITERS: usize = 20;
        const HOLD: Duration = Duration::from_millis(3);
        let pool = Arc::new(Pool::new(1));
        let order = Arc::new(Mutex::new(Vec::new()));
        let waits = Arc::new(Mutex::new(vec![Duration::ZERO; WAITERS]));
        let held = pool.lease(0).await;
        let spawn_op = {
            let (pool, order, waits) = (pool.clone(), order.clone(), waits.clone());
            move |id: usize, home: usize| {
                let (pool, order, waits) = (pool.clone(), order.clone(), waits.clone());
                tokio::spawn(async move {
                    let t = Instant::now();
                    let l = pool.lease(home).await;
                    order.lock().push(id);
                    if id < WAITERS {
                        waits.lock()[id] = t.elapsed();
                    }
                    tokio::time::sleep(HOLD).await;
                    drop(l);
                })
            }
        };
        let mut tasks = Vec::new();
        for k in 0..WAITERS {
            tasks.push(spawn_op(k, k));
            // the next op arrives only once this one is queued
            while pool.waiters.load(Ordering::SeqCst) < k + 1 {
                tokio::task::yield_now().await;
            }
        }
        let storm = {
            let spawn_op = spawn_op.clone();
            tokio::spawn(async move {
                let mut v = Vec::new();
                for j in 0..200 {
                    v.push(spawn_op(1000 + j, j));
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
                for t in v {
                    t.await.unwrap();
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        drop(held);
        for t in tasks {
            t.await.unwrap();
        }
        storm.await.unwrap();
        let order = order.lock().clone();
        let first: Vec<usize> = order.iter().copied().filter(|&id| id < WAITERS).collect();
        assert_eq!(
            first,
            (0..WAITERS).collect::<Vec<_>>(),
            "waiters overtaken: {order:?}"
        );
        let pos = order.iter().position(|&id| id == WAITERS - 1).unwrap();
        assert_eq!(pos, WAITERS - 1, "newcomers overtook waiters: {order:?}");
        // one slot, 3 ms per op: waiter k is served after about k holds (+ the 5 ms head start)
        let max = waits.lock().iter().copied().max().unwrap();
        assert!(max < Duration::from_millis(250), "max wait {max:?}");
    }

    /// A waiter stopped after a slot was handed to it (but before it ran) gives the slot back,
    /// and the next waiter or op can use it.
    #[tokio::test]
    async fn stopped_waiter_returns_a_handed_slot() {
        let pool = Pool::new(1);
        let held = pool.lease(0).await;
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut waiter = Box::pin(pool.lease(0));
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
        assert_eq!(pool.waiters.load(Ordering::SeqCst), 1);
        drop(held); // hands the slot to the queued waiter
        assert!(pool.claim(0).is_none(), "the slot went to the waiter");
        drop(waiter); // Stop before the waiter ran
        assert_eq!(pool.waiters.load(Ordering::SeqCst), 0);
        assert!(pool.claim(0).is_some(), "the handed slot is free again");
    }
}
