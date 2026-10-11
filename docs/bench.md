# Self-bench: `cargo xtask bench`

Two self-checks of the load generator on one machine (spec success criterion 2):

```bash
cargo xtask bench tcp-hold [--conns 50000] [--hold 60s] [--target host:port,...] [--hosts 127.0.0.1,::1]
cargo xtask bench http-rps [--rate 50000] [--hold 60s] [--target http://host:port/] [--connections 512]
```

Each command builds `loadfleet` and `loadfleet-testserver` in release mode, starts a local
testserver unless `--target` is given, runs a two-stage plan through `loadfleet run` and prints a
verdict. The plan, `report.json` and `report.md` are kept in `reports/bench-<kind>-<unix>/`.

| bench | plan | passes when (last stage) |
| --- | --- | --- |
| `tcp-hold` | closed model, `--conns` VUs ramped over 20 s at 5000 new connections/s, then held for `--hold`; each VU holds one TCP connection to an echo listener | the `connections` gauge never drops below `--conns` during the hold and the client is not saturated |
| `http-rps` | open model, GET at `--rate` req/s (10 s ramp, then `--hold`), `max_vus` 4096, a keep-alive pool of `--connections` | at least 95 % of the rate, error rate below 0.1 % (dropped sends count as errors) and the client is not saturated |

A saturated client never passes silently: the verdict prints `client saturated: …` with the
reason (agent CPU > 85 %, runtime lag p95 > 50 ms, ephemeral ports ≥ 80 % used, or client-side
socket errors). The bench also prints how busy the whole host was before the run and during the
hold. The agent's own check only sees its own process. If other processes keep the CPU busy,
the client and the target get only what is left, so a missed rate on a busy host is labelled
`host CPU limited`.

The xtask raises its own open-file limit to the hard limit (the children inherit it). On macOS
that is `kern.maxfilesperproc` (92160 by default), which is enough for 50k connections without
sudo. The client and the testserver each need about one descriptor per connection.

## macOS loopback: the ephemeral-port wall

On Linux the kernel only needs the 4-tuple to be unique, so spreading connections over several
destination ports multiplies the space. macOS is different: `connect()` picks an ephemeral port
that is free for every local address of the address family. So there is one pool of
`net.inet.ip.portrange.first..last` (49152–65535 = 16384 ports by default) for all IPv4
connections, whatever the destination, and a second pool for IPv6. Measured on this Mac:
16273 connections to `127.0.0.1:19001`, then **0** more to `127.0.0.1:19002` or to the LAN
address `192.168.1.20:19003`, then 16338 to `[::1]:19004`.

So the default local `tcp-hold` target on macOS is two ports on `127.0.0.1` plus two on `::1`, which
gives about 32.7k connections at most. Extra destination ports or IPv4 addresses do not help. The
health monitor counts ports per family on macOS (per destination on Linux and the BSDs), so the
"ephemeral ports ≥ 80 %" flag trips at about 13.1k connections per family.

Past the limit, macOS fails `connect()` with `EADDRNOTAVAIL` for IPv4 and with `EAGAIN` for IPv6.
Both count as `client:addr_not_available`. Every failing attempt also stalls the client: worker
threads sit in `socket()`/`connect()` syscalls (seen with `sample`), and runtime lag p95 rose to
about 22 s. After a run, the closed sockets stay in TIME_WAIT for 2 × MSL = 30 s and still hold
their ports, so wait before the next run.

To get the full 50k on macOS you need one of:
- `sudo sysctl -w net.inet.ip.portrange.first=10240`, which gives 55296 ports per family;
- the echo target on another machine (`--target a:port,b:port`);
- running the bench on Linux.

There is also a way without sudo, not implemented: macOS accepts an explicit source-port `bind()`
with `SO_REUSEADDR` on one local port toward several destinations (checked by hand). Assigning
source ports in the client would therefore lift the limit.

## Results — 2026-10-11, MacBook Pro (Mac14,7, Apple M2, 4P+4E cores, 24 GB), macOS 26.5.1, rustc 1.94.0

The host was **not idle**. Before every run, 89–97 % of all CPU was already busy, mostly four
editor helper processes at about 100 % each. The client and the testserver shared what was left
on the same machine.

| run | result | key numbers |
| --- | --- | --- |
| `tcp-hold --conns 50000` | FAIL (kernel) | 32596 held (max 32626); 34381 `client:addr_not_available`; ports 100 %; runtime lag p95 21.9 s; connect p50/p99 11.1/27.1 s |
| `tcp-hold --conns 32000` | FAIL (ports flag only) | 32000 / 32000 held, 0 errors, connect p50/p99 0.4/2.0 ms, client CPU ~0 %, lag p95 7.1 ms; flagged because ports were 98 % used |
| `tcp-hold --conns 26000` | **PASS** | 26000 / 26000 held, 0 errors, connect p50/p99 0.2/0.4 ms, ports 79 %, lag p95 6.8 ms |
| `http-rps --rate 50000` | FAIL (host CPU) | 45668 and 46545 req/s in two runs; 0 HTTP errors (the second run dropped 203867 planned sends = 6.8 %); p50 ~1.04 s from the planned time; host 100 % busy during the hold; client process ~1 core (13 % of 8) |
| `http-rps --rate 40000` | **PASS** | 39992 req/s, 0 errors, p50/p95/p99 2.6/76/128 ms, lag p95 17.4 ms |
| after fix round 1 (pool waiters on any slot, concurrent drain), 30 s holds: `http-rps --rate 40000` | **PASS** | 39984 req/s, error rate 0.041 % (496 dropped sends), p50/p95/p99 2.2/78/189 ms, lag p95 18.2 ms; host 90 % busy before, 99 % during |
| same, `http-rps --rate 50000` | FAIL (host CPU) | 46610 req/s, 6.6 % dropped sends, p50 ~0.99 s, `open.aborted` 0; host 88 % busy before, 100 % during |
| after fix round 2 (FIFO hand-off to pool waiters), 30 s: `http-rps --rate 40000` | **PASS** | 39998 req/s, error rate 0.003 % (32 dropped sends), p50/p95/p99 2.1/88/171 ms, lag p95 14.8 ms; host 87 % busy before, 99 % during |

What limited it:
- **TCP: the macOS ephemeral-port pool**, not the client. Up to the per-family limit, connections
  open in under 2 ms at p99 and are held at ~0 % client CPU.
- **HTTP: host CPU.** The machine was already ~90 % busy, and the client (~1 core) plus the
  testserver (~0.5 core) together got only about 1.5 of the 8 cores. The client's own flags stay
  under their limits (CPU 13 %, runtime lag p95 ~25 ms), so only the host line shows the cause.
  Before the fixes below, the same setup reached 42.8k req/s with 14 % dropped sends.

### Client fixes the bench showed were needed

Profiles taken with `sample` at 40k+ req/s were dominated by lock convoys: parking_lot spinning,
then `sched_yield`, on a host with no idle CPU. Hot spots and fixes:

- open-model dispatcher (`engine/src/open.rs`):
  - ops that are already due start without a timer round-trip; a dispatcher that is behind
    yields to its worker through the coop budget;
  - ops are spread over 64 shards, each with its own child cancel token and its own `JoinSet`
    (completing a task locks its set); the end-of-run drain joins all shards concurrently
    within the 800 ms budget, so `open.aborted` counts only the ops that were really still
    running at the drain deadline (counted before they are cancelled);
  - the in-flight cap is an atomic counter with a `Notify` instead of a semaphore, whose permit
    release always takes its waiter lock.
- HTTP keep-alive pool (`scenario/src/pool.rs`):
  - ops are not tied to a slot: the slots sit in up to 16 sharded FIFO queues, each with an
    atomic free count, and an op takes the first free slot of any shard;
  - when every slot is busy, an op joins one pool-wide FIFO queue, and the next freed slot of
    any shard goes straight to the oldest waiter. Waiters are served in arrival order, and none
    waits behind one slow request while another connection is idle. While ops are waiting, a new
    op joins the back of the queue rather than claiming a freed slot itself (tested: 20 queued
    waiters plus a stream of newcomers on a one-slot pool, served in arrival order);
  - Stop is watched once around the whole op instead of at three separate waits.
- per-request work: the request URI and `Host` header are built once when the path has no
  placeholders.

Parked items checked against the bench:
- **Health fd refresher**: it now keeps the last good count on `EMFILE`.
- **DNS cache**: not needed for these numbers. The bench targets are IP literals, and DNS runs
  once per new connection, not per request.
