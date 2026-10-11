# loadfleet

Distributed load testing from many machines without SSH. A small control server with a built-in panel;
agents on any machine (macOS, Linux, Docker) dial **out** to it, so NAT and firewalls are not a problem.
HTTP (1.1/2), TCP and WebSocket scenarios, and a plugin interface for custom protocol scenarios. One static
Rust binary, no GC pauses in the load generator, mergeable HDR histograms for honest global percentiles.

> **Responsible use.** Only test systems you own or are explicitly allowed to test. The server refuses
> plans whose targets are not on its allow-list (`--allow`, mandatory), every agent re-checks the targets,
> tests are capped at 2 hours (`--max-duration`), and agents stop on their own 15 s after losing the
> server. Never run agents on the target's own servers: the load competes with the system under test
> and the numbers stop meaning anything.

## Install

```sh
curl --proto '=https' --tlsv1.2 -fsSL https://github.com/alicankuklaci/loadfleet/releases/latest/download/install.sh | sh
# or, without installing anything:
docker run --rm ghcr.io/alicankuklaci/loadfleet version
```

The installer verifies the sha256 of the static binary and prints the OS tuning an agent needs.

## Quick start (one machine)

```yaml
name: hello
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:8080/" } }
load: { model: closed, stages: [{target: 50, ramp: 10s, hold: 30s}] }
stop: { error_rate: 0.02, p95_ms: { http: 300 } }
```

Start something listening on `127.0.0.1:8080` first, or the error-rate rule stops the run at once.

```sh
loadfleet run hello.yaml    # writes reports/hello-<time>/report.md, report.json and series.csv
```

`loadfleet run <plan> [--out DIR] [--allow ENTRY] [--quiet]` runs on this machine only. `--allow` narrows
the plan's own allow-list. More plans are in `examples/`.

## Distributed

```sh
# Control server. The allow-list is mandatory. Secrets go in the environment, not in argv (visible in `ps`).
export LOADFLEET_ADMIN_PASSWORD='<strong password>'
loadfleet server --allow api.example.com,"*.staging.example.com"
# prints:  agent token: <token>   (also on the panel's Agents page)

# Every load machine (raise the limits first, see "Tuning agents"):
export LOADFLEET_TOKEN='<token from the panel>'
loadfleet agent --server wss://loadfleet.example.com --name "$(hostname)"
```

If you set no admin password the server generates one and prints it once on stderr.

Open the panel: **Agents** (online agents, clock offset, public IPs, the agent token with Rotate, and a ready
Cloudflare WAF expression) then **New test** (YAML editor with templates) then **Live** (VUs, ops/s, p95,
errors, per-agent health, Stop) then **History** (reports, export JSON/CSV/Markdown, compare two runs).

### Command reference

| command | flags (all also readable from the listed env var) |
| --- | --- |
| `loadfleet run <PLAN>` | `--out`, `--allow`, `--quiet` |
| `loadfleet server` | `--listen` (`LOADFLEET_LISTEN`, default `0.0.0.0:8080`), `--db` (`LOADFLEET_DB`, default `loadfleet.db`), `--allow` (`LOADFLEET_ALLOW`, required), `--admin-password` (`LOADFLEET_ADMIN_PASSWORD`), `--trust-proxy <CIDRs>` (`LOADFLEET_TRUST_PROXY`), `--trust-cf-header` (`LOADFLEET_TRUST_CF_HEADER`), `--secure-cookies` (`LOADFLEET_SECURE_COOKIES`), `--max-conns-per-agent` (default 100000), `--max-duration` (default `2h`) |
| `loadfleet agent` | `--server` (`LOADFLEET_SERVER`), `--token` (`LOADFLEET_TOKEN`), `--name` (`LOADFLEET_NAME`, default hostname), `--max-conns` (`LOADFLEET_MAX_CONNS`, default 100000), `--allow` (`LOADFLEET_AGENT_ALLOW`), `--public-ip` (`LOADFLEET_PUBLIC_IP`) |
| `loadfleet version` | |

Prefer `LOADFLEET_TOKEN` and `LOADFLEET_ADMIN_PASSWORD` over `--token` / `--admin-password`: a flag value is
visible to every local user in `ps`.

### Behind a reverse proxy or Cloudflare Tunnel

Forwarded headers are believed only when the direct TCP peer is in `--trust-proxy`, which takes **IPs or
CIDRs** (never a bare flag):

- Local nginx next to a native loadfleet: `--trust-proxy 127.0.0.1`. If loadfleet runs in Docker and nginx on the host, the peer is the bridge gateway, not `127.0.0.1`.
- Cloudflare Tunnel: the direct peer is the `cloudflared` container, so trust the **docker network's CIDR**
  that `cloudflared` and `loadfleet` share, and add `--trust-cf-header` so the server uses
  `CF-Connecting-IP`. `deploy/docker-compose.server.yml` does exactly this (network `edge`, subnet
  `172.30.0.0/24`, `LOADFLEET_TRUST_PROXY=172.30.0.0/24`, `LOADFLEET_TRUST_CF_HEADER=true`). With
  your own network, read the subnet from `docker network inspect <network>`. The compose file also locks
  the container down (`cap_drop: [ALL]`, `no-new-privileges`, read-only root with only the data volume
  and a small `/tmp` writable) and reads `LOADFLEET_ALLOW` / `LOADFLEET_ADMIN_PASSWORD` / `TUNNEL_TOKEN`
  from `.env`.
- `--trust-cf-header` **only** behind Cloudflare/cloudflared. Any other proxy passes a client-sent
  `CF-Connecting-IP` through, which would let anyone spoof the address the WAF suggestions are built from.
- Without `--trust-cf-header` the client IP is the rightmost `X-Forwarded-For` entry that is not itself a
  trusted proxy, so **your proxy must append the bare client IP** (e.g. `203.0.113.7`) to
  `X-Forwarded-For`: no port (`203.0.113.7:51234`), no brackets, no quotes. nginx's
  `$proxy_add_x_forwarded_for` does this. A hop that does not parse makes the server fall back to the
  proxy's own address, so every client would share one login rate-limit bucket.
- Use `--secure-cookies` whenever the panel is served over HTTPS.
- `POST /login` only accepts posts whose `Origin` (or `Referer`) is the panel's own `scheme://host`. The
  scheme is `https` with `--secure-cookies` or when a trusted proxy sends `X-Forwarded-Proto: https`; the
  host is a trusted proxy's `X-Forwarded-Host`, else `Host`. Behind nginx pass one of them through
  (`proxy_set_header Host $host;` or `proxy_set_header X-Forwarded-Host $host;`); cloudflared keeps the
  public `Host` by default.

### Admin login limits

- `--admin-password` / `LOADFLEET_ADMIN_PASSWORD` must be at least 12 characters, or the server refuses to
  start. Leave it unset to have a random one generated (printed once, stored only as an argon2 hash).
- 5 failed logins per client IP (IPv6: per /64) per minute, at most 2 password checks at once.
- Once more than 200 logins have failed in the last hour, from all clients together, logins are
  throttled server-wide: one attempt per 10 seconds goes through, the others get HTTP 429. This is a
  slowdown, not a lockout: the admin still gets in on a free slot, and normal speed returns once the
  failures age out of the hour. Sessions already logged in keep working; restarting the server also
  clears the counter.

## Tuning agents

Raise the limits before a big test; the panel flags a stage when ephemeral ports or file descriptors run out.

- File descriptors: `ulimit -n 1048576` (systemd `LimitNOFILE=`, launchd `SoftResourceLimits`, see
  `deploy/`). Docker: `--ulimit nofile=1048576:1048576`.
- Ephemeral ports: Linux `sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535" fs.file-max=2097152 net.core.somaxconn=65535`;
  macOS `sudo sysctl -w kern.maxfiles=2097152 kern.maxfilesperproc=1048576 net.inet.ip.portrange.first=1024 net.inet.ip.portrange.hifirst=1024`.
- One source IP has about 64k ephemeral ports per target `ip:port`: plan about 25k connections per machine
  per target address. Agents spread connections over every DNS answer of the target.
- Keep agents NTP-synced. Clock offset above 50 ms shows a warning; above 1 s the agent is not ready and
  will not be given a run.

Docker agent (host network avoids the NAT port table):

```sh
export LOADFLEET_TOKEN='<token from the panel>'
docker run -d --name lf-agent --restart unless-stopped --network host \
  --ulimit nofile=1048576:1048576 -e LOADFLEET_TOKEN ghcr.io/alicankuklaci/loadfleet \
  agent --server wss://loadfleet.example.com --name s1
```

The service files assume `/usr/local/bin/loadfleet`; if the installer fell back to `~/.local/bin`, edit the path.
Service files: `deploy/loadfleet-agent.service` (systemd, reads `/etc/loadfleet/agent.env`; sandboxed:
read-only system, no home, private /tmp, no capabilities, IPv4/IPv6/unix sockets only, writable
`/var/lib/loadfleet` only) and `deploy/io.loadfleet.agent.plist` (launchd; logs to
`~/Library/Logs/loadfleet/agent.log`, which launchd needs as an absolute path). Both keep the token in the
environment.

## Cloudflare WAF allow-list

If the target sits behind Cloudflare, agent traffic will be challenged or rate-limited unless it is let
through. The **Agents** page shows a ready expression built from the connected agents' addresses as the
server observed them (IPv4 addresses and IPv6 as their `/64`):

```
(ip.src in {203.0.113.7 198.51.100.4 2001:db8:1:2::/64})
```

An agent started with `--public-ip` has that claim shown next to its observed address as "reported",
but the expression never uses it: anyone holding the agent token could otherwise put any address into
your WAF rule. If an agent's outgoing address to the target differs from the one the server sees (a
different NAT or egress), check the reported value yourself and add it to the rule by hand. In the target
zone, Security, WAF, Custom rules, create two rules in this order, scoped to the load-test hostnames only:

1. **Skip**: expression = the panel's expression `and (http.host in {"loadtest-api.example.com"})`; skip
   all remaining custom rules, rate limiting rules, managed rules and Super Bot Fight Mode rules. Plain
   Bot Fight Mode may not be skippable by custom rules: turn it off for the test zone while testing.
2. **Block**: expression = `(http.host in {"loadtest-api.example.com"}) and not ip.src in {203.0.113.7 ...}`
   (the same address set), so nobody but your agents can reach the load-test stack.

Remove or disable both rules when the test is over, and refresh the expression whenever agents change.

## Plans

| key | meaning |
| --- | --- |
| `target.allow` | hosts, `*.domain`, IPs, CIDRs this plan may hit (the server's `--allow` must also permit them) |
| `scenario.type` | `http`, `tcp`, `ws` (or a type a compiled-in scenario plugin registers) with the matching block |
| `load.model` | `closed` (stage target = concurrent VUs) or `open` (target = ops/s, `max_vus` caps in-flight ops) |
| `load.stages` | `{target, ramp, hold}`; ramps are linear from the previous target |
| `stop` | `error_rate`, `p95_ms: {metric: ms}`, `stop_on_agent_loss`, `max_duration` (at most 2h) |
| `agents` | `min` ready agents, `weights: auto \| equal \| {name: weight}` |

Templates in HTTP/TCP/WS payloads: `{{vu}}` (unique `run-<id>-<agent>-<vu>`), `{{iter}}`, `{{uuid}}`,
`{{run}}`, `{{agent}}`, `{{env.LOADFLEET_VAR_NAME}}`.

`{{env.…}}` reads a variable from each agent's own environment when the run starts, and only
variables whose name starts with `LOADFLEET_VAR_` (anything else is a plan error). What that means
for a secret such as `LOADFLEET_VAR_API_TOKEN`:

- its value is not in the plan, so it is never stored on the server, shown in the panel or sent
  over the agent connection; only the agents read it, and they send it to the plan's target;
- anyone who can start plans (the admin, or whoever has the panel password) can send any
  `LOADFLEET_VAR_*` value of every agent to any host the server's and that agent's `--allow` lists
  permit. Only put values there you are fine sending to those hosts;
- the agent host's other variables (cloud credentials, `LOADFLEET_TOKEN`, ...) cannot be read by a
  plan.

```yaml
name: post-json-open
target: { allow: ["api.example.com"] }
scenario:
  type: http
  http:
    method: POST
    url: "https://api.example.com/v1/score"
    headers: { content-type: application/json, authorization: "Bearer {{env.LOADFLEET_VAR_API_TOKEN}}" }
    body: '{"player":"{{vu}}","n":{{iter}}}'
    expect_status: [200, 201]
load: { model: open, max_vus: 4000, stages: [{target: 2000, ramp: 30s, hold: 120s}] }
stop: { error_rate: 0.01, p95_ms: { http: 250 } }
```

## How to read results

- **Open model** latencies are measured from the *planned* send time, so a stalling server cannot hide
  behind a slowed-down client (coordinated-omission correction). Operations that could not start because
  every VU slot was busy are counted as `open.dropped`: they count as errors and mark the stage
  **rate not achieved**, meaning the target ops/s was not delivered; do not read that stage's latency as
  "the server is fine".
- **Closed model** measures each request from its actual send; under saturation it under-reports latency
  (the client waits, so fewer slow requests are issued). Prefer the open model for latency SLOs.
- A stage marked **LIMITED** had an agent above 85 % CPU, above 50 ms runtime lag p95, at 80 % or more of
  its ephemeral ports, or client socket errors (`client:*`): the numbers describe the load generator
  as well as the target. Add agents or lower the per-agent share and re-run before drawing conclusions.
- Reports (`report.md`, `report.json`, `series.csv`) carry per-stage percentiles merged from per-agent
  histograms, the stop reason and these flags.

## Building

```sh
cargo test --workspace
sh scripts/build-dist.sh linux    # static musl binaries via cargo-zigbuild (needs zig); `macos` builds natively
```

Turkish guide: [docs/tr/kullanim.md](docs/tr/kullanim.md). License: Apache-2.0.
