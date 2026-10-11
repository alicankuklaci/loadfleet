// Unit tests for web/series.js (the live chart feed) and app.js's request gate. Run: node scripts/panel-js-test.mjs
// Also run by `cargo test -p loadfleet-server --test panel_js_tests` when node is installed.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const src = readFileSync(new URL("../web/series.js", import.meta.url), "utf8");
const ctx = { setTimeout, console };
vm.createContext(ctx);
vm.runInContext(src, ctx);
const { merge, cap, poller, summaryStatus } = ctx.LF.series;
const pts = (...ts) => ts.map((t_s) => ({ t_s }));
const ts = (a) => Array.from(a, (p) => p.t_s); // copy out of the vm realm
const flush = () => new Promise((r) => setImmediate(r));

// merge: duplicates, replays and out-of-order points are dropped
assert.deepEqual(ts(merge(pts(0, 1, 2), pts(2, 1, 3, 3, 4))), [0, 1, 2, 3, 4]);
assert.deepEqual(ts(merge([], pts(5, 4, 6))), [5, 6]);

// cap: bounded, ascending, newest kept
{
  const big = cap(pts(...Array.from({ length: 20000 }, (_, i) => i)), 7200);
  assert.ok(big.length <= 7200, `len ${big.length}`);
  assert.equal(big[big.length - 1].t_s, 19999);
  assert.equal(big[0].t_s, 0);
  for (let i = 1; i < big.length; i++) assert.ok(big[i].t_s > big[i - 1].t_s);
}

// a fake clock: timers fire only when the test says so
function fakeClock() {
  const q = [];
  return {
    setTimeout: (fn, ms) => (q.push({ fn, ms }), q.length),
    pending: () => q.length,
    lastDelay: () => q[q.length - 1].ms,
    async fire() {
      const t = q.shift();
      t.fn();
      await flush();
    },
  };
}

// one request in flight: a slow response never overlaps the next poll
{
  const clock = fakeClock();
  let inflight = 0, maxInflight = 0, calls = 0;
  const resolvers = [];
  let data = [];
  const p = poller({
    repeat: true,
    setTimeout: clock.setTimeout,
    fetchSeries: (since) => {
      calls++; inflight++; maxInflight = Math.max(maxInflight, inflight);
      return new Promise((r) => resolvers.push(() => { inflight--; r(pts(since + 1, since + 2)); }));
    },
    onData: (points) => (data = points),
  });
  p.start();
  await flush();
  assert.equal(calls, 1);
  assert.equal(clock.pending(), 0, "no timer armed while a request is in flight");
  resolvers.shift()();
  await flush();
  assert.equal(clock.pending(), 1);
  await clock.fire();
  resolvers.shift()();
  await flush();
  assert.equal(maxInflight, 1);
  assert.deepEqual(ts(data), [0, 1, 2, 3]);
}

// errors back off (doubling, capped) and recover
{
  const clock = fakeClock();
  let fail = true;
  const p = poller({
    repeat: true, every: 1000, maxBackoff: 8000, setTimeout: clock.setTimeout,
    fetchSeries: async () => { if (fail) throw new Error("offline"); return []; },
    onData() {}, onError() {},
  });
  p.start();
  await flush();
  const delays = [clock.lastDelay()];
  for (let i = 0; i < 4; i++) { await clock.fire(); delays.push(clock.lastDelay()); }
  assert.deepEqual(delays, [2000, 4000, 8000, 8000, 8000]);
  fail = false;
  await clock.fire();
  assert.equal(clock.lastDelay(), 1000);
}

// stops after one final fetch once the run is finished; null (401) stops at once
{
  const clock = fakeClock();
  let done = false, calls = 0;
  const p = poller({
    repeat: true, setTimeout: clock.setTimeout, finished: () => done,
    fetchSeries: async () => (calls++, []), onData() {},
  });
  p.start();
  await flush();
  done = true;
  await clock.fire();
  assert.equal(calls, 2);
  assert.ok(p.stopped);
  assert.equal(clock.pending(), 0);
  const q = poller({ repeat: true, setTimeout: clock.setTimeout, fetchSeries: async () => null, onData() {} });
  q.start();
  await flush();
  assert.ok(q.stopped);
}

// hidden pages pause; wake resumes
{
  const clock = fakeClock();
  let hidden = false, calls = 0;
  const p = poller({
    repeat: true, setTimeout: clock.setTimeout, hidden: () => hidden,
    fetchSeries: async () => (calls++, []), onData() {},
  });
  p.start();
  await flush();
  hidden = true;
  await clock.fire();
  assert.equal(calls, 1);
  assert.equal(clock.pending(), 0, "paused: nothing armed");
  hidden = false;
  p.wake();
  await flush();
  assert.equal(calls, 2);
  assert.equal(clock.pending(), 1);
}
// an unknown run (404) is final, so the feed stops; otherwise the fragment's data-status wins
assert.equal(summaryStatus(404, undefined, ""), "unknown run");
assert.equal(summaryStatus(200, "running", ""), "running");
assert.equal(summaryStatus(286, "passed", "running"), "passed");
assert.equal(summaryStatus(500, undefined, "running"), "running");

// app.js: hidden tabs skip fragment polls but never an element's first load; gone elements
// never request again
{
  const listeners = {};
  const doc = {
    hidden: true,
    readyState: "complete",
    addEventListener: (name, fn) => (listeners[name] = listeners[name] || []).push(fn),
  };
  const app = { document: doc, location: { search: "", pathname: "/" }, URLSearchParams, setTimeout, console };
  app.window = app;
  vm.createContext(app);
  vm.runInContext(readFileSync(new URL("../web/app.js", import.meta.url), "utf8"), app);
  const fire = (name, detail) => {
    let prevented = false;
    for (const fn of listeners[name] || []) fn({ detail, preventDefault: () => (prevented = true) });
    return prevented;
  };
  const elt = { dataset: {} };
  const get = { elt, requestConfig: { verb: "get" } };
  assert.equal(fire("htmx:beforeRequest", get), false, "first load runs in a background tab");
  fire("htmx:afterRequest", { elt });
  assert.equal(elt.dataset.lfLoaded, "1");
  assert.equal(fire("htmx:beforeRequest", get), true, "later polls wait for the tab");
  doc.hidden = false;
  assert.equal(fire("htmx:beforeRequest", get), false, "visible tabs poll");
  elt.dataset.lfGone = "1";
  assert.equal(fire("htmx:beforeRequest", get), true, "gone elements stop");
  const post = { elt: { dataset: {} }, requestConfig: { verb: "post" } };
  doc.hidden = true;
  assert.equal(fire("htmx:beforeRequest", post), false, "posts always go");
}
console.log("panel js: ok");
