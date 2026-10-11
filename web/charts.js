// Live and report charts: polls /api/runs/{id}/series and draws four uPlot charts.
(function () {
  const palette = ["#2a7ab0", "#d9822b", "#3a9a5b", "#b0413e", "#7b5ea7", "#8a8a8a"];
  const seen = {};
  function color(name) {
    if (!(name in seen)) seen[name] = palette[Object.keys(seen).length % palette.length];
    return seen[name];
  }
  function width(el) {
    return Math.max(240, Math.min(560, el.parentElement.clientWidth - 2));
  }
  function axis() {
    const cs = getComputedStyle(document.body);
    const fg = cs.getPropertyValue("--fg").trim() || "#222";
    const grid = cs.getPropertyValue("--grid").trim() || "#0001";
    return { stroke: fg, grid: { stroke: grid }, ticks: { stroke: grid } };
  }
  function chart(id, title, series) {
    const el = document.getElementById(id);
    el.innerHTML = "";
    return new uPlot(
      {
        title,
        width: width(el),
        height: 220,
        scales: { x: { time: false } },
        axes: [axis(), axis()],
        series: [{ label: "t (s)" }].concat(series.map((s) => ({ label: s, stroke: color(s), width: 2 }))),
      },
      [[]].concat(series.map(() => [])),
      el
    );
  }
  function build(points) {
    const metrics = [...new Set(points.flatMap((p) => Object.keys(p.latency)))].sort();
    const t = points.map((p) => p.t_s);
    return {
      metrics,
      vus: [t, points.map((p) => p.gauges.vus || 0), points.map((p) => p.gauges.connections || 0)],
      ops: [t, points.map((p) => p.ops)],
      p95: [t].concat(metrics.map((m) => points.map((p) => (p.latency[m] ? p.latency[m].p95_ms : null)))),
      err: [t, points.map((p) => (p.ops ? (100 * p.errors) / p.ops : 0))],
    };
  }
  let charts = null;
  let metricsKey = "";
  function draw(points) {
    const d = build(points);
    if (!charts || metricsKey !== d.metrics.join()) {
      if (charts) Object.values(charts).forEach((c) => c.destroy());
      charts = {
        vus: chart("c-vus", "VUs / connections", ["vus", "connections"]),
        ops: chart("c-ops", "ops/s", ["ops"]),
        p95: chart("c-p95", "p95 latency (ms)", d.metrics),
        err: chart("c-err", "error rate (%)", ["errors"]),
      };
      metricsKey = d.metrics.join();
    }
    charts.vus.setData(d.vus);
    charts.ops.setData(d.ops);
    charts.p95.setData(d.p95);
    charts.err.setData(d.err);
  }
  window.addEventListener("resize", () => {
    if (!charts) return;
    for (const [k, c] of Object.entries(charts)) c.setSize({ width: width(document.getElementById("c-" + k)), height: 220 });
  });
  // the run's status as last rendered by the summary fragment (`data-status`)
  let status = "";
  const FINAL = (s) => s !== "" && s !== "running" && s !== "pending";
  document.addEventListener("htmx:afterSwap", (e) => {
    if (e.detail.target.id !== "summary") return;
    const el = e.detail.target.querySelector("[data-status]");
    const code = e.detail.xhr ? e.detail.xhr.status : 200;
    status = LF.series.summaryStatus(code, el && el.dataset.status, status);
    if (code === 404 || code === 410) {
      // the run is gone: stop every fragment poll on the page
      document.querySelectorAll("[hx-trigger]").forEach((x) => (x.dataset.lfGone = "1"));
    }
  });
  function feed(id, repeat) {
    const p = LF.series.poller({
      repeat,
      hidden: () => document.hidden,
      finished: () => FINAL(status),
      async fetchSeries(since) {
        const r = await fetch(`/api/runs/${LF.enc(id)}/series?since=${since}`);
        if (r.status === 401) {
          location.href = "/login";
          return null;
        }
        if (r.status === 404 || r.status === 410) return null; // no such run: stop
        if (!r.ok) throw new Error(`series: HTTP ${r.status}`);
        return r.json();
      },
      onData(points, changed) {
        if (changed || !charts) draw(points);
      },
    });
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) p.wake();
    });
    p.start();
    return p;
  }
  LF.live = function () {
    const id = LF.runId();
    document.getElementById("runid").textContent = id;
    LF.load(document.getElementById("summary"), `/ui/runs/${LF.enc(id)}/summary`);
    LF.load(document.getElementById("agenthealth"), `/ui/runs/${LF.enc(id)}/agents`);
    document.getElementById("report").href = `/runs/${LF.enc(id)}`;
    const stop = document.getElementById("stop");
    stop.addEventListener("click", async () => {
      if (!confirm("Stop this test on every agent?")) return;
      try {
        const r = await fetch(`/api/runs/${LF.enc(id)}/stop`, { method: "POST", headers: { "x-loadfleet": "1" } });
        stop.textContent = r.ok ? "Stopping…" : "Not running";
        stop.disabled = true;
      } catch (_) {
        stop.textContent = "Stop (retry)";
      }
    });
    document.addEventListener("htmx:afterSwap", (e) => {
      if (e.detail.target.id === "summary" && FINAL(status)) {
        stop.disabled = true;
        stop.textContent = status;
      }
    });
    feed(id, true);
  };
  LF.run = function () {
    const id = LF.runId();
    document.getElementById("runid").textContent = id;
    LF.load(document.getElementById("summary"), `/ui/runs/${LF.enc(id)}/summary`);
    LF.load(document.getElementById("agenthealth"), `/ui/runs/${LF.enc(id)}/agents`);
    feed(id, false);
  };
  LF.compare = function () {
    LF.load(document.getElementById("cmp"), `/ui/compare${location.search}`);
  };
  const page = document.body.dataset.page;
  if (LF[page]) LF[page]();
})();
