// Series feed for the live and report charts, kept free of DOM and uPlot so it can be
// tested under node (scripts/panel-js-test.mjs).
(function (root) {
  const MAX_POINTS = 7200; // per series; older points are thinned 2:1 beyond this

  // Appends only points newer than the last one kept (a late or repeated response can
  // never duplicate or reorder x values, which uPlot requires ascending).
  function merge(points, fresh) {
    let last = points.length ? points[points.length - 1].t_s : -Infinity;
    for (const p of fresh) {
      if (p && typeof p.t_s === "number" && p.t_s > last) {
        points.push(p);
        last = p.t_s;
      }
    }
    return points;
  }

  // Keeps the browser's copy bounded on long runs: halves the older half, keeps the
  // newest half at full resolution.
  function cap(points, max = MAX_POINTS) {
    while (points.length > max) {
      const half = Math.floor(points.length / 2);
      const older = points.slice(0, half).filter((_, i) => i % 2 === 0);
      points = older.concat(points.slice(half));
    }
    return points;
  }

  // One request in flight at a time: the next poll is armed only after the previous one
  // settles. Errors back off (1 s doubling to 30 s). Stops for good once `finished()` is
  // true after a final fetch; pauses while `hidden()`; `wake()` resumes a paused feed.
  function poller(o) {
    const every = o.every || 1000;
    const maxBackoff = o.maxBackoff || 30000;
    const sleep = o.setTimeout || setTimeout;
    let points = [];
    let delay = every;
    let timer = null;
    let busy = false;
    let stopped = false;
    let paused = false;
    async function tick() {
      timer = null;
      if (stopped || busy) return;
      if (o.hidden && o.hidden()) {
        paused = true;
        return;
      }
      busy = true;
      const finalFetch = !!(o.finished && o.finished());
      let ok = false;
      try {
        const since = points.length ? points[points.length - 1].t_s : -1;
        const fresh = await o.fetchSeries(since);
        if (fresh === null) {
          stopped = true; // e.g. the session ended
        } else {
          const before = points.length;
          points = cap(merge(points, fresh), o.max);
          o.onData(points, points.length !== before || before === 0);
          ok = true;
        }
      } catch (e) {
        if (o.onError) o.onError(e);
      }
      busy = false;
      if (finalFetch && ok) stopped = true;
      if (stopped || !o.repeat) return;
      delay = ok ? every : Math.min(maxBackoff, delay * 2);
      timer = sleep(tick, delay);
    }
    return {
      start() {
        tick();
      },
      wake() {
        if (paused && !stopped && !busy && !timer) {
          paused = false;
          tick();
        }
      },
      stop() {
        stopped = true;
      },
      get points() {
        return points;
      },
      get stopped() {
        return stopped;
      },
      get delay() {
        return delay;
      },
    };
  }

  // The run status the charts go by after a summary fragment swap: an unknown run (404) is
  // final ("unknown run"), so the feed makes one last fetch and stops instead of polling on.
  function summaryStatus(httpStatus, dataStatus, prev) {
    if (httpStatus === 404 || httpStatus === 410) return "unknown run";
    return dataStatus || prev;
  }

  const api = { merge, cap, poller, summaryStatus, MAX_POINTS };
  root.LF = root.LF || {};
  root.LF.series = api;
})(typeof window !== "undefined" ? window : globalThis);
