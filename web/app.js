// loadfleet panel helpers (no build step). The pages run under a strict CSP, so there are no
// inline scripts or on* attributes: pages declare `<body data-page="...">` and hooks live here.
(function () {
  // CSRF: every htmx request carries the header the server requires on state-changing calls
  document.addEventListener("htmx:configRequest", (e) => {
    e.detail.headers["x-loadfleet"] = "1";
  });
  // 4xx fragments (plan errors, start refusals, unknown run) are shown in place;
  // a lost session goes back to the login page
  document.addEventListener("htmx:beforeSwap", (e) => {
    const s = e.detail.xhr.status;
    if (s === 401) {
      location.href = "/login";
    } else if (s >= 400 && s < 500) {
      e.detail.shouldSwap = true;
      e.detail.isError = false;
    } else if (s >= 500) {
      // never echo a server error body; say something fixed instead
      e.detail.serverResponse = '<p class="bad">Server error (HTTP ' + s + "). Check the server log and retry.</p>";
      e.detail.shouldSwap = true;
      e.detail.isError = false;
    }
  });
  // Background tabs skip htmx *polls* (a poll re-fires on its interval anyway), but never an
  // element's first load: a page opened in a background tab must not stay blank. Elements
  // marked `data-lf-gone` (their run does not exist) stop requesting altogether.
  function skipRequest(o) {
    return o.gone || (o.hidden && o.verb === "get" && o.loaded);
  }
  document.addEventListener("htmx:beforeRequest", (e) => {
    const elt = e.detail.elt || { dataset: {} };
    const verb = e.detail.requestConfig && e.detail.requestConfig.verb;
    if (skipRequest({ hidden: document.hidden, verb, loaded: elt.dataset.lfLoaded === "1", gone: elt.dataset.lfGone === "1" })) {
      e.preventDefault();
    }
  });
  document.addEventListener("htmx:afterRequest", (e) => {
    if (e.detail.elt && e.detail.elt.dataset) e.detail.elt.dataset.lfLoaded = "1";
  });
  document.addEventListener("htmx:sendError", (e) => {
    const t = e.detail.target;
    if (t && (t.id === "result" || t.id === "token")) t.textContent = "Network error — retry.";
  });
  // failed form logins come back as /login?e=<code>; only these fixed texts are ever shown
  const LOGIN_ERRORS = {
    bad: "Wrong password.",
    rate: "Too many attempts. Wait a minute and try again.",
    busy: "The server is busy. Try again in a moment.",
    throttled: "Too many failed logins on this server: logins are limited to one every 10 seconds. Try again shortly; open sessions still work.",
  };
  document.addEventListener("DOMContentLoaded", () => {
    const box = document.getElementById("login-error");
    const code = new URLSearchParams(location.search).get("e");
    if (box && code && Object.prototype.hasOwnProperty.call(LOGIN_ERRORS, code)) {
      box.textContent = LOGIN_ERRORS[code];
      box.hidden = false;
    }
  });
  window.LF = window.LF || {};
  LF.skipRequest = skipRequest;
  LF.runId = () => decodeURIComponent(location.pathname.split("/").pop());
  LF.enc = encodeURIComponent;
  // point an element at a fragment URL; before DOMContentLoaded htmx picks it up by itself
  LF.load = (el, url) => {
    el.setAttribute("hx-get", url);
    if (document.readyState !== "loading") htmx.process(el);
  };
  function copyText(text, button) {
    const done = () => {
      const was = button.textContent;
      button.textContent = "Copied";
      setTimeout(() => (button.textContent = was), 1500);
    };
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(done);
    } else {
      const t = document.createElement("textarea");
      t.value = text;
      document.body.appendChild(t);
      t.select();
      document.execCommand("copy");
      t.remove();
      done();
    }
  }
  document.addEventListener("click", (e) => {
    const b = e.target.closest("[data-copy]");
    if (b) copyText(document.getElementById(b.dataset.copy).textContent, b);
    const h = e.target.closest("[data-clear]");
    if (h) document.getElementById(h.dataset.clear).textContent = "";
  });
  document.addEventListener("htmx:afterSwap", (e) => {
    const t = e.detail.target;
    if (t.id === "token") {
      try {
        t.textContent = JSON.parse(t.textContent).token;
      } catch (_) {
        t.textContent = "";
      }
    }
    document.querySelectorAll("td[data-ms]").forEach((td) => {
      td.textContent = new Date(+td.dataset.ms).toLocaleString();
    });
  });
})();
