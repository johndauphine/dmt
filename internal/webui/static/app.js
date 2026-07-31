"use strict";
/* dmt WebUI single-page app (#582). Vanilla ES modules, no build step.
   Consumes the /api surface from #578-#581. Cookie auth (set at login) lets
   EventSource stream progress. */

// ---------- tiny helpers ----------
const $ = (sel, el = document) => el.querySelector(sel);
const el = (html) => { const t = document.createElement("template"); t.innerHTML = html.trim(); return t.content.firstElementChild; };
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));
const fmtTime = (t) => (t ? new Date(t).toLocaleString() : "—");
// Map a severity to a known CSS class — also neutralizes any unexpected value
// so it can never inject into the class attribute.
const sevClass = (s) => ({ error: "error", warn: "warning", warning: "warning", info: "info" }[String(s ?? "info").toLowerCase()] || "info");

// ---------- API client ----------
const api = {
  async req(method, path, body) {
    const opts = { method, headers: {}, credentials: "same-origin" };
    if (body !== undefined) { opts.headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(body); }
    const res = await fetch(path, opts);
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
    if (!res.ok) {
      const msg = (data && data.error && data.error.message) || res.statusText;
      const err = new Error(msg); err.status = res.status; err.code = data && data.error && data.error.code; err.data = data;
      throw err;
    }
    return data;
  },
  get(p) { return this.req("GET", p); },
  post(p, b) { return this.req("POST", p, b ?? {}); },
  del(p) { return this.req("DELETE", p); },
};

// origin fields the operator can attach to any command
function originBody() {
  const b = {};
  const c = $("#origin-config"), p = $("#origin-profile");
  if (c && c.value.trim()) b.config = c.value.trim();
  if (p && p.value.trim()) b.profile = p.value.trim();
  return b;
}

// ---------- toasts ----------
function toast(msg, kind = "ok") {
  let host = $(".toasts"); if (!host) { host = el(`<div class="toasts"></div>`); document.body.appendChild(host); }
  const t = el(`<div class="toast ${kind}"><div>${esc(msg)}</div></div>`);
  host.appendChild(t);
  setTimeout(() => t.remove(), 4200);
}

// ---------- theme ----------
function initTheme() {
  const saved = localStorage.getItem("dmt-theme");
  if (saved) document.documentElement.dataset.theme = saved;
}
function toggleTheme() {
  const cur = document.documentElement.dataset.theme
    || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  const next = cur === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  localStorage.setItem("dmt-theme", next);
}

// ---------- icons (inline, currentColor) ----------
const ico = {
  dash: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/></svg>`,
  history: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 4v4h4"/><path d="M12 8v4l3 2"/></svg>`,
  checks: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>`,
  setup: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-2.9 1.2 2 2 0 1 1-4 0 1.7 1.7 0 0 0-2.9-1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0-1.2-2.9 2 2 0 1 1 0-4 1.7 1.7 0 0 0 1.2-2.9l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 2.9-1.2 2 2 0 1 1 4 0 1.7 1.7 0 0 0 2.9 1.2l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.9Z"/></svg>`,
  profiles: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>`,
  settings: `<svg class="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 6h16M4 12h16M4 18h16"/><circle cx="9" cy="6" r="2" fill="var(--panel)"/><circle cx="15" cy="12" r="2" fill="var(--panel)"/><circle cx="8" cy="18" r="2" fill="var(--panel)"/></svg>`,
};

// ---------- app state ----------
const NAV = [
  { id: "dashboard", label: "Dashboard", icon: ico.dash },
  { id: "history", label: "History", icon: ico.history },
  { id: "checks", label: "Checks", icon: ico.checks },
  { id: "setup", label: "Setup", icon: ico.setup },
  { id: "profiles", label: "Profiles", icon: ico.profiles },
  { id: "settings", label: "Settings", icon: ico.settings },
];
let currentView = "dashboard";
let version = "";
let events = null; // EventSource

// ---------- boot ----------
initTheme();
boot();

async function boot() {
  // Consume and scrub a one-click ?token=… link unconditionally so the secret
  // never lingers in the address bar / history / Referer — even when a valid
  // session cookie already exists (in which case login is skipped).
  const urlToken = new URLSearchParams(location.search).get("token");
  if (urlToken) history.replaceState(null, "", location.pathname);
  try {
    const h = await api.get("/api/health"); // 401 if not signed in
    version = h.version || "";
    mountApp();
  } catch (e) {
    if (e.status === 401) mountLogin(urlToken);
    else document.getElementById("app").innerHTML = `<div class="empty">Cannot reach the server: ${esc(e.message)}</div>`;
  }
}

// ---------- login ----------
function mountLogin(urlToken) {
  const root = document.getElementById("app");
  root.innerHTML = "";
  const card = el(`
    <div class="login-wrap"><div class="card login-card">
      <div class="brand"><div class="logo">d</div><div><div class="word"><b>dmt</b> console</div></div></div>
      <p class="muted" style="text-align:center;margin:0 0 18px">Sign in with your access token.</p>
      <form id="login-form" class="stack">
        <div class="field"><label for="tok">Access token</label>
          <input id="tok" class="mono" type="password" autocomplete="off" placeholder="paste --webui-auth-token"></div>
        <button class="btn primary" type="submit">Sign in</button>
        <div id="login-msg" class="muted" style="min-height:18px;font-size:12.5px"></div>
      </form>
    </div></div>`);
  root.appendChild(card);
  const form = $("#login-form");
  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const btn = $("button", form); btn.disabled = true;
    $("#login-msg").textContent = "Signing in…";
    try {
      await api.post("/api/login", { token: $("#tok").value.trim() });
      await boot();
    } catch (e) {
      $("#login-msg").innerHTML = `<span style="color:var(--danger)">${esc(e.message)}</span>`;
      btn.disabled = false;
    }
  });

  // The one-click loopback token (already scrubbed from the URL in boot) is
  // passed in; auto-submit with it. requestSubmit() is Safari 16+ — fall back
  // to dispatching a cancelable submit event so the handler above still runs
  // (form.submit() would bypass it and do a native page-reload POST). (#596)
  if (urlToken) {
    $("#tok").value = urlToken;
    if (typeof form.requestSubmit === "function") form.requestSubmit();
    else form.dispatchEvent(new Event("submit", { cancelable: true }));
  }
}

// ---------- app shell ----------
function mountApp() {
  const root = document.getElementById("app");
  root.innerHTML = "";
  const shell = el(`
    <div class="shell">
      <aside class="sidebar">
        <div class="brand"><div class="logo">d</div>
          <div><div class="word"><b>dmt</b> console</div><div class="ver mono">v${esc(version)}</div></div></div>
        <nav class="nav" id="nav"></nav>
        <div class="sidebar-foot">
          <button class="btn ghost sm" id="palette-btn">⌘K &nbsp;Command palette</button>
          <div class="form-row" style="gap:6px">
            <button class="btn ghost sm" id="theme-btn" style="flex:1">Theme</button>
            <button class="btn ghost sm" id="logout-btn" style="flex:1">Sign out</button>
          </div>
        </div>
      </aside>
      <div class="content">
        <header class="topbar">
          <h1 id="view-title">Dashboard</h1>
          <div class="spacer"></div>
          <span id="run-pill" class="badge idle"><span class="dot"></span>idle</span>
        </header>
        <main id="view" class="view"></main>
      </div>
    </div>`);
  root.appendChild(shell);

  const nav = $("#nav");
  NAV.forEach((n) => {
    const b = el(`<button data-view="${n.id}">${n.icon}<span>${n.label}</span></button>`);
    b.addEventListener("click", () => go(n.id));
    nav.appendChild(b);
  });
  $("#theme-btn").addEventListener("click", toggleTheme);
  $("#palette-btn").addEventListener("click", openPalette);
  $("#logout-btn").addEventListener("click", async () => { try { await api.post("/api/logout"); } catch {} location.reload(); });

  connectEvents();
  startHeartbeat();
  registerServiceWorker();
  go(location.hash.replace("#", "") || "dashboard");
  refreshRunPill();
}

// registerServiceWorker enables the offline/instant-load app shell (sw.js).
// This is a pure enhancement: feature-detected and swallowing its own
// rejection, so a browser that refuses to register (older Safari has
// historically been inconsistent on http://localhost) is left with a fully
// working app — nothing here is load-bearing.
function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}

// ---------- desktop notifications ----------
// Fired on migration completion so a --gui window backgrounded during a long
// transfer still tells the operator when it's done.
function notifyPermissionPrimed() {
  // Safari requires the permission request to originate synchronously from a
  // user gesture — stricter than Chrome, which merely discourages requesting
  // on page load. So this must be called directly inside a click handler,
  // before any `await`; never from inside an async function body, where
  // WebKit no longer considers the call part of the gesture.
  if (typeof Notification === "undefined" || Notification.permission !== "default") return;
  Notification.requestPermission();
}
function notifyRunEnded(status) {
  if (typeof Notification === "undefined" || Notification.permission !== "granted") return;
  if (!document.hidden) return; // the operator is already looking at it
  const title = { completed: "Migration complete", failed: "Migration failed", cancelled: "Migration cancelled" }[status];
  if (!title) return;
  try { new Notification(title, { body: "dmt console", icon: "/icon-192.png" }); } catch {}
}

// startHeartbeat keeps the session alive while the tab is open — an
// authenticated ping every few minutes slides the server-side session and the
// cookie, so passively watching a long migration never expires the session
// (and Cancel keeps working). Closing the tab stops the pings, so the session
// still lapses on its TTL when unattended. (#601)
let heartbeat = null;
function startHeartbeat() {
  if (heartbeat) clearInterval(heartbeat);
  heartbeat = setInterval(() => {
    // Pinging slides the session; a 401 means it lapsed anyway (e.g. server
    // restart or the absolute cap) — stop pinging and bounce to login.
    api.get("/api/health").catch((e) => { if (e.status === 401) { clearInterval(heartbeat); heartbeat = null; mountLogin(); } });
  }, 4 * 60 * 1000);
}

function go(view) {
  if (!NAV.some((n) => n.id === view)) view = "dashboard";
  currentView = view;
  location.hash = view;
  $$(".nav button").forEach((b) => b.classList.toggle("active", b.dataset.view === view));
  $("#view-title").textContent = NAV.find((n) => n.id === view).label;
  renderView();
}
const $$ = (s, e = document) => [...e.querySelectorAll(s)];

function renderView() {
  const v = $("#view");
  v.innerHTML = "";
  ({ dashboard: viewDashboard, history: viewHistory, checks: viewChecks,
     setup: viewSetup, profiles: viewProfiles, settings: viewSettings }[currentView] || viewDashboard)(v);
}

// ---------- shared origin form ----------
function originFields() {
  return `<div>
    <div class="form-grid">
      <div class="field"><label>Config file</label><input id="origin-config" class="mono" type="text" placeholder="config.yaml (or leave blank)"></div>
      <div class="field"><label>Profile</label><input id="origin-profile" class="mono" type="text" placeholder="saved profile name"></div>
    </div>
    <button type="button" class="btn sm ghost" data-action="origin-browse" style="margin-top:8px">Browse configs & profiles…</button>
  </div>`;
}

// openOriginPicker shows a dialog to pick a saved profile or a config file
// found on the server, instead of typing a path. Selecting one fills the
// origin inputs in the current view.
async function openOriginPicker() {
  if ($(".overlay")) return;
  const ov = el(`<div class="overlay"><div class="dialog">
    <div class="dialog-head">Choose a config or profile</div>
    <div class="dialog-body"><div class="empty"><span class="spinner"></span> Loading…</div></div>
  </div></div>`);
  document.body.appendChild(ov);
  // All three close paths (Escape, click-outside, pick) route through close()
  // so the keydown listener is always removed — no listener/DOM leak.
  const onKey = (e) => { if (e.key === "Escape") close(); };
  const close = () => { ov.remove(); document.removeEventListener("keydown", onKey); };
  ov.addEventListener("click", (e) => { if (e.target === ov) close(); });
  document.addEventListener("keydown", onKey);

  const [cfgs, profs] = await Promise.all([
    api.get("/api/configs").catch(() => ({ configs: [] })),
    api.get("/api/profiles").catch(() => ({ profiles: [] })),
  ]);
  const body = $(".dialog-body", ov);
  body.innerHTML = "";
  body.appendChild(pickSection("Saved profiles", (profs.profiles || []).map((p) => ({
    label: p.name, sub: p.description || "", pick: () => { fillOrigin({ profile: p.name }); close(); },
  })), "No saved profiles yet — create one in Profiles."));
  body.appendChild(pickSection("Config files", (cfgs.configs || []).map((c) => ({
    label: c.name, sub: c.path, pick: () => { fillOrigin({ config: c.path }); close(); },
  })), "No config files found where dmt was launched."));
}

function pickSection(title, items, emptyMsg) {
  const wrap = el(`<div class="dialog-section"><div class="eyebrow">${esc(title)}</div></div>`);
  if (!items.length) { wrap.appendChild(el(`<div class="muted" style="padding:6px 2px;font-size:13px">${esc(emptyMsg)}</div>`)); return wrap; }
  const ul = el(`<ul class="pick-list"></ul>`);
  items.forEach((it) => {
    const li = el(`<li><div class="pick-label mono">${esc(it.label)}</div>${it.sub ? `<div class="pick-sub muted">${esc(it.sub)}</div>` : ""}</li>`);
    li.addEventListener("click", it.pick);
    ul.appendChild(li);
  });
  wrap.appendChild(ul);
  return wrap;
}

// fillOrigin sets the origin inputs (config XOR profile — mutually exclusive).
// The caller closes the dialog.
function fillOrigin({ config, profile }) {
  const c = $("#origin-config"), p = $("#origin-profile");
  if (c) c.value = config || "";
  if (p) p.value = profile || "";
  toast(config ? `Config: ${config.split("/").pop()}` : `Profile: ${profile}`);
}

// ---------- Dashboard ----------
function viewDashboard(v) {
  v.appendChild(el(`<header><h2>Migration</h2><p>Launch and monitor a data migration in real time.</p></header>`));

  const launcher = el(`<div class="card">
    <h3>Launch</h3><div class="sub">Pick a config or profile, then run — or preview the plan first.</div>
    ${originFields()}
    <div class="form-grid" style="margin-top:12px">
      <label class="checkline"><input type="checkbox" id="opt-dryrun"> Dry run (preview plan only)</label>
      <label class="checkline"><input type="checkbox" id="opt-confirm"> Confirm backup (allow dropping populated tables)</label>
      <label class="checkline"><input type="checkbox" id="opt-advisor"> AI schema advisor (with dry run)</label>
    </div>
    <div class="form-row" style="margin-top:16px">
      <button class="btn primary" id="btn-run">Run migration</button>
      <button class="btn" id="btn-resume">Resume</button>
      <button class="btn" id="btn-cancel" disabled>Cancel</button>
    </div>
    <div id="dryrun-out" class="result"></div>
  </div>`);
  v.appendChild(launcher);

  const telem = el(`<div class="card" id="telemetry-card">
    <div class="phase-row"><span id="run-phase" class="badge idle"><span class="dot"></span>idle</span>
      <span class="muted mono" id="run-id"></span></div>
    <div class="progress"><i id="pbar"></i></div>
    <div class="telemetry" style="margin-top:16px">
      <div class="stat"><div class="k">Progress</div><div class="v"><span id="s-pct">0</span><small>%</small></div></div>
      <div class="stat"><div class="k">Rows transferred</div><div class="v num"><span id="s-rows">0</span><small id="s-rtot"></small></div></div>
      <div class="stat"><div class="k">Rows / sec</div><div class="v num" id="s-rps">0</div></div>
      <div class="stat"><div class="k">Tables</div><div class="v num"><span id="s-tdone">0</span><small> / <span id="s-ttot">0</span></small></div></div>
      <div class="stat"><div class="k">Running now</div><div class="v num" id="s-trun">0</div></div>
      <div class="stat"><div class="k">Failed tables</div><div class="v num" id="s-errs">0</div></div>
    </div>
    <div class="tablegrid" id="tablegrid"></div>
  </div>`);
  v.appendChild(telem);

  $("#btn-run").addEventListener("click", () => {
    notifyPermissionPrimed();
    startRun("/api/run", {
      dry_run: $("#opt-dryrun").checked,
      confirm_backup: $("#opt-confirm").checked,
      ai_schema_advisor: $("#opt-advisor").checked,
    });
  });
  $("#btn-resume").addEventListener("click", () => { notifyPermissionPrimed(); startRun("/api/resume", {}); });
  $("#btn-cancel").addEventListener("click", async () => {
    try { await api.post("/api/run/cancel"); toast("Cancelling migration…"); }
    catch (e) { toast(e.message, "err"); }
  });

  syncDashboardFromState();
}

async function startRun(path, opts) {
  $("#dryrun-out").innerHTML = "";
  try {
    const body = Object.assign(originBody(), opts);
    const r = await api.post(path, body);
    if (r && "tables" in r) { // dry-run returns the plan synchronously (the tables key is present even when empty)
      $("#dryrun-out").appendChild(renderDryRun(r));
      toast("Dry run complete");
      return;
    }
    toast(`Started ${r.kind} ${r.id}`);
    setRunPill("run", "running");
  } catch (e) {
    if (e.status === 409) toast("A migration is already running", "err");
    else toast(e.message, "err");
  }
}

function syncDashboardFromState() {
  api.get("/api/run").then((s) => {
    if (s.run) applyRunState(s.run);
  }).catch(() => {});
}

// ---------- SSE ----------
function connectEvents() {
  if (events) events.close();
  events = new EventSource("/api/events");
  const onProgress = (e) => applyProgress(JSON.parse(e.data).progress);
  ["progress"].forEach((t) => events.addEventListener(t, onProgress));
  ["started", "done", "failed", "cancelled"].forEach((t) =>
    events.addEventListener(t, (e) => applyRunState(JSON.parse(e.data).run)));
  events.onerror = () => {
    // Transient drops auto-reconnect. A fatal close (readyState CLOSED) means
    // the reconnect got a non-2xx — most likely an expired session — so probe
    // and bounce to login rather than freezing progress silently.
    if (events.readyState === EventSource.CLOSED) {
      api.get("/api/health").catch((e) => { if (e.status === 401) { events.close(); mountLogin(); } });
    }
  };
}

function applyProgress(p) {
  if (!p || currentView !== "dashboard") return;
  const pct = Math.round(p.progress_pct || 0);
  set("#s-pct", pct); set("#pbar", null, (i) => (i.style.width = pct + "%"));
  set("#s-rows", fmtNum(p.rows_transferred));
  set("#s-rtot", p.rows_total ? ` / ${fmtNum(p.rows_total)}` : "");
  set("#s-rps", fmtNum(p.rows_per_second));
  set("#s-tdone", fmtNum(p.tables_complete)); set("#s-ttot", fmtNum(p.tables_total));
  set("#s-trun", fmtNum(p.tables_running || 0));
  setFailedTables(p.error_count || 0);
  const phase = $("#run-phase"); if (phase) { phase.className = "badge run"; phase.innerHTML = `<span class="dot"></span>${esc(p.phase || "running")}`; }
  if (Array.isArray(p.current_tables)) {
    const g = $("#tablegrid");
    if (g) { g.innerHTML = ""; p.current_tables.forEach((t) => g.appendChild(el(`<span class="cell running">${esc(t)}</span>`))); }
  }
}

function applyRunState(run) {
  if (!run) return;
  const st = run.status;
  const kind = { running: "run", completed: "ok", failed: "err", cancelled: "warn" }[st] || "idle";
  setRunPill(kind, st);
  const btnCancel = $("#btn-cancel"); if (btnCancel) btnCancel.disabled = st !== "running";
  if (currentView === "dashboard") {
    const phase = $("#run-phase");
    if (phase) { phase.className = `badge ${kind}`; phase.innerHTML = `<span class="dot"></span>${esc(st)}`; }
    set("#run-id", run.id ? "run " + run.id : "");
    if (st === "completed") set("#s-pct", 100), set("#pbar", null, (i) => (i.style.width = "100%"));
    // Finished runs carry their final tally (#591); a fresh page load has no
    // progress stream, so populate the telemetry from the runState. The Go
    // side omits zero counts (omitempty), so any present count means the tally
    // was captured — default the rest to 0. When ALL are absent the tally was
    // never captured (e.g. a failure before transfer); keep whatever the live
    // stream last showed rather than zeroing it.
    const hasTally = run.rows_transferred != null || run.tables_total != null;
    if (hasTally && (st === "completed" || st === "failed" || st === "cancelled")) {
      set("#s-rows", fmtNum(run.rows_transferred ?? 0));
      set("#s-rps", fmtNum(run.rows_per_second ?? 0));
      set("#s-tdone", fmtNum(run.tables_complete ?? 0));
      set("#s-ttot", fmtNum(run.tables_total ?? 0));
      set("#s-trun", "0");
      setFailedTables(run.tables_failed || 0);
    }
    if (run.error) $("#dryrun-out").innerHTML = `<div class="finding error"><div class="sev"></div><div>${esc(run.error)}</div></div>`;
  }
  if (st === "completed" || st === "failed") toast(`Migration ${st}`, st === "completed" ? "ok" : "err");
  if (st === "completed" || st === "failed" || st === "cancelled") notifyRunEnded(st);
}

function set(sel, text, fn) { const e = $(sel); if (!e) return; if (text != null) e.textContent = text; if (fn) fn(e); }
// Failed-table count turns red the moment it is non-zero so accumulating
// failures are visible mid-run, not just at the end.
function setFailedTables(n) { set("#s-errs", fmtNum(n), (e) => { e.style.color = n > 0 ? "var(--danger)" : ""; }); }
function setRunPill(kind, label) { const p = $("#run-pill"); if (p) { p.className = `badge ${kind}`; p.innerHTML = `<span class="dot"></span>${esc(label)}`; } }
function refreshRunPill() { api.get("/api/run").then((s) => { if (s.run) applyRunState(s.run); }).catch(() => {}); }

function renderDryRun(r) {
  const rows = (r.tables || []).map((t) =>
    `<tr><td class="mono">${esc(t.name)}</td><td class="num">${fmtNum(t.row_count)}</td><td class="mono">${esc(t.pagination_method)}</td><td>${t.has_pk ? "yes" : "no"}</td><td class="num">${t.columns}</td></tr>`).join("");
  return el(`<div>
    <dl class="kvs">
      <dt>Route</dt><dd>${esc(r.source_type)} → ${esc(r.target_type)}</dd>
      <dt>Tables</dt><dd class="num">${fmtNum(r.total_tables)}</dd>
      <dt>Total rows</dt><dd class="num">${fmtNum(r.total_rows)}</dd>
      <dt>Target mode</dt><dd>${esc(r.target_mode)}</dd>
      <dt>Workers</dt><dd class="num">${fmtNum(r.workers)}</dd>
    </dl>
    <div class="table-wrap" style="margin-top:14px"><table class="data"><thead><tr><th>Table</th><th>Rows</th><th>Pagination</th><th>PK</th><th>Cols</th></tr></thead><tbody>${rows}</tbody></table></div>
  </div>`);
}

// ---------- History ----------
const HISTORY_STATUSES = ["running", "success", "partial", "failed"];
const HISTORY_PAGE = 20;
// Paging/filter state for the current History visit. Reset on each render
// because the origin inputs (config/profile) are also re-entered per render —
// carrying a stale offset into a fresh origin would show an empty page.
let histState = { status: "", offset: 0 };
function viewHistory(v) {
  histState = { status: "", offset: 0 };
  v.appendChild(el(`<header><h2>Run history</h2><p>Runs are recorded per config — pick the config or profile whose history you want.</p></header>`));
  const card = el(`<div class="card">${originFields()}
    <div class="form-row" style="margin-top:12px;align-items:flex-end">
      <div class="field"><label for="hist-status">Status</label>
        <select id="hist-status" class="mono">
          <option value="">All statuses</option>
          ${HISTORY_STATUSES.map((s) => `<option value="${s}">${s}</option>`).join("")}
        </select></div>
      <button class="btn primary" id="hist-load">Load history</button>
    </div>
    <div id="hist-body" style="margin-top:16px"></div>
    <div id="hist-pager" class="pager" style="margin-top:14px"></div></div>`);
  v.appendChild(card);
  // Reloading (new config/profile) or changing the status filter resets to
  // page 1; the pager's Prev/Next advance the offset within a result set.
  $("#hist-load").addEventListener("click", () => { histState.offset = 0; loadHistory(); });
  $("#hist-status").addEventListener("change", (e) => { histState.status = e.target.value; histState.offset = 0; loadHistory(); });
  loadHistory();
}
async function loadHistory() {
  const host = $("#hist-body"), pager = $("#hist-pager"); if (!host) return;
  host.innerHTML = `<span class="spinner"></span> Loading…`;
  if (pager) pager.innerHTML = "";
  const params = originBody();
  if (histState.status) params.status = histState.status;
  params.limit = HISTORY_PAGE;
  params.offset = histState.offset;
  const q = new URLSearchParams(params).toString();
  try {
    const { runs, total, offset } = await api.get("/api/history?" + q);
    if (!runs.length) {
      host.innerHTML = `<div class="empty">${histState.status || histState.offset ? "No runs match this filter." : "No runs recorded for this config yet."}</div>`;
      return;
    }
    const rows = runs.map((r) => `<tr>
      <td class="mono">${esc(r.id)}</td>
      <td>${statusBadge(r.status)}</td>
      <td class="mono">${esc(r.phase || "—")}</td>
      <td class="mono num">${fmtTime(r.started_at)}</td>
      <td>${r.error ? `<span style="color:var(--danger)">${esc(r.error)}</span>` : "—"}</td>
    </tr>`).join("");
    host.innerHTML = `<div class="table-wrap"><table class="data"><thead><tr><th>Run</th><th>Status</th><th>Phase</th><th>Started</th><th>Error</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    renderPager(pager, offset, runs.length, total);
  } catch (e) { host.innerHTML = `<div class="finding error"><div class="sev"></div><div>${esc(e.message)}</div></div>`; }
}
// renderPager draws the "X–Y of Z" summary and Prev/Next buttons, disabling
// each end when there is no further page.
function renderPager(host, offset, count, total) {
  if (!host || total <= HISTORY_PAGE) return;
  const from = offset + 1, to = offset + count;
  const prev = el(`<button class="btn sm" ${offset <= 0 ? "disabled" : ""}>← Prev</button>`);
  const next = el(`<button class="btn sm" ${to >= total ? "disabled" : ""}>Next →</button>`);
  prev.addEventListener("click", () => { histState.offset = Math.max(0, offset - HISTORY_PAGE); loadHistory(); });
  next.addEventListener("click", () => { histState.offset = offset + HISTORY_PAGE; loadHistory(); });
  host.innerHTML = "";
  host.appendChild(prev);
  host.appendChild(el(`<span class="muted mono" style="font-size:12.5px">${from}–${to} of ${fmtNum(total)}</span>`));
  host.appendChild(next);
}
function statusBadge(s) {
  const k = { success: "ok", completed: "ok", running: "run", failed: "err", partial: "warn", cancelled: "warn", interrupted: "warn" }[s] || "idle";
  return `<span class="badge ${k}"><span class="dot"></span>${esc(s)}</span>`;
}

// ---------- Checks ----------
const CHECKS = [
  { id: "preflight", label: "Preflight", path: "/api/preflight", render: renderPreflight, blurb: "Verify connectivity, privileges, versions, and encoding." },
  { id: "config", label: "Config check", path: "/api/config/check", render: renderDryRun, blurb: "Preview the migration plan (dry run)." },
  { id: "validate", label: "Validate", path: "/api/validate", render: renderValidate, blurb: "Compare source and target row counts." },
  { id: "diagnose", label: "Diagnose", path: "/api/diagnose", render: renderTriage, blurb: "Build deterministic failure facts for a run." },
  { id: "analyze", label: "Analyze", path: "/api/analyze", render: renderAnalyze, blurb: "Suggest tuning based on the source database." },
];
let checkTab = "preflight";
function viewChecks(v) {
  v.appendChild(el(`<header><h2>Checks</h2><p>Advisory and validation commands — read-only, safe to run anytime.</p></header>`));
  const tabs = el(`<div class="tabs"></div>`);
  CHECKS.forEach((c) => { const b = el(`<button data-c="${c.id}">${c.label}</button>`); b.addEventListener("click", () => { checkTab = c.id; renderView(); }); tabs.appendChild(b); });
  v.appendChild(tabs);
  $$("button", tabs).forEach((b) => b.classList.toggle("active", b.dataset.c === checkTab));
  const c = CHECKS.find((x) => x.id === checkTab);
  const card = el(`<div class="card"><h3>${c.label}</h3><div class="sub">${c.blurb}</div>
    ${originFields()}
    <div class="form-row" style="margin-top:14px"><button class="btn primary" id="run-check">Run ${c.label.toLowerCase()}</button></div>
    <div id="check-out" class="result"></div></div>`);
  v.appendChild(card);
  $("#run-check").addEventListener("click", async () => {
    const out = $("#check-out"); out.innerHTML = `<span class="spinner"></span> Running…`;
    try {
      const r = await api.post(c.path, originBody());
      out.innerHTML = ""; out.appendChild(c.render(r));
    } catch (e) { out.innerHTML = `<div class="finding error"><div class="sev"></div><div>${esc(e.message)}</div></div>`; }
  });
}
function renderPreflight(r) {
  const conn = (name, ok, lat, type, err) => `<dt>${name}</dt><dd>${ok ? `<span class="badge ok"><span class="dot"></span>connected</span> ${esc(type)} · ${lat}ms` : `<span class="badge err"><span class="dot"></span>failed</span> ${esc(err || "")}`}</dd>`;
  const findings = (r.preflight_findings || []).map((f) =>
    `<div class="finding ${sevClass(f.severity)}"><div class="sev"></div><div><b class="mono">${esc(f.check || "")}</b><div class="muted">${esc(f.message || "")}</div>${f.remedy ? `<div class="muted" style="margin-top:3px">↳ ${esc(f.remedy)}</div>` : ""}</div></div>`).join("");
  return el(`<div>
    <div class="phase-row">${r.healthy ? `<span class="badge ok"><span class="dot"></span>healthy</span>` : `<span class="badge warn"><span class="dot"></span>attention needed</span>`}</div>
    <dl class="kvs">${conn("Source", r.source_connected, r.source_latency_ms, r.source_db_type, r.source_error)}${conn("Target", r.target_connected, r.target_latency_ms, r.target_db_type, r.target_error)}</dl>
    ${findings ? `<div style="margin-top:14px">${findings}</div>` : `<p class="muted" style="margin-top:12px">No preflight findings.</p>`}
  </div>`);
}
function renderValidate(r) {
  const rows = (r.rows || []).map((x) => {
    const bad = x.failed || x.difference !== 0;
    return `<tr><td class="mono">${esc(x.table)}</td><td class="num">${fmtNum(x.source_count)}</td><td class="num">${fmtNum(x.target_count)}</td><td class="num" style="color:${bad ? "var(--danger)" : "var(--ok)"}">${x.difference > 0 ? "+" : ""}${fmtNum(x.difference)}</td></tr>`;
  }).join("");
  return el(`<div>
    <div class="phase-row">${r.failed ? `<span class="badge err"><span class="dot"></span>mismatch</span>` : `<span class="badge ok"><span class="dot"></span>counts match</span>`}<span class="muted mono">mode: ${esc(r.mode)}</span></div>
    <div class="table-wrap"><table class="data"><thead><tr><th>Table</th><th>Source</th><th>Target</th><th>Δ</th></tr></thead><tbody>${rows}</tbody></table></div>
  </div>`);
}
function renderTriage(r) {
  const findings = (r.findings || []).map((f) =>
    `<div class="finding ${sevClass(f.severity)}"><div class="sev"></div><div><b>${esc(f.category || "finding")}</b><div class="muted">${esc(f.likely_cause || f.next_action || f.manual_inspection || f.affected || "")}</div></div></div>`).join("");
  return el(`<div><p class="muted">${esc(r.summary || "Deterministic diagnosis")}</p>${findings || `<p class="muted">No findings.</p>`}</div>`);
}
function renderAnalyze(r) {
  return el(`<div><dl class="kvs">
    <dt>Tables</dt><dd class="num">${fmtNum(r.total_tables)}</dd>
    <dt>Total rows</dt><dd class="num">${fmtNum(r.total_rows)}</dd>
    <dt>Suggested workers</dt><dd class="num">${fmtNum(r.workers)}</dd>
    <dt>Chunk size</dt><dd class="num">${fmtNum(r.chunk_size_recommendation)}</dd>
    <dt>Write-ahead writers</dt><dd class="num">${fmtNum(r.write_ahead_writers)}</dd>
    <dt>Tier</dt><dd>${esc(r.tier || "—")}</dd>
  </dl>${r.reasoning ? `<p class="muted" style="margin-top:12px">${esc(r.reasoning)}</p>` : ""}</div>`);
}

// ---------- Setup ----------
async function viewSetup(v) {
  v.appendChild(el(`<header><h2>Guided setup</h2><p>Configure secrets, connections, and a migration config step by step.</p></header>`));
  const card = el(`<div class="card"><div id="setup-body"></div></div>`);
  v.appendChild(card);
  let prompt;
  try { prompt = await api.get("/api/setup/prompt"); }
  catch { prompt = null; }
  renderSetupPrompt(prompt);
}
async function setupStart() { renderSetupPrompt(await api.post("/api/setup/start")); }
async function setupSend(input) {
  const body = $("#setup-body"); body.innerHTML = `<span class="spinner"></span> Working…`;
  try { renderSetupPrompt(await api.post("/api/setup/input", { input })); }
  catch (e) { toast(e.message, "err"); }
}
function renderSetupPrompt(p) {
  const body = $("#setup-body"); if (!body) return;
  if (!p || p.done === undefined) {
    body.innerHTML = `<div class="empty">Start a guided setup to configure dmt.</div>`;
    body.appendChild(el(`<div style="text-align:center"><button class="btn primary" id="s-start">Start setup</button></div>`));
    $("#s-start").addEventListener("click", setupStart);
    return;
  }
  if (p.done) {
    body.innerHTML = `<div class="phase-row"><span class="badge ok"><span class="dot"></span>done</span></div>
      <p>Setup complete. Configuration saved to <span class="mono">${esc(p.config_path)}</span>.</p>`;
    body.appendChild(el(`<button class="btn" id="s-again">Run setup again</button>`));
    $("#s-again").addEventListener("click", setupStart);
    return;
  }
  body.innerHTML = "";
  if (p.section_header) body.appendChild(el(`<div class="setup-section">${esc(p.section_header)}</div>`));
  if (p.error) body.appendChild(el(`<div class="finding error"><div class="sev"></div><div>${esc(p.error)}</div></div>`));
  body.appendChild(el(`<div class="setup-prompt">${esc(p.text)}${p.default ? ` <span class="muted mono">[${esc(p.default)}]</span>` : ""}</div>`));

  if (p.choices && p.choices.length) {
    const row = el(`<div class="choice-row"></div>`);
    p.choices.forEach((c) => { const b = el(`<button class="btn sm">${esc(c)}</button>`); b.addEventListener("click", () => setupSend(c)); row.appendChild(b); });
    body.appendChild(row);
  }
  const form = el(`<form class="form-row" style="margin-top:12px">
    <input id="s-input" class="mono" type="${p.is_masked ? "password" : "text"}" autocomplete="off" style="flex:1;min-width:220px" placeholder="${p.default ? "Enter to accept default" : "type your answer"}">
    <button class="btn primary" type="submit">Continue</button></form>`);
  form.addEventListener("submit", (e) => { e.preventDefault(); setupSend($("#s-input").value); });
  body.appendChild(form);
  const inp = $("#s-input"); if (inp) inp.focus();
}

// ---------- Profiles ----------
async function viewProfiles(v) {
  v.appendChild(el(`<header><h2>Profiles</h2><p>Encrypted, reusable connection configs stored in the checkpoint database.</p></header>`));
  const save = el(`<div class="card"><h3>Save a profile</h3><div class="sub">Encrypt a config file as a named profile (requires a master key).</div>
    <div class="form-row"><div class="field" style="flex:1"><label>Config file</label><input id="p-config" class="mono" type="text" placeholder="config.yaml"></div>
    <div class="field"><label>Name (optional)</label><input id="p-name" class="mono" type="text" placeholder="from profile.name"></div>
    <button class="btn primary" id="p-save">Save</button></div></div>`);
  v.appendChild(save);
  $("#p-save").addEventListener("click", async () => {
    try { const r = await api.post("/api/profiles/save", { config: $("#p-config").value.trim(), name: $("#p-name").value.trim() }); toast(`Saved profile ${r.name}`); viewProfilesList(); }
    catch (e) { toast(e.message, "err"); }
  });
  const list = el(`<div class="card"><h3>Saved profiles</h3><div id="p-list" style="margin-top:12px"><span class="spinner"></span></div></div>`);
  v.appendChild(list);
  viewProfilesList();
}
async function viewProfilesList() {
  const host = $("#p-list"); if (!host) return;
  try {
    const { profiles } = await api.get("/api/profiles");
    if (!profiles.length) { host.innerHTML = `<div class="empty">No profiles saved.</div>`; return; }
    host.innerHTML = `<div class="table-wrap"><table class="data"><thead><tr><th>Name</th><th>Description</th><th>Updated</th><th></th></tr></thead><tbody>${
      profiles.map((p) => `<tr><td class="mono">${esc(p.name)}</td><td>${esc(p.description || "—")}</td><td class="mono num">${fmtTime(p.updated_at)}</td>
        <td style="text-align:right"><button class="btn sm danger" data-del="${esc(p.name)}">Delete</button></td></tr>`).join("")}</tbody></table></div>`;
    $$("[data-del]", host).forEach((b) => b.addEventListener("click", async () => {
      try { await api.del("/api/profiles/" + encodeURIComponent(b.dataset.del)); toast("Profile deleted"); viewProfilesList(); }
      catch (e) { toast(e.message, "err"); }
    }));
  } catch (e) { host.innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
}

// ---------- Settings ----------
async function viewSettings(v) {
  v.appendChild(el(`<header><h2>Settings</h2><p>Session defaults, secrets, and cache — applied to this server for its lifetime.</p></header>`));
  const sess = el(`<div class="card"><h3>Session defaults</h3><div class="sub">Set once; honored by subsequent commands.</div><div id="sess-body"><span class="spinner"></span></div></div>`);
  v.appendChild(sess);
  const maint = el(`<div class="card"><h3>Maintenance</h3>
    <div class="form-row" style="margin-top:6px">
      <label class="checkline"><input type="checkbox" id="c-aionly"> AI entries only</label>
      <button class="btn" id="c-clear">Clear type cache</button>
      <button class="btn" id="c-secrets">Create secrets file</button>
    </div></div>`);
  v.appendChild(maint);
  $("#c-clear").addEventListener("click", async () => { try { const r = await api.post("/api/cache/clear", { ai_only: $("#c-aionly").checked }); toast(r.message || "Cache cleared"); } catch (e) { toast(e.message, "err"); } });
  $("#c-secrets").addEventListener("click", async () => { try { const r = await api.post("/api/init-secrets", {}); toast(r.message || "Secrets file created"); } catch (e) { toast(e.message, "err"); } });
  loadSession();
}
async function loadSession() {
  const host = $("#sess-body"); if (!host) return;
  try {
    const { keys } = await api.get("/api/session");
    host.innerHTML = `<div class="stack">${keys.map((k) => `<div class="form-row" style="align-items:center">
      <div class="field" style="flex:1"><label>${esc(k.name)}</label><input data-k="${esc(k.name)}" class="mono" type="text" value="${esc(k.value || "")}" placeholder="${esc(k.description)}"></div>
      <button class="btn sm" data-set="${esc(k.name)}">Set</button></div>`).join("")}</div>`;
    $$("[data-set]", host).forEach((b) => b.addEventListener("click", async () => {
      const inp = $(`[data-k="${b.dataset.set}"]`, host);
      try { await api.post("/api/session", { key: b.dataset.set, value: inp.value.trim() }); toast(`Set ${b.dataset.set}`); }
      catch (e) { toast(e.message, "err"); }
    }));
  } catch (e) { host.innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
}

// ---------- command palette ----------
const COMMANDS = [
  { label: "Go to Dashboard", run: () => go("dashboard") },
  { label: "Go to History", run: () => go("history") },
  { label: "Go to Checks", run: () => go("checks") },
  { label: "Go to Setup", run: () => go("setup") },
  { label: "Go to Profiles", run: () => go("profiles") },
  { label: "Go to Settings", run: () => go("settings") },
  { label: "Run migration", run: () => { go("dashboard"); setTimeout(() => $("#btn-run")?.click(), 60); } },
  { label: "Resume migration", run: () => { go("dashboard"); setTimeout(() => $("#btn-resume")?.click(), 60); } },
  { label: "Cancel migration", run: () => api.post("/api/run/cancel").then(() => toast("Cancelling…")).catch((e) => toast(e.message, "err")) },
  // Every check tab is reachable by name — generated from CHECKS so the
  // palette can't drift from the Checks view.
  ...CHECKS.map((c) => ({ label: c.label, run: () => { checkTab = c.id; go("checks"); } })),
  { label: "Toggle theme", run: toggleTheme },
  { label: "Sign out", run: () => api.post("/api/logout").finally(() => location.reload()) },
];
let paletteSel = 0, paletteItems = COMMANDS;
function openPalette() {
  if ($(".overlay")) return;
  const ov = el(`<div class="overlay"><div class="palette">
    <input id="pal-in" type="text" placeholder="Type a command…" autocomplete="off">
    <ul id="pal-list"></ul></div></div>`);
  document.body.appendChild(ov);
  ov.addEventListener("click", (e) => { if (e.target === ov) closePalette(); });
  const inp = $("#pal-in", ov); paletteSel = 0;
  const draw = (q = "") => {
    paletteItems = COMMANDS.filter((c) => c.label.toLowerCase().includes(q.toLowerCase()));
    if (paletteSel >= paletteItems.length) paletteSel = 0;
    $("#pal-list", ov).innerHTML = paletteItems.map((c, i) => `<li aria-selected="${i === paletteSel}" data-i="${i}">${esc(c.label)}<span class="hint">↵</span></li>`).join("");
    $$("#pal-list li", ov).forEach((li) => li.addEventListener("click", () => { runPalette(+li.dataset.i); }));
  };
  inp.addEventListener("input", () => draw(inp.value));
  inp.addEventListener("keydown", (e) => {
    if (e.key === "Escape") return closePalette();
    if (e.key === "ArrowDown") { paletteSel = Math.min(paletteSel + 1, paletteItems.length - 1); draw(inp.value); e.preventDefault(); }
    if (e.key === "ArrowUp") { paletteSel = Math.max(paletteSel - 1, 0); draw(inp.value); e.preventDefault(); }
    if (e.key === "Enter") { runPalette(paletteSel); }
  });
  draw(""); inp.focus();
}
function runPalette(i) { const c = paletteItems[i]; closePalette(); if (c) c.run(); }
function closePalette() { $(".overlay")?.remove(); }

document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openPalette(); }
});

// Delegated: the "Browse configs & profiles…" button appears in several views.
document.addEventListener("click", (e) => {
  if (e.target.closest("[data-action='origin-browse']")) openOriginPicker();
});
