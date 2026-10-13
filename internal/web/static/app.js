// pipeline-runner dashboard. No framework, no inline handlers (strict CSP),
// and every server value reaches the DOM through textContent only.
"use strict";

const TOKEN_KEY = "plr_token";
const $ = (id) => document.getElementById(id);

function token() {
  try { return sessionStorage.getItem(TOKEN_KEY) || ""; } catch { return ""; }
}
function setToken(v) {
  try { v ? sessionStorage.setItem(TOKEN_KEY, v) : sessionStorage.removeItem(TOKEN_KEY); } catch { /* private mode */ }
}

class ApiError extends Error {
  constructor(status, code, message) { super(message); this.status = status; this.code = code; }
}

async function api(method, path, body) {
  const opts = { method, headers: { Authorization: "Bearer " + token() } };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = typeof body === "string" ? body : JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  if (res.status === 401) { signOut("Token rejected."); throw new ApiError(401, "unauthorized", "unauthorized"); }
  if (res.status === 204) return null;
  const ct = res.headers.get("Content-Type") || "";
  const data = ct.includes("json") ? await res.json() : await res.text();
  if (!res.ok) {
    const err = (data && data.error) || {};
    throw new ApiError(res.status, err.code || "error", err.message || res.statusText);
  }
  return data;
}

function el(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) {
    if (c === null || c === undefined) continue;
    node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return node;
}

const STATUS_PILL = {
  success: "pill-ok", failed: "pill-bad", timeout: "pill-bad", interrupted: "pill-bad", missed: "pill-warn",
  skipped: "pill-muted", cancelled: "pill-muted", running: "pill-info", waiting: "pill-info", pending: "pill-muted",
  deferred: "pill-muted", active: "pill-ok", disabled: "pill-muted", done: "pill-muted",
};
const pill = (s) => el("span", { class: "pill " + (STATUS_PILL[s] || "pill-muted") }, s);
const fmt = (t) => (t ? new Date(t).toLocaleString() : "—");

function describeTrigger(t) {
  if (t.type === "cron") return `cron ${t.cron} (${t.timezone || "UTC"})`;
  if (t.type === "at") return "at " + fmt(t.at);
  return "invoke only";
}

// ---------------------------------------------------------------- views

let jobsById = {};

async function loadJobs() {
  const box = $("jobs");
  const data = await api("GET", "/v1/jobs?limit=100");
  jobsById = {};
  box.replaceChildren();
  const filter = $("run-job-filter");
  const keep = filter.value;
  filter.replaceChildren(el("option", { value: "" }, "All jobs"));
  if (!data.items.length) { box.append(el("p", { class: "empty" }, "No jobs yet.")); return; }
  for (const j of data.items) {
    jobsById[j.id] = j;
    filter.append(el("option", { value: j.id }, j.name));
    const target = JSON.parse(JSON.stringify(j.target));
    const where = target.http ? target.http.url : target.type;
    box.append(el("div", { class: "pipeline" + (j.state !== "active" ? " disabled-row" : "") },
      el("div", { class: "pipeline-top" },
        el("span", { class: "pipeline-name" }, j.name), pill(j.state),
        el("div", { class: "pipeline-actions" },
          el("button", { class: "btn btn-sm", type: "button", onclick: () => act("POST", `/v1/jobs/${j.id}/invoke`) }, "Run now"),
          j.state === "disabled"
            ? el("button", { class: "btn btn-sm", type: "button", onclick: () => act("POST", `/v1/jobs/${j.id}/enable`) }, "Enable")
            : el("button", { class: "btn btn-sm", type: "button", onclick: () => act("POST", `/v1/jobs/${j.id}/disable`) }, "Disable"),
          el("button", { class: "btn btn-sm", type: "button", onclick: () => showRuns(j.id) }, "Runs"),
          el("button", { class: "btn btn-sm", type: "button", onclick: () => deleteJob(j) }, "Delete"))),
      el("div", { class: "pipeline-meta" },
        el("span", {}, describeTrigger(j.trigger)), el("code", {}, where),
        el("span", {}, "rev " + j.rev),
        j.next_fires && j.next_fires.length ? el("span", {}, "next " + fmt(j.next_fires[0])) : null)));
  }
  filter.value = keep;
}

async function loadRuns() {
  const qs = new URLSearchParams({ limit: "100" });
  if ($("run-job-filter").value) qs.set("job", $("run-job-filter").value);
  if ($("run-status-filter").value) qs.set("status", $("run-status-filter").value);
  const data = await api("GET", "/v1/runs?" + qs);
  const box = $("runs");
  box.replaceChildren();
  if (!data.items.length) { box.append(el("p", { class: "empty" }, "No runs.")); return; }
  const rows = data.items.map((r) => el("tr", {},
    el("td", {}, r.job_name), el("td", {}, pill(r.status)), el("td", {}, r.trigger + (r.attempt > 1 ? ` #${r.attempt}` : "")),
    el("td", {}, fmt(r.scheduled_for)), el("td", {}, r.late_ms != null ? r.late_ms + " ms" : "—"),
    el("td", {}, r.http_status != null ? r.http_status : (r.exit_code != null ? "exit " + r.exit_code : "—")),
    el("td", { class: "row-actions" },
      el("button", { class: "btn btn-sm", type: "button", onclick: () => openRun(r.id) }, "Details"),
      ["pending", "running", "waiting"].includes(r.status)
        ? el("button", { class: "btn btn-sm", type: "button", onclick: () => act("POST", `/v1/runs/${r.id}/cancel`) }, "Cancel")
        : null)));
  box.append(el("table", {},
    el("thead", {}, el("tr", {}, ...["Job", "Status", "Trigger", "Scheduled", "Late", "Result", ""].map((h) => el("th", {}, h)))),
    el("tbody", {}, ...rows)));
}

async function loadClients() {
  const box = $("clients");
  box.replaceChildren();
  let data;
  try { data = await api("GET", "/v1/clients"); } catch (e) {
    box.append(el("p", { class: "empty" }, e.status === 403 ? "Clients are visible to the admin token only." : e.message));
    return;
  }
  if (!data.items.length) { box.append(el("p", { class: "empty" }, "No clients yet.")); return; }
  const rows = data.items.map((c) => el("tr", {},
    el("td", {}, c.name), el("td", {}, c.scopes.join(", ")), el("td", {}, c.callback_hosts.join(", ")),
    el("td", {}, c.secret_ref_prefixes.join(", ")), el("td", {}, c.container_allowed ? "yes" : "no"),
    el("td", { class: "row-actions" },
      el("button", { class: "btn btn-sm", type: "button", onclick: () => rotate(c) }, "Rotate token"))));
  box.append(el("table", {},
    el("thead", {}, el("tr", {}, ...["Name", "Scopes", "Callback hosts", "Secret prefixes", "Containers", ""].map((h) => el("th", {}, h)))),
    el("tbody", {}, ...rows)));
}

// ---------------------------------------------------------------- actions

async function act(method, path) {
  try { await api(method, path); } catch (e) { alertError(e); }
  refresh();
}

async function deleteJob(j) {
  if (!window.confirm(`Delete job ${j.name}? Its pending and running runs are cancelled.`)) return;
  act("DELETE", `/v1/jobs/${j.id}`);
}

async function rotate(c) {
  if (!window.confirm(`Rotate the token for ${c.name}? The old token stops working now.`)) return;
  try {
    const r = await api("POST", `/v1/clients/${c.id}/rotate`, {});
    showOnce("New token for " + c.name, r.token);
  } catch (e) { alertError(e); }
}

function showOnce(title, text) {
  $("client-report").hidden = false;
  $("client-report").textContent = title + " (shown once):\n" + text;
  $("client-json").hidden = true;
  $("client-save").hidden = true;
  $("client-dialog").showModal();
}

function alertError(e) { window.alert(e.code ? `${e.code}: ${e.message}` : String(e)); }

function showRuns(jobId) {
  $("run-job-filter").value = jobId;
  selectTab("runs");
}

async function openRun(id) {
  const run = await api("GET", `/v1/runs/${id}`);
  $("run-title").textContent = `${run.job_name} · ${run.status}`;
  $("run-json").textContent = JSON.stringify(run, null, 2);
  $("log-body").textContent = "loading…";
  $("run-dialog").showModal();
  try { $("log-body").textContent = (await api("GET", `/v1/runs/${id}/logs`)) || "(empty)"; } catch (e) { $("log-body").textContent = e.message; }
}

const JOB_TEMPLATE = {
  name: "my-job",
  trigger: { type: "cron", cron: "*/15 * * * *", timezone: "Asia/Kolkata" },
  target: { type: "http", http: { url: "http://my-service:3000/internal/tick", auth: { bearer_ref: "MyService/PLR_TOKEN" } } },
  policy: { max_late_sec: 600 },
};

async function validateJob() {
  const report = $("job-report");
  report.hidden = false;
  try {
    const r = await api("POST", "/v1/jobs/validate", $("job-json").value);
    report.textContent = JSON.stringify(r, null, 2);
  } catch (e) { report.textContent = `${e.code}: ${e.message}`; }
}

async function saveJob() {
  const report = $("job-report");
  report.hidden = false;
  try {
    const r = await api("POST", "/v1/jobs", $("job-json").value);
    report.textContent = "Created " + r.name + "\n" + JSON.stringify(r.next_fires || [], null, 2);
    refresh();
  } catch (e) { report.textContent = `${e.code}: ${e.message}`; }
}

async function saveClient() {
  try {
    const r = await api("POST", "/v1/clients", $("client-json").value);
    showOnce("Token for " + r.client.name, r.token);
    refresh();
  } catch (e) { $("client-report").hidden = false; $("client-report").textContent = `${e.code}: ${e.message}`; }
}

// ---------------------------------------------------------------- shell

let activeTab = "jobs";

function selectTab(name) {
  activeTab = name;
  for (const b of document.querySelectorAll("nav.tabs .btn")) b.setAttribute("aria-selected", String(b.dataset.tab === name));
  for (const t of ["jobs", "runs", "clients"]) $("tab-" + t).hidden = t !== name;
  refresh();
}

async function refresh() {
  try {
    if (activeTab === "jobs") await loadJobs();
    if (activeTab === "runs") { if (!Object.keys(jobsById).length) await loadJobs(); await loadRuns(); }
    if (activeTab === "clients") await loadClients();
  } catch (e) { if (e.status !== 401) console.error(e); }
}

async function health() {
  const h = $("health");
  try {
    const r = await fetch("/healthz");
    const d = await r.json();
    h.className = "pill " + (d.ok ? "pill-ok" : "pill-bad");
    h.textContent = d.ok ? "engine ok" : `engine stalled (${Math.round(d.loop_age_ms / 1000)}s)`;
  } catch { h.className = "pill pill-bad"; h.textContent = "unreachable"; }
}

function signOut(msg) {
  setToken("");
  $("app").hidden = true;
  $("signout-btn").hidden = true;
  $("auth-gate").hidden = false;
  $("token-error").hidden = !msg;
  $("token-error").textContent = msg || "";
}

function signedIn() {
  $("auth-gate").hidden = true;
  $("app").hidden = false;
  $("signout-btn").hidden = false;
  refresh();
}

document.addEventListener("DOMContentLoaded", () => {
  $("token-form").addEventListener("submit", (ev) => {
    ev.preventDefault();
    setToken($("token-input").value.trim());
    $("token-input").value = "";
    signedIn();
  });
  $("signout-btn").addEventListener("click", () => signOut(""));
  for (const b of document.querySelectorAll("nav.tabs .btn")) b.addEventListener("click", () => selectTab(b.dataset.tab));
  $("run-job-filter").addEventListener("change", loadRuns);
  $("run-status-filter").addEventListener("change", loadRuns);

  $("new-job-btn").addEventListener("click", () => {
    $("job-json").value = JSON.stringify(JOB_TEMPLATE, null, 2);
    $("job-report").hidden = true;
    $("job-dialog").showModal();
  });
  $("job-cancel").addEventListener("click", () => $("job-dialog").close());
  $("job-validate").addEventListener("click", validateJob);
  $("job-save").addEventListener("click", saveJob);

  $("new-client-btn").addEventListener("click", () => {
    $("client-json").hidden = false;
    $("client-save").hidden = false;
    $("client-report").hidden = true;
    $("client-json").value = JSON.stringify({ name: "my-service", scopes: ["jobs:read", "jobs:write", "runs:invoke"],
      callback_hosts: ["my-service"], secret_ref_prefixes: ["MyService/"] }, null, 2);
    $("client-dialog").showModal();
  });
  $("client-cancel").addEventListener("click", () => $("client-dialog").close());
  $("client-save").addEventListener("click", saveClient);
  $("run-close").addEventListener("click", () => $("run-dialog").close());

  health();
  setInterval(health, 30000);
  setInterval(() => { if (!$("app").hidden && !document.querySelector("dialog[open]")) refresh(); }, 15000);
  token() ? signedIn() : signOut("");
});
