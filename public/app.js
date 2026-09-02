/* Dashboard for pipeline-runner. No framework, no build step. */
(() => {
  "use strict";

  const TOKEN_KEY = "pipeline-runner-token";
  let token = localStorage.getItem(TOKEN_KEY) || "";
  let pipelines = [];
  let editingId = null;
  let refreshTimer = null;

  const $ = (id) => document.getElementById(id);

  // ------------------------------------------------------------------ api
  async function api(path, options = {}) {
    const res = await fetch(path, {
      ...options,
      headers: {
        ...(options.body ? { "content-type": "application/json" } : {}),
        Authorization: `Bearer ${token}`,
        ...(options.headers || {}),
      },
    });
    if (res.status === 401) {
      signOut("That token was rejected.");
      throw new Error("Unauthorized");
    }
    const text = await res.text();
    const body = text && res.headers.get("content-type")?.includes("json")
      ? JSON.parse(text)
      : text;
    if (!res.ok) throw new Error((body && body.error) || `HTTP ${res.status}`);
    return body;
  }

  // -------------------------------------------------------------- helpers
  const STATUS_CLASS = {
    success: "pill-ok",
    running: "pill-info",
    pending: "pill-muted",
    failed: "pill-bad",
    missed: "pill-bad",
    timeout: "pill-bad",
    interrupted: "pill-warn",
  };

  const esc = (s) =>
    String(s ?? "").replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  function relative(ts) {
    if (!ts) return "—";
    const diff = ts - Date.now();
    const abs = Math.abs(diff);
    const unit =
      abs < 60_000 ? [Math.round(abs / 1000), "s"]
      : abs < 3_600_000 ? [Math.round(abs / 60_000), "m"]
      : abs < 86_400_000 ? [Math.round(abs / 3_600_000), "h"]
      : [Math.round(abs / 86_400_000), "d"];
    return diff >= 0 ? `in ${unit[0]}${unit[1]}` : `${unit[0]}${unit[1]} ago`;
  }

  const stamp = (ts) => (ts ? new Date(ts).toLocaleString() : "—");

  // --------------------------------------------------------------- render
  function renderPipelines() {
    const host = $("pipelines");
    if (!pipelines.length) {
      host.innerHTML = `<div class="card"><div class="empty">No pipelines yet. Create one to get started.</div></div>`;
      return;
    }

    host.innerHTML = pipelines.map((p) => {
      const last = p.last_run;
      const lastPill = last
        ? `<span class="pill ${STATUS_CLASS[last.status] || "pill-muted"}">${esc(last.status)}</span>`
        : `<span class="pill pill-muted">never run</span>`;
      return `
        <div class="pipeline ${p.enabled ? "" : "disabled-row"}">
          <div class="pipeline-top">
            <span class="pipeline-name">${esc(p.name)}</span>
            ${lastPill}
            ${p.busy ? '<span class="pill pill-info">running now</span>' : ""}
            ${p.enabled ? "" : '<span class="pill pill-muted">disabled</span>'}
            <span class="pipeline-actions">
              <button class="btn btn-sm" data-act="trigger" data-id="${p.id}">Run now</button>
              <button class="btn btn-sm" data-act="toggle" data-id="${p.id}">${p.enabled ? "Disable" : "Enable"}</button>
              <button class="btn btn-sm" data-act="edit" data-id="${p.id}">Edit</button>
              <button class="btn btn-sm" data-act="delete" data-id="${p.id}">Delete</button>
            </span>
          </div>
          <div class="pipeline-meta">
            <span><code>${esc(p.cron)}</code> ${esc(p.timezone)}</span>
            <span>next: ${p.enabled ? esc(relative(p.next_run)) : "—"}</span>
            <span>last: ${esc(last ? relative(last.finished_at || last.scheduled_for) : "—")}</span>
            <span>image: <code>${esc(p.image)}</code></span>
            ${p.env_keys.length ? `<span>env: ${p.env_keys.map(esc).join(", ")}</span>` : ""}
          </div>
        </div>`;
    }).join("");
  }

  function renderRuns(runs) {
    const byId = Object.fromEntries(pipelines.map((p) => [p.id, p.name]));
    const host = $("runs");
    if (!runs.length) {
      host.innerHTML = `<div class="empty">No runs recorded yet.</div>`;
      return;
    }
    host.innerHTML = `
      <div class="table-wrap"><table>
        <thead><tr>
          <th>Pipeline</th><th>Status</th><th>Scheduled</th><th>Late</th>
          <th>Duration</th><th>Commit</th><th></th>
        </tr></thead>
        <tbody>${runs.map((r) => {
          const dur = r.started_at && r.finished_at
            ? `${Math.round((r.finished_at - r.started_at) / 1000)}s` : "—";
          const late = r.late_sec ? `${r.late_sec}s` : "—";
          return `<tr>
            <td>${esc(byId[r.pipeline_id] || r.pipeline_id.slice(0, 8))}</td>
            <td><span class="pill ${STATUS_CLASS[r.status] || "pill-muted"}">${esc(r.status)}</span></td>
            <td>${esc(stamp(r.scheduled_for))}</td>
            <td>${esc(late)}</td>
            <td>${esc(dur)}</td>
            <td>${r.commit_sha ? `<code>${esc(r.commit_sha.slice(0, 8))}</code>` : "—"}</td>
            <td>
              <button class="btn btn-sm" data-act="logs" data-id="${r.id}">Logs</button>
              ${r.status === "running" ? `<button class="btn btn-sm" data-act="cancel" data-id="${r.id}">Cancel</button>` : ""}
            </td>
          </tr>`;
        }).join("")}</tbody>
      </table></div>`;
  }

  function renderRunFilter() {
    const sel = $("run-filter");
    const current = sel.value;
    sel.innerHTML = `<option value="">All pipelines</option>` +
      pipelines.map((p) => `<option value="${p.id}">${esc(p.name)}</option>`).join("");
    sel.value = current;
  }

  // --------------------------------------------------------------- loading
  async function refresh() {
    try {
      const [pipeRes, health] = await Promise.all([
        api("/api/pipelines"),
        fetch("/healthz").then((r) => r.json()).catch(() => null),
      ]);
      pipelines = pipeRes.pipelines;
      renderPipelines();
      renderRunFilter();

      const badge = $("health");
      if (!health) {
        badge.className = "pill pill-bad";
        badge.textContent = "unreachable";
      } else {
        const okay = health.status === "ok";
        badge.className = `pill ${okay ? "pill-ok" : "pill-bad"}`;
        badge.textContent = okay ? "scheduler ok" : "scheduler stalled";
      }

      const filter = $("run-filter").value;
      const runs = await api(`/api/runs?limit=50${filter ? `&pipeline=${filter}` : ""}`);
      renderRuns(runs.runs);
    } catch (e) {
      if (e.message !== "Unauthorized") console.error(e);
    }
  }

  // ----------------------------------------------------------------- form
  function envToText(keys) {
    // Values are never returned by the API, so an edit shows keys only and a
    // blank box means "leave the stored env untouched".
    return keys.map((k) => `${k}=`).join("\n");
  }

  function openForm(pipeline) {
    editingId = pipeline ? pipeline.id : null;
    $("form-title").textContent = pipeline ? `Edit ${pipeline.name}` : "New pipeline";
    $("form-error").hidden = true;
    const f = $("pipeline-form");
    f.reset();

    if (pipeline) {
      for (const [k, v] of Object.entries(pipeline)) {
        const el = f.elements[k];
        if (!el) continue;
        if (el.type === "checkbox") el.checked = v === 1 || v === true;
        else el.value = v ?? "";
      }
      f.elements.env.value = envToText(pipeline.env_keys);
      f.elements.env.placeholder = "Leave blank to keep the stored values";
    }
    $("form-dialog").showModal();
  }

  function collectForm() {
    const f = $("pipeline-form");
    const body = {};
    for (const el of f.elements) {
      if (!el.name) continue;
      if (el.type === "checkbox") body[el.name] = el.checked;
      else if (el.type === "number") body[el.name] = el.value === "" ? undefined : Number(el.value);
      else body[el.name] = el.value;
    }

    const envText = (body.env || "").trim();
    if (envText) {
      const env = {};
      for (const line of envText.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const idx = trimmed.indexOf("=");
        if (idx < 1) throw new Error(`Env line is not KEY=value: ${trimmed}`);
        env[trimmed.slice(0, idx).trim()] = trimmed.slice(idx + 1);
      }
      body.env = env;
    } else {
      delete body.env;
    }
    return body;
  }

  async function saveForm() {
    try {
      const body = collectForm();
      if (editingId) {
        await api(`/api/pipelines/${editingId}`, { method: "PATCH", body: JSON.stringify(body) });
      } else {
        await api("/api/pipelines", { method: "POST", body: JSON.stringify(body) });
      }
      $("form-dialog").close();
      await refresh();
    } catch (e) {
      const err = $("form-error");
      err.textContent = e.message;
      err.hidden = false;
    }
  }

  // ---------------------------------------------------------------- events
  document.addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button[data-act]");
    if (!btn) return;
    const { act, id } = btn.dataset;

    try {
      if (act === "trigger") {
        btn.disabled = true;
        await api(`/api/pipelines/${id}/trigger`, { method: "POST" });
        setTimeout(refresh, 500);
      } else if (act === "toggle") {
        const p = pipelines.find((x) => x.id === id);
        await api(`/api/pipelines/${id}/${p.enabled ? "disable" : "enable"}`, { method: "POST" });
        await refresh();
      } else if (act === "edit") {
        openForm(pipelines.find((x) => x.id === id));
      } else if (act === "delete") {
        const p = pipelines.find((x) => x.id === id);
        if (!confirm(`Delete pipeline "${p.name}" and all its run history?`)) return;
        await api(`/api/pipelines/${id}`, { method: "DELETE" });
        await refresh();
      } else if (act === "logs") {
        $("log-title").textContent = `Logs · ${id.slice(0, 8)}`;
        $("log-body").textContent = "loading…";
        $("log-dialog").showModal();
        $("log-body").textContent = (await api(`/api/runs/${id}/logs`)) || "(empty)";
      } else if (act === "cancel") {
        await api(`/api/runs/${id}/cancel`, { method: "POST" });
        await refresh();
      }
    } catch (e) {
      alert(e.message);
    } finally {
      btn.disabled = false;
    }
  });

  $("run-filter").addEventListener("change", refresh);
  $("new-btn").addEventListener("click", () => openForm(null));
  $("form-cancel").addEventListener("click", () => $("form-dialog").close());
  $("form-save").addEventListener("click", saveForm);
  $("log-close").addEventListener("click", () => $("log-dialog").close());
  $("signout-btn").addEventListener("click", () => signOut());

  $("token-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    token = $("token-input").value.trim();
    try {
      await api("/api/pipelines");
      localStorage.setItem(TOKEN_KEY, token);
      showApp();
    } catch {
      const err = $("token-error");
      err.textContent = "That token was rejected.";
      err.hidden = false;
    }
  });

  function signOut(message) {
    token = "";
    localStorage.removeItem(TOKEN_KEY);
    clearInterval(refreshTimer);
    $("app").hidden = true;
    $("signout-btn").hidden = true;
    $("auth-gate").hidden = false;
    if (message) {
      const err = $("token-error");
      err.textContent = message;
      err.hidden = false;
    }
  }

  function showApp() {
    $("auth-gate").hidden = true;
    $("app").hidden = false;
    $("signout-btn").hidden = false;
    refresh();
    clearInterval(refreshTimer);
    refreshTimer = setInterval(refresh, 10_000);
  }

  // ------------------------------------------------------------------ boot
  if (token) {
    api("/api/pipelines").then(showApp).catch(() => signOut());
  } else {
    $("auth-gate").hidden = false;
  }
})();
