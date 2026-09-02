/**
 * HTTP API + dashboard host.
 *
 * Auth policy: every /api/* route is token-gated, reads included, because run
 * logs routinely contain data a pipeline pulled from a private endpoint. The
 * static dashboard shell is served unauthenticated - it holds no data and
 * prompts for the token itself, which is the only way a browser can bootstrap.
 */
import { join } from "node:path";
import { config } from "./config.ts";
import { requireAuth } from "./auth.ts";
import { encryptEnv, decryptEnv } from "./crypto.ts";
import { ValidationError, parsePipelineInput } from "./lib/validate.ts";
import { executeRun, killRun, readLog } from "./executor.ts";
import {
  isHealthy,
  isPipelineBusy,
  materialise,
  nextOccurrence,
  tickAge,
  withPipelineLock,
} from "./scheduler.ts";
import {
  deletePendingRuns,
  deletePipeline,
  getPipeline,
  getPipelineByName,
  getRun,
  insertManualRun,
  insertPipeline,
  lastFinishedRun,
  listPipelines,
  nextPendingRun,
  queryRuns,
  updatePipeline,
  updateRun,
  type Pipeline,
  type RunStatus,
} from "./db.ts";

const PUBLIC_DIR = join(import.meta.dir, "..", "public");

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });

const ok = (body: unknown) => json(200, body);

// ------------------------------------------------------------ serialisation

function envKeys(p: Pipeline): string[] {
  try {
    return Object.keys(decryptEnv(p.encrypted_env)).sort();
  } catch {
    return [];
  }
}

/** Never returns encrypted_env - only which keys exist. */
function serialisePipeline(p: Pipeline) {
  const pending = nextPendingRun(p.id);
  const last = lastFinishedRun(p.id);
  let nextRun: number | null = pending?.scheduled_for ?? null;
  if (nextRun === null && p.enabled) {
    try {
      nextRun = nextOccurrence(p.cron, p.timezone, new Date()).getTime();
    } catch {
      nextRun = null;
    }
  }
  const { encrypted_env: _ignored, ...rest } = p;
  return {
    ...rest,
    env_keys: envKeys(p),
    next_run: nextRun,
    busy: isPipelineBusy(p.id),
    last_run: last
      ? { id: last.id, status: last.status, scheduled_for: last.scheduled_for, finished_at: last.finished_at }
      : null,
  };
}

function pipelineToInput(p: Pipeline): Record<string, unknown> {
  return {
    name: p.name,
    repo_url: p.repo_url,
    branch: p.branch,
    output_branch: p.output_branch,
    image: p.image,
    command: p.command,
    cron: p.cron,
    timezone: p.timezone,
    enabled: p.enabled === 1,
    timeout_sec: p.timeout_sec,
    max_late_sec: p.max_late_sec,
    late_warn_sec: p.late_warn_sec,
    memory_mb: p.memory_mb,
    cpus: p.cpus,
    idempotent: p.idempotent === 1,
    overlap_policy: p.overlap_policy,
    notify_on_success: p.notify_on_success === 1,
    commit_results: p.commit_results === 1,
  };
}

async function readJsonBody(req: Request): Promise<Record<string, unknown>> {
  try {
    const body = await req.json();
    if (typeof body !== "object" || body === null || Array.isArray(body)) {
      throw new ValidationError("Request body must be a JSON object");
    }
    return body as Record<string, unknown>;
  } catch (e) {
    if (e instanceof ValidationError) throw e;
    throw new ValidationError("Request body must be valid JSON");
  }
}

/**
 * Re-plans a pipeline after any change. Without this, an edited cron would not
 * take effect until the next occurrence of the OLD schedule.
 */
async function replan(p: Pipeline): Promise<void> {
  deletePendingRuns(p.id);
  if (p.enabled) await materialise(p);
}

// ------------------------------------------------------------------- routes

async function handleApi(req: Request, path: string, url: URL): Promise<Response> {
  const method = req.method;

  // ---- collections
  if (path === "/api/pipelines") {
    if (method === "GET") return ok({ pipelines: listPipelines().map(serialisePipeline) });

    if (method === "POST") {
      const body = await readJsonBody(req);
      const { pipeline, env } = parsePipelineInput(body);
      if (getPipelineByName(pipeline.name)) {
        return json(409, { error: `A pipeline named '${pipeline.name}' already exists` });
      }
      const created = insertPipeline({ ...pipeline, encrypted_env: encryptEnv(env) });
      await replan(created);
      return json(201, serialisePipeline(getPipeline(created.id)!));
    }
    return json(405, { error: "Method not allowed" });
  }

  // ---- single pipeline + sub-actions
  const pipeMatch = path.match(/^\/api\/pipelines\/([^/]+)(?:\/(trigger|enable|disable))?$/);
  if (pipeMatch) {
    const id = pipeMatch[1]!;
    const action = pipeMatch[2];
    const pipeline = getPipeline(id) ?? getPipelineByName(id);
    if (!pipeline) return json(404, { error: "Pipeline not found" });

    if (!action) {
      if (method === "GET") return ok(serialisePipeline(pipeline));

      if (method === "PATCH") {
        const body = await readJsonBody(req);
        // Validate the MERGED definition so a partial edit can never produce
        // an invalid pipeline.
        const merged = { ...pipelineToInput(pipeline), ...body };
        const { pipeline: parsed, env } = parsePipelineInput(merged);
        if (parsed.name !== pipeline.name && getPipelineByName(parsed.name)) {
          return json(409, { error: `A pipeline named '${parsed.name}' already exists` });
        }
        const patch: Partial<Pipeline> = { ...parsed };
        if ("env" in body) patch.encrypted_env = encryptEnv(env);
        const updated = updatePipeline(pipeline.id, patch)!;
        await replan(updated);
        return ok(serialisePipeline(updated));
      }

      if (method === "DELETE") {
        deletePipeline(pipeline.id);
        return ok({ deleted: pipeline.id });
      }
      return json(405, { error: "Method not allowed" });
    }

    if (method !== "POST") return json(405, { error: "Method not allowed" });

    if (action === "trigger") {
      if (isPipelineBusy(pipeline.id)) {
        return json(409, { error: "A run for this pipeline is already in progress" });
      }
      const run = insertManualRun(pipeline.id);
      void withPipelineLock(pipeline.id, () => executeRun(pipeline, run, { lateSec: 0 })).catch(
        (e) => console.error(`[trigger] run ${run.id} threw: ${(e as Error).message}`),
      );
      return json(202, { run_id: run.id, status: "running" });
    }

    const enabled = action === "enable" ? 1 : 0;
    const updated = updatePipeline(pipeline.id, { enabled })!;
    await replan(updated);
    return ok(serialisePipeline(updated));
  }

  // ---- runs
  if (path === "/api/runs" && method === "GET") {
    const statusParam = url.searchParams.get("status");
    return ok({
      runs: queryRuns({
        pipelineId: url.searchParams.get("pipeline") ?? undefined,
        status: (statusParam as RunStatus | null) ?? undefined,
        limit: Number(url.searchParams.get("limit") ?? 50),
      }),
    });
  }

  const runMatch = path.match(/^\/api\/runs\/([^/]+)(?:\/(logs|cancel))?$/);
  if (runMatch) {
    const runId = runMatch[1]!;
    const action = runMatch[2];
    const run = getRun(runId);
    if (!run) return json(404, { error: "Run not found" });

    if (!action && method === "GET") return ok(run);

    if (action === "logs" && method === "GET") {
      return new Response(await readLog(runId), {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (action === "cancel" && method === "POST") {
      if (run.status !== "running") {
        return json(409, { error: `Run is ${run.status}, not running` });
      }
      const killed = await killRun(runId);
      if (killed) {
        updateRun(runId, { error: "Cancelled by an operator" });
      }
      return ok({ cancelled: killed });
    }
    return json(405, { error: "Method not allowed" });
  }

  return json(404, { error: "Not found" });
}

// ---- static dashboard (no data, hence no auth)
async function serveStatic(path: string): Promise<Response> {
  const rel = path === "/" ? "index.html" : path.replace(/^\/+/, "");
  if (rel.includes("..")) return new Response("Not found", { status: 404 });
  const file = Bun.file(join(PUBLIC_DIR, rel));
  if (!(await file.exists())) return new Response("Not found", { status: 404 });
  return new Response(file);
}

export function startServer() {
  return Bun.serve({
    port: config.PORT,
    hostname: "0.0.0.0",
    idleTimeout: 60,

    async fetch(req) {
      const url = new URL(req.url);
      const path = url.pathname;

      // The only unauthenticated data route. A scheduler cannot page about its
      // own death, so this is what Docker and Uptime Kuma watch.
      if (path === "/healthz") {
        const healthy = isHealthy();
        return json(healthy ? 200 : 503, {
          status: healthy ? "ok" : "stalled",
          service: config.SERVICE_NAME,
          tick_age_ms: tickAge(),
        });
      }

      if (path.startsWith("/api/")) {
        const auth = requireAuth(req);
        if (!auth.ok) return auth.response!;
        try {
          return await handleApi(req, path, url);
        } catch (e) {
          if (e instanceof ValidationError) return json(400, { error: e.message });
          console.error(`[api] ${path} failed:`, e);
          return json(500, { error: (e as Error).message });
        }
      }

      return serveStatic(path);
    },
  });
}
