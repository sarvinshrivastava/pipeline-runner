/**
 * Runs one pipeline in a throwaway sibling container.
 *
 * Crash-safety follows nidra-daily-push's ordering discipline: the run is
 * marked `running` WITH its container name BEFORE the container is started, so
 * a crash mid-run always leaves enough on disk for recovery to work out what
 * happened rather than having to guess.
 */
import { unlink, writeFile, chmod } from "node:fs/promises";
import { join } from "node:path";
import { config } from "./config.ts";
import { decryptEnv } from "./crypto.ts";
import { commitAndPush, gitTokenFor, outputBranchOf, syncWorkspace, workspacePath } from "./git.ts";
import { LogSink } from "./lib/logsink.ts";
import { notifyPipeline } from "./lib/ntfy.ts";
import { run as spawnProc, redact } from "./lib/proc.ts";
import { resolveRefs } from "./secretsmanager.ts";
import { getRun, updateRun, type Pipeline, type Run, type RunStatus } from "./db.ts";

export function containerNameFor(runId: string): string {
  return `plr-${runId}`;
}

function logPathFor(runId: string): string {
  return join(config.LOGS_DIR, `${runId}.log`);
}

/**
 * The complete docker argv. Note what a pipeline can influence: image, command,
 * and resource numbers. It can never contribute a flag - every flag here is a
 * literal written by us.
 */
function buildDockerArgs(
  pipeline: Pipeline,
  containerName: string,
  workspace: string,
  envFile: string,
): string[] {
  return [
    "docker", "run", "--rm",
    "--name", containerName,
    "--label", "pipeline-runner=1",
    // Host path on both sides - see the bind-mount note in docker-compose.yml.
    "-v", `${workspace}:/workspace`,
    "-w", "/workspace",
    "--cap-drop", "ALL",
    "--security-opt", "no-new-privileges",
    "--memory", `${pipeline.memory_mb}m`,
    // Equal to --memory disables swap, so memory_mb is a real ceiling.
    "--memory-swap", `${pipeline.memory_mb}m`,
    "--cpus", String(pipeline.cpus),
    "--pids-limit", "512",
    "--env-file", envFile,
    pipeline.image,
    // The pipeline's command is interpreted by sh INSIDE its own container.
    "sh", "-c", pipeline.command,
  ];
}

export interface ExecuteOptions {
  /** Seconds late this run fired, recorded for visibility. */
  lateSec?: number;
}

/**
 * Executes a run end to end and persists its terminal state. Never throws:
 * a failure is a recorded outcome, not an exception for the caller.
 */
export async function executeRun(
  pipeline: Pipeline,
  run: Run,
  opts: ExecuteOptions = {},
): Promise<RunStatus> {
  const containerName = containerNameFor(run.id);
  const startedAt = Date.now();
  const lateSec = opts.lateSec ?? 0;

  // ---- write BEFORE doing anything, so a crash is recoverable -------------
  updateRun(run.id, {
    status: "running",
    started_at: startedAt,
    container_name: containerName,
    late_sec: lateSec,
  });

  const sink = new LogSink(logPathFor(run.id), config.LOG_MAX_BYTES);
  const envFile = `/tmp/${containerName}.env`;
  let secrets: string[] = [];

  try {
    // Referenced secrets are fetched fresh from the vault every run, so a
    // rotated credential takes effect immediately with no pipeline edit. A
    // reference that has since been deleted throws here and fails the run
    // loudly - never start a run with a silently-absent credential.
    const storedEnv = decryptEnv(pipeline.encrypted_env);
    const refs = JSON.parse(pipeline.secret_refs || "{}") as Record<string, string>;
    const refNames = Object.keys(refs);
    if (refNames.length) {
      sink.writeText(`resolving ${refNames.length} secret reference(s): ${refNames.join(", ")}\n`);
    }
    const pipelineEnv = { ...storedEnv, ...(await resolveRefs(refs)) };
    const token = gitTokenFor(pipelineEnv);
    secrets = [token, ...Object.values(pipelineEnv)].filter(Boolean);

    // GIT_TOKEN is consumed here for auth and deliberately withheld from the
    // container, keeping push credentials out of reach of pipeline code.
    const containerEnv = { ...pipelineEnv };
    delete containerEnv.GIT_TOKEN;

    sink.writeText(`=== ${pipeline.name} :: run ${run.id} ===\n`);
    if (lateSec > 0) sink.writeText(`WARNING: started ${lateSec}s later than scheduled\n`);
    sink.writeText(`syncing ${pipeline.repo_url} @ ${pipeline.branch}\n`);

    const workspace = await syncWorkspace(pipeline, token);

    await writeFile(envFile, envFileBody(containerEnv), { mode: 0o600 });
    await chmod(envFile, 0o600);

    sink.writeText(`running ${pipeline.image}\n\n`);
    const { code, timedOut } = await runContainer(
      buildDockerArgs(pipeline, containerName, workspace, envFile),
      containerName,
      pipeline.timeout_sec * 1000,
      sink,
      secrets,
    );

    if (timedOut) {
      return await finish(pipeline, run, "timeout", {
        exit_code: code,
        error: `Killed after exceeding timeout_sec=${pipeline.timeout_sec}`,
        lateSec,
      }, sink);
    }

    if (code !== 0) {
      return await finish(pipeline, run, "failed", {
        exit_code: code,
        error: `Script exited with code ${code}`,
        lateSec,
      }, sink);
    }

    // ---- success: commit whatever the script wrote ------------------------
    let commitSha: string | null = null;
    if (pipeline.commit_results) {
      const msg =
        `${pipeline.name}: scheduled run ${new Date(run.scheduled_for).toISOString()}\n\n` +
        `run-id: ${run.id}\ntrigger: ${run.trigger}`;
      const result = await commitAndPush(pipeline, token, msg);
      commitSha = result.sha;
      sink.writeText(
        result.noop
          ? `\nno file changes to commit\n`
          : `\ncommitted ${result.sha} to ${outputBranchOf(pipeline)}\n`,
      );
    }

    return await finish(pipeline, run, "success", { exit_code: 0, commit_sha: commitSha, lateSec }, sink);
  } catch (e) {
    const msg = redact((e as Error).message ?? String(e), secrets);
    sink.writeText(`\nERROR: ${msg}\n`);
    return await finish(pipeline, run, "failed", { error: msg, lateSec }, sink);
  } finally {
    sink.close();
    await unlink(envFile).catch(() => {});
  }
}

function envFileBody(env: Record<string, string>): string {
  return Object.entries(env)
    .map(([k, v]) => `${k}=${v}`)
    .join("\n") + "\n";
}

/**
 * Spawns the container and streams its output.
 *
 * On timeout the docker CLI is not simply killed - that would orphan a running
 * container - so `docker kill` is issued against the container by name first.
 */
async function runContainer(
  argv: string[],
  containerName: string,
  timeoutMs: number,
  sink: LogSink,
  secrets: string[],
): Promise<{ code: number; timedOut: boolean }> {
  const proc = Bun.spawn(argv, { stdout: "pipe", stderr: "pipe", stdin: "ignore" });

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    void spawnProc(["docker", "kill", containerName]).catch(() => {});
  }, timeoutMs);

  const pump = async (stream: ReadableStream<Uint8Array> | null) => {
    if (!stream) return;
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      // Redact per chunk so a secret never reaches disk, at the cost of
      // missing a value split across a chunk boundary - acceptable, since the
      // env file is the real containment boundary.
      sink.writeText(redact(decoder.decode(value, { stream: true }), secrets));
    }
  };

  try {
    await Promise.all([pump(proc.stdout as ReadableStream<Uint8Array>), pump(proc.stderr as ReadableStream<Uint8Array>)]);
    const code = await proc.exited;
    return { code, timedOut };
  } finally {
    clearTimeout(timer);
  }
}

interface FinishPatch {
  exit_code?: number | null;
  commit_sha?: string | null;
  error?: string | null;
  lateSec: number;
}

async function finish(
  pipeline: Pipeline,
  run: Run,
  status: RunStatus,
  patch: FinishPatch,
  sink: LogSink,
): Promise<RunStatus> {
  const finishedAt = Date.now();
  updateRun(run.id, {
    status,
    finished_at: finishedAt,
    exit_code: patch.exit_code ?? null,
    commit_sha: patch.commit_sha ?? null,
    error: patch.error ?? null,
  });

  const current = getRun(run.id);
  const durationSec = current?.started_at
    ? Math.round((finishedAt - current.started_at) / 1000)
    : 0;

  sink.writeText(`\n=== ${status} in ${durationSec}s ===\n`);

  await notifyOutcome(pipeline, run, status, durationSec, patch);
  return status;
}

async function notifyOutcome(
  pipeline: Pipeline,
  run: Run,
  status: RunStatus,
  durationSec: number,
  patch: FinishPatch,
): Promise<void> {
  const when = new Date(run.scheduled_for).toISOString();

  if (status !== "success") {
    await notifyPipeline(
      "fail",
      pipeline.name,
      status === "timeout" ? "timed out" : "run failed",
      `Scheduled ${when}\nRan ${durationSec}s\n${patch.error ?? ""}`.trim(),
      ["rotating_light"],
    );
    return;
  }

  // A run that succeeded but started late is still a problem worth hearing
  // about - the whole reason for moving off GitHub Actions was that a late
  // run looks identical to an on-time one.
  if (patch.lateSec > pipeline.late_warn_sec) {
    await notifyPipeline(
      "fail",
      pipeline.name,
      "ran late",
      `Succeeded, but started ${patch.lateSec}s after its scheduled time (${when}).`,
      ["warning"],
    );
    return;
  }

  if (pipeline.notify_on_success) {
    await notifyPipeline(
      "ok",
      pipeline.name,
      "run succeeded",
      `Scheduled ${when}\nRan ${durationSec}s` +
        (patch.commit_sha ? `\nCommit ${patch.commit_sha.slice(0, 8)}` : "\nNo changes to commit"),
    );
  }
}

/** True if a container for this run still exists on the host. */
export async function containerExists(runId: string): Promise<boolean> {
  const res = await spawnProc(["docker", "inspect", "--type=container", containerNameFor(runId)]);
  return res.code === 0;
}

export async function killRun(runId: string): Promise<boolean> {
  const res = await spawnProc(["docker", "kill", containerNameFor(runId)]);
  return res.code === 0;
}

export function readLog(runId: string): Promise<string> {
  return Bun.file(logPathFor(runId))
    .text()
    .catch(() => "");
}

export { workspacePath };
