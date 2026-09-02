/**
 * The clock. Generalises the in-process tick loop that nidra-daily-push
 * adopted on 2026-08-30 after GitHub Actions' `schedule` event ran 4-12 hours
 * late for four consecutive days.
 *
 * The design rules that failure taught us, all enforced here:
 *
 *  - The trigger is purely local: a SQL query plus a timestamp comparison. No
 *    shared queue, so nothing can contend with us and precision costs nothing.
 *  - Lateness is a first-class outcome. A run later than max_late_sec is
 *    ABANDONED as `missed` and paged, never quietly run hours after the fact.
 *    A run that fires late but within the window still pages. "Green" must
 *    never be able to mean "ran, but ten hours late".
 *  - Silence is the enemy. Every abnormal path notifies; none of them exit 0
 *    quietly, which is what turned a real outage into five green no-op runs.
 */
import parser from "cron-parser";
import { config } from "./config.ts";
import { notifyPipeline } from "./lib/ntfy.ts";
import { containerExists, executeRun, killRun } from "./executor.ts";
import { commitAndPush, gitTokenFor, outputBranchOf } from "./git.ts";
import { decryptEnv } from "./crypto.ts";
import { run as spawnProc } from "./lib/proc.ts";
import {
  dueRuns,
  getPipeline,
  insertScheduledRun,
  lastScheduledFor,
  listEnabledPipelines,
  nextPendingRun,
  pruneOldRuns,
  runsInState,
  updateRun,
  type Pipeline,
  type Run,
} from "./db.ts";

// ---------------------------------------------------------------- liveness

let lastTickAt = Date.now();
export function tickAge(): number {
  return Date.now() - lastTickAt;
}
export function isHealthy(): boolean {
  return tickAge() < config.HEALTH_STALL_MS;
}

// ------------------------------------------------------------------ locking

/**
 * In-memory per-pipeline lock, shared between the tick loop and the manual
 * trigger route so a "Run now" click cannot race the scheduler.
 */
const busy = new Set<string>();

export function isPipelineBusy(pipelineId: string): boolean {
  return busy.has(pipelineId);
}

export async function withPipelineLock<T>(
  pipelineId: string,
  fn: () => Promise<T>,
): Promise<T | null> {
  if (busy.has(pipelineId)) return null;
  busy.add(pipelineId);
  try {
    return await fn();
  } finally {
    busy.delete(pipelineId);
  }
}

// ---------------------------------------------------------------------- cron

export function nextOccurrence(cron: string, timezone: string, after: Date): Date {
  return parser.parseExpression(cron, { currentDate: after, tz: timezone }).next().toDate();
}

const CATCHUP_SCAN_CAP = 5000;

/**
 * How many occurrences fall between `after` and `floor`, and the first one at
 * or past `floor`. Used to collapse a long outage into a single notification
 * instead of a flood of individually-missed runs.
 */
export function skipOccurrencesBefore(
  cron: string,
  timezone: string,
  after: Date,
  floor: Date,
): { skipped: number; next: Date; capped: boolean } {
  const it = parser.parseExpression(cron, { currentDate: after, tz: timezone });
  let skipped = 0;
  for (let i = 0; i < CATCHUP_SCAN_CAP; i++) {
    const candidate = it.next().toDate();
    if (candidate.getTime() >= floor.getTime()) {
      return { skipped, next: candidate, capped: false };
    }
    skipped++;
  }
  return {
    skipped,
    next: nextOccurrence(cron, timezone, floor),
    capped: true,
  };
}

// -------------------------------------------------------- run materialisation

/**
 * Ensures the pipeline has exactly one `pending` row for its next occurrence.
 *
 * Materialising as rows (rather than holding a timer) means "next run" is
 * persisted, survives restarts, and is directly visible on the dashboard.
 */
export async function materialise(pipeline: Pipeline, now = Date.now()): Promise<void> {
  // Exactly one occurrence is queued at a time. Without this guard the tick
  // appends a FURTHER occurrence on every pass - at a 10s tick that is six new
  // rows a minute, growing without bound. The queued row is consumed when it
  // fires, and the next one is planned on the following tick.
  if (nextPendingRun(pipeline.id)) return;

  const floor = new Date(now - pipeline.max_late_sec * 1000);
  const last = lastScheduledFor(pipeline.id);
  const after = last === null ? new Date(now) : new Date(last);

  let target = nextOccurrence(pipeline.cron, pipeline.timezone, after);

  // The next occurrence is already past saving: the service was down, or the
  // pipeline was disabled, long enough to blow through the window. Jump the
  // clock forward rather than queueing a backlog we would only mark missed one
  // tick at a time.
  if (target.getTime() < floor.getTime()) {
    const { skipped, next, capped } = skipOccurrencesBefore(
      pipeline.cron,
      pipeline.timezone,
      after,
      floor,
    );
    target = next;
    if (skipped > 0) {
      await notifyPipeline(
        "fail",
        pipeline.name,
        `${capped ? `${skipped}+` : skipped} scheduled run(s) missed`,
        `No runs happened between ${after.toISOString()} and ${floor.toISOString()}. ` +
          `The runner was down, or the pipeline was disabled, for that whole window. ` +
          `Next run: ${target.toISOString()}.`,
        ["rotating_light"],
      );
    }
  }

  insertScheduledRun(pipeline.id, target.getTime());
}

// ---------------------------------------------------------------- boot sweeps

/**
 * Pending runs whose window closed while we were down. Marked missed and paged
 * BEFORE the first tick, so an outage is announced rather than inferred.
 */
export async function sweepStale(now = Date.now()): Promise<number> {
  let count = 0;
  for (const run of dueRuns(now)) {
    const pipeline = getPipeline(run.pipeline_id);
    if (!pipeline) continue;
    const lateSec = Math.round((now - run.scheduled_for) / 1000);
    if (lateSec <= pipeline.max_late_sec) continue;
    await markMissed(pipeline, run, lateSec);
    count++;
  }
  return count;
}

async function markMissed(pipeline: Pipeline, run: Run, lateSec: number, reason?: string): Promise<void> {
  updateRun(run.id, {
    status: "missed",
    finished_at: Date.now(),
    late_sec: lateSec,
    error: reason ?? `Not run: ${lateSec}s late exceeds max_late_sec=${pipeline.max_late_sec}`,
  });
  await notifyPipeline(
    "fail",
    pipeline.name,
    "run missed",
    reason
      ? reason
      : `Scheduled ${new Date(run.scheduled_for).toISOString()} but was ${lateSec}s late ` +
        `(max_late_sec=${pipeline.max_late_sec}). It was NOT run late.`,
    ["rotating_light"],
  );
}

/**
 * Runs left `running` by a crash or redeploy.
 *
 * If the container outlived us, we wait for it and finish the bookkeeping -
 * the work is still valid, so killing it would waste a good run. If it is
 * gone, the outcome is genuinely unknown; we only auto-retry when the pipeline
 * declares itself idempotent, and otherwise page rather than guess.
 */
export async function recoverRunning(): Promise<void> {
  for (const run of runsInState("running")) {
    const pipeline = getPipeline(run.pipeline_id);
    if (!pipeline) continue;

    if (await containerExists(run.id)) {
      console.log(`[recover] run ${run.id} container still alive; waiting on it`);
      void withPipelineLock(pipeline.id, () => resumeRun(pipeline, run));
      continue;
    }

    updateRun(run.id, {
      status: "interrupted",
      finished_at: Date.now(),
      error: "Runner restarted while this run was in flight; container is gone",
    });

    if (pipeline.idempotent) {
      const retry = insertScheduledRun(pipeline.id, Date.now());
      await notifyPipeline(
        "fail",
        pipeline.name,
        "run interrupted, retrying",
        `Run ${run.id} was in flight when the runner restarted. The pipeline is ` +
          `marked idempotent, so it has been re-queued${retry ? "" : " (a run was already queued)"}.`,
        ["warning"],
      );
    } else {
      await notifyPipeline(
        "fail",
        pipeline.name,
        "run interrupted - NEEDS A HUMAN",
        `Run ${run.id} was in flight when the runner restarted and its outcome is ` +
          `unknown. This pipeline is not marked idempotent, so it has NOT been ` +
          `retried automatically. Check whether its side effects landed.`,
        ["rotating_light"],
      );
    }
  }
}

/** Waits out a container that survived a restart, then finishes its bookkeeping. */
async function resumeRun(pipeline: Pipeline, run: Run): Promise<void> {
  const name = `plr-${run.id}`;
  const waited = await spawnProc(["docker", "wait", name], { timeoutMs: pipeline.timeout_sec * 1000 });
  const code = Number.parseInt(waited.stdout.trim(), 10);

  if (!Number.isFinite(code)) {
    updateRun(run.id, {
      status: "interrupted",
      finished_at: Date.now(),
      error: "Could not determine the exit code of the resumed container",
    });
    await notifyPipeline("fail", pipeline.name, "resumed run outcome unknown", `Run ${run.id}.`);
    return;
  }

  if (code !== 0) {
    updateRun(run.id, { status: "failed", finished_at: Date.now(), exit_code: code });
    await notifyPipeline("fail", pipeline.name, "run failed", `Run ${run.id} exited ${code}.`);
    return;
  }

  let commitSha: string | null = null;
  try {
    if (pipeline.commit_results) {
      const token = gitTokenFor(decryptEnv(pipeline.encrypted_env));
      const res = await commitAndPush(pipeline, token, `${pipeline.name}: resumed run ${run.id}`);
      commitSha = res.sha;
    }
    updateRun(run.id, {
      status: "success",
      finished_at: Date.now(),
      exit_code: 0,
      commit_sha: commitSha,
    });
    if (pipeline.notify_on_success) {
      await notifyPipeline("ok", pipeline.name, "run succeeded (resumed)", `Run ${run.id}.`);
    }
  } catch (e) {
    updateRun(run.id, {
      status: "failed",
      finished_at: Date.now(),
      error: `Resumed run succeeded but committing failed: ${(e as Error).message}`,
    });
    await notifyPipeline("fail", pipeline.name, "commit failed after resumed run", `Run ${run.id}.`);
  }
}

// ----------------------------------------------------------------- the tick

export async function tick(now = Date.now()): Promise<void> {
  for (const pipeline of listEnabledPipelines()) {
    try {
      await materialise(pipeline, now);
    } catch (e) {
      console.error(`[tick] materialise failed for ${pipeline.name}: ${(e as Error).message}`);
    }
  }

  for (const run of dueRuns(now)) {
    const pipeline = getPipeline(run.pipeline_id);
    if (!pipeline) continue;

    // A disabled pipeline's queued run is dropped rather than left to rot into
    // a `missed` page the moment it is re-enabled.
    if (!pipeline.enabled && run.trigger === "schedule") {
      updateRun(run.id, {
        status: "missed",
        finished_at: now,
        error: "Pipeline was disabled before this run was due",
      });
      continue;
    }

    const lateSec = Math.round((now - run.scheduled_for) / 1000);

    if (lateSec > pipeline.max_late_sec) {
      await markMissed(pipeline, run, lateSec);
      continue;
    }

    if (isPipelineBusy(pipeline.id)) {
      if (pipeline.overlap_policy === "skip") {
        await markMissed(
          pipeline,
          run,
          lateSec,
          `Skipped: the previous run was still going when this one came due. ` +
            `The pipeline is taking longer than its schedule allows.`,
        );
      }
      // 'queue' leaves the row pending; it fires when the lock frees, or is
      // marked missed once it blows through max_late_sec.
      continue;
    }

    void withPipelineLock(pipeline.id, () => executeRun(pipeline, run, { lateSec })).catch((e) =>
      console.error(`[tick] run ${run.id} threw: ${(e as Error).message}`),
    );
  }

  lastTickAt = Date.now();
}

export async function startScheduler(): Promise<() => void> {
  const pruned = pruneOldRuns();
  if (pruned > 0) console.log(`[boot] pruned ${pruned} old run rows`);

  await recoverRunning();
  const stale = await sweepStale();
  if (stale > 0) console.log(`[boot] marked ${stale} stale pending run(s) as missed`);

  await tick();
  const handle = setInterval(() => {
    void tick().catch((e) => console.error(`[tick] failed: ${(e as Error).message}`));
  }, config.TICK_MS);

  console.log(`[boot] scheduler running, tick every ${config.TICK_MS}ms`);
  return () => clearInterval(handle);
}

export { killRun };
