import { beforeEach, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import {
  getRun,
  insertPipeline,
  insertScheduledRun,
  lastScheduledFor,
  nextPendingRun,
  openDb,
  queryRuns,
  updatePipeline,
  updateRun,
  type NewPipeline,
  type Pipeline,
} from "../src/db.ts";
import {
  materialise,
  nextOccurrence,
  skipOccurrencesBefore,
  sweepStale,
  tick,
  withPipelineLock,
} from "../src/scheduler.ts";

function mkPipeline(overrides: Partial<NewPipeline> = {}): Pipeline {
  return insertPipeline({
    name: `p-${randomUUID().slice(0, 8)}`,
    repo_url: "https://github.com/owner/repo.git",
    branch: "main",
    output_branch: "",
    image: "alpine",
    command: "true",
    // Yearly by default, so materialise() never manufactures a due run and
    // tests only see the rows they create themselves.
    cron: "0 0 1 1 *",
    timezone: "UTC",
    enabled: 1,
    timeout_sec: 60,
    max_late_sec: 600,
    late_warn_sec: 60,
    memory_mb: 128,
    cpus: 1,
    idempotent: 1,
    overlap_policy: "skip",
    notify_on_success: 0,
    commit_results: 0,
    encrypted_env: "",
    ...overrides,
  });
}

beforeEach(() => {
  openDb(join(process.env.DATA_DIR!, `${randomUUID()}.db`));
});

describe("cron", () => {
  test("honours the timezone", () => {
    const after = new Date("2026-06-01T00:00:00Z");
    const utc = nextOccurrence("0 12 * * *", "UTC", after);
    const ist = nextOccurrence("0 12 * * *", "Asia/Kolkata", after);
    // 12:00 IST is 06:30 UTC, so IST fires 5h30m earlier in absolute terms.
    expect(utc.getTime() - ist.getTime()).toBe(5.5 * 3600 * 1000);
  });

  test("collapses a long outage into a count plus one future occurrence", () => {
    const floor = new Date("2026-06-01T12:00:00Z");
    const after = new Date("2026-06-01T09:00:00Z");
    const { skipped, next, capped } = skipOccurrencesBefore("*/5 * * * *", "UTC", after, floor);
    // 09:05 through 11:55 inclusive. The 12:00 occurrence is `next`, not a skip.
    expect(skipped).toBe(35);
    expect(capped).toBe(false);
    expect(next.getTime()).toBeGreaterThanOrEqual(floor.getTime());
  });
});

describe("materialise", () => {
  test("creates exactly one pending run at the next occurrence", async () => {
    const p = mkPipeline({ cron: "*/10 * * * *" });
    await materialise(p);
    const pending = nextPendingRun(p.id);
    expect(pending).not.toBeNull();
    expect(pending!.scheduled_for).toBeGreaterThan(Date.now());

    // Regression: materialise used to plan the occurrence after MAX(scheduled_for)
    // every pass, so each tick appended another future row without bound.
    // nextPendingRun alone would not catch it - the EARLIEST row stays correct
    // while later ones pile up behind it - so assert on the count.
    for (let i = 0; i < 5; i++) await materialise(p);
    expect(nextPendingRun(p.id)!.id).toBe(pending!.id);
    expect(queryRuns({ pipelineId: p.id, status: "pending", limit: 500 }).length).toBe(1);
  });

  test("plans the next occurrence once the queued one has been consumed", async () => {
    const p = mkPipeline({ cron: "*/10 * * * *" });
    await materialise(p);
    const first = nextPendingRun(p.id)!;

    // Simulate the run firing.
    updateRun(first.id, { status: "success", finished_at: Date.now() });
    await materialise(p);

    const second = nextPendingRun(p.id)!;
    expect(second.id).not.toBe(first.id);
    expect(second.scheduled_for).toBeGreaterThan(first.scheduled_for);
    expect(queryRuns({ pipelineId: p.id, status: "pending", limit: 500 }).length).toBe(1);
  });

  // A month of downtime must not queue 8,640 doomed runs that would then be
  // marked missed one tick at a time.
  test("does not build a backlog after a long outage", async () => {
    const p = mkPipeline({ cron: "*/5 * * * *", max_late_sec: 600 });
    const twoDaysAgo = Date.now() - 2 * 86_400_000;
    insertScheduledRun(p.id, twoDaysAgo);

    // Boot order matters: stale pending rows are swept BEFORE anything new is
    // planned, because materialise defers while an occurrence is still queued.
    await sweepStale();
    await materialise(p);

    // nextPendingRun alone is not enough here, so assert on the NEWEST
    // scheduled occurrence - the one materialise created.
    const newest = lastScheduledFor(p.id)!;
    expect(newest).toBeGreaterThan(Date.now() - p.max_late_sec * 1000);
    expect(newest).toBeGreaterThan(twoDaysAgo);
    expect(queryRuns({ pipelineId: p.id, status: "pending", limit: 500 }).length).toBe(1);
  });
});

describe("lateness", () => {
  // The core lesson from the GitHub Actions incident: a run that blew its
  // window must be abandoned and announced, never silently run hours late.
  test("a run past max_late_sec is marked missed and not executed", async () => {
    const p = mkPipeline({ max_late_sec: 600 });
    const run = insertScheduledRun(p.id, Date.now() - 3600_000)!;

    await tick();

    const after = getRun(run.id)!;
    expect(after.status).toBe("missed");
    expect(after.started_at).toBeNull(); // never handed to docker
    expect(after.error).toContain("max_late_sec");
  });

  test("sweepStale marks an overdue pending run before the first tick", async () => {
    const p = mkPipeline({ max_late_sec: 60 });
    const run = insertScheduledRun(p.id, Date.now() - 3600_000)!;

    const count = await sweepStale();

    expect(count).toBe(1);
    expect(getRun(run.id)!.status).toBe("missed");
  });

  test("a run still inside its window is not marked missed", async () => {
    const p = mkPipeline({ max_late_sec: 3600 });
    const run = insertScheduledRun(p.id, Date.now() - 5_000)!;
    expect(await sweepStale()).toBe(0);
    expect(getRun(run.id)!.status).toBe("pending");
  });
});

describe("overlap", () => {
  async function tickWhileBusy(p: Pipeline): Promise<void> {
    let release!: () => void;
    const held = new Promise<void>((r) => { release = r; });
    // withPipelineLock marks the pipeline busy synchronously before awaiting.
    const lock = withPipelineLock(p.id, () => held);
    await tick();
    release();
    await lock;
  }

  test("'skip' abandons the overlapping run and says why", async () => {
    const p = mkPipeline({ max_late_sec: 3600, overlap_policy: "skip" });
    const run = insertScheduledRun(p.id, Date.now() - 1000)!;

    await tickWhileBusy(p);

    const after = getRun(run.id)!;
    expect(after.status).toBe("missed");
    expect(after.error).toContain("previous run was still going");
  });

  test("'queue' leaves the run pending for a later tick", async () => {
    const p = mkPipeline({ max_late_sec: 3600, overlap_policy: "queue" });
    const run = insertScheduledRun(p.id, Date.now() - 1000)!;

    await tickWhileBusy(p);

    expect(getRun(run.id)!.status).toBe("pending");
  });
});

describe("disabled pipelines", () => {
  test("a queued run is dropped rather than firing when re-enabled", async () => {
    const p = mkPipeline({ max_late_sec: 3600 });
    const run = insertScheduledRun(p.id, Date.now() - 1000)!;
    updatePipeline(p.id, { enabled: 0 });

    await tick();

    const after = getRun(run.id)!;
    expect(after.status).toBe("missed");
    expect(after.error).toContain("disabled");
    expect(after.started_at).toBeNull();
  });
});
