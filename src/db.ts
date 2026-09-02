/**
 * SQLite store. Run history needs filtering and ordering, which is why this is
 * a real DB rather than the flat JSON files nidra-daily-push uses.
 *
 * All timestamps are epoch milliseconds (INTEGER).
 */
import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { config } from "./config.ts";

/**
 * Note there is no `late` status. A run that fired late still either succeeded
 * or failed, and collapsing that into one `late` state would throw away the
 * outcome. Lateness is recorded on `runs.late_sec` and notified separately,
 * so "ran, but 4 minutes late" stays visible without masking the result.
 */
export type RunStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "missed"
  | "timeout"
  | "interrupted";

export type TriggerKind = "schedule" | "manual";
export type OverlapPolicy = "skip" | "queue";

export interface Pipeline {
  id: string;
  name: string;
  repo_url: string;
  branch: string;
  /** "" means commit back onto `branch`. */
  output_branch: string;
  image: string;
  command: string;
  cron: string;
  timezone: string;
  enabled: number;
  timeout_sec: number;
  /** Past this much lateness a run is abandoned as `missed`, never run late. */
  max_late_sec: number;
  /** Past this much lateness a run still executes but pings ntfy. */
  late_warn_sec: number;
  memory_mb: number;
  cpus: number;
  idempotent: number;
  overlap_policy: OverlapPolicy;
  notify_on_success: number;
  commit_results: number;
  encrypted_env: string;
  /**
   * JSON map of env-var name -> secrets-manager reference ("Folder/KEY").
   * Stored in the clear: these are names, never values. Values are fetched
   * fresh at run time, so rotating a secret needs no pipeline edit.
   */
  secret_refs: string;
  created_at: number;
  updated_at: number;
}

export interface Run {
  id: string;
  pipeline_id: string;
  scheduled_for: number;
  started_at: number | null;
  finished_at: number | null;
  status: RunStatus;
  exit_code: number | null;
  container_name: string | null;
  commit_sha: string | null;
  error: string | null;
  late_sec: number | null;
  trigger: TriggerKind;
  attempt: number;
}

let db: Database;

export function openDb(path = config.DB_PATH): Database {
  mkdirSync(config.DATA_DIR, { recursive: true });
  mkdirSync(config.WORKSPACES_DIR, { recursive: true });
  mkdirSync(config.LOGS_DIR, { recursive: true });

  db = new Database(path, { create: true });
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec("PRAGMA busy_timeout = 5000;");
  migrate();
  return db;
}

export function getDb(): Database {
  if (!db) throw new Error("openDb() must be called before getDb()");
  return db;
}

function migrate(): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS pipelines (
      id                TEXT PRIMARY KEY,
      name              TEXT NOT NULL UNIQUE,
      repo_url          TEXT NOT NULL,
      branch            TEXT NOT NULL DEFAULT 'main',
      output_branch     TEXT NOT NULL DEFAULT '',
      image             TEXT NOT NULL,
      command           TEXT NOT NULL,
      cron              TEXT NOT NULL,
      timezone          TEXT NOT NULL DEFAULT 'UTC',
      enabled           INTEGER NOT NULL DEFAULT 1,
      timeout_sec       INTEGER NOT NULL DEFAULT 900,
      max_late_sec      INTEGER NOT NULL DEFAULT 600,
      late_warn_sec     INTEGER NOT NULL DEFAULT 60,
      memory_mb         INTEGER NOT NULL DEFAULT 512,
      cpus              REAL    NOT NULL DEFAULT 1.0,
      idempotent        INTEGER NOT NULL DEFAULT 1,
      overlap_policy    TEXT NOT NULL DEFAULT 'skip',
      notify_on_success INTEGER NOT NULL DEFAULT 0,
      commit_results    INTEGER NOT NULL DEFAULT 1,
      encrypted_env     TEXT NOT NULL DEFAULT '',
      secret_refs       TEXT NOT NULL DEFAULT '{}',
      created_at        INTEGER NOT NULL,
      updated_at        INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS runs (
      id             TEXT PRIMARY KEY,
      pipeline_id    TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
      scheduled_for  INTEGER NOT NULL,
      started_at     INTEGER,
      finished_at    INTEGER,
      status         TEXT NOT NULL,
      exit_code      INTEGER,
      container_name TEXT,
      commit_sha     TEXT,
      error          TEXT,
      late_sec       INTEGER,
      trigger        TEXT NOT NULL DEFAULT 'schedule',
      attempt        INTEGER NOT NULL DEFAULT 1
    );

    -- Makes run materialisation idempotent: a tick can always attempt to
    -- insert the next scheduled occurrence and let the DB reject duplicates.
    -- Partial, so manual triggers (which share a scheduled_for of "now") are
    -- exempt.
    CREATE UNIQUE INDEX IF NOT EXISTS runs_pipeline_sched
      ON runs(pipeline_id, scheduled_for) WHERE trigger = 'schedule';

    CREATE INDEX IF NOT EXISTS runs_status_sched ON runs(status, scheduled_for);
    CREATE INDEX IF NOT EXISTS runs_pipeline_time ON runs(pipeline_id, scheduled_for DESC);
  `);

  // Additive migrations for databases created before a column existed.
  addColumnIfMissing("pipelines", "secret_refs", "TEXT NOT NULL DEFAULT '{}'");
}

function addColumnIfMissing(table: string, column: string, definition: string): void {
  const cols = db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (!cols.some((c) => c.name === column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

// ---------------------------------------------------------------- pipelines

export function listPipelines(): Pipeline[] {
  return getDb().query("SELECT * FROM pipelines ORDER BY name").all() as Pipeline[];
}

export function listEnabledPipelines(): Pipeline[] {
  return getDb()
    .query("SELECT * FROM pipelines WHERE enabled = 1 ORDER BY name")
    .all() as Pipeline[];
}

export function getPipeline(id: string): Pipeline | null {
  return (getDb().query("SELECT * FROM pipelines WHERE id = ?").get(id) as Pipeline) ?? null;
}

export function getPipelineByName(name: string): Pipeline | null {
  return (getDb().query("SELECT * FROM pipelines WHERE name = ?").get(name) as Pipeline) ?? null;
}

export type NewPipeline = Omit<Pipeline, "id" | "created_at" | "updated_at">;

export function insertPipeline(p: NewPipeline): Pipeline {
  const id = randomUUID();
  const now = Date.now();
  getDb()
    .query(
      `INSERT INTO pipelines (
         id, name, repo_url, branch, output_branch, image, command, cron, timezone,
         enabled, timeout_sec, max_late_sec, late_warn_sec, memory_mb, cpus,
         idempotent, overlap_policy, notify_on_success, commit_results,
         encrypted_env, secret_refs, created_at, updated_at
       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      id, p.name, p.repo_url, p.branch, p.output_branch, p.image, p.command, p.cron,
      p.timezone, p.enabled, p.timeout_sec, p.max_late_sec, p.late_warn_sec,
      p.memory_mb, p.cpus, p.idempotent, p.overlap_policy, p.notify_on_success,
      p.commit_results, p.encrypted_env, p.secret_refs, now, now,
    );
  return getPipeline(id)!;
}

const UPDATABLE = new Set<keyof Pipeline>([
  "name", "repo_url", "branch", "output_branch", "image", "command", "cron",
  "timezone", "enabled", "timeout_sec", "max_late_sec", "late_warn_sec",
  "memory_mb", "cpus", "idempotent", "overlap_policy", "notify_on_success",
  "commit_results", "encrypted_env", "secret_refs",
]);

export function updatePipeline(id: string, patch: Partial<Pipeline>): Pipeline | null {
  const fields = Object.keys(patch).filter((k) => UPDATABLE.has(k as keyof Pipeline));
  if (fields.length === 0) return getPipeline(id);

  const set = fields.map((f) => `${f} = ?`).join(", ");
  const values = fields.map((f) => (patch as Record<string, unknown>)[f]);
  getDb()
    .query(`UPDATE pipelines SET ${set}, updated_at = ? WHERE id = ?`)
    .run(...(values as never[]), Date.now(), id);
  return getPipeline(id);
}

export function deletePipeline(id: string): boolean {
  const res = getDb().query("DELETE FROM pipelines WHERE id = ?").run(id);
  return res.changes > 0;
}

// --------------------------------------------------------------------- runs

export function getRun(id: string): Run | null {
  return (getDb().query("SELECT * FROM runs WHERE id = ?").get(id) as Run) ?? null;
}

/**
 * Materialises a scheduled run. Relies on the partial unique index to make
 * concurrent/repeated ticks harmless, so callers never need to check first.
 * Returns null when the occurrence already existed.
 */
export function insertScheduledRun(pipelineId: string, scheduledFor: number): Run | null {
  const id = randomUUID();
  const res = getDb()
    .query(
      `INSERT OR IGNORE INTO runs (id, pipeline_id, scheduled_for, status, trigger)
       VALUES (?, ?, ?, 'pending', 'schedule')`,
    )
    .run(id, pipelineId, scheduledFor);
  return res.changes > 0 ? getRun(id) : null;
}

export function insertManualRun(pipelineId: string): Run {
  const id = randomUUID();
  getDb()
    .query(
      `INSERT INTO runs (id, pipeline_id, scheduled_for, status, trigger)
       VALUES (?, ?, ?, 'pending', 'manual')`,
    )
    .run(id, pipelineId, Date.now());
  return getRun(id)!;
}

export function updateRun(id: string, patch: Partial<Run>): void {
  const fields = Object.keys(patch);
  if (fields.length === 0) return;
  const set = fields.map((f) => `${f} = ?`).join(", ");
  const values = fields.map((f) => (patch as Record<string, unknown>)[f]);
  getDb().query(`UPDATE runs SET ${set} WHERE id = ?`).run(...(values as never[]), id);
}

/** Runs whose moment has arrived. Ordered oldest-first so backlogs drain in order. */
export function dueRuns(now = Date.now()): Run[] {
  return getDb()
    .query("SELECT * FROM runs WHERE status = 'pending' AND scheduled_for <= ? ORDER BY scheduled_for ASC")
    .all(now) as Run[];
}

export function runsInState(status: RunStatus): Run[] {
  return getDb().query("SELECT * FROM runs WHERE status = ?").all(status) as Run[];
}

export function hasActiveRun(pipelineId: string): boolean {
  const row = getDb()
    .query("SELECT COUNT(*) AS n FROM runs WHERE pipeline_id = ? AND status = 'running'")
    .get(pipelineId) as { n: number };
  return row.n > 0;
}

/** Latest scheduled occurrence we have already materialised, if any. */
export function lastScheduledFor(pipelineId: string): number | null {
  const row = getDb()
    .query(
      "SELECT MAX(scheduled_for) AS t FROM runs WHERE pipeline_id = ? AND trigger = 'schedule'",
    )
    .get(pipelineId) as { t: number | null };
  return row.t;
}

export function nextPendingRun(pipelineId: string): Run | null {
  return (
    (getDb()
      .query(
        `SELECT * FROM runs WHERE pipeline_id = ? AND status = 'pending'
         ORDER BY scheduled_for ASC LIMIT 1`,
      )
      .get(pipelineId) as Run) ?? null
  );
}

export interface RunQuery {
  pipelineId?: string;
  status?: RunStatus;
  limit?: number;
}

export function queryRuns(q: RunQuery = {}): Run[] {
  const where: string[] = [];
  const args: unknown[] = [];
  if (q.pipelineId) { where.push("pipeline_id = ?"); args.push(q.pipelineId); }
  if (q.status) { where.push("status = ?"); args.push(q.status); }
  const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const limit = Math.min(Math.max(q.limit ?? 50, 1), 500);
  return getDb()
    .query(`SELECT * FROM runs ${clause} ORDER BY scheduled_for DESC LIMIT ?`)
    .all(...(args as never[]), limit) as Run[];
}

/** Keeps the DB from growing without bound; called once on boot. */
export function pruneOldRuns(retentionDays = config.RUN_RETENTION_DAYS): number {
  const cutoff = Date.now() - retentionDays * 86_400_000;
  const res = getDb()
    .query("DELETE FROM runs WHERE scheduled_for < ? AND status NOT IN ('pending','running')")
    .run(cutoff);
  return res.changes;
}

/** Dropped when a pipeline is disabled, so re-enabling never fires a stale run. */
export function deletePendingRuns(pipelineId: string): number {
  const res = getDb()
    .query("DELETE FROM runs WHERE pipeline_id = ? AND status = 'pending' AND trigger = 'schedule'")
    .run(pipelineId);
  return res.changes;
}

export function lastFinishedRun(pipelineId: string): Run | null {
  return (
    (getDb()
      .query(
        `SELECT * FROM runs WHERE pipeline_id = ? AND status NOT IN ('pending','running')
         ORDER BY scheduled_for DESC LIMIT 1`,
      )
      .get(pipelineId) as Run) ?? null
  );
}
