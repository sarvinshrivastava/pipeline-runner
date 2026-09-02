/**
 * Central config. Everything is read once at import time.
 *
 * Gotcha this file exists to contain: docker's `env_file` delivers an unset
 * secret as an EMPTY STRING, not as `undefined`. `process.env.X ?? "default"`
 * therefore never fires for a secret the manager didn't have. `env()` treats
 * "" as absent, which is almost always what we mean.
 */
import { join } from "node:path";

function env(name: string, fallback = ""): string {
  const v = process.env[name];
  return v === undefined || v.trim() === "" ? fallback : v.trim();
}

function envInt(name: string, fallback: number): number {
  const raw = env(name);
  if (!raw) return fallback;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : fallback;
}

const DATA_DIR = env("DATA_DIR", "/root/pipeline-runner-data");

export const config = {
  PORT: envInt("PORT", 3000),
  SERVICE_NAME: env("SERVICE_NAME", "pipeline-runner"),

  /**
   * Bind-mounted at the IDENTICAL path on host and in-container. `docker run -v`
   * arguments we generate are resolved by the host daemon, so any path we hand
   * it must exist on the host under the same name.
   */
  DATA_DIR,
  DB_PATH: join(DATA_DIR, "runner.db"),
  WORKSPACES_DIR: join(DATA_DIR, "workspaces"),
  LOGS_DIR: join(DATA_DIR, "logs"),

  API_TOKEN: env("API_TOKEN"),
  ENCRYPTION_KEY: env("ENCRYPTION_KEY"),

  GITHUB_TOKEN: env("GITHUB_TOKEN"),
  GIT_AUTHOR_NAME: env("GIT_AUTHOR_NAME", "pipeline-runner"),
  GIT_AUTHOR_EMAIL: env("GIT_AUTHOR_EMAIL", "pipeline-runner@vps.local"),

  NTFY_BASE: env("NTFY_BASE", "https://ntfy.sh"),
  NTFY_TOPIC_OK: env("NTFY_TOPIC_OK"),
  NTFY_TOPIC_FAIL: env("NTFY_TOPIC_FAIL"),
  NTFY_TOKEN: env("NTFY_TOKEN"),
  DASHBOARD_URL: env("DASHBOARD_URL"),

  /** Fire decision is a local SQL query + timestamp compare, so it is cheap. */
  TICK_MS: envInt("TICK_MS", 10_000),
  /** /healthz flips to 503 once a tick has not completed in this long. */
  HEALTH_STALL_MS: envInt("HEALTH_STALL_MS", 180_000),
  /** Per-run log file cap; oldest bytes are dropped first. */
  LOG_MAX_BYTES: envInt("LOG_MAX_BYTES", 5 * 1024 * 1024),
  /** Run rows older than this are pruned on boot. */
  RUN_RETENTION_DAYS: envInt("RUN_RETENTION_DAYS", 60),
} as const;

export function assertConfig(): void {
  const missing: string[] = [];
  if (!config.API_TOKEN) missing.push("API_TOKEN");
  if (!config.ENCRYPTION_KEY) missing.push("ENCRYPTION_KEY");
  if (missing.length) {
    throw new Error(
      `Refusing to start: missing required config: ${missing.join(", ")}. ` +
        `Add them to the secrets manager as PIPELINE_RUNNER_<NAME> and redeploy.`,
    );
  }
  if (config.API_TOKEN.length < 32) {
    throw new Error("Refusing to start: API_TOKEN must be at least 32 characters.");
  }
}
