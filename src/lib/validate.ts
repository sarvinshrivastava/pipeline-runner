/**
 * Input validation for pipeline definitions.
 *
 * The threat model: the API token is a host-root credential because the
 * service holds the Docker socket. Validation here is defence in depth on top
 * of the argv-array rule in proc.ts - the goal is that no pipeline field can
 * ever be read by docker as a FLAG rather than as a value.
 */
import parser from "cron-parser";
import { config } from "../config.ts";
import { parseRef, SecretRefError } from "../secretsmanager.ts";
import type { NewPipeline, OverlapPolicy } from "../db.ts";

export class ValidationError extends Error {}

function fail(msg: string): never {
  throw new ValidationError(msg);
}

/**
 * Must begin with an alphanumeric, which is what actually blocks flag
 * smuggling (`--privileged`, `-v /:/host`). The charset then excludes
 * whitespace and shell metacharacters.
 */
const IMAGE_RE = /^[A-Za-z0-9][A-Za-z0-9._\-/:@]{0,254}$/;

export function validateImage(image: string): string {
  const v = (image ?? "").trim();
  if (!v) fail("image is required");
  if (!IMAGE_RE.test(v)) {
    fail(
      "image must start with an alphanumeric and contain only [A-Za-z0-9._-/:@] " +
        "(this is what prevents a value being parsed as a docker flag)",
    );
  }
  if ((v.match(/@/g) ?? []).length > 1) fail("image may contain at most one '@' digest separator");
  return v;
}

const NAME_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;

export function validateName(name: string): string {
  const v = (name ?? "").trim();
  if (!NAME_RE.test(v)) {
    fail("name must be lowercase alphanumeric with dashes, 1-63 chars, starting alphanumeric");
  }
  return v;
}

export function validateRepoUrl(url: string): string {
  const v = (url ?? "").trim();
  if (!v) fail("repo_url is required");
  let parsed: URL;
  try {
    parsed = new URL(v);
  } catch {
    fail("repo_url must be a valid URL");
  }
  // https only: the token is injected into the URL at fetch/push time, and
  // ssh/git/file schemes would each need a different (and riskier) auth path.
  if (parsed.protocol !== "https:") fail("repo_url must use https://");
  if (parsed.username || parsed.password) {
    fail("repo_url must not embed credentials; the runner injects its token at push time");
  }
  return v;
}

const REF_RE = /^[A-Za-z0-9][A-Za-z0-9._\-/]{0,254}$/;

export function validateRef(ref: string, field: string): string {
  const v = (ref ?? "").trim();
  if (!v) fail(`${field} is required`);
  if (!REF_RE.test(v)) fail(`${field} contains invalid characters`);
  if (v.includes("..")) fail(`${field} must not contain '..'`);
  return v;
}

export function validateCron(expr: string, timezone: string): string {
  const v = (expr ?? "").trim();
  if (!v) fail("cron is required");
  try {
    parser.parseExpression(v, { tz: timezone });
  } catch (e) {
    fail(`cron is not a valid expression: ${(e as Error).message}`);
  }
  return v;
}

export function validateTimezone(tz: string): string {
  const v = (tz ?? "").trim() || "UTC";
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: v });
  } catch {
    fail(`timezone '${v}' is not a recognised IANA zone`);
  }
  return v;
}

const ENV_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function validateEnv(env: unknown): Record<string, string> {
  if (env === undefined || env === null) return {};
  if (typeof env !== "object" || Array.isArray(env)) fail("env must be an object");
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(env as Record<string, unknown>)) {
    if (!ENV_KEY_RE.test(k)) fail(`env key '${k}' is not a valid environment variable name`);
    if (typeof v !== "string") fail(`env value for '${k}' must be a string`);
    if (v.includes("\n")) fail(`env value for '${k}' must not contain newlines`);
    out[k] = v;
  }
  return out;
}

function num(value: unknown, field: string, min: number, max: number, dflt: number): number {
  if (value === undefined || value === null) return dflt;
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) fail(`${field} must be a number`);
  if (n < min || n > max) fail(`${field} must be between ${min} and ${max}`);
  return n;
}

function bool(value: unknown, dflt: boolean): number {
  if (value === undefined || value === null) return dflt ? 1 : 0;
  return value === true || value === 1 || value === "true" ? 1 : 0;
}

/**
 * Validates the SHAPE of secret references. Whether each one actually exists in
 * the vault is checked separately, against the live secrets manager.
 */
export function validateSecretRefs(value: unknown): Record<string, string> {
  if (value === undefined || value === null) return {};
  if (typeof value !== "object" || Array.isArray(value)) fail("secret_refs must be an object");

  const out: Record<string, string> = {};
  for (const [envName, ref] of Object.entries(value as Record<string, unknown>)) {
    if (!ENV_KEY_RE.test(envName)) {
      fail(`secret_refs key '${envName}' is not a valid environment variable name`);
    }
    if (typeof ref !== "string") fail(`secret_refs value for '${envName}' must be a string`);
    try {
      out[envName] = parseRef(ref).full;
    } catch (e) {
      if (e instanceof SecretRefError) fail(e.message);
      throw e;
    }
  }
  return out;
}

export interface ParsedPipeline {
  pipeline: Omit<NewPipeline, "encrypted_env">;
  env: Record<string, string>;
}

/** Parses and validates a create/update body. Throws ValidationError. */
export function parsePipelineInput(body: Record<string, unknown>): ParsedPipeline {
  const timezone = validateTimezone(String(body.timezone ?? "UTC"));
  const command = String(body.command ?? "").trim();
  if (!command) fail("command is required");
  if (command.length > 8192) fail("command must be 8192 characters or fewer");

  const overlapRaw = String(body.overlap_policy ?? "skip");
  if (overlapRaw !== "skip" && overlapRaw !== "queue") {
    fail("overlap_policy must be 'skip' or 'queue'");
  }

  const timeout_sec = num(body.timeout_sec, "timeout_sec", 1, 86_400, 900);
  const max_late_sec = num(body.max_late_sec, "max_late_sec", 0, 86_400, 600);
  const late_warn_sec = num(body.late_warn_sec, "late_warn_sec", 0, 86_400, 60);
  if (late_warn_sec > max_late_sec) {
    fail("late_warn_sec must not exceed max_late_sec, or every run would warn before it is abandoned");
  }
  // The scheduler fires within one tick of the scheduled moment, so a healthy
  // run is routinely a few seconds "late". A threshold below the tick interval
  // would page on every single successful run.
  const tickSec = Math.ceil(config.TICK_MS / 1000);
  if (late_warn_sec < tickSec) {
    fail(
      `late_warn_sec must be at least ${tickSec}s (the scheduler tick interval). ` +
        `Below that, normal scheduling granularity would make every run page.`,
    );
  }

  return {
    env: validateEnv(body.env),
    pipeline: {
      secret_refs: JSON.stringify(validateSecretRefs(body.secret_refs)),
      name: validateName(String(body.name ?? "")),
      repo_url: validateRepoUrl(String(body.repo_url ?? "")),
      branch: validateRef(String(body.branch ?? "main"), "branch"),
      output_branch: body.output_branch
        ? validateRef(String(body.output_branch), "output_branch")
        : "",
      image: validateImage(String(body.image ?? "")),
      command,
      cron: validateCron(String(body.cron ?? ""), timezone),
      timezone,
      enabled: bool(body.enabled, true),
      timeout_sec,
      max_late_sec,
      late_warn_sec,
      memory_mb: num(body.memory_mb, "memory_mb", 16, 16_384, 512),
      cpus: num(body.cpus, "cpus", 0.1, 8, 1),
      idempotent: bool(body.idempotent, true),
      overlap_policy: overlapRaw as OverlapPolicy,
      notify_on_success: bool(body.notify_on_success, false),
      commit_results: bool(body.commit_results, true),
    },
  };
}
