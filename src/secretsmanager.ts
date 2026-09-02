/**
 * Client for the VPS secrets manager.
 *
 * A pipeline does not store credentials itself - it stores a *reference* to a
 * key in the vault ("Root/PIPELINE_RUNNER_GITHUB_TOKEN"). Values are fetched
 * fresh at run time and never persisted here, so rotating a secret in the
 * vault takes effect on the next run with no pipeline edit.
 *
 * References are validated when a pipeline is created or updated, so a typo
 * fails immediately with a clear message rather than at 4am when the run fires.
 */
import { config } from "./config.ts";
import { run } from "./lib/proc.ts";

export class SecretRefError extends Error {}

export interface ParsedRef {
  folder: string;
  key: string;
  /** Canonical "Folder/KEY" form. */
  full: string;
}

const KEY_RE = /^[A-Za-z0-9_.-]{1,128}$/;

/** Accepts "Folder/KEY" or a bare "KEY", which defaults to the Root folder. */
export function parseRef(ref: string): ParsedRef {
  const raw = (ref ?? "").trim();
  if (!raw) throw new SecretRefError("Secret reference must not be empty");

  const parts = raw.split("/");
  if (parts.length > 2) {
    throw new SecretRefError(`Secret reference '${raw}' must be "KEY" or "Folder/KEY"`);
  }
  const folder = parts.length === 2 ? parts[0]!.trim() : "Root";
  const key = (parts.length === 2 ? parts[1]! : parts[0]!).trim();

  if (!KEY_RE.test(folder)) throw new SecretRefError(`Invalid folder in reference '${raw}'`);
  if (!KEY_RE.test(key)) throw new SecretRefError(`Invalid key in reference '${raw}'`);

  return { folder, key, full: `${folder}/${key}` };
}

// ------------------------------------------------------------------ network

/**
 * Joins secret-manager's docker network so it resolves by container name.
 *
 * secret-manager binds 127.0.0.1:8000 on the host, so it is unreachable from a
 * container via host-gateway, and the two live in different compose projects
 * (hence different bridge networks, which are isolated from one another).
 * Rather than modify that service, we attach ourselves to its network using the
 * docker socket we already hold. Idempotent, and re-run on every boot so it
 * self-heals if either container is recreated.
 */
export async function ensureSecretManagerNetwork(): Promise<boolean> {
  const self = process.env.HOSTNAME;
  if (!self) {
    console.warn("[secrets] no HOSTNAME; not running in docker? skipping network join");
    return false;
  }

  const inspect = await run([
    "docker", "inspect", config.SECRET_MANAGER_CONTAINER,
    "--format", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}\n{{end}}",
  ]);
  if (inspect.code !== 0) {
    console.warn(
      `[secrets] cannot inspect '${config.SECRET_MANAGER_CONTAINER}': ${inspect.stderr.trim()}`,
    );
    return false;
  }

  const networks = inspect.stdout.split("\n").map((n) => n.trim()).filter(Boolean);
  if (!networks.length) {
    console.warn("[secrets] secret-manager reports no networks");
    return false;
  }

  for (const net of networks) {
    const res = await run(["docker", "network", "connect", net, self]);
    if (res.code === 0) {
      console.log(`[secrets] joined network '${net}' to reach secret-manager`);
      return true;
    }
    // Already attached is the normal steady state, not a failure.
    if (/already exists|endpoint with name/i.test(res.stderr)) {
      console.log(`[secrets] already on network '${net}'`);
      return true;
    }
    console.warn(`[secrets] could not join '${net}': ${res.stderr.trim()}`);
  }
  return false;
}

// --------------------------------------------------------------------- api

function authHeaders(): Record<string, string> {
  if (!config.SECRET_MANAGER_TOKEN) {
    throw new SecretRefError(
      "SECRET_MANAGER_TOKEN is not configured, so secret references cannot be used. " +
        "Add PIPELINE_RUNNER_SECRET_MANAGER_TOKEN (a read token) to the secrets manager.",
    );
  }
  return { Authorization: `Bearer ${config.SECRET_MANAGER_TOKEN}` };
}

const CACHE_TTL_MS = 30_000;
let keyCache: { at: number; keys: Set<string> } | null = null;

/** Canonical "Folder/KEY" strings for every secret in the vault. */
export async function listKeys(force = false): Promise<Set<string>> {
  if (!force && keyCache && Date.now() - keyCache.at < CACHE_TTL_MS) {
    return keyCache.keys;
  }

  const res = await fetch(`${config.SECRET_MANAGER_URL.replace(/\/$/, "")}/api/secrets`, {
    headers: authHeaders(),
    signal: AbortSignal.timeout(10_000),
  }).catch((e) => {
    throw new SecretRefError(
      `Cannot reach the secrets manager at ${config.SECRET_MANAGER_URL}: ${(e as Error).message}`,
    );
  });

  if (!res.ok) {
    throw new SecretRefError(
      `Secrets manager returned HTTP ${res.status} listing keys. Check SECRET_MANAGER_TOKEN.`,
    );
  }

  const body = (await res.json()) as { keys?: Array<{ key: string; folder: string }> };
  const keys = new Set((body.keys ?? []).map((k) => `${k.folder}/${k.key}`));
  keyCache = { at: Date.now(), keys };
  return keys;
}

export async function getSecret(ref: ParsedRef): Promise<string> {
  const url =
    `${config.SECRET_MANAGER_URL.replace(/\/$/, "")}/api/secrets/` +
    `${encodeURIComponent(ref.folder)}/${encodeURIComponent(ref.key)}`;

  const res = await fetch(url, {
    headers: authHeaders(),
    signal: AbortSignal.timeout(10_000),
  }).catch((e) => {
    throw new SecretRefError(
      `Cannot reach the secrets manager at ${config.SECRET_MANAGER_URL}: ${(e as Error).message}`,
    );
  });

  if (res.status === 404) {
    throw new SecretRefError(`Secret '${ref.full}' no longer exists in the secrets manager`);
  }
  if (!res.ok) {
    throw new SecretRefError(`Secrets manager returned HTTP ${res.status} for '${ref.full}'`);
  }

  const body = (await res.json()) as { value?: string };
  if (typeof body.value !== "string") {
    throw new SecretRefError(`Secret '${ref.full}' returned no value`);
  }
  return body.value;
}

export interface RefValidation {
  ok: boolean;
  missing: string[];
  /** Set when the vault itself could not be consulted. */
  error?: string;
}

/**
 * Checks every reference exists. On a miss the key list is refetched once
 * before reporting, so a secret added seconds ago is not falsely reported
 * missing because of the cache.
 */
export async function validateRefs(refs: Record<string, string>): Promise<RefValidation> {
  const wanted = Object.values(refs);
  if (wanted.length === 0) return { ok: true, missing: [] };

  let parsed: ParsedRef[];
  try {
    parsed = wanted.map(parseRef);
  } catch (e) {
    return { ok: false, missing: [], error: (e as Error).message };
  }

  try {
    let keys = await listKeys();
    let missing = parsed.filter((r) => !keys.has(r.full)).map((r) => r.full);

    if (missing.length > 0) {
      keys = await listKeys(true);
      missing = parsed.filter((r) => !keys.has(r.full)).map((r) => r.full);
    }
    return { ok: missing.length === 0, missing };
  } catch (e) {
    return { ok: false, missing: [], error: (e as Error).message };
  }
}

/**
 * Fetches every referenced value. Throws if any is missing - a run must never
 * start with a silently-absent credential.
 */
export async function resolveRefs(refs: Record<string, string>): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const [envName, ref] of Object.entries(refs)) {
    out[envName] = await getSecret(parseRef(ref));
  }
  return out;
}

/** Test seam. */
export function _clearKeyCache(): void {
  keyCache = null;
}
