/**
 * AES-256-GCM for per-pipeline env vars at rest, mirroring the at-rest
 * encryption key-distributor uses for provisioned API keys.
 *
 * Ciphertext format: `v1.<iv>.<tag>.<ciphertext>`, each part base64url. The
 * version prefix is there so the format can change later without ambiguity.
 * GCM is authenticated, so tampering surfaces as a decrypt error rather than
 * as garbage env vars handed to a pipeline.
 */
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { config } from "./config.ts";

const ALGO = "aes-256-gcm";
const IV_BYTES = 12;

function key(): Buffer {
  if (!config.ENCRYPTION_KEY) throw new Error("ENCRYPTION_KEY is not set");
  const buf = Buffer.from(config.ENCRYPTION_KEY, "hex");
  if (buf.length !== 32) {
    throw new Error("ENCRYPTION_KEY must be 64 hex characters (32 bytes)");
  }
  return buf;
}

const b64 = (b: Buffer) => b.toString("base64url");

export function encryptJSON(value: unknown): string {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv(ALGO, key(), iv);
  const ct = Buffer.concat([
    cipher.update(Buffer.from(JSON.stringify(value), "utf8")),
    cipher.final(),
  ]);
  return ["v1", b64(iv), b64(cipher.getAuthTag()), b64(ct)].join(".");
}

export function decryptJSON<T>(blob: string): T {
  const parts = blob.split(".");
  if (parts.length !== 4 || parts[0] !== "v1") {
    throw new Error("Malformed ciphertext");
  }
  const [, ivB64, tagB64, ctB64] = parts as [string, string, string, string];
  const decipher = createDecipheriv(ALGO, key(), Buffer.from(ivB64, "base64url"));
  decipher.setAuthTag(Buffer.from(tagB64, "base64url"));
  const pt = Buffer.concat([
    decipher.update(Buffer.from(ctB64, "base64url")),
    decipher.final(),
  ]);
  return JSON.parse(pt.toString("utf8")) as T;
}

/** Empty env is stored as "" rather than a ciphertext of `{}`. */
export function encryptEnv(env: Record<string, string>): string {
  return Object.keys(env).length === 0 ? "" : encryptJSON(env);
}

export function decryptEnv(blob: string): Record<string, string> {
  if (!blob) return {};
  return decryptJSON<Record<string, string>>(blob);
}
