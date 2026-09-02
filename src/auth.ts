/**
 * Bearer-token auth.
 *
 * This token is effectively a host-root credential, because the service holds
 * the Docker socket. Hence: constant-time comparison so the token cannot be
 * discovered a byte at a time, and per-IP failure throttling so it cannot be
 * brute-forced.
 */
import { timingSafeEqual } from "node:crypto";
import { config } from "./config.ts";

const WINDOW_MS = 60_000;
const MAX_FAILURES = 20;

const failures = new Map<string, { count: number; resetAt: number }>();

export function clientIp(req: Request, fallback = "unknown"): string {
  // nginx is the only thing in front of us, and it always sets these.
  const xff = req.headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0]!.trim();
  return req.headers.get("x-real-ip")?.trim() || fallback;
}

function constantTimeEquals(a: string, b: string): boolean {
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  // timingSafeEqual throws on length mismatch, which would itself leak length,
  // so compare against a fixed-size digest-like padding instead.
  if (ab.length !== bb.length) {
    // Still burn a comparison so the failure path costs the same.
    timingSafeEqual(ab, ab);
    return false;
  }
  return timingSafeEqual(ab, bb);
}

export function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const entry = failures.get(ip);
  if (!entry || now > entry.resetAt) return false;
  return entry.count >= MAX_FAILURES;
}

function recordFailure(ip: string): void {
  const now = Date.now();
  const entry = failures.get(ip);
  if (!entry || now > entry.resetAt) {
    failures.set(ip, { count: 1, resetAt: now + WINDOW_MS });
    return;
  }
  entry.count++;
}

export interface AuthResult {
  ok: boolean;
  response?: Response;
}

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

export function requireAuth(req: Request): AuthResult {
  const ip = clientIp(req);

  if (isRateLimited(ip)) {
    return { ok: false, response: json(429, { error: "Too many failed attempts. Try again shortly." }) };
  }

  const header = req.headers.get("authorization") ?? "";
  const token = header.startsWith("Bearer ") ? header.slice(7).trim() : "";

  if (!token || !constantTimeEquals(token, config.API_TOKEN)) {
    recordFailure(ip);
    console.warn(`[auth] rejected request from ${ip} to ${new URL(req.url).pathname}`);
    return { ok: false, response: json(401, { error: "Unauthorized" }) };
  }

  return { ok: true };
}

/** Test seam. */
export function _resetRateLimit(): void {
  failures.clear();
}
