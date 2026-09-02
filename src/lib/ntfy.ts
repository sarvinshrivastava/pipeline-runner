/**
 * ntfy status pings. Ported from nidra-daily-push, whose two design
 * constraints still apply here:
 *
 * 1. Published as JSON to the base URL rather than via X-Title/X-Tags headers,
 *    because header values must be ASCII and notification copy routinely
 *    carries emoji and typographic punctuation.
 * 2. Fail-soft. A dead status channel must never fail a run that otherwise
 *    succeeded, so every error is warned and swallowed.
 */
import { config } from "../config.ts";

export type Level = "ok" | "fail";

export async function notify(
  level: Level,
  title: string,
  message: string,
  tags: string[] = [],
): Promise<void> {
  const topic = level === "fail" ? config.NTFY_TOPIC_FAIL : config.NTFY_TOPIC_OK;
  if (!topic) {
    console.warn(`[ntfy] no topic configured for level=${level}; skipping ping`);
    return;
  }

  const link = config.DASHBOARD_URL.replace(/\/$/, "");
  const body: Record<string, unknown> = {
    topic,
    title,
    message: link ? `${message}\n\n${link}` : message,
    tags: tags.length ? tags : [level === "fail" ? "rotating_light" : "white_check_mark"],
    priority: level === "fail" ? 4 : 3,
  };
  if (link) body.click = link;

  try {
    const res = await fetch(`${config.NTFY_BASE.replace(/\/$/, "")}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(config.NTFY_TOKEN ? { Authorization: `Bearer ${config.NTFY_TOKEN}` } : {}),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) {
      console.warn(`[ntfy] publish failed: HTTP ${res.status} ${await res.text()}`);
    }
  } catch (e) {
    console.warn(`[ntfy] publish error: ${(e as Error).message}`);
  }
}

/** Convenience wrapper so every ping is prefixed with the pipeline it concerns. */
export function notifyPipeline(
  level: Level,
  pipelineName: string,
  headline: string,
  message: string,
  tags: string[] = [],
): Promise<void> {
  return notify(level, `${pipelineName}: ${headline}`, message, tags);
}
