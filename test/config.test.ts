import { describe, expect, test } from "bun:test";
import { config } from "../src/config.ts";

describe("config", () => {
  // The vault may hold a bare host; fetch() needs a scheme or it throws, and a
  // status channel that cannot publish defeats the point of the service.
  test("NTFY_BASE is always a fetchable absolute URL", () => {
    expect(config.NTFY_BASE).toMatch(/^https?:\/\//);
    expect(() => new URL(config.NTFY_BASE)).not.toThrow();
  });

  test("data paths are derived from DATA_DIR", () => {
    expect(config.WORKSPACES_DIR.startsWith(config.DATA_DIR)).toBe(true);
    expect(config.LOGS_DIR.startsWith(config.DATA_DIR)).toBe(true);
    expect(config.DB_PATH.startsWith(config.DATA_DIR)).toBe(true);
  });
});
