import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { LogSink } from "../src/lib/logsink.ts";

const tmp = (name: string) => join(process.env.DATA_DIR!, name);

describe("LogSink", () => {
  test("writes short output verbatim", () => {
    const path = tmp("short.log");
    const sink = new LogSink(path, 1000);
    sink.writeText("hello\nworld\n");
    sink.close();
    expect(readFileSync(path, "utf8")).toBe("hello\nworld\n");
  });

  // Keeping only the head loses the error that killed the run; keeping only
  // the tail loses how it started. This keeps both.
  test("keeps the head and the tail when output exceeds the cap", () => {
    const path = tmp("long.log");
    const sink = new LogSink(path, 100);
    sink.writeText("A".repeat(50));
    sink.writeText("B".repeat(500));
    sink.writeText("C".repeat(40));
    sink.close();

    const content = readFileSync(path, "utf8");
    expect(content.startsWith("A".repeat(50))).toBe(true);
    expect(content.endsWith("C".repeat(40))).toBe(true);
    expect(content).toContain("bytes of output dropped from the middle");
    expect(content).not.toContain("B".repeat(500));
  });
});
