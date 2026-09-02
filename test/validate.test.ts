import { describe, expect, test } from "bun:test";
import {
  ValidationError,
  parsePipelineInput,
  validateEnv,
  validateImage,
  validateRepoUrl,
} from "../src/lib/validate.ts";

describe("validateImage", () => {
  test("accepts ordinary image references", () => {
    for (const img of [
      "alpine",
      "python:3.12",
      "oven/bun:1-alpine",
      "ghcr.io/owner/repo:v1.2.3",
      "alpine@sha256:" + "a".repeat(64),
    ]) {
      expect(validateImage(img)).toBe(img);
    }
  });

  // The whole point of the leading-alphanumeric rule: a pipeline must never be
  // able to smuggle a docker flag through the image field.
  test("rejects values that would be parsed as docker flags", () => {
    for (const evil of ["--privileged", "-v", "--network=host", "-e", "--user=0"]) {
      expect(() => validateImage(evil)).toThrow(ValidationError);
    }
  });

  test("rejects shell metacharacters and whitespace", () => {
    for (const evil of ["alpine; rm -rf /", "alpine && id", "alpine $(id)", "alp ine", "alpine|cat"]) {
      expect(() => validateImage(evil)).toThrow(ValidationError);
    }
  });

  test("rejects an empty image", () => {
    expect(() => validateImage("")).toThrow(ValidationError);
  });
});

describe("validateRepoUrl", () => {
  test("requires https", () => {
    expect(() => validateRepoUrl("git@github.com:o/r.git")).toThrow(ValidationError);
    expect(() => validateRepoUrl("http://github.com/o/r")).toThrow(ValidationError);
    expect(() => validateRepoUrl("file:///etc/passwd")).toThrow(ValidationError);
  });

  test("rejects embedded credentials", () => {
    expect(() => validateRepoUrl("https://user:tok@github.com/o/r")).toThrow(ValidationError);
  });

  test("accepts a plain https url", () => {
    const u = "https://github.com/owner/repo.git";
    expect(validateRepoUrl(u)).toBe(u);
  });
});

describe("validateEnv", () => {
  test("rejects invalid env var names", () => {
    expect(() => validateEnv({ "not-valid": "x" })).toThrow(ValidationError);
    expect(() => validateEnv({ "1STARTS_WITH_DIGIT": "x" })).toThrow(ValidationError);
  });

  test("rejects newlines, which would forge extra entries in the env file", () => {
    expect(() => validateEnv({ OK: "a\nEVIL=b" })).toThrow(ValidationError);
  });

  test("accepts a normal mapping", () => {
    expect(validateEnv({ API_KEY: "abc" })).toEqual({ API_KEY: "abc" });
  });
});

describe("parsePipelineInput", () => {
  const base = {
    name: "my-pipeline",
    repo_url: "https://github.com/owner/repo.git",
    image: "python:3.12",
    command: "python fetch.py",
    cron: "*/5 * * * *",
  };

  test("applies documented defaults", () => {
    const { pipeline } = parsePipelineInput({ ...base });
    expect(pipeline.branch).toBe("main");
    expect(pipeline.timezone).toBe("UTC");
    expect(pipeline.timeout_sec).toBe(900);
    expect(pipeline.max_late_sec).toBe(600);
    expect(pipeline.idempotent).toBe(1);
    expect(pipeline.overlap_policy).toBe("skip");
  });

  test("rejects an invalid cron expression", () => {
    expect(() => parsePipelineInput({ ...base, cron: "not a cron" })).toThrow(ValidationError);
  });

  test("rejects an unknown timezone", () => {
    expect(() => parsePipelineInput({ ...base, timezone: "Mars/Olympus" })).toThrow(ValidationError);
  });

  // Otherwise every single run would warn before it could ever be abandoned.
  test("rejects late_warn_sec greater than max_late_sec", () => {
    expect(() =>
      parsePipelineInput({ ...base, late_warn_sec: 900, max_late_sec: 600 }),
    ).toThrow(ValidationError);
  });

  // A healthy run is a few seconds late by construction (it fires within one
  // tick), so a sub-tick threshold would page on every success.
  test("rejects late_warn_sec below the tick interval", () => {
    expect(() => parsePipelineInput({ ...base, late_warn_sec: 0 })).toThrow(ValidationError);
    expect(() => parsePipelineInput({ ...base, late_warn_sec: 1 })).toThrow(ValidationError);
  });

  test("enforces resource bounds", () => {
    expect(() => parsePipelineInput({ ...base, memory_mb: 1 })).toThrow(ValidationError);
    expect(() => parsePipelineInput({ ...base, cpus: 99 })).toThrow(ValidationError);
    expect(() => parsePipelineInput({ ...base, timeout_sec: 0 })).toThrow(ValidationError);
  });

  test("rejects a name that is not container/path safe", () => {
    expect(() => parsePipelineInput({ ...base, name: "My Pipeline!" })).toThrow(ValidationError);
    expect(() => parsePipelineInput({ ...base, name: "../escape" })).toThrow(ValidationError);
  });
});
