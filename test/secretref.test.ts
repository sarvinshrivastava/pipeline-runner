import { describe, expect, test } from "bun:test";
import { SecretRefError, parseRef } from "../src/secretsmanager.ts";
import { ValidationError, validateSecretRefs } from "../src/lib/validate.ts";

describe("parseRef", () => {
  test("a bare key defaults to the Root folder", () => {
    expect(parseRef("PIPELINE_RUNNER_GITHUB_TOKEN")).toEqual({
      folder: "Root",
      key: "PIPELINE_RUNNER_GITHUB_TOKEN",
      full: "Root/PIPELINE_RUNNER_GITHUB_TOKEN",
    });
  });

  test("an explicit folder is preserved", () => {
    expect(parseRef("otp/TOTP_SECRET").full).toBe("otp/TOTP_SECRET");
  });

  test("trims surrounding whitespace", () => {
    expect(parseRef("  Root/VPS_HOST  ").full).toBe("Root/VPS_HOST");
  });

  test("rejects nested paths, empties and stray characters", () => {
    for (const bad of ["", "   ", "a/b/c", "Root/", "/KEY", "Root/BAD KEY", "Root/../etc"]) {
      expect(() => parseRef(bad)).toThrow(SecretRefError);
    }
  });
});

describe("validateSecretRefs", () => {
  test("normalises bare keys to canonical Folder/KEY form", () => {
    expect(validateSecretRefs({ GIT_TOKEN: "MY_KEY" })).toEqual({ GIT_TOKEN: "Root/MY_KEY" });
  });

  test("accepts an empty or absent map", () => {
    expect(validateSecretRefs(undefined)).toEqual({});
    expect(validateSecretRefs({})).toEqual({});
  });

  test("rejects keys that are not valid env var names", () => {
    expect(() => validateSecretRefs({ "not-valid": "Root/K" })).toThrow(ValidationError);
    expect(() => validateSecretRefs({ "1BAD": "Root/K" })).toThrow(ValidationError);
  });

  test("rejects non-string and malformed references", () => {
    expect(() => validateSecretRefs({ TOKEN: 42 })).toThrow(ValidationError);
    expect(() => validateSecretRefs({ TOKEN: "a/b/c" })).toThrow(ValidationError);
  });

  test("rejects a non-object", () => {
    expect(() => validateSecretRefs(["Root/K"])).toThrow(ValidationError);
  });
});
