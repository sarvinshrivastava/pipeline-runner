import { describe, expect, test } from "bun:test";
import { decryptEnv, decryptJSON, encryptEnv, encryptJSON } from "../src/crypto.ts";

describe("crypto", () => {
  test("round-trips a value", () => {
    const value = { API_KEY: "s3cret", OTHER: "x" };
    expect(decryptJSON<typeof value>(encryptJSON(value))).toEqual(value);
  });

  test("produces a different ciphertext each time (random IV)", () => {
    expect(encryptJSON({ a: 1 })).not.toBe(encryptJSON({ a: 1 }));
  });

  // GCM is authenticated precisely so tampering surfaces as an error rather
  // than as garbage env vars handed to a pipeline.
  test("rejects tampered ciphertext", () => {
    const blob = encryptJSON({ API_KEY: "s3cret" });
    const parts = blob.split(".");
    const flipped = Buffer.from(parts[3]!, "base64url");
    flipped[0] = flipped[0]! ^ 0xff;
    parts[3] = flipped.toString("base64url");
    expect(() => decryptJSON(parts.join("."))).toThrow();
  });

  test("rejects a malformed blob", () => {
    expect(() => decryptJSON("nonsense")).toThrow("Malformed ciphertext");
    expect(() => decryptJSON("v2.a.b.c")).toThrow("Malformed ciphertext");
  });

  test("stores empty env as an empty string rather than a ciphertext", () => {
    expect(encryptEnv({})).toBe("");
    expect(decryptEnv("")).toEqual({});
  });
});
