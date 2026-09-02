/**
 * Runs before any test module imports src/config.ts, which reads env at import
 * time. Notably leaves the ntfy topics empty so notify() short-circuits and no
 * test ever touches the network.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

process.env.DATA_DIR = mkdtempSync(join(tmpdir(), "plr-test-"));
process.env.API_TOKEN = "t".repeat(40);
process.env.ENCRYPTION_KEY = "ab".repeat(32);
process.env.NTFY_TOPIC_OK = "";
process.env.NTFY_TOPIC_FAIL = "";
process.env.GITHUB_TOKEN = "";
