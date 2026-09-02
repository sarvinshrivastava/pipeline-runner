/**
 * Git-as-storage. A pipeline's repo is cloned here in the SERVICE container,
 * the run container only ever sees the resulting working tree at /workspace,
 * and the commit/push happens back here afterwards.
 *
 * The consequence, which is the point: the git token never enters the
 * pipeline's container, so a pipeline script cannot exfiltrate it.
 */
import { existsSync } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { config } from "./config.ts";
import { run, runOrThrow } from "./lib/proc.ts";
import type { Pipeline } from "./db.ts";

/**
 * The workspace is owned by root and git refuses to operate on trees it
 * considers foreign, so every invocation carries safe.directory. Terminal
 * prompts are disabled so a bad credential fails fast instead of hanging.
 */
const GIT = ["git", "-c", "safe.directory=*"];
const GIT_ENV = { GIT_TERMINAL_PROMPT: "0" };

export function workspacePath(pipeline: Pipeline): string {
  return join(config.WORKSPACES_DIR, pipeline.id);
}

/** Which token authenticates this pipeline's repo. */
export function gitTokenFor(env: Record<string, string>): string {
  return env.GIT_TOKEN || config.GITHUB_TOKEN;
}

/**
 * Credentials are passed on the command line per-invocation and the remote is
 * always reset to the clean URL, so no token is ever written to .git/config.
 */
function authedUrl(repoUrl: string, token: string): string {
  if (!token) return repoUrl;
  const u = new URL(repoUrl);
  u.username = "x-access-token";
  u.password = token;
  return u.toString();
}

export function outputBranchOf(pipeline: Pipeline): string {
  return pipeline.output_branch || pipeline.branch;
}

/**
 * Brings the workspace to an exact copy of origin/<branch>.
 *
 * `clean -fdx` is deliberate: a leftover file from a previous run silently
 * changing this run's behaviour is a genuinely horrible class of bug, and
 * determinism is worth more than the cache. Git history is still reused.
 */
export async function syncWorkspace(
  pipeline: Pipeline,
  token: string,
): Promise<string> {
  const dir = workspacePath(pipeline);
  const url = authedUrl(pipeline.repo_url, token);
  const secrets = token ? [token] : [];

  if (!existsSync(join(dir, ".git"))) {
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });
    await runOrThrow([...GIT, "clone", "--branch", pipeline.branch, url, dir], {
      env: GIT_ENV,
      redact: secrets,
      timeoutMs: 300_000,
    });
    // Drop the credential that `clone` persisted into .git/config.
    await runOrThrow([...GIT, "remote", "set-url", "origin", pipeline.repo_url], {
      cwd: dir,
      env: GIT_ENV,
      redact: secrets,
    });
  } else {
    await runOrThrow([...GIT, "fetch", "--prune", url, pipeline.branch], {
      cwd: dir,
      env: GIT_ENV,
      redact: secrets,
      timeoutMs: 300_000,
    });
    await runOrThrow([...GIT, "reset", "--hard", "FETCH_HEAD"], {
      cwd: dir,
      env: GIT_ENV,
      redact: secrets,
    });
    await runOrThrow([...GIT, "clean", "-fdx"], { cwd: dir, env: GIT_ENV, redact: secrets });
  }

  return dir;
}

export interface CommitResult {
  sha: string | null;
  /** True when the run produced no file changes - a success, not a failure. */
  noop: boolean;
}

/**
 * Commits whatever the run wrote and pushes it to the output branch.
 * Returns noop:true when the tree is clean, which is a normal outcome for a
 * pipeline whose upstream data hasn't changed.
 */
export async function commitAndPush(
  pipeline: Pipeline,
  token: string,
  message: string,
): Promise<CommitResult> {
  const dir = workspacePath(pipeline);
  const secrets = token ? [token] : [];
  const branch = outputBranchOf(pipeline);
  const url = authedUrl(pipeline.repo_url, token);

  await runOrThrow([...GIT, "add", "-A"], { cwd: dir, env: GIT_ENV, redact: secrets });

  const status = await runOrThrow([...GIT, "status", "--porcelain"], {
    cwd: dir,
    env: GIT_ENV,
    redact: secrets,
  });
  if (!status.trim()) return { sha: null, noop: true };

  await runOrThrow(
    [
      ...GIT,
      "-c", `user.name=${config.GIT_AUTHOR_NAME}`,
      "-c", `user.email=${config.GIT_AUTHOR_EMAIL}`,
      "commit", "-m", message,
    ],
    { cwd: dir, env: GIT_ENV, redact: secrets },
  );

  const sha = (
    await runOrThrow([...GIT, "rev-parse", "HEAD"], { cwd: dir, env: GIT_ENV, redact: secrets })
  ).trim();

  // One rebase-and-retry covers the realistic race: something else pushed to
  // the output branch between our fetch and our push.
  const push = () =>
    run([...GIT, "push", url, `HEAD:refs/heads/${branch}`], {
      cwd: dir,
      env: GIT_ENV,
      redact: secrets,
      timeoutMs: 120_000,
    });

  let res = await push();
  if (res.code !== 0) {
    await runOrThrow([...GIT, "pull", "--rebase", url, branch], {
      cwd: dir,
      env: GIT_ENV,
      redact: secrets,
      timeoutMs: 120_000,
    });
    res = await push();
    if (res.code !== 0) {
      throw new Error(`git push failed after rebase: ${res.stderr.trim() || res.stdout.trim()}`);
    }
  }

  return { sha, noop: false };
}
