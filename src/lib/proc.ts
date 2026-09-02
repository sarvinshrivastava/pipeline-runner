/**
 * Subprocess helper.
 *
 * Everything here takes an argv ARRAY and never a shell string. That is the
 * single most important property in this codebase: pipeline-supplied values
 * (image, repo URL, branch) reach `docker` and `git` as discrete argv entries,
 * so there is no host shell to inject into. A pipeline's own `command` string
 * is only ever interpreted by `sh -c` *inside* its own container.
 */
export interface ProcResult {
  code: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

export interface RunOptions {
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs?: number;
  /** Substrings scrubbed from captured output before it is returned. */
  redact?: string[];
}

export function redact(text: string, secrets: string[]): string {
  let out = text;
  for (const s of secrets) {
    if (s && s.length >= 4) out = out.split(s).join("***");
  }
  return out;
}

export async function run(argv: string[], opts: RunOptions = {}): Promise<ProcResult> {
  const proc = Bun.spawn(argv, {
    cwd: opts.cwd,
    env: { ...process.env, ...(opts.env ?? {}) },
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });

  let timedOut = false;
  const timer = opts.timeoutMs
    ? setTimeout(() => {
        timedOut = true;
        proc.kill("SIGKILL");
      }, opts.timeoutMs)
    : null;

  try {
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    const secrets = opts.redact ?? [];
    return {
      code,
      stdout: redact(stdout, secrets),
      stderr: redact(stderr, secrets),
      timedOut,
    };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Throws on non-zero exit, with redacted output in the message. */
export async function runOrThrow(argv: string[], opts: RunOptions = {}): Promise<string> {
  const res = await run(argv, opts);
  if (res.code !== 0) {
    const why = res.timedOut ? "timed out" : `exited ${res.code}`;
    throw new Error(`${argv[0]} ${why}: ${res.stderr.trim() || res.stdout.trim()}`);
  }
  return res.stdout;
}
