# pipeline-runner

Scheduled data pipelines that actually run when they say they will.

Register a pipeline over the API: a git repo, a container image, a command, and
a cron expression. The runner clones the repo, runs your script in a throwaway
container at the scheduled moment, commits whatever the script wrote back to a
branch, and pings ntfy when something goes wrong.

## Why this exists

GitHub Actions' `schedule` event was the incumbent for this and proved
unreliable — see `/root/apps/nidra-daily-push/learnings/github-cron-lag.md`.
Two escalating episodes: a 30-minute lag onto a contended queue boundary, then
**4–12 hour lags on every trigger for four consecutive days**. Three lessons are
baked into this service's design:

1. **More cron triggers is not redundancy.** All crons in a repo share one
   queue, so extra triggers fail together, as one correlated batch.
2. **A late run looks exactly like an on-time run.** Green ≠ on-time.
3. **Suppressing false alarms creates silent failures.** The fix for lag noise
   turned four genuinely missed days into green no-op runs with no alert at all.

So here: the trigger is a local 10-second tick with no shared queue; lateness is
recorded and paged rather than absorbed; and every abnormal path notifies.

## How scheduling behaves

- Each enabled pipeline always has one `pending` run row for its next
  occurrence, so "next run" is persisted and survives restarts.
- A run more than `max_late_sec` late is **abandoned** as `missed` and paged.
  It is never quietly run hours after the fact.
- A run that fires late but inside the window still executes, records
  `late_sec`, and pages if it exceeded `late_warn_sec`.
- A long outage is collapsed into **one** notification naming how many
  occurrences were missed, rather than a flood.
- On boot, a container that outlived a restart is waited on and finished
  properly. If it is gone, the run is `interrupted`; it is retried only if the
  pipeline is marked `idempotent`, and otherwise pages a human.

## Security model

The service mounts `/var/run/docker.sock`, so **the API token is effectively a
host-root credential**. Containment:

- Every `/api/*` route requires the bearer token, compared in constant time,
  with per-IP failure throttling. Only `/healthz` and the static dashboard shell
  are open.
- Pipelines supply an image and a command — **never docker flags**. Every flag
  in the `docker run` argv is a literal written by the service.
- All subprocesses are spawned with an **argv array**, never a shell string, so
  no pipeline field reaches a host shell. `image` must start with an
  alphanumeric, which is what blocks flag smuggling.
- Runs get `--cap-drop ALL`, `--security-opt no-new-privileges`, memory/CPU/PID
  caps and a hard timeout.
- Per-pipeline env vars are AES-256-GCM encrypted at rest and delivered via a
  `0600` env file that is deleted after the run.
- **The git token never enters a pipeline's container.** Clone and push happen
  service-side; the container only sees `/workspace`.

## Configuration

Secrets live in the secrets manager under the `PIPELINE_RUNNER_` prefix;
`vps-deploy` strips it and writes `.env`. See `.env.example`.

| Var | Purpose |
|---|---|
| `API_TOKEN` | Bearer token for the API. 32+ chars. Required. |
| `ENCRYPTION_KEY` | 64 hex chars. Encrypts pipeline env at rest. Required. |
| `GITHUB_TOKEN` | Default PAT (Contents r/w) for cloning and pushing. |
| `HOST_DATA_DIR` | Host path bind-mounted at the identical path in-container. |
| `NTFY_BASE` / `NTFY_TOPIC_OK` / `NTFY_TOPIC_FAIL` / `NTFY_TOKEN` | Status pings. |
| `DASHBOARD_URL` | Linked from notifications. |

> An unset secret arrives as an **empty string** through docker's `env_file`,
> not as absent, so `config.ts` treats `""` as missing rather than relying on
> `??`.

A pipeline may override the git credential by setting `GIT_TOKEN` in its own
env or secret refs; that key is consumed for auth and stripped before the
container starts.

## Secret references

A pipeline does not have to store credentials itself. It can **reference** a key
in the secrets manager, and the value is fetched fresh at run time:

```json
{
  "secret_refs": {
    "GIT_TOKEN": "Root/PIPELINE_RUNNER_GITHUB_TOKEN",
    "WEATHER_API_KEY": "Root/MY_WEATHER_KEY"
  }
}
```

The value maps to the env var on the left. A bare `"MY_KEY"` is shorthand for
`"Root/MY_KEY"`.

Why references beat storing the value:

- **Rotation is free.** Change the secret in the vault; the next run picks it up
  with no pipeline edit and no redeploy.
- **Nothing is duplicated.** The credential lives in exactly one place.
- **Typos fail immediately.** References are checked against the live vault when
  a pipeline is created or updated. A reference that does not resolve is a
  `400` naming the exact bad key — not a 4am failure.

```
These secret references do not exist in the secrets manager:
Root/DOES_NOT_EXST. Add them to the vault first, or fix the reference.
```

If a referenced secret is **deleted later**, two things catch it:
`GET /api/pipelines/:id/validate` reports it (and the dashboard's "Check refs"
button surfaces it), and the run itself fails loudly rather than starting
without the credential.

`GET /api/secrets` lists the key names available in the vault — names only,
never values.

### Reaching the vault

`secret-manager` publishes only on `127.0.0.1:8000` of the *host*, so a
container cannot reach it via `host-gateway`, and the two services live in
different compose projects (hence isolated bridge networks). Rather than modify
that service, the runner **joins secret-manager's docker network at boot** using
the socket it already holds, then addresses it as `http://secret-manager:8000`.
This is idempotent and re-run every boot, so it self-heals if either container
is recreated.

It needs `SECRET_MANAGER_TOKEN` — a **read** token is sufficient. Without it,
pipelines that use no references still run normally.

## API

All routes need `Authorization: Bearer $API_TOKEN`.

| Route | Purpose |
|---|---|
| `GET /healthz` | Liveness. 503 once the tick loop stalls. Unauthenticated. |
| `GET/POST /api/pipelines` | List / create |
| `GET/PATCH/DELETE /api/pipelines/:id` | Read / update / remove (`:id` may be the name) |
| `POST /api/pipelines/:id/trigger` | Run now |
| `POST /api/pipelines/:id/enable\|disable` | Toggle |
| `GET /api/runs?pipeline=&status=&limit=` | Run history |
| `GET /api/runs/:id` · `GET /api/runs/:id/logs` | Detail / logs |
| `POST /api/runs/:id/cancel` | Kill a running container |

Create a pipeline:

```bash
curl -X POST https://pipeline-runner.vps.sarvinshrivastava.space/api/pipelines \
  -H "Authorization: Bearer $API_TOKEN" -H 'content-type: application/json' \
  -d '{
    "name": "rates-daily",
    "repo_url": "https://github.com/owner/data-repo.git",
    "branch": "main",
    "image": "python:3.12-slim",
    "command": "pip install -q requests && python fetch.py",
    "cron": "17 6 * * *",
    "timezone": "Asia/Kolkata",
    "env": { "API_KEY": "…" }
  }'
```

Your script just writes files into the working directory; whatever changed is
committed and pushed. No file changes is a success, not a failure.

### Pipeline fields

| Field | Default | Notes |
|---|---|---|
| `branch` / `output_branch` | `main` / same as `branch` | Read from / commit to |
| `timeout_sec` | 900 | Container killed past this |
| `max_late_sec` | 600 | Past this a run is abandoned as `missed` |
| `late_warn_sec` | 60 | Past this a run still executes but pages |
| `memory_mb` / `cpus` | 512 / 1.0 | Hard caps; swap disabled |
| `idempotent` | `true` | May be auto-retried after an interrupted run |
| `overlap_policy` | `skip` | `skip` or `queue` when a run is still going |
| `commit_results` | `true` | Set false for a pipeline with no output |
| `secret_refs` | `{}` | `ENV_VAR` → `Folder/KEY` in the vault; validated on save |
| `notify_on_success` | `false` | Failures always notify |

## Run statuses

`pending` · `running` · `success` · `failed` · `missed` · `timeout` · `interrupted`

There is deliberately no `late` status — a late run still succeeded or failed,
and collapsing that into one state would hide the outcome. Lateness is recorded
on `late_sec`.

## Development

```bash
bun install
bun test
bun run typecheck
bun run dev
```

No host `bun` needed — the container image works too:

```bash
docker run --rm -v "$PWD:/app" -w /app oven/bun:1-alpine bun test
```

## Deploy

Push to `main`, then run the `deploy` workflow (manual `workflow_dispatch`, so
a stray push cannot deploy). It SSHes to the VPS, pulls, and runs
`vps-deploy pipeline-runner /root/apps/pipeline-runner`.
