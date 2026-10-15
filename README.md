# pipeline-runner

The VPS's central time service. Other services register **jobs** (a trigger plus an HTTP callback), and pipeline-runner calls them at the right moment, records every run, and pages ntfy when something goes wrong.

A single Go binary (`plr`) with SQLite and an embedded dashboard. Delivery 1a ships HTTP callback targets. Container and inline targets arrive in 1b.

## How it behaves

- **Triggers:**

  | Trigger | Fires |
  |---|---|
  | `cron` | on a cron schedule; DST-aware with `timezone` |
  | `at` | once, at a fixed instant |
  | `delay` | once, after a duration from creation |
  | `invoke` | on demand only (`POST /v1/jobs/{id}/invoke`, optionally `?wait=N`) |

  Ad hoc one-off runs use `POST /v1/runs`.
- **Firing:**
  - Timers have millisecond precision, from an in-memory heap.
  - Each job's next occurrence is persisted, so it survives restarts.
- **Lateness:**
  - A run later than `max_late_sec` is recorded as `missed` and paged. It never runs hours late.
  - Occurrences that fell inside an outage or a backlog are **coalesced** into one run, counted in `plr_coalesced_occurrences_total`, and paged at most once per job every 10 minutes.
- **Delivery:**
  - `at_least_once` is the default; send `X-PLR-Run-Id` handling for idempotency.
  - `at_most_once` never re-sends an ambiguous request; such runs are paged as **NEEDS A HUMAN**.
  - Retries with backoff are opt-in through `policy.retry`.
- **Callbacks:**
  - In `sync` mode the HTTP response is the result.
  - In `async` mode the target returns 202, then calls `POST /v1/runs/{id}/heartbeat` and `/complete` with the run token it received.
  - The target can steer the schedule with `Retry-After` or `defer_until`.
- **Concurrency:**
  - `overlap` is `skip`, `queue` or `allow` per job.
  - `concurrency_key` and `concurrency_limit` apply across a client's jobs.
  - A global HTTP slot pool (`MAX_HTTP_INFLIGHT`) and a per-client cap (`MAX_HTTP_INFLIGHT_PER_CLIENT`) keep one client from starving the rest. For HTTP jobs, a `concurrency_limit` above the cap is effectively the cap.
- **Priority:**
  - `policy.priority` is `high`, `normal` (the default) or `low`. It orders runs that are waiting for a slot.
  - Every 30 s of waiting gains a run one level, so a `low` run is never starved.
  - A job's own queue (`overlap: queue`) stays first in, first out.
  - Limitation: runs due at the same instant start in arrival order while free slots remain.

Every callback carries `X-PLR-Run-Id`, `X-PLR-Job`, `X-PLR-Trigger`, `X-PLR-Attempt` and `X-PLR-Scheduled-For`. When the job has an `hmac_ref`, it also carries `X-PLR-Signature`.

## API

OpenAPI: `GET /v1/openapi.json`. Auth is `Authorization: Bearer <token>`. The admin token comes from `API_TOKEN`; per-client tokens come from `POST /v1/clients`.

| Area | Endpoints | Scope |
|---|---|---|
| Jobs | `GET/POST /v1/jobs`, `GET/PATCH/DELETE /v1/jobs/{id}`, `POST /v1/jobs/{id}/{invoke,enable,disable,validate}`, `POST /v1/jobs/validate` | `jobs:read`, `jobs:write`, `runs:invoke` |
| Runs | `GET /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/runs/{id}/logs`, `POST /v1/runs/{id}/cancel`, `POST /v1/runs` (ad hoc) | `jobs:read`, `runs:invoke`, `runs:adhoc` |
| Async callbacks | `POST /v1/runs/{id}/heartbeat`, `POST /v1/runs/{id}/complete` | run token |
| Clients | `GET/POST /v1/clients`, `PATCH/DELETE /v1/clients/{id}`, `POST /v1/clients/{id}/rotate` | `admin` |
| Other | `GET /healthz`, `GET /v1/status`, `GET /v1/secrets` | none / bearer / `jobs:write` |

**Client limits:** each client is limited to its own `callback_hosts` and `secret_ref_prefixes`, and an admin-scoped client sees every client's jobs and runs. **Write requests:** writes accept an `Idempotency-Key` header.

Minimal job:

```json
{
  "name": "publish-tick",
  "trigger": { "type": "cron", "cron": "*/5 * * * *", "timezone": "Asia/Kolkata" },
  "target": { "type": "http", "http": { "method": "POST", "url": "http://reel-drop:8080/internal/tick",
              "mode": "sync", "auth": { "bearer_ref": "ReelDrop/TICK_TOKEN" } } },
  "policy": { "max_late_sec": 120, "overlap": "skip" }
}
```

Secrets are vault references (`Folder/KEY`), resolved at fire time from the secrets manager and never stored here.

## Security model

- **Binding:** the API is published only on `127.0.0.1` and fronted by the host nginx. `X-Forwarded-For` is honoured only from `TRUSTED_PROXIES`.
- **Callbacks:**
  - They go through an SSRF-guarded dialer. Loopback, link-local (including cloud metadata) and special-use ranges are always refused.
  - Private addresses are allowed only for single-label Docker service names such as `reel-drop`. A dotted name or IP literal that resolves to one is refused, which also defeats DNS rebinding.
  - The runner's own hosts and the vault are denied.
  - A job with auth must use `https` unless its host is a Docker service name.
  - Callback hosts are re-checked at fire time.
- **Tokens and secrets:**
  - Tokens are compared as digests.
  - Run tokens are HMAC-signed with `RUN_TOKEN_KEY`.
  - Sealed env values are encrypted with `ENCRYPTION_KEY`.
  - Logs are redacted.
- **Container:** non-root (uid 10001), read-only root filesystem, `cap_drop: ALL`, `no-new-privileges`, memory and pids limits. The dashboard is served under a strict CSP.

## Configuration

Real values come from the secrets manager under the `PIPELINE_RUNNER_` prefix; `vps-deploy` writes `.env` at deploy time. `.env.example` lists everything.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `API_TOKEN` | yes | | admin bearer token (32+ chars) |
| `ENCRYPTION_KEY`, `RUN_TOKEN_KEY` | yes | | 64 hex chars each, and different from each other |
| `TRUSTED_PROXIES` | yes | | proxy address as the container sees it, or `none` |
| `SECRET_MANAGER_URL`, `SECRET_MANAGER_TOKEN` | yes | | vault read access |
| `NTFY_BASE`, `NTFY_TOPIC_OK`, `NTFY_TOPIC_FAIL`, `NTFY_TOKEN` | | `https://ntfy.sh` | pages |
| `HEARTBEAT_URL` | | | Uptime Kuma push URL, pinged every 60 s while the engine is healthy |
| `MAX_HTTP_INFLIGHT` | | 64 | global HTTP callback slots |
| `MAX_HTTP_INFLIGHT_PER_CLIENT` | | 16 | per-client share of those slots (0 = no cap) |
| `RUN_RETENTION_DAYS` | | 60 | run history kept; sub-minute jobs keep 7 days |

## Operations

- **Health:** `GET /healthz` returns 503 if the engine loop stalls for more than 60 s. The compose healthcheck runs `plr healthcheck`, and autoheal restarts the container.
- **Metrics:** Prometheus on `:9464`, internal networks only. Key series:
  - `plr_fire_delay_seconds`, `plr_runs_total{status}` and `plr_pending_runs`;
  - `plr_queue_depth`, `plr_inflight` and `plr_coalesced_occurrences_total`. `plr_queue_depth{target="http"}` also counts runs waiting for their client's share; if `plr_inflight` is below `MAX_HTTP_INFLIGHT` at the same time, a client is at its cap.
- **Shutdown:** in-flight callbacks get a 20 s drain. Boot recovery then re-dispatches `at_least_once` runs inside their window and interrupts the rest.
- **Capacity:** a 1-CPU, 512 MB container sustained 800 req/s of mixed API load with 100 every-second cron jobs in load tests (2026-10-14). Expected production load is about 5 req/s.

## Development

```sh
go build ./...
golangci-lint run ./... --build-tags bench
go test -race ./...
DATA_DIR=$PWD/data go run ./cmd/plr serve   # also needs the required variables above
```

The in-process load hold runs behind the `bench` tag: `go test -tags bench -run TestLoadHold ./cmd/plr`.

## Deploy

Deploys are manual through the **deploy** GitHub Actions workflow (`workflow_dispatch`), which deploys `main`:

1. `git pull` on the VPS, then `vps-deploy`. The image is tagged `pipeline-runner:<short-sha>`, and the five newest tags are kept.
2. **Rollback:** run the same workflow with `rollback_sha`. It re-tags a kept image, with no rebuild.
   - A binary older than the database schema refuses to start. In that case, roll forward.

The data directory (`HOST_DATA_DIR`, default `/root/pipeline-runner-data`) is created and chowned to uid 10001 by the workflow.
