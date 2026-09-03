# Using the pipeline runner

Everything here assumes the service is live at
`https://pipeline-runner.vps.sarvinshrivastava.space`.

---

## 1. Get your API token

Every `/api/*` call needs it. It lives in the secrets manager, not in this repo:

```bash
export PLR_TOKEN=$(curl -s https://secrets.sarvinshrivastava.space/api/secrets/Root/PIPELINE_RUNNER_API_TOKEN \
  -H "Authorization: Bearer $(grep SECRET_MANAGER_ADMIN_TOKEN /root/secrets-manager/.env | cut -d= -f2)" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["value"])')
```

For the dashboard, open the URL above and paste the same token once — it is kept
in that browser's `localStorage`.

> This token is effectively a host-root credential, because the service holds the
> Docker socket. Treat it like a root password: don't paste it into shared
> terminals, chat, or CI logs.

---

## 2. Subscribe to the alerts

Failures are pushed over [ntfy](https://ntfy.sh). Two topics:

| Topic | Carries |
|---|---|
| `plr-fail-f0e313269c917305` | failures, timeouts, missed runs, late runs, interrupted runs |
| `plr-ok-24e4155717160698` | successes — only for pipelines with `notify_on_success` |

**Subscribe on your phone:** install the ntfy app (iOS / Android) → *Add
subscription* → leave the server as `ntfy.sh` → paste the topic name.

**Subscribe in a browser:** open `https://ntfy.sh/plr-fail-f0e313269c917305`.

**Check from the terminal:**

```bash
curl -s "https://ntfy.sh/plr-fail-f0e313269c917305/json?poll=1"
```

You almost certainly want the **fail** topic. Subscribe to the ok topic only if
you like a heartbeat.

> These are public ntfy.sh topics. The names are long and random precisely
> because anyone who learns a topic name can read its messages or post fake
> ones — so don't paste them into public places. To lock them down properly,
> create an ntfy account with reserved topics and add the token to the vault as
> `PIPELINE_RUNNER_NTFY_TOKEN`; the service sends it automatically when present.

---

## 3. Add a scheduled pipeline

A pipeline is: **a git repo + a container image + a command + a cron**. Your
script writes files into the working directory; whatever changed gets committed
and pushed to a branch.

### The minimum

```bash
curl -X POST https://pipeline-runner.vps.sarvinshrivastava.space/api/pipelines \
  -H "Authorization: Bearer $PLR_TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "name": "rates-daily",
    "repo_url": "https://github.com/sarvinshrivastava/my-data-repo.git",
    "image": "python:3.12-slim",
    "command": "pip install -q requests && python fetch.py",
    "cron": "17 6 * * *",
    "timezone": "Asia/Kolkata"
  }'
```

That's it. At 06:17 IST every day the runner clones the repo, runs the command
in a throwaway `python:3.12-slim` container, commits whatever changed, and
pushes it back to `main`.

Or use the dashboard: **New pipeline**, fill the form, save.

### Writing the script

Your script runs with the repo checked out as the working directory. So:

```python
# fetch.py
import json, pathlib, requests

data = requests.get("https://api.example.com/rates").json()
pathlib.Path("out").mkdir(exist_ok=True)
pathlib.Path("out/rates.json").write_text(json.dumps(data, indent=2))
```

Then `git pull` your data repo from anywhere to get the results.

Two things worth knowing:

- **The workspace is wiped clean before every run** (`git clean -fdx`). A file
  left behind by a previous run can never silently change this run's behaviour.
  Install dependencies as part of your command.
- **No file changes is a success, not a failure.** A pipeline whose upstream
  data hasn't moved just commits nothing.

---

## 4. Cron syntax

Standard five fields: `minute hour day-of-month month day-of-week`.

| You want | `cron` | `timezone` |
|---|---|---|
| Every 15 minutes | `*/15 * * * *` | — |
| Hourly, on the hour | `0 * * * *` | — |
| Daily at 06:17 IST | `17 6 * * *` | `Asia/Kolkata` |
| Weekdays at 09:00 IST | `0 9 * * 1-5` | `Asia/Kolkata` |
| Mondays at 03:30 UTC | `30 3 * * 1` | `UTC` |
| First of the month, 00:05 | `5 0 1 * *` | `Asia/Kolkata` |

`timezone` is any IANA zone and defaults to `UTC`. **Set it explicitly** if you
think in local time — DST is handled for you.

Pick an odd minute (`17`, `23`) over `0` or `30`. Nothing here contends the way
GitHub's shared queue did, but it keeps your own jobs from bunching up.

The API rejects an invalid expression at save time, so a typo can't silently
mean "never runs".

---

## 5. Credentials: reference the vault, don't paste secrets

If your script needs an API key, put the key in the secrets manager and have the
pipeline **reference** it:

```json
"secret_refs": { "WEATHER_API_KEY": "Root/MY_WEATHER_KEY" }
```

The value is fetched fresh on every run and injected as `$WEATHER_API_KEY`. So
rotating the secret in the vault takes effect on the next run — no pipeline edit,
no redeploy.

A bare `"MY_KEY"` is shorthand for `"Root/MY_KEY"`. List what's available:

```bash
curl -s .../api/secrets -H "Authorization: Bearer $PLR_TOKEN"   # names only
```

References are checked against the live vault when you save. A bad one is
rejected immediately, naming the exact key — not at 6am three weeks later.

**Private repos** need a GitHub PAT (fine-grained, Contents read/write). Put it
in the vault and reference it as `GIT_TOKEN`:

```json
"secret_refs": { "GIT_TOKEN": "Root/PIPELINE_RUNNER_GITHUB_TOKEN" }
```

`GIT_TOKEN` is special-cased: it authenticates the clone and push, and is
deliberately **stripped before your container starts**, so pipeline code can
never read it.

You can also inline values with `"env": {"KEY": "value"}` — encrypted at rest —
but references are better for anything you might rotate.

---

## 6. Watch it run

```bash
BASE=https://pipeline-runner.vps.sarvinshrivastava.space
AUTH="Authorization: Bearer $PLR_TOKEN"

curl -s "$BASE/api/pipelines" -H "$AUTH"                 # all pipelines + next run
curl -s "$BASE/api/runs?limit=20" -H "$AUTH"             # recent runs
curl -s "$BASE/api/runs/<run-id>/logs" -H "$AUTH"        # full log of one run
curl -s -X POST "$BASE/api/pipelines/<name>/trigger" -H "$AUTH"   # run now
curl -s -X POST "$BASE/api/pipelines/<name>/disable" -H "$AUTH"   # pause it
curl -s "$BASE/api/pipelines/<name>/validate" -H "$AUTH"          # re-check secret refs
```

`<name>` works anywhere an id does.

Always trigger a new pipeline manually once before trusting the schedule.

### Run statuses

| Status | Means |
|---|---|
| `success` | Exit 0. Results committed (or nothing to commit). |
| `failed` | Non-zero exit, or the commit/push failed. |
| `timeout` | Exceeded `timeout_sec`; container killed. |
| `missed` | **Not run.** Too late, or overlapped a still-running run. |
| `interrupted` | The runner restarted mid-run. |
| `pending` / `running` | Queued / in flight. |

There is no `late` status on purpose — a late run still succeeded or failed, so
lateness is recorded separately on `late_sec` and alerted on.

---

## 7. Tuning knobs

All optional; defaults are sensible.

| Field | Default | Set it when |
|---|---|---|
| `timeout_sec` | 900 | Your job legitimately takes longer than 15 min |
| `max_late_sec` | 600 | Past this the run is **abandoned** and paged rather than run late |
| `late_warn_sec` | 60 | Alert if it started later than this but still ran |
| `memory_mb` / `cpus` | 512 / 1.0 | Heavy job. Swap is disabled, so memory is a hard ceiling |
| `overlap_policy` | `skip` | `queue` if runs must never be dropped |
| `idempotent` | `true` | `false` if re-running could double-charge, double-send, etc. |
| `commit_results` | `true` | `false` for a pipeline that produces no files |
| `notify_on_success` | `false` | `true` for a heartbeat on the ok topic |
| `branch` / `output_branch` | `main` / same | Reading and writing different branches |

**`max_late_sec` is the important one.** Data that is useless if it arrives late
(a market snapshot) wants a small value — better a loud miss than stale data
committed as if it were fresh. A job that just needs to happen eventually can
take a large one.

---

## 8. When something breaks

**A failure ntfy arrives** → open the dashboard, find the run, read the log.
Logs keep the head and the tail, so both the setup and the fatal error survive
truncation.

**"Missed" runs** → the runner was down, or the previous run was still going.
Check `/healthz`; if the pipeline routinely overruns its schedule, lengthen the
interval or set `overlap_policy: "queue"`.

**A secret reference broke** → `GET /api/pipelines/<name>/validate`, or the
dashboard's **Check refs** button. Usually the vault key was renamed or deleted.

**Nothing is running at all** → check the service itself:

```bash
curl -s https://pipeline-runner.vps.sarvinshrivastava.space/healthz
docker logs pipeline-runner --tail 50
```

`/healthz` returns 503 once the tick loop has stalled for 3 minutes. A stalled
scheduler cannot alert about its own death, which is exactly why that endpoint
exists and why it is worth an Uptime Kuma monitor.

**Values look wrong in a log** → secret values are redacted to `***`. If you see
a real credential in a log, that's a bug worth reporting.
