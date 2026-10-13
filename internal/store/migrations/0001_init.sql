CREATE TABLE clients (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE CHECK (name GLOB '[a-z0-9]*' AND length(name) BETWEEN 1 AND 32),
  token_hash BLOB NOT NULL UNIQUE,
  prev_token_hash BLOB,
  prev_valid_until INTEGER,
  scopes TEXT NOT NULL,
  callback_hosts TEXT NOT NULL,
  secret_ref_prefixes TEXT NOT NULL,
  container_allowed INTEGER NOT NULL DEFAULT 0,
  root_allowed INTEGER NOT NULL DEFAULT 0,
  repo_prefixes TEXT NOT NULL DEFAULT '[]',
  ntfy_topic TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);

CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  name TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active','disabled','done')),
  trigger_json TEXT NOT NULL,
  target_type TEXT NOT NULL CHECK (target_type IN ('http','container','inline')),
  target_json TEXT NOT NULL,
  policy_json TEXT NOT NULL,
  concurrency_key TEXT,
  rev INTEGER NOT NULL DEFAULT 1,
  last_nominal_for INTEGER,
  consecutive_skips INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  UNIQUE (client_id, name)
);

-- Snapshots live once per revision, not once per run (should-fix 12).
-- Container env inside spec_json is ciphertext (blocking 4).
CREATE TABLE job_revisions (
  job_id TEXT NOT NULL,
  rev INTEGER NOT NULL,
  spec_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (job_id, rev)
);

CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  job_name TEXT NOT NULL,
  client_id TEXT NOT NULL,
  trigger TEXT NOT NULL CHECK (trigger IN ('schedule','invoke','retry','defer','adhoc')),
  attempt INTEGER NOT NULL DEFAULT 1,
  parent_run_id TEXT,
  chain_root_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','running','waiting','success','failed',
           'timeout','missed','skipped','interrupted','cancelled','deferred')),
  nominal_for INTEGER,
  scheduled_for INTEGER NOT NULL,
  origin_for INTEGER NOT NULL,
  context_for INTEGER NOT NULL,
  lateness_ref INTEGER NOT NULL,
  rev INTEGER,
  adhoc_spec_json TEXT,
  payload BLOB,
  started_at INTEGER, finished_at INTEGER,
  late_ms INTEGER,
  lease_expires_at INTEGER,
  deadline_at INTEGER,
  lease_grace_used INTEGER NOT NULL DEFAULT 0,
  early_result TEXT,
  delivery_attempted INTEGER NOT NULL DEFAULT 0,
  http_status INTEGER, exit_code INTEGER,
  output BLOB, output_truncated INTEGER NOT NULL DEFAULT 0, output_error TEXT,
  error TEXT,
  container_name TEXT,
  created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX runs_schedule_once ON runs(job_id, nominal_for) WHERE trigger = 'schedule' AND status <> 'cancelled' AND job_id IS NOT NULL;
CREATE UNIQUE INDEX runs_one_pending ON runs(job_id) WHERE trigger = 'schedule' AND status = 'pending' AND job_id IS NOT NULL;
CREATE INDEX runs_due ON runs(status, scheduled_for);
CREATE INDEX runs_by_job ON runs(job_id, id DESC);
CREATE INDEX runs_by_client ON runs(client_id, id DESC);
CREATE INDEX runs_chain ON runs(chain_root_id);

CREATE TABLE idempotency (
  client_id TEXT NOT NULL,
  route TEXT NOT NULL,
  key TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 128),
  body_hash BLOB NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('in_progress','done')),
  status INTEGER, response BLOB,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (client_id, route, key)
);
