package api

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// stepClock is a settable clock for the API's Deps.Now and the limiter.
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *stepClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// waitRun polls GET /v1/runs/{id} until the run reaches status.
func (e *env) waitRun(tok, runID string, status model.Status) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r := e.do("GET", "/v1/runs/"+runID, tok, nil)
		if r.status == 200 && r.json(e.t)["status"] == string(status) {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("run %s never reached %s: %d %s", runID, status, r.status, r.body)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func items(t *testing.T, r resp) []any {
	t.Helper()
	if r.status != 200 {
		t.Fatalf("list: %d %s", r.status, r.body)
	}
	return r.json(t)["items"].([]any)
}

func ids(t *testing.T, r resp) []string {
	t.Helper()
	var out []string
	for _, it := range items(t, r) {
		out = append(out, it.(map[string]any)["id"].(string))
	}
	return out
}

func fieldOf(r resp) string {
	var m struct {
		Error struct{ Field string } `json:"error"`
	}
	_ = json.Unmarshal(r.body, &m)
	return m.Error.Field
}

// ---------------------------------------------------------------- 1. cross-tenant

func TestCrossTenantJobAndRunAccess(t *testing.T) {
	e := newEnv(t)
	_, tokA := e.client("a", all...)
	_, tokB := e.client("b", all...)
	_, tokOps := e.client("ops", "admin")

	jobA := e.do("POST", "/v1/jobs", tokA, fnJob("fa")).json(t)["id"].(string)
	jobB := e.do("POST", "/v1/jobs", tokB, fnJob("fb")).json(t)["id"].(string)
	runA := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil).json(t)["id"].(string)
	runB := e.do("POST", "/v1/jobs/"+jobB+"/invoke", tokB, nil).json(t)["id"].(string)
	e.waitRun(tokA, runA, model.StatusSuccess)
	e.waitRun(tokB, runB, model.StatusSuccess)

	// The convention is 404 (no existence leak), never 403.
	for _, c := range []struct {
		m, p string
		body any
	}{
		{"GET", "/v1/jobs/" + jobA, nil},
		{"PATCH", "/v1/jobs/" + jobA, map[string]any{"name": "stolen"}},
		{"DELETE", "/v1/jobs/" + jobA, nil},
		{"POST", "/v1/jobs/" + jobA + "/invoke", nil},
		{"POST", "/v1/jobs/" + jobA + "/enable", nil},
		{"POST", "/v1/jobs/" + jobA + "/disable", nil},
		{"POST", "/v1/jobs/" + jobA + "/validate", nil},
		{"GET", "/v1/runs/" + runA, nil},
		{"GET", "/v1/runs/" + runA + "/logs", nil},
		{"POST", "/v1/runs/" + runA + "/cancel", nil},
	} {
		r := e.do(c.m, c.p, tokB, c.body)
		if r.status != 404 || r.code() != "not_found" {
			t.Errorf("%s %s by client b: %d %s", c.m, c.p, r.status, r.body)
		}
	}
	// Nothing b tried changed a's job.
	got := e.do("GET", "/v1/jobs/"+jobA, tokA, nil)
	if got.status != 200 || got.json(t)["name"] != "fa" || got.json(t)["state"] != model.JobActive {
		t.Fatalf("a's job after b's attempts: %d %s", got.status, got.body)
	}
	// An unknown id answers exactly like another client's id.
	if r := e.do("GET", "/v1/jobs/00000000-0000-0000-0000-000000000000", tokB, nil); r.status != 404 || r.code() != "not_found" {
		t.Fatalf("unknown job: %d %s", r.status, r.body)
	}

	// Lists return only the caller's own rows, even when filtered by a's job.
	if got := ids(t, e.do("GET", "/v1/jobs", tokB, nil)); len(got) != 1 || got[0] != jobB {
		t.Fatalf("b's jobs = %v", got)
	}
	if got := ids(t, e.do("GET", "/v1/runs", tokB, nil)); len(got) != 1 || got[0] != runB {
		t.Fatalf("b's runs = %v", got)
	}
	if got := ids(t, e.do("GET", "/v1/runs?job="+jobA, tokB, nil)); len(got) != 0 {
		t.Fatalf("b filtering by a's job sees %v", got)
	}

	// An admin-scoped client and the bootstrap token see every client's rows.
	for name, tok := range map[string]string{"admin client": tokOps, "bootstrap": adminToken} {
		if n := len(items(t, e.do("GET", "/v1/jobs", tok, nil))); n != 2 {
			t.Errorf("%s sees %d jobs", name, n)
		}
		if n := len(items(t, e.do("GET", "/v1/runs", tok, nil))); n != 2 {
			t.Errorf("%s sees %d runs", name, n)
		}
		if r := e.do("GET", "/v1/runs/"+runA, tok, nil); r.status != 200 {
			t.Errorf("%s get a's run: %d", name, r.status)
		}
		if r := e.do("GET", "/v1/runs/"+runA+"/logs", tok, nil); r.status != 200 {
			t.Errorf("%s a's run logs: %d", name, r.status)
		}
	}
	if got := ids(t, e.do("GET", "/v1/jobs?client=a", adminToken, nil)); len(got) != 1 || got[0] != jobA {
		t.Fatalf("admin ?client=a = %v", got)
	}
	if r := e.do("GET", "/v1/jobs?client=nobody", adminToken, nil); r.status != 400 {
		t.Fatalf("admin ?client=unknown: %d", r.status)
	}
}

// ---------------------------------------------------------------- 2. PATCH

func TestPatchJobValidationAndPriority(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	id := e.do("POST", "/v1/jobs", tok, fnJob("f")).json(t)["id"].(string)
	e.do("POST", "/v1/jobs", tok, fnJob("taken"))

	policy := func(r resp) map[string]any { return r.json(t)["policy"].(map[string]any) }

	r := e.do("PATCH", "/v1/jobs/"+id, tok, map[string]any{"policy": map[string]any{"priority": "high"}})
	if r.status != 200 || policy(r)["priority"] != "high" {
		t.Fatalf("patch priority: %d %s", r.status, r.body)
	}
	// A PATCH of the target alone keeps the stored priority.
	tgt := fnJob("f")["target"].(map[string]any)
	tgt["http"].(map[string]any)["url"] = "http://svc/other"
	r = e.do("PATCH", "/v1/jobs/"+id, tok, map[string]any{"target": tgt})
	if r.status != 200 || policy(r)["priority"] != "high" {
		t.Fatalf("patch target dropped priority: %d %s", r.status, r.body)
	}
	if got := e.do("GET", "/v1/jobs/"+id, tok, nil); policy(got)["priority"] != "high" ||
		!strings.Contains(string(got.json(t)["target"].(map[string]any)["http"].(map[string]any)["url"].(string)), "/other") {
		t.Fatalf("stored job: %s", got.body)
	}
	// A PATCH of the name alone keeps it too.
	if r := e.do("PATCH", "/v1/jobs/"+id, tok, map[string]any{"name": "renamed"}); r.status != 200 || policy(r)["priority"] != "high" {
		t.Fatalf("rename: %d %s", r.status, r.body)
	}

	for _, c := range []struct {
		name        string
		body        any
		status      int
		code, field string
	}{
		{"bad priority", map[string]any{"policy": map[string]any{"priority": "urgent"}}, 400, "invalid", "policy.priority"},
		{"bad name", map[string]any{"name": "Bad Name"}, 400, "invalid", "name"},
		{"name taken", map[string]any{"name": "taken"}, 409, "name_taken", ""},
		{"unknown field", `{"nme":"x"}`, 400, "invalid_json", ""},
		{"host outside owner", map[string]any{"target": map[string]any{"type": "http",
			"http": map[string]any{"url": "http://elsewhere/x", "auth": map[string]any{"bearer_ref": "Svc/TOKEN"}}}}, 400, "host_not_allowed", ""},
		{"ref outside owner", map[string]any{"target": map[string]any{"type": "http",
			"http": map[string]any{"url": "http://svc/x", "auth": map[string]any{"bearer_ref": "Other/TOKEN"}}}}, 400, "secret_ref_not_permitted", ""},
	} {
		r := e.do("PATCH", "/v1/jobs/"+id, tok, c.body)
		if r.status != c.status || r.code() != c.code || (c.field != "" && fieldOf(r) != c.field) {
			t.Errorf("%s: %d %s", c.name, r.status, r.body)
		}
	}
	// None of the rejected patches changed the job.
	got := e.do("GET", "/v1/jobs/"+id, tok, nil).json(t)
	if got["name"] != "renamed" || got["policy"].(map[string]any)["priority"] != "high" {
		t.Fatalf("job after rejected patches: %v", got)
	}

	// The bootstrap admin may patch any job; the owner's limits still apply.
	if r := e.do("PATCH", "/v1/jobs/"+id, adminToken, map[string]any{"policy": map[string]any{"priority": "low"}}); r.status != 200 || policy(r)["priority"] != "low" {
		t.Fatalf("admin patch: %d %s", r.status, r.body)
	}
	if r := e.do("PATCH", "/v1/jobs/"+id, adminToken, map[string]any{"target": map[string]any{"type": "http",
		"http": map[string]any{"url": "http://elsewhere/x", "auth": map[string]any{"bearer_ref": "Svc/TOKEN"}}}}); r.status != 400 || r.code() != "host_not_allowed" {
		t.Fatalf("admin patch outside owner's hosts: %d %s", r.status, r.body)
	}
	if r := e.do("PATCH", "/v1/jobs/missing", tok, map[string]any{"name": "x"}); r.status != 404 {
		t.Fatalf("patch unknown: %d", r.status)
	}
}

// ---------------------------------------------------------------- 3. pagination

func walk(t *testing.T, e *env, tok, path string, limit int) (pages [][]string) {
	t.Helper()
	cursor := ""
	for i := 0; i < 20; i++ {
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		url := path + sep + "limit=" + itoa(limit)
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		r := e.do("GET", url, tok, nil)
		pages = append(pages, ids(t, r))
		next, _ := r.json(t)["next_cursor"].(string)
		if next == "" {
			return pages
		}
		cursor = next
	}
	t.Fatal("pagination did not end")
	return nil
}

func itoa(n int) string { return strconv.Itoa(n) }

func checkPages(t *testing.T, what string, pages [][]string, sizes ...int) {
	t.Helper()
	if len(pages) != len(sizes) {
		t.Fatalf("%s: %d pages %v, want sizes %v", what, len(pages), pages, sizes)
	}
	seen := map[string]bool{}
	prev := ""
	for i, p := range pages {
		if len(p) != sizes[i] {
			t.Fatalf("%s: page %d has %d items, want %d", what, i, len(p), sizes[i])
		}
		for _, id := range p {
			if seen[id] {
				t.Fatalf("%s: %s on two pages", what, id)
			}
			if prev != "" && id >= prev {
				t.Fatalf("%s: not newest first: %s after %s", what, id, prev)
			}
			seen[id], prev = true, id
		}
	}
}

func TestPaginationJobsAndRuns(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	var jobID string
	for i := 0; i < 5; i++ {
		jobID = e.do("POST", "/v1/jobs", tok, fnJob("f"+itoa(i))).json(t)["id"].(string)
	}
	for i := 0; i < 5; i++ {
		run := e.do("POST", "/v1/jobs/"+jobID+"/invoke", tok, nil).json(t)["id"].(string)
		e.waitRun(tok, run, model.StatusSuccess)
	}
	for _, path := range []string{"/v1/jobs", "/v1/runs", "/v1/runs?job=" + jobID, "/v1/runs?status=success"} {
		checkPages(t, path+" limit 2", walk(t, e, tok, path, 2), 2, 2, 1)
		// Exactly limit rows: the extra row is absent, so no next_cursor.
		checkPages(t, path+" limit 5", walk(t, e, tok, path, 5), 5)
		checkPages(t, path+" limit 100", walk(t, e, tok, path, 100), 5)
		for _, bad := range []string{"0", "-1", "101", "abc", "1.5"} {
			sep := "?"
			if strings.Contains(path, "?") {
				sep = "&"
			}
			if r := e.do("GET", path+sep+"limit="+bad, tok, nil); r.status != 400 || r.code() != "invalid" {
				t.Errorf("%s limit=%s: %d %s", path, bad, r.status, r.body)
			}
		}
	}
	// Default limit is 50, and a cursor past the end is an empty page.
	if n := len(items(t, e.do("GET", "/v1/jobs", tok, nil))); n != 5 {
		t.Fatalf("default page: %d", n)
	}
	last := walk(t, e, tok, "/v1/jobs", 5)[0][4]
	r := e.do("GET", "/v1/jobs?cursor="+last, tok, nil)
	if len(items(t, r)) != 0 || r.json(t)["next_cursor"] != nil {
		t.Fatalf("past the end: %s", r.body)
	}
	if r := e.do("GET", "/v1/runs?status=bogus", tok, nil); r.status != 400 {
		t.Fatalf("bad status filter: %d", r.status)
	}
}

// ---------------------------------------------------------------- 4. env redaction

func TestSealedEnvNeverReturned(t *testing.T) {
	e := newEnvWith(t, func(d *engine.Deps) { d.ContainersEnabled = true }, nil)
	id, tok := e.client("a", all...)
	if r := e.do("PATCH", "/v1/clients/"+id, adminToken, map[string]any{"container_allowed": true}); r.status != 200 {
		t.Fatalf("allow containers: %d %s", r.status, r.body)
	}
	const secret = "s3cr3t-env-value-xyz"
	job := map[string]any{"name": "box", "trigger": map[string]any{"type": "invoke"},
		"target": map[string]any{"type": "inline", "inline": map[string]any{"image": "alpine:3", "script": "env",
			"env": map[string]any{"API_KEY": secret, "OTHER": "plain-value-123"}}}}
	created := e.do("POST", "/v1/jobs", tok, job)
	if created.status != 201 {
		t.Fatalf("create inline: %d %s", created.status, created.body)
	}
	jobID := created.json(t)["id"].(string)

	check := func(what string, r resp) {
		t.Helper()
		body := string(r.body)
		if r.status/100 != 2 {
			t.Fatalf("%s: %d %s", what, r.status, body)
		}
		for _, leak := range []string{secret, "plain-value-123", `"sealed"`, `"v1.`, `"env":`} {
			if strings.Contains(body, leak) {
				t.Fatalf("%s leaks %q: %s", what, leak, body)
			}
		}
		if !strings.Contains(body, `"env_keys":["API_KEY","OTHER"]`) {
			t.Fatalf("%s lacks env_keys: %s", what, body)
		}
	}
	check("create", created)
	check("get", e.do("GET", "/v1/jobs/"+jobID, tok, nil))
	check("list", e.do("GET", "/v1/jobs", tok, nil))
	check("admin list", e.do("GET", "/v1/jobs", adminToken, nil))
	check("disable", e.do("POST", "/v1/jobs/"+jobID+"/disable", tok, nil))
	// A PATCH without env keeps the sealed values; the response still hides them.
	check("patch", e.do("PATCH", "/v1/jobs/"+jobID, tok, map[string]any{"target": map[string]any{"type": "inline",
		"inline": map[string]any{"image": "alpine:3.20", "script": "env"}}}))
	// An idempotent replay serves the stored response, which must be redacted too.
	j2 := map[string]any{"name": "box2", "trigger": job["trigger"], "target": job["target"]}
	e.do("POST", "/v1/jobs", tok, j2, "Idempotency-Key", "env-k")
	replay := e.do("POST", "/v1/jobs", tok, j2, "Idempotency-Key", "env-k")
	if replay.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("not a replay: %s", replay.body)
	}
	check("replay", replay)
	// A container target hides its env the same way.
	ctr := map[string]any{"name": "ctr", "trigger": map[string]any{"type": "invoke"},
		"target": map[string]any{"type": "container", "container": map[string]any{"image": "alpine:3", "command": "true",
			"env": map[string]any{"API_KEY": secret, "OTHER": "plain-value-123"}}}}
	check("container create", e.do("POST", "/v1/jobs", tok, ctr))
}

// ---------------------------------------------------------------- 5. limiter expiry

func TestLimiterBlocksThenExpires(t *testing.T) {
	c := &stepClock{t: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)}
	l := newLimiter(3, time.Minute, 10, c.now)
	for i := 0; i < 2; i++ {
		l.fail("1.2.3.4")
	}
	if l.blocked("1.2.3.4") {
		t.Fatal("blocked below max")
	}
	l.fail("1.2.3.4")
	if !l.blocked("1.2.3.4") || l.blocked("5.6.7.8") {
		t.Fatal("max failures must block that IP only")
	}
	c.add(time.Minute) // the window is inclusive
	if !l.blocked("1.2.3.4") {
		t.Fatal("unblocked at exactly the window")
	}
	c.add(time.Second)
	if l.blocked("1.2.3.4") {
		t.Fatal("still blocked after the window")
	}
	// The count restarts: one new failure does not re-block.
	l.fail("1.2.3.4")
	if l.blocked("1.2.3.4") {
		t.Fatal("old failures counted after expiry")
	}
	// Failures spread over more than a window never accumulate to a block.
	for i := 0; i < 6; i++ {
		c.add(31 * time.Second)
		l.fail("9.9.9.9")
	}
	if l.blocked("9.9.9.9") {
		t.Fatal("failures across windows accumulated")
	}
}

func TestLimiterExpiryThroughAPI(t *testing.T) {
	c := &stepClock{t: time.Now()}
	e := newEnvWith(t, nil, func(d *Deps) { d.Now = c.now })
	for i := 0; i < 10; i++ {
		e.do("GET", "/v1/jobs", "wrong", nil)
	}
	if r := e.do("GET", "/v1/jobs", adminToken, nil); r.status != 429 || r.code() != "rate_limited" {
		t.Fatalf("blocked: %d %s", r.status, r.body)
	}
	// The run-token routes share the lockout.
	if r := e.do("POST", "/v1/runs/x/heartbeat", "", nil, "X-PLR-Run-Token", "t"); r.status != 429 {
		t.Fatalf("run-token route while blocked: %d", r.status)
	}
	c.add(5*time.Minute + time.Second)
	if r := e.do("GET", "/v1/jobs", adminToken, nil); r.status != 200 {
		t.Fatalf("after the window: %d %s", r.status, r.body)
	}
}

// ---------------------------------------------------------------- 6. idempotency scope

func TestIdempotencyKeyScopedPerClient(t *testing.T) {
	e := newEnv(t)
	_, tokA := e.client("a", all...)
	_, tokB := e.client("b", all...)
	ra := e.do("POST", "/v1/jobs", tokA, fnJob("f"), "Idempotency-Key", "shared")
	rb := e.do("POST", "/v1/jobs", tokB, fnJob("f"), "Idempotency-Key", "shared")
	if ra.status != 201 || rb.status != 201 || rb.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("same key, two clients: %d %d %s", ra.status, rb.status, rb.body)
	}
	if ra.json(t)["id"] == rb.json(t)["id"] || rb.json(t)["client_id"] == ra.json(t)["client_id"] {
		t.Fatalf("b got a's response: %s / %s", ra.body, rb.body)
	}
	// A different body under the same key is a conflict only within one client.
	if r := e.do("POST", "/v1/jobs", tokB, fnJob("g"), "Idempotency-Key", "shared"); r.status != 409 || r.code() != "idempotency_key_reused" {
		t.Fatalf("b reuse: %d %s", r.status, r.body)
	}
	// Each client's replay returns its own job.
	if r := e.do("POST", "/v1/jobs", tokA, fnJob("f"), "Idempotency-Key", "shared"); r.json(t)["id"] != ra.json(t)["id"] || r.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("a replay: %s", r.body)
	}
	// The same key on another route is independent too.
	jobA := ra.json(t)["id"].(string)
	if r := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil, "Idempotency-Key", "shared"); r.status != 202 || r.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("other route: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/jobs", tokA, fnJob("h"), "Idempotency-Key", strings.Repeat("k", 129)); r.status != 400 {
		t.Fatalf("long key: %d", r.status)
	}
}

// ---------------------------------------------------------------- 13 + 6. pending limit

func TestPendingLimitIs429AndNotCached(t *testing.T) {
	e := newEnvWith(t, func(d *engine.Deps) { d.MaxPendingPerClient = 2 }, nil)
	_, tokA := e.client("a", all...)
	_, tokB := e.client("b", all...)
	release := make(chan struct{})
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-release:
		case <-ctx.Done():
		}
		code := 200
		return target.Result{Status: model.StatusSuccess, HTTPStatus: &code}
	})
	jobA := e.do("POST", "/v1/jobs", tokA, fnJob("f")).json(t)["id"].(string)
	jobB := e.do("POST", "/v1/jobs", tokB, fnJob("f")).json(t)["id"].(string)
	var held []string
	for i := 0; i < 2; i++ {
		r := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil)
		if r.status != 202 {
			t.Fatalf("invoke %d: %d %s", i, r.status, r.body)
		}
		held = append(held, r.json(t)["id"].(string))
	}
	r := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil, "Idempotency-Key", "retry-me")
	if r.status != 429 || r.code() != "pending_limit" {
		t.Fatalf("third invoke: %d %s", r.status, r.body)
	}
	adhoc := map[string]any{"target": fnJob("x")["target"]}
	if r := e.do("POST", "/v1/runs", tokA, adhoc); r.status != 429 || r.code() != "pending_limit" {
		t.Fatalf("adhoc at the limit: %d %s", r.status, r.body)
	}
	// The limit is per client.
	if r := e.do("POST", "/v1/jobs/"+jobB+"/invoke", tokB, nil); r.status != 202 {
		t.Fatalf("other client blocked: %d %s", r.status, r.body)
	}
	// The 429 left no claim behind: an immediate retry is another 429, not 409.
	if r := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil, "Idempotency-Key", "retry-me"); r.status != 429 || r.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("retry while full: %d %s", r.status, r.body)
	}
	close(release)
	for _, id := range held {
		e.waitRun(tokA, id, model.StatusSuccess)
	}
	r = e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil, "Idempotency-Key", "retry-me")
	if r.status != 202 || r.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("retry after capacity freed: %d %s", r.status, r.body)
	}
	// And that success is what the key now replays.
	again := e.do("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil, "Idempotency-Key", "retry-me")
	if again.status != 202 || again.header.Get("Idempotent-Replayed") != "true" || again.json(t)["id"] != r.json(t)["id"] {
		t.Fatalf("replay: %d %s", again.status, again.body)
	}
}

// ---------------------------------------------------------------- 7. validate

func checksOf(t *testing.T, r resp) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, c := range r.json(t)["checks"].([]any) {
		m := c.(map[string]any)
		out[m["name"].(string)] = m
	}
	return out
}

func TestValidateEndpoints(t *testing.T) {
	e := newEnv(t)
	idA, tok := e.client("a", all...)
	_, tokB := e.client("b", all...)
	_, tokRead := e.client("reader", "jobs:read")

	cron := fnJob("tick")
	cron["trigger"] = map[string]any{"type": "cron", "cron": "0 9 * * *", "timezone": "Asia/Kolkata"}
	r := e.do("POST", "/v1/jobs/validate", tok, cron)
	if r.status != 200 || r.json(t)["ok"] != true {
		t.Fatalf("valid: %d %s", r.status, r.body)
	}
	cs := checksOf(t, r)
	for _, name := range []string{"validation", "callback_host", "secret Svc/TOKEN", "schedule"} {
		if cs[name] == nil || cs[name]["ok"] != true {
			t.Errorf("check %s: %v", name, cs[name])
		}
	}
	if nf, _ := r.json(t)["next_fires"].([]any); len(nf) != 5 {
		t.Errorf("next_fires: %v", r.json(t)["next_fires"])
	}
	if n := len(items(t, e.do("GET", "/v1/jobs", tok, nil))); n != 0 {
		t.Fatalf("validate stored a job: %d", n)
	}

	bad := fnJob("f")
	bad["target"].(map[string]any)["http"].(map[string]any)["url"] = "http://elsewhere/x"
	r = e.do("POST", "/v1/jobs/validate", tok, bad)
	if r.status != 200 || r.json(t)["ok"] != false {
		t.Fatalf("invalid job: %d %s", r.status, r.body)
	}
	cs = checksOf(t, r)
	if len(cs) != 1 || cs["validation"]["ok"] != false || !strings.Contains(cs["validation"]["detail"].(string), "elsewhere") {
		t.Fatalf("invalid job checks: %s", r.body)
	}
	missing := fnJob("f")
	missing["target"].(map[string]any)["http"].(map[string]any)["auth"] = map[string]any{"bearer_ref": "Svc/NOPE"}
	if r := e.do("POST", "/v1/jobs/validate", tok, missing); r.json(t)["ok"] != false ||
		!strings.Contains(checksOf(t, r)["validation"]["detail"].(string), "Svc/NOPE") {
		t.Fatalf("missing ref: %s", r.body)
	}
	if r := e.do("POST", "/v1/jobs/validate", tok, `{"bogus":1}`); r.status != 400 || r.code() != "invalid_json" {
		t.Fatalf("bad json: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/jobs/validate", tokRead, cron); r.status != 403 {
		t.Fatalf("validate needs jobs:write: %d", r.status)
	}
	if r := e.do("POST", "/v1/jobs/validate", adminToken, cron); r.status != 400 || r.code() != "required" {
		t.Fatalf("admin without client: %d %s", r.status, r.body)
	}

	// Stored-job validate reports drift in the owner's settings.
	id := e.do("POST", "/v1/jobs", tok, cron).json(t)["id"].(string)
	r = e.do("POST", "/v1/jobs/"+id+"/validate", tokRead, nil)
	if r.status != 404 {
		t.Fatalf("reader of another client: %d", r.status)
	}
	r = e.do("POST", "/v1/jobs/"+id+"/validate", tok, nil)
	if r.status != 200 || r.json(t)["ok"] != true || len(r.json(t)["next_fires"].([]any)) != 5 {
		t.Fatalf("stored valid: %d %s", r.status, r.body)
	}
	e.do("PATCH", "/v1/clients/"+idA, adminToken, map[string]any{"callback_hosts": []string{"other"}, "secret_ref_prefixes": []string{"Other/"}})
	r = e.do("POST", "/v1/jobs/"+id+"/validate", tok, nil)
	cs = checksOf(t, r)
	if r.status != 200 || r.json(t)["ok"] != false || cs["callback_host"]["ok"] != false || cs["secret Svc/TOKEN"]["ok"] != false {
		t.Fatalf("stored drifted: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/jobs/"+id+"/validate", tokB, nil); r.status != 404 {
		t.Fatalf("other client: %d", r.status)
	}
	if r := e.do("POST", "/v1/jobs/missing/validate", tok, nil); r.status != 404 {
		t.Fatalf("unknown: %d", r.status)
	}
}

// ---------------------------------------------------------------- 8. clients admin

func TestClientAdminRoutes(t *testing.T) {
	c := &stepClock{t: time.Now()}
	e := newEnvWith(t, nil, func(d *Deps) { d.Now = c.now })
	idA, tokA := e.client("a", all...)
	idB, tokB := e.client("b", all...)
	_, tokOps := e.client("ops", "admin")

	// A client without the admin scope can touch no client, itself included.
	for _, target := range []string{idA, idB} {
		for _, rq := range []struct {
			m, p string
			body any
		}{
			{"PATCH", "/v1/clients/" + target, map[string]any{"callback_hosts": []string{"x"}}},
			{"DELETE", "/v1/clients/" + target, nil},
			{"POST", "/v1/clients/" + target + "/rotate", map[string]any{}},
			{"POST", "/v1/clients", map[string]any{"name": "new"}},
		} {
			if r := e.do(rq.m, rq.p, tokA, rq.body); r.status != 403 || r.code() != "forbidden" {
				t.Errorf("%s %s by non-admin: %d %s", rq.m, rq.p, r.status, r.body)
			}
		}
	}
	if e.do("GET", "/v1/jobs", tokB, nil).status != 200 {
		t.Fatal("b's token broken by refused calls")
	}

	// Admin PATCH.
	r := e.do("PATCH", "/v1/clients/"+idB, tokOps, map[string]any{"callback_hosts": []string{"Svc2"}, "ntfy_topic": "b-alerts"})
	if r.status != 200 || r.json(t)["callback_hosts"].([]any)[0] != "svc2" || r.json(t)["ntfy_topic"] != "b-alerts" {
		t.Fatalf("admin patch: %d %s", r.status, r.body)
	}
	for _, body := range []map[string]any{
		{"name": "renamed"}, {"scopes": []string{"root"}}, {"callback_hosts": []string{"secret-manager"}},
		{"callback_hosts": []string{"bad_host"}}, {"repo_prefixes": []string{"noslash"}}, {"ntfy_topic": "has space"},
	} {
		if r := e.do("PATCH", "/v1/clients/"+idB, adminToken, body); r.status != 400 {
			t.Errorf("patch %v: %d %s", body, r.status, r.body)
		}
	}
	for _, rq := range []struct {
		m, p string
		body any
	}{
		{"PATCH", "/v1/clients/missing", map[string]any{"ntfy_topic": "x"}},
		{"DELETE", "/v1/clients/missing", nil},
		{"POST", "/v1/clients/missing/rotate", map[string]any{}},
	} {
		if r := e.do(rq.m, rq.p, adminToken, rq.body); r.status != 404 {
			t.Errorf("%s %s: %d %s", rq.m, rq.p, r.status, r.body)
		}
	}

	// Rotation without grace invalidates the old token at once.
	r = e.do("POST", "/v1/clients/"+idA+"/rotate", adminToken, map[string]any{})
	newA := r.json(t)["token"].(string)
	if r.status != 200 || r.json(t)["previous_valid_until"] != nil {
		t.Fatalf("rotate: %d %s", r.status, r.body)
	}
	if e.do("GET", "/v1/jobs", tokA, nil).status != 401 || e.do("GET", "/v1/jobs", newA, nil).status != 200 {
		t.Fatal("rotation without grace must swap tokens at once")
	}
	// With grace, the old token works until the grace ends, then never again.
	r = e.do("POST", "/v1/clients/"+idA+"/rotate", tokOps, map[string]any{"grace_sec": 60})
	newerA := r.json(t)["token"].(string)
	if r.status != 200 || e.do("GET", "/v1/jobs", newA, nil).status != 200 || e.do("GET", "/v1/jobs", newerA, nil).status != 200 {
		t.Fatalf("grace rotation: %d %s", r.status, r.body)
	}
	c.add(61 * time.Second)
	if e.do("GET", "/v1/jobs", newA, nil).status != 401 || e.do("GET", "/v1/jobs", newerA, nil).status != 200 {
		t.Fatal("old token must expire with the grace")
	}
	for _, g := range []int{-1, 3601} {
		if r := e.do("POST", "/v1/clients/"+idA+"/rotate", adminToken, map[string]any{"grace_sec": g}); r.status != 400 {
			t.Errorf("grace %d: %d", g, r.status)
		}
	}

	// Delete: a client with no jobs goes, and its token with it.
	if r := e.do("DELETE", "/v1/clients/"+idB, tokOps, nil); r.status != 204 {
		t.Fatalf("delete: %d %s", r.status, r.body)
	}
	if e.do("GET", "/v1/jobs", tokB, nil).status != 401 {
		t.Fatal("deleted client's token still works")
	}
	if r := e.do("DELETE", "/v1/clients/"+idB, adminToken, nil); r.status != 404 {
		t.Fatalf("second delete: %d", r.status)
	}
}

// ---------------------------------------------------------------- 9. job push fields

func TestJobPolicyPushFieldsRoundTrip(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	job := fnJob("tick")
	job["policy"] = map[string]any{"push_url_ref": "Svc/TOKEN", "push_on_failure": false}
	r := e.do("POST", "/v1/jobs", tok, job)
	if r.status != 201 {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	id := r.json(t)["id"].(string)
	got := e.do("GET", "/v1/jobs/"+id, tok, nil)
	pol := got.json(t)["policy"].(map[string]any)
	if pol["push_url_ref"] != "Svc/TOKEN" || pol["push_on_failure"] != false {
		t.Fatalf("policy = %v", pol)
	}
	if strings.Contains(string(got.body), "/api/push/") {
		t.Fatalf("a push URL leaked: %s", got.body)
	}
	// A policy PATCH that omits the push fields keeps them.
	r = e.do("PATCH", "/v1/jobs/"+id, tok, map[string]any{"policy": map[string]any{"timeout_sec": 60}})
	if r.status != 200 {
		t.Fatalf("patch: %d %s", r.status, r.body)
	}
	pol = r.json(t)["policy"].(map[string]any)
	if pol["push_url_ref"] != "Svc/TOKEN" || pol["push_on_failure"] != false {
		t.Fatalf("patched policy = %v", pol)
	}
	// A job without the field shows the default.
	r = e.do("POST", "/v1/jobs", tok, fnJob("plain"))
	if pol := r.json(t)["policy"].(map[string]any); pol["push_on_failure"] != true || pol["push_url_ref"] != nil {
		t.Fatalf("default policy = %v", pol)
	}
	missing := fnJob("missing")
	missing["policy"] = map[string]any{"push_url_ref": "Svc/NOPE"}
	if r := e.do("POST", "/v1/jobs", tok, missing); r.status != 400 || fieldOf(r) != "policy.push_url_ref" {
		t.Fatalf("missing ref: %d %s", r.status, r.body)
	}
}

func TestAdhocPushRefRejected(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	adhoc := map[string]any{"target": fnJob("x")["target"], "policy": map[string]any{"push_url_ref": "Svc/TOKEN"}}
	if r := e.do("POST", "/v1/runs", tok, adhoc); r.status != 400 || fieldOf(r) != "policy.push_url_ref" {
		t.Fatalf("adhoc with push_url_ref: %d %s", r.status, r.body)
	}
	adhoc["policy"] = map[string]any{"push_on_failure": false}
	if r := e.do("POST", "/v1/runs", tok, adhoc); r.status != 202 {
		t.Fatalf("adhoc push_on_failure must be ignored: %d %s", r.status, r.body)
	}
}
