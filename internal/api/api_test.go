package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

const adminToken = "admin-token-0123456789-0123456789-xyz"

type fakeVault struct{ keys []string }

func (f fakeVault) Get(_ context.Context, r vault.Ref) (string, error) { return "v-" + r.Key, nil }
func (f fakeVault) Exists(_ context.Context, r vault.Ref) (bool, error) {
	for _, k := range f.keys {
		if k == r.String() {
			return true, nil
		}
	}
	return false, nil
}
func (f fakeVault) List(context.Context, bool) ([]string, error) { return f.keys, nil }
func (f fakeVault) Ping(context.Context) error                   { return nil }

type fakeExec struct {
	mu sync.Mutex
	fn func(ctx context.Context, in target.ExecInput) target.Result
}

func (f *fakeExec) Execute(ctx context.Context, in target.ExecInput) target.Result {
	f.mu.Lock()
	fn := f.fn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	code := 200
	return target.Result{Status: model.StatusSuccess, HTTPStatus: &code, Output: json.RawMessage(`{"answer":42}`)}
}

func (f *fakeExec) set(fn func(ctx context.Context, in target.ExecInput) target.Result) {
	f.mu.Lock()
	f.fn = fn
	f.mu.Unlock()
}

type env struct {
	t    *testing.T
	url  string
	exec *fakeExec
	st   *store.Store
	srv  *Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, "v2", "plr.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ex := &fakeExec{}
	fv := fakeVault{keys: []string{"Svc/TOKEN", "Other/TOKEN"}}
	e := engine.New(engine.Deps{Store: st, Executors: map[string]target.Executor{model.TargetHTTP: ex}, Secrets: fv, Log: log,
		RunTokenKey: []byte("run-key"), EnvKey: bytes.Repeat([]byte{1}, 32), MaxHTTPInflight: 8, MaxContainers: 1, DataDir: dir, RetentionDays: 60})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Engine: e, Store: st, Secrets: fv, Log: log, APIToken: adminToken, RunTokenKey: []byte("run-key"),
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, Version: "test"})
	hs := httptest.NewServer(s)
	t.Cleanup(func() {
		hs.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx, time.Now())
		_ = st.Close()
	})
	return &env{t: t, url: hs.URL, exec: ex, st: st, srv: s}
}

type resp struct {
	status int
	body   []byte
	header http.Header
}

func (r resp) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("not JSON (%d): %s", r.status, r.body)
	}
	return m
}

func (r resp) code() string {
	var m struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(r.body, &m)
	return m.Error.Code
}

func (e *env) do(method, path, token string, body any, hdr ...string) resp {
	e.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, e.url+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = r.Body.Close() }()
	b, _ := io.ReadAll(r.Body)
	return resp{status: r.StatusCode, body: b, header: r.Header}
}

func (e *env) client(name string, scopes ...string) (id, token string) {
	e.t.Helper()
	r := e.do("POST", "/v1/clients", adminToken, map[string]any{
		"name": name, "scopes": scopes, "callback_hosts": []string{"svc"}, "secret_ref_prefixes": []string{"Svc/"},
	})
	if r.status != 201 {
		e.t.Fatalf("create client: %d %s", r.status, r.body)
	}
	m := r.json(e.t)
	return m["client"].(map[string]any)["id"].(string), m["token"].(string)
}

func fnJob(name string) map[string]any {
	return map[string]any{"name": name, "trigger": map[string]any{"type": "invoke"},
		"target": map[string]any{"type": "http", "http": map[string]any{"url": "http://svc/fn", "auth": map[string]any{"bearer_ref": "Svc/TOKEN"}}}}
}

var all = []string{"jobs:read", "jobs:write", "runs:invoke", "runs:adhoc"}

// ---------------------------------------------------------------- tests

func TestOpenAPIParity(t *testing.T) {
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(openapiYAML, &doc); err != nil {
		t.Fatal(err)
	}
	inDoc := map[string]string{}
	for path, ops := range doc.Paths {
		for method, op := range ops {
			if method == "parameters" {
				continue
			}
			var codes []string
			if m, ok := op.(map[string]any); ok {
				if rs, ok := m["responses"].(map[string]any); ok {
					for c := range rs {
						codes = append(codes, c)
					}
				}
			}
			sort.Strings(codes)
			inDoc[strings.ToUpper(method)+" "+path] = strings.Join(codes, ",")
		}
	}
	s := &Server{}
	inCode := map[string]string{}
	for _, rt := range s.routeTable() {
		if rt.static {
			continue
		}
		var codes []string
		for _, c := range rt.codes {
			codes = append(codes, strconv.Itoa(c))
		}
		sort.Strings(codes)
		inCode[rt.method+" "+rt.pattern] = strings.Join(codes, ",")
	}
	for k, v := range inCode {
		if d, ok := inDoc[k]; !ok {
			t.Errorf("route %s missing from openapi.yaml", k)
		} else if d != v {
			t.Errorf("%s: codes in code %s, in openapi.yaml %s", k, v, d)
		}
	}
	for k := range inDoc {
		if _, ok := inCode[k]; !ok {
			t.Errorf("openapi.yaml documents %s, which is not served", k)
		}
	}
	if _, err := OpenAPIJSON(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthMatrixAndOwnership(t *testing.T) {
	e := newEnv(t)
	_, tokA := e.client("a", all...)
	_, tokB := e.client("b", all...)
	_, tokRead := e.client("reader", "jobs:read")

	if r := e.do("GET", "/v1/jobs", "", nil); r.status != 401 {
		t.Fatalf("no token: %d", r.status)
	}
	if r := e.do("GET", "/v1/jobs", "plr_x_bogus", nil); r.status != 401 {
		t.Fatalf("bad token: %d", r.status)
	}
	if r := e.do("POST", "/v1/jobs", tokRead, fnJob("f")); r.status != 403 {
		t.Fatalf("missing scope: %d", r.status)
	}
	r := e.do("POST", "/v1/jobs", tokA, fnJob("f"))
	if r.status != 201 {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	id := r.json(t)["id"].(string)
	for _, c := range []struct{ m, p string }{
		{"GET", "/v1/jobs/" + id}, {"POST", "/v1/jobs/" + id + "/invoke"}, {"DELETE", "/v1/jobs/" + id},
	} {
		if r := e.do(c.m, c.p, tokB, nil); r.status != 404 {
			t.Fatalf("%s %s by other client: %d", c.m, c.p, r.status)
		}
	}
	if r := e.do("GET", "/v1/jobs", tokB, nil); len(r.json(t)["items"].([]any)) != 0 {
		t.Fatal("other client sees jobs")
	}
	if r := e.do("GET", "/v1/clients", tokA, nil); r.status != 403 {
		t.Fatalf("client listing clients: %d", r.status)
	}
	if r := e.do("POST", "/v1/jobs", adminToken, fnJob("g")); r.status != 400 || r.code() != "required" {
		t.Fatalf("admin must name a client: %d %s", r.status, r.body)
	}
	job := fnJob("g")
	job["client"] = "b"
	if r := e.do("POST", "/v1/jobs", adminToken, job); r.status != 201 {
		t.Fatalf("admin on behalf: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/jobs", tokA, job); r.status != 403 {
		t.Fatalf("client acting for another: %d", r.status)
	}
}

func TestHostRefAndHeaderControls(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	j := fnJob("f")
	j["target"].(map[string]any)["http"].(map[string]any)["url"] = "http://secret-manager:8000/api/secrets"
	if r := e.do("POST", "/v1/jobs", tok, j); r.status != 400 || r.code() != "host_not_allowed" {
		t.Fatalf("host: %d %s", r.status, r.body)
	}
	j = fnJob("f")
	j["target"].(map[string]any)["http"].(map[string]any)["auth"] = map[string]any{"bearer_ref": "Other/TOKEN"}
	if r := e.do("POST", "/v1/jobs", tok, j); r.status != 400 || r.code() != "secret_ref_not_permitted" {
		t.Fatalf("ref: %d %s", r.status, r.body)
	}
	j = fnJob("f")
	j["target"].(map[string]any)["http"].(map[string]any)["auth"] = map[string]any{"bearer_ref": "Svc/MISSING"}
	if r := e.do("POST", "/v1/jobs", tok, j); r.status != 400 || r.code() != "secret_ref_missing" {
		t.Fatalf("missing ref: %d %s", r.status, r.body)
	}
	j = fnJob("f")
	j["target"].(map[string]any)["http"].(map[string]any)["headers"] = map[string]any{"X-PLR-Run-Token": "x"}
	if r := e.do("POST", "/v1/jobs", tok, j); r.code() != "header_reserved" {
		t.Fatalf("header: %d %s", r.status, r.body)
	}
	j = fnJob("f")
	j["surprise"] = true
	if r := e.do("POST", "/v1/jobs", tok, j); r.status != 400 || r.code() != "invalid_json" {
		t.Fatalf("unknown field: %d %s", r.status, r.body)
	}
	c := map[string]any{"name": "box", "trigger": map[string]any{"type": "invoke"},
		"target": map[string]any{"type": "inline", "inline": map[string]any{"image": "alpine:3", "script": "id"}}}
	if r := e.do("POST", "/v1/jobs", tok, c); r.status != 400 || r.code() != "target_not_enabled" {
		t.Fatalf("1a container: %d %s", r.status, r.body)
	}
}

func TestIdempotency(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	r1 := e.do("POST", "/v1/jobs", tok, fnJob("f"), "Idempotency-Key", "k1")
	r2 := e.do("POST", "/v1/jobs", tok, `{"target":{"type":"http","http":{"auth":{"bearer_ref":"Svc/TOKEN"},"url":"http://svc/fn"}},"name":"f","trigger":{"type":"invoke"}}`, "Idempotency-Key", "k1")
	if r1.status != 201 || r2.status != 201 || r1.json(t)["id"] != r2.json(t)["id"] || r2.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %d %s / %s", r1.status, r2.status, r1.body, r2.body)
	}
	if r := e.do("POST", "/v1/jobs", tok, fnJob("other"), "Idempotency-Key", "k1"); r.status != 409 || r.code() != "idempotency_key_reused" {
		t.Fatalf("different body: %d %s", r.status, r.body)
	}
	bad := fnJob("g")
	bad["target"].(map[string]any)["http"].(map[string]any)["url"] = "http://nope/"
	if r := e.do("POST", "/v1/jobs", tok, bad, "Idempotency-Key", "k2"); r.status != 400 {
		t.Fatalf("bad: %d", r.status)
	}
	if r := e.do("POST", "/v1/jobs", tok, fnJob("g"), "Idempotency-Key", "k2"); r.status != 201 {
		t.Fatalf("a failed request must not keep the key: %d %s", r.status, r.body)
	}
	id := r1.json(t)["id"].(string)
	i1 := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, map[string]any{"payload": map[string]any{"n": 12345678901234567}}, "Idempotency-Key", "inv")
	i2 := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, map[string]any{"payload": map[string]any{"n": 12345678901234567}}, "Idempotency-Key", "inv")
	if i1.json(t)["id"] != i2.json(t)["id"] || i2.status != 202 {
		t.Fatalf("invoke replay: %s / %s", i1.body, i2.body)
	}
	i3 := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, map[string]any{"payload": map[string]any{"n": 12345678901234568}}, "Idempotency-Key", "inv")
	if i3.status != 409 {
		t.Fatalf("large ints must hash exactly: %d", i3.status)
	}
}

func TestInvokeWait(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	id := e.do("POST", "/v1/jobs", tok, fnJob("f")).json(t)["id"].(string)
	r := e.do("POST", "/v1/jobs/"+id+"/invoke?wait=30s", tok, map[string]any{"payload": 1})
	if r.status != 200 || r.json(t)["status"] != "success" || r.json(t)["output"].(map[string]any)["answer"] != float64(42) {
		t.Fatalf("wait result: %d %s", r.status, r.body)
	}
	release := make(chan struct{})
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-release:
		case <-ctx.Done():
		}
		code := 200
		return target.Result{Status: model.StatusSuccess, HTTPStatus: &code}
	})
	r = e.do("POST", "/v1/jobs/"+id+"/invoke?wait=1", tok, nil)
	if r.status != 202 || r.json(t)["id"] == "" {
		t.Fatalf("wait expiry: %d %s", r.status, r.body)
	}
	close(release)
	for _, w := range []string{"0", "56", "abc", "30m"} {
		if r := e.do("POST", "/v1/jobs/"+id+"/invoke?wait="+w, tok, nil); r.status != 400 {
			t.Fatalf("wait=%s accepted", w)
		}
	}
}

func TestCancelCodesAndDisabledInvoke(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	release := make(chan struct{})
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return target.Result{Status: model.StatusFailed, Ambiguous: true}
	})
	id := e.do("POST", "/v1/jobs", tok, fnJob("f")).json(t)["id"].(string)
	run := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, nil).json(t)["id"].(string)
	deadline := time.Now().Add(5 * time.Second)
	for e.do("GET", "/v1/runs/"+run, tok, nil).json(t)["status"] != "running" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.do("POST", "/v1/runs/"+run+"/cancel", tok, nil); r.status != 202 {
		t.Fatalf("running cancel: %d %s", r.status, r.body)
	}
	for e.do("GET", "/v1/runs/"+run, tok, nil).json(t)["status"] != "cancelled" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	r := e.do("POST", "/v1/runs/"+run+"/cancel", tok, nil)
	if r.status != 409 || r.code() != "run_terminal" || !strings.Contains(string(r.body), `"status":"cancelled"`) {
		t.Fatalf("terminal cancel: %d %s", r.status, r.body)
	}
	close(release)
	e.do("POST", "/v1/jobs/"+id+"/disable", tok, nil)
	if r := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, nil); r.status != 409 || r.code() != "job_disabled" {
		t.Fatalf("disabled invoke: %d %s", r.status, r.body)
	}
}

func TestRunTokenRoutes(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	e.exec.set(func(context.Context, target.ExecInput) target.Result {
		code := 202
		return target.Result{Waiting: true, HTTPStatus: &code, Delivered: true}
	})
	j := fnJob("pub")
	j["target"].(map[string]any)["http"].(map[string]any)["mode"] = "async"
	id := e.do("POST", "/v1/jobs", tok, j).json(t)["id"].(string)
	run := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, nil).json(t)["id"].(string)
	deadline := time.Now().Add(5 * time.Second)
	for e.do("GET", "/v1/runs/"+run, tok, nil).json(t)["status"] != "waiting" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.do("POST", "/v1/runs/"+run+"/complete", "", `{"status":"success"}`, "X-PLR-Run-Token", "forged"); r.status != 401 {
		t.Fatalf("forged token: %d", r.status)
	}
	good := secure.RunToken([]byte("run-key"), run)
	if r := e.do("POST", "/v1/runs/"+run+"/heartbeat", "", nil, "X-PLR-Run-Token", good); r.status != 200 || r.json(t)["lease_expires_at"] == nil {
		t.Fatalf("heartbeat: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/runs/"+run+"/complete", "", `{"status":"done"}`, "X-PLR-Run-Token", good); r.status != 400 {
		t.Fatalf("bad status: %d", r.status)
	}
	if r := e.do("POST", "/v1/runs/"+run+"/complete", "", `{"status":"success","output":{"media":7}}`, "X-PLR-Run-Token", good); r.status != 200 || r.json(t)["status"] != "success" {
		t.Fatalf("complete: %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/runs/"+run+"/complete", "", `{"status":"success"}`, "X-PLR-Run-Token", good); r.status != 409 {
		t.Fatalf("second complete: %d", r.status)
	}
}

func TestLimiterTrustsOnlyProxyXFF(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 10; i++ {
		e.do("GET", "/v1/jobs", "wrong", nil, "X-Forwarded-For", "203.0.113.7")
	}
	if r := e.do("GET", "/v1/jobs", adminToken, nil, "X-Forwarded-For", "203.0.113.7"); r.status != 429 {
		t.Fatalf("blocked IP: %d", r.status)
	}
	if r := e.do("GET", "/v1/jobs", adminToken, nil, "X-Forwarded-For", "203.0.113.8"); r.status != 200 {
		t.Fatalf("other IP must not be blocked: %d", r.status)
	}
	// A spoofed left-most hop does not help: the right-most untrusted hop counts.
	if r := e.do("GET", "/v1/jobs", adminToken, nil, "X-Forwarded-For", "198.51.100.1, 203.0.113.7"); r.status != 429 {
		t.Fatalf("spoof: %d", r.status)
	}
}

func TestClientIPIgnoresUntrustedXFF(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "172.18.0.9:1234"
	r.Header.Set("X-Forwarded-For", "1.2.3.4")
	if ip := clientIP(r, []netip.Prefix{netip.MustParsePrefix("172.31.250.1/32")}); ip != "172.18.0.9" {
		t.Fatalf("untrusted peer's XFF honoured: %s", ip)
	}
}

func TestLimitsHeadersAndUnknownRoutes(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	big := `{"name":"` + strings.Repeat("x", 600<<10) + `"}`
	if r := e.do("POST", "/v1/jobs", tok, big); r.status != 413 {
		t.Fatalf("big body: %d", r.status)
	}
	id := e.do("POST", "/v1/jobs", tok, fnJob("f")).json(t)["id"].(string)
	huge := `{"payload":"` + strings.Repeat("x", 300<<10) + `"}`
	if r := e.do("POST", "/v1/jobs/"+id+"/invoke", tok, huge); r.status != 413 {
		t.Fatalf("big payload: %d", r.status)
	}
	r := e.do("GET", "/v1/nope", tok, nil)
	if r.status != 404 || r.code() != "not_found" {
		t.Fatalf("unknown route: %d %s", r.status, r.body)
	}
	r = e.do("PUT", "/v1/jobs", tok, nil)
	if r.status != 405 || r.code() != "method_not_allowed" {
		t.Fatalf("wrong method: %d %s", r.status, r.body)
	}
	if r.header.Get("Content-Security-Policy") == "" || r.header.Get("X-Frame-Options") != "DENY" || r.header.Get("Cache-Control") != "no-store" {
		t.Fatalf("security headers: %v", r.header)
	}
	if r := e.do("GET", "/healthz", "", nil); r.status != 200 || r.json(t)["ok"] != true {
		t.Fatalf("healthz: %d %s", r.status, r.body)
	}
}

func TestUnknownErrorIs500WithoutDetail(t *testing.T) {
	rec := httptest.NewRecorder()
	fail(rec, slog.New(slog.NewTextHandler(io.Discard, nil)), io.ErrUnexpectedEOF)
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "unexpected EOF") {
		t.Fatalf("500 leaks: %d %s", rec.Code, rec.Body)
	}
}

func TestClientsLifecycle(t *testing.T) {
	e := newEnv(t)
	id, tok := e.client("a", all...)
	if r := e.do("POST", "/v1/clients", adminToken, map[string]any{"name": "a"}); r.status != 409 {
		t.Fatalf("duplicate name: %d", r.status)
	}
	if r := e.do("POST", "/v1/clients", adminToken, map[string]any{"name": "Bad_Name"}); r.status != 400 {
		t.Fatalf("bad name: %d", r.status)
	}
	if r := e.do("PATCH", "/v1/clients/"+id, adminToken, map[string]any{"secret_ref_prefixes": []string{"Svc"}}); r.status != 400 {
		t.Fatalf("prefix without slash: %d", r.status)
	}
	e.do("POST", "/v1/jobs", tok, fnJob("f"))
	if r := e.do("DELETE", "/v1/clients/"+id, adminToken, nil); r.status != 409 || r.code() != "client_has_jobs" {
		t.Fatalf("delete with jobs: %d %s", r.status, r.body)
	}
	r := e.do("POST", "/v1/clients/"+id+"/rotate", adminToken, map[string]any{"grace_sec": 60})
	newTok := r.json(t)["token"].(string)
	if e.do("GET", "/v1/jobs", tok, nil).status != 200 || e.do("GET", "/v1/jobs", newTok, nil).status != 200 {
		t.Fatal("both tokens must work during grace")
	}
	e.do("POST", "/v1/clients/"+id+"/rotate", adminToken, map[string]any{})
	if e.do("GET", "/v1/jobs", newTok, nil).status != 401 {
		t.Fatal("rotation without grace must invalidate at once")
	}
	list := e.do("GET", "/v1/clients", adminToken, nil)
	if strings.Contains(string(list.body), "token_hash") || strings.Contains(string(list.body), "TokenHash") {
		t.Fatal("client listing leaks hashes")
	}
}

func TestSecretsFiltered(t *testing.T) {
	e := newEnv(t)
	_, tok := e.client("a", all...)
	r := e.do("GET", "/v1/secrets", tok, nil)
	names := r.json(t)["names"].([]any)
	if len(names) != 1 || names[0] != "Svc/TOKEN" {
		t.Fatalf("names = %v", names)
	}
	if n := len(e.do("GET", "/v1/secrets", adminToken, nil).json(t)["names"].([]any)); n != 2 {
		t.Fatalf("admin sees %d", n)
	}
}
