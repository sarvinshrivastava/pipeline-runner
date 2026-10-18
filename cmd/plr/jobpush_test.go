package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/config"
	"github.com/sarvinshrivastava/pipeline-runner/internal/jobpush"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

const pushToken = "Tok3nSecretXYZ"

// pushRig is a running plr serve with a fake vault holding Svc/KUMA, a fake
// Kuma push endpoint and a callback target.
type pushRig struct {
	t      *testing.T
	base   string
	cancel context.CancelFunc
	done   chan error
}

func port(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Port()
}

// fakeVault serves Svc/KUMA with the given value.
func fakeVault(t *testing.T, value string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/secrets":
			_, _ = w.Write([]byte(`{"keys":[{"key":"KUMA","folder":"Svc"}]}`))
		case "/api/secrets/Svc/KUMA":
			b, _ := json.Marshal(map[string]string{"value": value})
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startRig swaps callbackPolicy for one that reaches loopback (restored on
// cleanup), so these tests must not run in parallel.
func startRig(t *testing.T, mod func(*config.Config), vaultURL string) *pushRig {
	t.Helper()
	old := callbackPolicy
	callbackPolicy = func(string, netip.Addr) bool { return true }
	t.Cleanup(func() { callbackPolicy = old })

	cfg := config.Config{
		Port: 0, MetricsPort: 0, DataDir: t.TempDir(), APIToken: "admin-token-0123456789-0123456789",
		RunTokenKey: bytes.Repeat([]byte{1}, 32), EncryptionKey: bytes.Repeat([]byte{2}, 32),
		SecretManagerURL: vaultURL, SecretManagerToken: "vault-token",
		PushURLHosts: []string{"localhost"}, MaxHTTPInflight: 4, MaxContainers: 1, RunRetentionDays: 60,
	}
	if mod != nil {
		mod(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	addrs := make(chan string, 1)
	rig := &pushRig{t: t, cancel: cancel, done: make(chan error, 1)}
	go func() {
		// Closed after the result, so the cleanup's receive never blocks once
		// a test has read it.
		defer close(rig.done)
		rig.done <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), func(a, _ string) { addrs <- a })
	}()
	select {
	case rig.base = <-addrs:
	case err := <-rig.done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-rig.done:
		case <-time.After(30 * time.Second):
		}
	})
	return rig
}

func (r *pushRig) call(method, path, tok string, body any) map[string]any {
	r.t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(context.Background(), method, "http://"+r.base+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var m map[string]any
	_ = json.NewDecoder(res.Body).Decode(&m)
	if res.StatusCode >= 300 {
		r.t.Fatalf("%s %s: %d %v", method, path, res.StatusCode, m)
	}
	return m
}

// pushJob creates a client and an invoke job with push_url_ref aimed at target.
func (r *pushRig) pushJob(name, targetURL string) (tok, jobID string) {
	r.t.Helper()
	tok = r.call("POST", "/v1/clients", "admin-token-0123456789-0123456789", map[string]any{"name": "c-" + name,
		"scopes": []string{"jobs:read", "jobs:write", "runs:invoke"}, "callback_hosts": []string{"127.0.0.1"},
		"secret_ref_prefixes": []string{"Svc/"}})["token"].(string)
	jobID = r.call("POST", "/v1/jobs", tok, map[string]any{"name": name,
		"trigger": map[string]any{"type": "invoke"},
		"target":  map[string]any{"type": "http", "http": map[string]any{"url": targetURL}},
		"policy":  map[string]any{"push_url_ref": "Svc/KUMA"}})["id"].(string)
	return tok, jobID
}

func (r *pushRig) waitRun(tok, runID, status string) {
	r.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for r.call("GET", "/v1/runs/"+runID, tok, nil)["status"] != status {
		if time.Now().After(deadline) {
			r.t.Fatalf("run %s never reached %s", runID, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeWiresJobPush(t *testing.T) {
	var pushes, ups atomic.Int32
	kuma := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/push/"+pushToken {
			pushes.Add(1)
			if r.URL.Query().Get("status") == "up" {
				ups.Add(1)
			}
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer kuma.Close()
	tgt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer tgt.Close()
	vault := fakeVault(t, "http://localhost:"+port(t, kuma)+"/api/push/"+pushToken)

	rig := startRig(t, nil, vault.URL)
	tok, job := rig.pushJob("tick", "http://127.0.0.1:"+port(t, tgt)+"/hook")

	// The engine got PUSH_URL_HOSTS too: validate passes the push_url check.
	resp := rig.call("POST", "/v1/jobs/"+job+"/validate", tok, nil)
	cs, ok := resp["checks"].([]any)
	if !ok {
		t.Fatalf("validate = %v", resp)
	}
	checks := map[string]bool{}
	for _, c := range cs {
		m, ok := c.(map[string]any)
		name, nok := m["name"].(string)
		if !ok || !nok {
			t.Fatalf("validate check = %v", c)
		}
		checks[name], _ = m["ok"].(bool)
	}
	if !checks["push_url_ref"] || !checks["push_url"] || !checks["push_dns"] {
		t.Fatalf("validate push checks = %v", checks)
	}

	runID := rig.call("POST", "/v1/jobs/"+job+"/invoke", tok, nil)["id"].(string)
	rig.waitRun(tok, runID, "success")
	deadline := time.Now().Add(10 * time.Second)
	for pushes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no push reached Kuma")
		}
		time.Sleep(5 * time.Millisecond)
	}
	rig.cancel()
	select {
	case err := <-rig.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("shutdown hung")
	}
	// run has returned: the worker is joined and drained, so every push it
	// would ever send has been sent.
	if n, u := pushes.Load(), ups.Load(); n != 1 || u != 1 {
		t.Fatalf("pushes = %d (up %d), want exactly one up", n, u)
	}
}

// At shutdown the pager drain starts at once; it is not held up by the push
// worker finishing a push that is stuck in flight.
func TestShutdownPagerDrainNotDelayedByPushJoin(t *testing.T) {
	// Hold push A far beyond the 2 s window below: with the production 3 s
	// HTTP timeout, a serial drain could still squeeze page B in under 2 s.
	oldHTTP, oldBudget := pushHTTPTimeout, pushBudget
	pushHTTPTimeout, pushBudget = 30*time.Second, 30*time.Second
	t.Cleanup(func() { pushHTTPTimeout, pushBudget = oldHTTP, oldBudget })
	pages := make(chan string, 10)
	var first atomic.Bool
	ntfy := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var m struct{ Title string }
		_ = json.NewDecoder(r.Body).Decode(&m)
		if first.CompareAndSwap(false, true) {
			pages <- "blocked:" + m.Title
			<-r.Context().Done() // held until stopNotify aborts the publish
			return
		}
		pages <- m.Title
	}))
	defer ntfy.Close()
	got, release := make(chan struct{}, 10), make(chan struct{})
	kuma := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer kuma.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	tgt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(400) }))
	defer tgt.Close()
	vault := fakeVault(t, "http://localhost:"+port(t, kuma)+"/api/push/"+pushToken)

	rig := startRig(t, func(c *config.Config) { c.NtfyBase, c.NtfyTopicFail = ntfy.URL, "fail" }, vault.URL)
	hook := "http://127.0.0.1:" + port(t, tgt) + "/hook"
	tokA, jobA := rig.pushJob("a", hook)
	tokB, jobB := rig.pushJob("b", hook)

	runA := rig.call("POST", "/v1/jobs/"+jobA+"/invoke", tokA, nil)["id"].(string)
	rig.waitRun(tokA, runA, "failed")
	wait := func(desc string, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal(desc)
		}
	}
	wait("push A never reached Kuma", got)
	select {
	case p := <-pages:
		if !strings.HasPrefix(p, "blocked:a:") {
			t.Fatalf("first page = %q", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("page A never reached ntfy")
	}

	runB := rig.call("POST", "/v1/jobs/"+jobB+"/invoke", tokB, nil)["id"].(string)
	rig.waitRun(tokB, runB, "failed")
	time.Sleep(100 * time.Millisecond) // the page is sent just after the status commits

	start := time.Now()
	rig.cancel()
	// Push A is held for up to 30 s (released below, after this check): if
	// the pager drain waited for the push worker's join, page B would miss
	// this 2 s window by a wide margin.
	select {
	case p := <-pages:
		if !strings.HasPrefix(p, "b:") {
			t.Fatalf("drained page = %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the queued page was not delivered while the push was stuck")
	}
	select {
	case <-release:
		t.Fatal("release closed early")
	default:
	}
	close(release)
	select {
	case err := <-rig.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("shutdown hung")
	}
	if el := time.Since(start); el > 6*time.Second {
		t.Fatalf("shutdown took %s", el)
	}
}

// On an early return (start or listen failure) the deferred join still
// drains what boot recovery queued, instead of dropping it uncounted.
func TestJoinPushDrainsOnEarlyReturn(t *testing.T) {
	var mu sync.Mutex
	results := map[string]int{}
	p := jobpush.New(jobpush.Config{
		Clients: func(context.Context, string) (model.Principal, error) { return model.Principal{}, jobpush.ErrClientGone },
		Hosts:   []string{"localhost"},
		Count: func(_, _, r string) {
			mu.Lock()
			results[r]++
			mu.Unlock()
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	p.Enqueue(jobpush.Push{JobID: "j", JobName: "j", ClientID: "c", RunID: "r", Ref: "Svc/KUMA", Status: "interrupted", PingMs: -1})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done) // the worker never ran: Start failed before it popped anything
	joinPush(stop, done, p)
	if ctx.Err() == nil {
		t.Fatal("joinPush must stop the worker")
	}
	mu.Lock()
	defer mu.Unlock()
	if results[jobpush.ResultRefused] != 1 || len(results) != 1 {
		t.Fatalf("results = %v, want the queued push handled once", results)
	}
}
