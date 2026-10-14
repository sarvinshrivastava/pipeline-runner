package engine

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

var t0 = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)

type fakeSecrets struct{}

func (fakeSecrets) Get(_ context.Context, r vault.Ref) (string, error) { return "secret-" + r.Key, nil }
func (fakeSecrets) Exists(context.Context, vault.Ref) (bool, error)    { return true, nil }

type fakePager struct {
	mu   sync.Mutex
	msgs []notify.Msg
}

func (p *fakePager) Send(m notify.Msg) {
	p.mu.Lock()
	p.msgs = append(p.msgs, m)
	p.mu.Unlock()
}

func (p *fakePager) titles() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.msgs))
	for i, m := range p.msgs {
		out[i] = m.Title
	}
	return out
}

func (p *fakePager) count(substr string) int {
	n := 0
	for _, t := range p.titles() {
		if bytes.Contains([]byte(t), []byte(substr)) {
			n++
		}
	}
	return n
}

// fakeExec runs a per-test behaviour; by default runs succeed immediately.
type fakeExec struct {
	mu    sync.Mutex
	fn    func(ctx context.Context, in target.ExecInput) target.Result
	calls []target.ExecInput
}

func (f *fakeExec) Execute(ctx context.Context, in target.ExecInput) target.Result {
	f.mu.Lock()
	f.calls = append(f.calls, in)
	fn := f.fn
	f.mu.Unlock()
	if fn == nil {
		code := 200
		return target.Result{Status: model.StatusSuccess, HTTPStatus: &code, Output: []byte(`{"ok":true}`)}
	}
	return fn(ctx, in)
}

func (f *fakeExec) set(fn func(ctx context.Context, in target.ExecInput) target.Result) {
	f.mu.Lock()
	f.fn = fn
	f.mu.Unlock()
}

func (f *fakeExec) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type harness struct {
	t       *testing.T
	e       *Engine
	st      *store.Store
	clk     *FakeClock
	exec    *fakeExec
	pager   *fakePager
	owner   model.Principal
	dir     string
	maxHTTP int
}

type opt func(*harness)

func withMaxHTTP(n int) opt { return func(h *harness) { h.maxHTTP = n } }

func newHarness(t *testing.T, opts ...opt) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), clk: NewFakeClock(t0), exec: &fakeExec{}, pager: &fakePager{}, maxHTTP: 64}
	for _, o := range opts {
		o(h)
	}
	st, err := store.Open(context.Background(), filepath.Join(h.dir, "v2", "plr.db"))
	if err != nil {
		t.Fatal(err)
	}
	h.st = st
	h.owner = h.addClient("svc")
	h.start()
	return h
}

func (h *harness) addClient(name string) model.Principal {
	c := model.Client{ID: newID(), Name: name, TokenHash: []byte("hash-" + name), Scopes: []string{model.ScopeJobsWrite},
		CallbackHosts: []string{"svc"}, SecretRefPrefixes: []string{"Svc/"}, CreatedAt: t0, UpdatedAt: t0}
	if err := store.CreateClient(context.Background(), h.st.W, c); err != nil {
		h.t.Fatal(err)
	}
	return model.PrincipalOf(c)
}

func (h *harness) start() {
	h.e = New(Deps{
		Store: h.st, Clock: h.clk, Executors: map[string]target.Executor{model.TargetHTTP: h.exec},
		Secrets: fakeSecrets{}, Pager: h.pager, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		RunTokenKey: []byte("run-key"), EnvKey: bytes.Repeat([]byte{1}, 32), MaxHTTPInflight: h.maxHTTP, MaxContainers: 4,
		DataDir: h.dir, RetentionDays: 60, Rand: func(int64) int64 { return 0 },
	})
	if err := h.e.Start(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(h.stop)
}

func (h *harness) stop() {
	if h.e == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.e.stopLife()
	<-h.e.done
	_ = ctx
	h.e = nil
}

func (h *harness) sync() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // generous for -race
	defer cancel()
	if err := h.e.Sync(ctx); err != nil {
		h.t.Fatalf("sync: %v", err)
	}
}

func (h *harness) advance(d time.Duration) {
	h.clk.Advance(d)
	h.sync()
}

// onLoop runs f on the loop goroutine.
func (h *harness) onLoop(f func()) {
	h.t.Helper()
	done := make(chan struct{})
	if err := h.e.send(context.Background(), evCall{f: f, done: done}); err != nil {
		h.t.Fatal(err)
	}
	<-done
}

func (h *harness) counts() (jobBusy, keyBusy, inflight map[string]int) {
	h.onLoop(func() {
		jobBusy, keyBusy, inflight = clone(h.e.jobBusy), clone(h.e.keyBusy), clone(h.e.inflight)
	})
	return
}

func clone(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (h *harness) run(id string) model.Run {
	h.t.Helper()
	r, err := store.GetRun(context.Background(), h.st.W, id)
	if err != nil {
		h.t.Fatalf("get run %s: %v", id, err)
	}
	return r
}

func (h *harness) runs(jobID string) []model.Run {
	h.t.Helper()
	rs, err := store.ListRuns(context.Background(), h.st.W, store.RunFilter{JobID: jobID, Limit: 1000})
	if err != nil {
		h.t.Fatal(err)
	}
	// oldest first
	for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
		rs[i], rs[j] = rs[j], rs[i]
	}
	return rs
}

func (h *harness) waitFor(desc string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.sync()
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", desc)
}

func (h *harness) waitStatus(id string, want model.Status) model.Run {
	h.t.Helper()
	var r model.Run
	h.waitFor("run "+id+" → "+string(want), func() bool {
		r = h.run(id)
		return r.Status == want
	})
	// The status commits before the handler's post-commit work (pages,
	// pushes); let the loop finish that handler too.
	h.sync()
	return r
}

func (h *harness) create(in model.JobCreate) model.Job {
	h.t.Helper()
	j, err := h.e.CreateJob(context.Background(), h.owner, in, nil)
	if err != nil {
		h.t.Fatalf("create job: %v", err)
	}
	h.sync()
	return j
}

func ptr[T any](v T) *T { return &v }

func httpTarget() model.TargetIn {
	return model.TargetIn{Type: model.TargetHTTP, HTTP: &model.HTTPIn{URL: "http://svc/hook"}}
}

func cronJob(name, expr string, p *model.PolicyIn) model.JobCreate {
	return model.JobCreate{Name: name, Trigger: model.TriggerIn{Type: model.TriggerCron, Cron: expr}, Target: httpTarget(), Policy: p}
}

func invokeJob(name string, p *model.PolicyIn) model.JobCreate {
	return model.JobCreate{Name: name, Trigger: model.TriggerIn{Type: model.TriggerInvoke}, Target: httpTarget(), Policy: p}
}

// gate blocks exec calls until released.
type gate struct{ ch chan struct{} }

func newGate() *gate  { return &gate{ch: make(chan struct{})} }
func (g *gate) open() { close(g.ch) }

func (g *gate) behaviour(res target.Result) func(ctx context.Context, in target.ExecInput) target.Result {
	return func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-g.ch:
			return res
		case <-ctx.Done():
			return target.Result{Status: model.StatusFailed, Error: "context done", Ambiguous: true}
		}
	}
}

func okResult() target.Result {
	code := 200
	return target.Result{Status: model.StatusSuccess, HTTPStatus: &code}
}

type (
	targetInput  = target.ExecInput
	targetResult = target.Result
)
