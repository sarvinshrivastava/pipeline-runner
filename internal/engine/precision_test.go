package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target/httpt"
)

// TestFireDelayP99 is acceptance #1: 100 pending `at` HTTP jobs fire with a
// p99 delay under 50 ms, measured at the receiver (so HTTP time is included).
func TestFireDelayP99(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("timing assertion: run without -race and -short (CI runs it alone)")
	}
	var mu sync.Mutex
	var delays []time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := time.Now()
		sched, err := time.Parse(time.RFC3339Nano, r.Header.Get("X-PLR-Scheduled-For"))
		if err == nil {
			mu.Lock()
			delays = append(delays, got.Sub(sched))
			mu.Unlock()
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	dir := t.TempDir()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(dir, "v2", "plr.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	c := model.Client{ID: newID(), Name: "svc", TokenHash: []byte("h"), Scopes: []string{model.ScopeJobsWrite},
		CallbackHosts: []string{u.Hostname()}, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := store.CreateClient(ctx, st.W, c); err != nil {
		t.Fatal(err)
	}
	exec := httpt.New(secure.NewHTTPClient(2*time.Second, func(string, netip.Addr) bool { return true }))
	e := New(Deps{
		Store: st, Executors: map[string]target.Executor{model.TargetHTTP: exec}, Secrets: fakeSecrets{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), RunTokenKey: []byte("k"), EnvKey: bytes.Repeat([]byte{1}, 32),
		MaxHTTPInflight: 64, MaxContainers: 4, DataDir: dir, RetentionDays: 60,
	})
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { e.stopLife(); <-e.done }()

	base := time.Now().Add(1500 * time.Millisecond).Truncate(time.Millisecond)
	const n = 100
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(i) * 7 * time.Millisecond)
		_, err := e.CreateJob(ctx, model.PrincipalOf(c), model.JobCreate{
			Name: fmt.Sprintf("p%03d", i), Trigger: model.TriggerIn{Type: model.TriggerAt, At: &at},
			Target: model.TargetIn{Type: model.TargetHTTP, HTTP: &model.HTTPIn{URL: srv.URL + "/hook"}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(delays)
		mu.Unlock()
		if got == n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(delays) != n {
		t.Fatalf("received %d/%d callbacks", len(delays), n)
	}
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	p50, p99 := delays[n/2], delays[n*99/100-1]
	t.Logf("fire delay p50=%s p99=%s max=%s", p50, p99, delays[n-1])
	if p99 > 50*time.Millisecond {
		t.Fatalf("p99 fire delay %s exceeds 50ms", p99)
	}
}
