//go:build bench

// Load benchmark (the user's acceptance bar): hold a mixed load for 5 minutes
// with API p95 < 500 ms, p99 < 1 s and errors < 1%. Cron callbacks fire in the
// background the whole time and their fire delay is reported too.
//
//	go test -tags bench -run TestLoadHold -timeout 20m -v ./cmd/plr
//
// Knobs: BENCH_DURATION (default 5m), BENCH_RPS (100), BENCH_CRON_JOBS (100,
// each firing every second).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/config"
)

const benchAdmin = "bench-admin-token-0123456789-0123456789"

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

type sample struct {
	op  string
	d   time.Duration
	err bool
}

type recorder struct {
	mu      sync.Mutex
	samples []sample
}

func (r *recorder) add(s sample) {
	r.mu.Lock()
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	i := int(float64(len(ds))*p+0.999999) - 1
	return ds[max(min(i, len(ds)-1), 0)]
}

func TestLoadHold(t *testing.T) {
	duration := 5 * time.Minute
	if v, err := time.ParseDuration(os.Getenv("BENCH_DURATION")); err == nil {
		duration = v
	}
	rps := envInt("BENCH_RPS", 100)
	cronJobs := envInt("BENCH_CRON_JOBS", 100)

	// Callback receiver: records cron fire delay; answers functions quickly.
	var fireMu sync.Mutex
	var fires []time.Duration
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-PLR-Trigger") == "schedule" {
			if sched, err := time.Parse(time.RFC3339Nano, r.Header.Get("X-PLR-Scheduled-For")); err == nil {
				fireMu.Lock()
				fires = append(fires, time.Since(sched))
				fireMu.Unlock()
			}
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer recv.Close()
	host, _, _ := net.SplitHostPort(recv.Listener.Addr().String())

	callbackPolicy = func(string, netip.Addr) bool { return true }
	cfg := config.Config{
		DataDir: t.TempDir(), APIToken: benchAdmin, RunTokenKey: bytes.Repeat([]byte{1}, 32), EncryptionKey: bytes.Repeat([]byte{2}, 32),
		SecretManagerURL: "http://127.0.0.1:1", MaxHTTPInflight: 64, MaxContainers: 1, RunRetentionDays: 60,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), func(a, _ string) { addrs <- a })
	}()
	base := "http://" + <-addrs

	tr := &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 1000, IdleConnTimeout: 90 * time.Second}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	call := func(method, path, token string, body any) (int, []byte, error) {
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, _ := http.NewRequestWithContext(context.Background(), method, base+path, rd)
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b, nil
	}

	// Setup: a client, background cron jobs, and some functions.
	st, b, err := call("POST", "/v1/clients", benchAdmin, map[string]any{
		"name": "bench", "scopes": []string{"jobs:read", "jobs:write", "runs:invoke"}, "callback_hosts": []string{host},
	})
	if st != 201 || err != nil {
		t.Fatalf("client: %d %s %v", st, b, err)
	}
	var cr struct{ Token string }
	_ = json.Unmarshal(b, &cr)
	tok := cr.Token
	for i := 0; i < cronJobs; i++ {
		st, b, _ := call("POST", "/v1/jobs", tok, map[string]any{
			"name": fmt.Sprintf("cron-%03d", i), "trigger": map[string]any{"type": "cron", "cron": "* * * * * *"},
			"target": map[string]any{"type": "http", "http": map[string]any{"url": recv.URL + "/cron"}},
		})
		if st != 201 {
			t.Fatalf("cron job: %d %s", st, b)
		}
	}
	var fnIDs []string
	for i := 0; i < 20; i++ {
		st, b, _ := call("POST", "/v1/jobs", tok, map[string]any{
			"name": fmt.Sprintf("fn-%02d", i), "trigger": map[string]any{"type": "invoke"},
			"target": map[string]any{"type": "http", "http": map[string]any{"url": recv.URL + "/fn"}},
		})
		if st != 201 {
			t.Fatalf("fn job: %d %s", st, b)
		}
		var j struct{ ID string }
		_ = json.Unmarshal(b, &j)
		fnIDs = append(fnIDs, j.ID)
	}

	ops := []struct {
		name   string
		weight int
		do     func(r *rand.Rand) (int, []int, error)
	}{
		{"invoke", 40, func(r *rand.Rand) (int, []int, error) {
			st, _, err := call("POST", "/v1/jobs/"+fnIDs[r.IntN(len(fnIDs))]+"/invoke", tok, map[string]any{"payload": map[string]any{"n": r.IntN(1000)}})
			return st, []int{202}, err
		}},
		{"invoke_wait", 10, func(r *rand.Rand) (int, []int, error) {
			st, _, err := call("POST", "/v1/jobs/"+fnIDs[r.IntN(len(fnIDs))]+"/invoke?wait=10", tok, map[string]any{})
			return st, []int{200, 202}, err
		}},
		{"list_runs", 25, func(*rand.Rand) (int, []int, error) {
			st, _, err := call("GET", "/v1/runs?limit=50", tok, nil)
			return st, []int{200}, err
		}},
		{"get_job", 15, func(r *rand.Rand) (int, []int, error) {
			st, _, err := call("GET", "/v1/jobs/"+fnIDs[r.IntN(len(fnIDs))], tok, nil)
			return st, []int{200}, err
		}},
		{"create_delete_job", 5, func(r *rand.Rand) (int, []int, error) {
			st, b, err := call("POST", "/v1/jobs", tok, map[string]any{
				"name": fmt.Sprintf("tmp-%d", r.Int64()), "trigger": map[string]any{"type": "cron", "cron": "*/5 * * * *"},
				"target": map[string]any{"type": "http", "http": map[string]any{"url": recv.URL + "/tmp"}},
			})
			if err != nil || st != 201 {
				return st, []int{201}, err
			}
			var j struct{ ID string }
			_ = json.Unmarshal(b, &j)
			st, _, err = call("DELETE", "/v1/jobs/"+j.ID, tok, nil)
			return st, []int{204}, err
		}},
		{"healthz", 5, func(*rand.Rand) (int, []int, error) {
			st, _, err := call("GET", "/healthz", "", nil)
			return st, []int{200}, err
		}},
	}
	total := 0
	for _, o := range ops {
		total += o.weight
	}

	// Open model: one request every 1/rps regardless of how slow the last was.
	rec := &recorder{}
	var inflight atomic.Int64
	var wg sync.WaitGroup
	interval := time.Second / time.Duration(rps)
	start := time.Now()
	tick := time.NewTicker(interval)
	seq := uint64(0)
	t.Logf("holding %d req/s for %s with %d cron callbacks/s", rps, duration, cronJobs)
	for time.Since(start) < duration {
		<-tick.C
		seq++
		r := rand.New(rand.NewPCG(seq, 7))
		pick := r.IntN(total)
		var op = ops[0]
		for _, o := range ops {
			if pick < o.weight {
				op = o
				break
			}
			pick -= o.weight
		}
		wg.Add(1)
		inflight.Add(1)
		go func() {
			defer wg.Done()
			defer inflight.Add(-1)
			t0 := time.Now()
			st, ok, err := op.do(r)
			bad := err != nil || !containsInt(ok, st)
			rec.add(sample{op: op.name, d: time.Since(t0), err: bad})
		}()
	}
	tick.Stop()
	wg.Wait()
	elapsed := time.Since(start)

	// Report.
	byOp := map[string][]time.Duration{}
	var all []time.Duration
	errs := 0
	for _, s := range rec.samples {
		byOp[s.op] = append(byOp[s.op], s.d)
		all = append(all, s.d)
		if s.err {
			errs++
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	t.Logf("requests=%d achieved=%.1f req/s errors=%d (%.3f%%)", len(all), float64(len(all))/elapsed.Seconds(), errs, 100*float64(errs)/float64(len(all)))
	t.Logf("API   p50=%s p95=%s p99=%s max=%s", pct(all, .50), pct(all, .95), pct(all, .99), all[len(all)-1])
	names := make([]string, 0, len(byOp))
	for n := range byOp {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		ds := byOp[n]
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		t.Logf("  %-18s n=%-6d p50=%-10s p95=%-10s p99=%s", n, len(ds), pct(ds, .5), pct(ds, .95), pct(ds, .99))
	}
	fireMu.Lock()
	sort.Slice(fires, func(i, j int) bool { return fires[i] < fires[j] })
	t.Logf("cron fire delay n=%d p50=%s p95=%s p99=%s max=%s", len(fires), pct(fires, .5), pct(fires, .95), pct(fires, .99), fires[len(fires)-1])
	fireMu.Unlock()

	if p95 := pct(all, .95); p95 >= 500*time.Millisecond {
		t.Errorf("API p95 %s ≥ 500ms", p95)
	}
	if p99 := pct(all, .99); p99 >= time.Second {
		t.Errorf("API p99 %s ≥ 1s", p99)
	}
	if rate := float64(errs) / float64(len(all)); rate >= 0.01 {
		t.Errorf("error rate %.3f%% ≥ 1%%", 100*rate)
	}
	if got := float64(len(all)) / elapsed.Seconds(); got < 0.95*float64(rps) {
		t.Errorf("generator only achieved %.1f req/s of %d", got, rps)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
