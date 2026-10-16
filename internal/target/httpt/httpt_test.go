package httpt

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

var now = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)

func exec() *Executor {
	x := New(secure.NewHTTPClient(2*time.Second, func(string, netip.Addr) bool { return true }))
	x.Now = func() time.Time { return now }
	return x
}

func input(t *testing.T, url, mode string) target.ExecInput {
	t.Helper()
	return target.ExecInput{
		Run: model.Run{ID: "run-1", JobName: "tick", Attempt: 2, Kind: model.KindRetry, ContextFor: now,
			Payload: json.RawMessage(`{"n":1}`)},
		Spec: model.RunSpec{Target: model.Target{Type: model.TargetHTTP, HTTP: &model.HTTPTarget{
			Method: "POST", URL: url, Mode: mode, LeaseSec: 300, Headers: map[string]string{"X-Trace": "abc"},
			Auth: model.HTTPAuth{BearerRef: "Svc/TOKEN", HMACRef: "Svc/HMAC"},
		}}},
		Secrets:  map[string]string{"Svc/TOKEN": "bearer-secret-value", "Svc/HMAC": "hmac-secret"},
		RunToken: "rt-1",
		LogPath:  filepath.Join(t.TempDir(), "run.log"),
	}
}

func serve(t *testing.T, h http.HandlerFunc) string {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func reply(code int, body string, hdr ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(hdr); i += 2 {
			w.Header().Set(hdr[i], hdr[i+1])
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
}

func TestRequestShape(t *testing.T) {
	var got *http.Request
	var body []byte
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	})
	in := input(t, url, "async")
	res := exec().Execute(context.Background(), in)
	if res.Status != model.StatusSuccess {
		t.Fatalf("res = %+v", res)
	}
	for k, want := range map[string]string{
		"X-Plr-Run-Id": "run-1", "X-Plr-Job": "tick", "X-Plr-Attempt": "2", "X-Plr-Trigger": "retry",
		"X-Plr-Scheduled-For": "2026-10-13T09:00:00.000Z", "Authorization": "Bearer bearer-secret-value",
		"X-Plr-Run-Token": "rt-1", "X-Trace": "abc", "Content-Type": "application/json",
	} {
		if got.Header.Get(k) != want {
			t.Errorf("%s = %q, want %q", k, got.Header.Get(k), want)
		}
	}
	if want := secure.Sign([]byte("hmac-secret"), "1791882000", body); got.Header.Get("X-Plr-Signature") != want {
		t.Errorf("signature = %q, want %q", got.Header.Get("X-Plr-Signature"), want)
	}
	var env map[string]any
	_ = json.Unmarshal(body, &env)
	if env["run_id"] != "run-1" || env["trigger"] != "retry" || env["payload"].(map[string]any)["n"] != float64(1) {
		t.Fatalf("envelope = %s", body)
	}
	log, _ := os.ReadFile(in.LogPath)
	if strings.Contains(string(log), "bearer-secret-value") || !strings.Contains(string(log), "← 200") {
		t.Fatalf("log = %s", log)
	}
}

func TestSyncModeOmitsRunToken(t *testing.T) {
	var tok string
	url := serve(t, func(w http.ResponseWriter, r *http.Request) {
		tok = r.Header.Get("X-PLR-Run-Token")
		w.WriteHeader(202)
	})
	res := exec().Execute(context.Background(), input(t, url, "sync"))
	if tok != "" || res.Status != model.StatusSuccess || res.Waiting {
		t.Fatalf("sync 202 = success without a run token: tok=%q res=%+v", tok, res)
	}
}

func TestResponseTable(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		h     http.HandlerFunc
		check func(target.Result) bool
	}{
		{"async 202 waits", "async", reply(202, ""), func(r target.Result) bool { return r.Waiting }},
		{"async 200 finishes inline", "async", reply(200, `{"a":1}`), func(r target.Result) bool { return r.Status == model.StatusSuccess && !r.Waiting }},
		{"defer_until", "sync", reply(200, `{"defer_until":"2026-10-13T10:00:00Z"}`), func(r target.Result) bool {
			return r.DeferUntil != nil && r.DeferUntil.Equal(now.Add(time.Hour)) && r.Status == ""
		}},
		{"defer_until bad", "sync", reply(200, `{"defer_until":"tomorrow"}`), func(r target.Result) bool {
			return r.Status == model.StatusSuccess && r.OutputError != ""
		}},
		{"redirect", "sync", reply(302, "", "Location", "/x"), func(r target.Result) bool { return r.Status == model.StatusFailed && r.NoRetry }},
		{"429 delta", "sync", reply(429, "", "Retry-After", "120"), func(r target.Result) bool {
			return r.DeferUntil != nil && r.DeferUntil.Equal(now.Add(2*time.Minute))
		}},
		{"503 date", "sync", reply(503, "", "Retry-After", "Tue, 13 Oct 2026 09:05:00 GMT"), func(r target.Result) bool {
			return r.DeferUntil != nil && r.DeferUntil.Equal(now.Add(5*time.Minute))
		}},
		{"503 no retry-after", "sync", reply(503, ""), func(r target.Result) bool { return r.Status == model.StatusFailed && r.Ambiguous }},
		{"404", "sync", reply(404, "nope"), func(r target.Result) bool {
			return r.Status == model.StatusFailed && r.NoRetry && string(r.Output) == `"nope"`
		}},
		{"500", "sync", reply(500, ""), func(r target.Result) bool { return r.Status == model.StatusFailed && r.Ambiguous && r.Delivered }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := exec().Execute(context.Background(), input(t, serve(t, c.h), c.mode))
			if !c.check(res) {
				t.Fatalf("res = %+v", res)
			}
		})
	}
}

func TestBodyTruncatedAndRedacted(t *testing.T) {
	big := strings.Repeat("x", model.MaxOutputBytes+10)
	res := exec().Execute(context.Background(), input(t, serve(t, reply(200, big)), "sync"))
	if res.Status != model.StatusSuccess || !res.OutputTruncated {
		t.Fatalf("res = %+v", res.OutputTruncated)
	}
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, `{"echo":"bearer-secret-value"}`)), "sync"))
	if strings.Contains(string(res.Output), "bearer-secret-value") {
		t.Fatalf("secret stored in output: %s", res.Output)
	}
}

func TestDeliveryClassification(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: connection refused
	res := exec().Execute(context.Background(), input(t, "http://"+addr+"/", "sync"))
	if !res.Unsent || res.Delivered || res.Ambiguous {
		t.Fatalf("refused must be provably unsent: %+v", res)
	}

	ln, _ = net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = ln.Close() }()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 4096)
		_, _ = c.Read(buf) // read the request, then hang up
		_ = c.Close()
	}()
	res = exec().Execute(context.Background(), input(t, "http://"+ln.Addr().String()+"/", "sync"))
	if res.Unsent || !res.Ambiguous {
		t.Fatalf("reset after send must be ambiguous: %+v", res)
	}
}

func TestTimeout(t *testing.T) {
	url := serve(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res := exec().Execute(ctx, input(t, url, "sync"))
	if res.Status != model.StatusTimeout || !res.Ambiguous {
		t.Fatalf("res = %+v", res)
	}
}
