package jobpush

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

const token = "Tok3nSecretXYZ"

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type mapSecrets struct {
	mu   sync.Mutex
	vals map[string]string
	err  error
}

func (m *mapSecrets) Get(_ context.Context, r vault.Ref) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	v, ok := m.vals[r.String()]
	if !ok {
		return "", fmt.Errorf("%w: %s", vault.ErrMissing, r)
	}
	return v, nil
}

type env struct {
	t       *testing.T
	srv     *httptest.Server
	base    string // http://localhost:<port>
	reqs    chan url.Values
	got     chan struct{}
	release chan struct{}
	block   func(r *http.Request) bool
	status  int
	body    string

	secrets  *mapSecrets
	prefixes []string
	gone     bool
	cmu      sync.Mutex

	nmu sync.Mutex
	now time.Time

	rmu     sync.Mutex
	results map[string]int

	logs *syncBuf
	p    *Pusher
}

func newEnv(t *testing.T, mod func(*Config)) *env {
	t.Helper()
	e := &env{
		t: t, reqs: make(chan url.Values, 2000), got: make(chan struct{}, 100), release: make(chan struct{}),
		status: 200, body: `{"ok":true}`, secrets: &mapSecrets{vals: map[string]string{}},
		prefixes: []string{"Root/"}, now: time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC),
		results: map[string]int{}, logs: &syncBuf{},
	}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.reqs <- r.URL.Query()
		if e.block != nil && e.block(r) {
			e.got <- struct{}{}
			select {
			case <-e.release:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(e.status)
		_, _ = w.Write([]byte(e.body))
	}))
	t.Cleanup(func() {
		select {
		case <-e.release:
		default:
			close(e.release)
		}
		e.srv.Close()
	})
	u, _ := url.Parse(e.srv.URL)
	e.base = "http://localhost:" + u.Port()
	e.secrets.vals["Root/KUMA"] = e.base + "/api/push/" + token + "?status=up&msg=OK&ping="
	cfg := Config{
		Secrets: e.secrets,
		Clients: func(context.Context, string) (model.Principal, error) {
			e.cmu.Lock()
			defer e.cmu.Unlock()
			if e.gone {
				return model.Principal{}, ErrClientGone
			}
			return model.Principal{ClientID: "c1", SecretRefPrefixes: e.prefixes}, nil
		},
		Hosts:   []string{"localhost"},
		HTTP:    secure.NewHTTPClient(time.Second, func(string, netip.Addr) bool { return true }),
		Timeout: 200 * time.Millisecond,
		Count: func(_, _, result string) {
			e.rmu.Lock()
			e.results[result]++
			e.rmu.Unlock()
		},
		Log: slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now: func() time.Time {
			e.nmu.Lock()
			defer e.nmu.Unlock()
			return e.now
		},
	}
	if mod != nil {
		mod(&cfg)
	}
	e.p = New(cfg)
	return e
}

func (e *env) advance(d time.Duration) {
	e.nmu.Lock()
	e.now = e.now.Add(d)
	e.nmu.Unlock()
}

func (e *env) result(r string) int {
	e.rmu.Lock()
	defer e.rmu.Unlock()
	return e.results[r]
}

func (e *env) waitResult(r string, n int) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for e.result(r) < n {
		if time.Now().After(deadline) {
			e.t.Fatalf("result %s = %d, want %d (all: %v)", r, e.result(r), n, e.results)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (e *env) nextReq() url.Values {
	e.t.Helper()
	select {
	case q := <-e.reqs:
		return q
	case <-time.After(5 * time.Second):
		e.t.Fatal("no push request")
		return nil
	}
}

func (e *env) noReq() {
	e.t.Helper()
	select {
	case q := <-e.reqs:
		e.t.Fatalf("unexpected push request %v", q)
	default:
	}
}

func (e *env) waitGot() {
	e.t.Helper()
	select {
	case <-e.got:
	case <-time.After(5 * time.Second):
		e.t.Fatal("server never got the request")
	}
}

func (e *env) start(ctx context.Context) chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); e.p.Run(ctx) }()
	return done
}

func up(job string) Push {
	return Push{JobID: job, JobName: job, ClientID: "c1", RunID: "r-" + job, Ref: "Root/KUMA", Up: true, Status: "up", PingMs: 1234}
}

func down(job, status string) Push {
	return Push{JobID: job, JobName: job, ClientID: "c1", RunID: "r-" + job, Ref: "Root/KUMA", Status: status, PingMs: -1}
}

func TestPushUpSendsQuery(t *testing.T) {
	e := newEnv(t, nil)
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	q := e.nextReq()
	if q.Get("status") != "up" || q.Get("msg") != "OK" || q.Get("ping") != "1234" || len(q) != 3 {
		t.Fatalf("query = %v", q)
	}
	if e.result(ResultOK) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestPushDownMsgIsStatusOnlyNoPing(t *testing.T) {
	e := newEnv(t, nil)
	x := down("a", "timeout")
	x.PingMs = 500
	e.p.Enqueue(x)
	e.p.Drain(context.Background())
	q := e.nextReq()
	if q.Get("status") != "down" || q.Get("msg") != "timeout" || q.Has("ping") {
		t.Fatalf("query = %v", q)
	}
}

func TestCheckURLRules(t *testing.T) {
	hosts := []string{"status.example.com", "uptime-kuma", "127.0.0.1"}
	tok := "/api/push/" + token
	for _, c := range []struct {
		raw   string
		hosts []string
		want  error
	}{
		{"https://status.example.com" + tok, hosts, nil},
		{"https://Status.Example.com." + tok, hosts, nil},
		{"https://status.example.com:443" + tok, hosts, nil},
		{"http://uptime-kuma:3001" + tok, hosts, nil},
		{"https://status.example.com" + tok + "?status=up&msg=OK&ping=", hosts, nil},
		{"https://status.example.com/" + strings.Repeat("a", 2048), hosts, ErrURLTooLong},
		{"://bad", hosts, ErrURLParse},
		{"/api/push/" + token, hosts, ErrURLParse},
		{"http://status.example.com" + tok, hosts, ErrURLScheme},
		{"ftp://uptime-kuma" + tok, hosts, ErrURLScheme},
		{"http://127.0.0.1" + tok, hosts, ErrURLScheme},
		{"https://u:p@status.example.com" + tok, hosts, ErrURLUserinfo},
		{"https://status.example.com" + tok, nil, ErrHostsEmpty},
		{"https://evil.example.com" + tok, hosts, ErrHostNotListed},
		{"https://status.example.com:8443" + tok, hosts, ErrURLPort},
		{"https://status.example.com/api/push/short", hosts, ErrURLPath},
		{"https://status.example.com/api/v1/monitors", hosts, ErrURLPath},
		{"https://status.example.com/api/push/abc%41defgh", hosts, ErrURLPath},
		{"https://status.example.com/api/push/" + token + "/x", hosts, ErrURLPath},
	} {
		_, err := CheckURL(c.raw, c.hosts)
		if !errors.Is(err, c.want) {
			t.Errorf("%.80s: err = %v, want %v", c.raw, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), token) {
			t.Errorf("error echoes the token: %v", err)
		}
	}
}

func TestRefRevokedRefused(t *testing.T) {
	e := newEnv(t, nil)
	e.prefixes = []string{"Other/"}
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.noReq()
	if e.result(ResultRefused) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestClientGoneRefused(t *testing.T) {
	e := newEnv(t, nil)
	e.gone = true
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.noReq()
	if e.result(ResultRefused) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestVaultMissingRefused(t *testing.T) {
	e := newEnv(t, nil)
	delete(e.secrets.vals, "Root/KUMA")
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.noReq()
	if e.result(ResultRefused) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestVaultDownFailed(t *testing.T) {
	e := newEnv(t, nil)
	e.secrets.err = errors.New("vault unreachable")
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.noReq()
	if e.result(ResultFailed) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestNon2xxFailed(t *testing.T) {
	e := newEnv(t, nil)
	e.status, e.body = 404, `{"ok":false,"msg":"Monitor not found or not active."}`
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.nextReq()
	if e.result(ResultFailed) != 1 {
		t.Fatalf("results = %v", e.results)
	}
	logs := e.logs.String()
	if !strings.Contains(logs, "Monitor not found or not active.") || !strings.Contains(logs, "status=404") {
		t.Fatalf("log lacks Kuma's reply: %s", logs)
	}
	if strings.Contains(logs, token) {
		t.Fatalf("log has the token: %s", logs)
	}
}

func TestURLNeverLogged(t *testing.T) {
	e := newEnv(t, nil)
	e.block = func(r *http.Request) bool { return r.URL.Query().Get("msg") == "timeout" }
	// ok, non-2xx, timeout, refused by rule, refused at dial.
	e.p.Enqueue(up("ok"))
	e.p.Drain(context.Background())
	e.status = 500
	e.p.Enqueue(up("non2xx"))
	e.p.Drain(context.Background())
	e.p.Enqueue(down("slow", "timeout"))
	e.p.Drain(context.Background())
	e.secrets.vals["Root/KUMA"] = "https://evil.example.com/api/push/" + token
	e.p.Enqueue(up("rule"))
	e.p.Drain(context.Background())

	d := newEnv(t, func(c *Config) {
		c.HTTP = secure.NewHTTPClient(time.Second, secure.IPAllowed)
		c.Hosts = []string{"127.0.0.1"}
		c.Log = e.p.cfg.Log
	})
	d.secrets.vals["Root/KUMA"] = "https://127.0.0.1/api/push/" + token
	d.p.Enqueue(up("dial"))
	d.p.Drain(context.Background())

	logs := e.logs.String()
	for _, want := range []string{"result=ok", "status=500", "result=refused", "not allowed"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q: %s", want, logs)
		}
	}
	if e.result(ResultFailed) != 2 {
		t.Errorf("results = %v", e.results)
	}
	if strings.Contains(logs, token) {
		t.Fatalf("log has the token: %s", logs)
	}
}

func TestCoalesceSameState20s(t *testing.T) {
	e := newEnv(t, nil)
	send := func(x Push) {
		e.p.Enqueue(x)
		e.p.Drain(context.Background())
		e.advance(5 * time.Second)
	}
	send(up("a"))
	send(up("a"))
	if len(e.reqs) != 1 || e.result(ResultCoalesced) != 1 {
		t.Fatalf("two ups: %d requests, results %v", len(e.reqs), e.results)
	}
	send(down("a", "failed"))
	send(up("a"))
	if len(e.reqs) != 3 {
		t.Fatalf("up, down, up: %d requests", len(e.reqs))
	}
	send(down("b", "failed"))
	send(down("b", "failed"))
	if len(e.reqs) != 4 || e.result(ResultCoalesced) != 2 {
		t.Fatalf("two downs: %d requests, results %v", len(e.reqs), e.results)
	}
	e.advance(20 * time.Second)
	send(down("b", "failed"))
	if len(e.reqs) != 5 {
		t.Fatalf("same state after the window must be sent: %d requests", len(e.reqs))
	}
}

func TestLatestWinsReplacesPending(t *testing.T) {
	e := newEnv(t, nil)
	e.block = func(r *http.Request) bool { return r.URL.Query().Get("status") == "up" }
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	e.p.Enqueue(up("a"))
	e.waitGot()
	e.p.Enqueue(up("a"))
	e.p.Enqueue(down("a", "failed"))
	if e.result(ResultReplaced) != 1 {
		t.Fatalf("results = %v", e.results)
	}
	close(e.release)
	e.nextReq()
	if q := e.nextReq(); q.Get("status") != "down" {
		t.Fatalf("second push = %v", q)
	}
	e.waitResult(ResultOK, 2)
	e.noReq()
	cancel()
	<-done
}

func TestTimeoutFailed(t *testing.T) {
	e := newEnv(t, nil)
	e.block = func(r *http.Request) bool { return r.URL.Query().Get("msg") == "timeout" }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := e.start(ctx)
	e.p.Enqueue(down("slow", "timeout"))
	e.p.Enqueue(up("fast"))
	e.waitResult(ResultFailed, 1)
	e.waitResult(ResultOK, 1)
	cancel()
	<-done
}

func TestForgetDropsPending(t *testing.T) {
	e := newEnv(t, nil)
	e.p.Enqueue(up("a"))
	e.p.Forget("a")
	e.p.Drain(context.Background())
	e.noReq()
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	e.nextReq()
}

func TestEnqueueBeforeRun(t *testing.T) {
	e := newEnv(t, nil)
	for i := range 1000 {
		e.p.Enqueue(up(fmt.Sprintf("job-%d", i)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	e.waitResult(ResultOK, 1000)
	if e.result(ResultReplaced)+e.result(ResultCoalesced)+e.result(ResultFailed) != 0 {
		t.Fatalf("results = %v", e.results)
	}
	cancel()
	<-done
}

func TestRunReturnsAfterWorkerExits(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Timeout = 5 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle Run did not return on cancel")
	}

	e.block = func(*http.Request) bool { return true }
	ctx, cancel = context.WithCancel(context.Background())
	done = e.start(ctx)
	e.p.Enqueue(up("a"))
	e.waitGot()
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while a push was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(e.release)
	<-done
	if e.result(ResultOK) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestStopDoesNotAbortInflightPush(t *testing.T) {
	e := newEnv(t, nil)
	e.block = func(*http.Request) bool { return true }
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	e.p.Enqueue(up("a"))
	e.waitGot()
	cancel()
	close(e.release) // within Timeout (200 ms)
	<-done
	if e.result(ResultOK) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestDrainStopsWhenUnderOneSecondLeft(t *testing.T) {
	e := newEnv(t, nil)
	e.p.Enqueue(up("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	start := time.Now()
	e.p.Drain(ctx)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("Drain took %s", time.Since(start))
	}
	e.noReq()
}

// The plan's 100 ms ctx would pop nothing under the < 1 s rule, so the
// deadline sits just above it.
func TestDrainHonoursDeadlineDuringSlowPush(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.Timeout = 5 * time.Second })
	e.block = func(*http.Request) bool { return true }
	e.p.Enqueue(up("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	defer cancel()
	start := time.Now()
	e.p.Drain(ctx)
	if el := time.Since(start); el > 1400*time.Millisecond {
		t.Fatalf("Drain took %s", el)
	}
	if e.result(ResultFailed) != 1 {
		t.Fatalf("results = %v", e.results)
	}
}

func TestForgetConcurrentWithSend(t *testing.T) {
	e := newEnv(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	for i := range 200 {
		job := fmt.Sprintf("job-%d", i%7)
		e.p.Enqueue(up(job))
		e.p.Forget(job)
		e.p.Enqueue(down(job, "failed"))
	}
	cancel()
	<-done
}

func TestDrainAfterRunDeliversPending(t *testing.T) {
	e := newEnv(t, nil)
	e.block = func(r *http.Request) bool { return r.URL.Query().Get("status") == "up" }
	ctx, cancel := context.WithCancel(context.Background())
	done := e.start(ctx)
	e.p.Enqueue(up("a"))
	e.waitGot()
	e.p.Enqueue(down("b", "failed"))
	cancel()
	close(e.release)
	<-done
	e.nextReq() // a
	e.noReq()

	cancelled, c2 := context.WithCancel(context.Background())
	c2()
	e.p.Drain(cancelled)
	e.noReq()

	dctx, c3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer c3()
	e.p.Drain(dctx)
	if q := e.nextReq(); q.Get("status") != "down" {
		t.Fatalf("drained push = %v", q)
	}
}

func TestBlockedAddressRefusedAtDial(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.HTTP = secure.NewHTTPClient(time.Second, secure.IPAllowed)
		c.Hosts = []string{"127.0.0.1"}
	})
	e.secrets.vals["Root/KUMA"] = "https://127.0.0.1/api/push/" + token
	e.p.Enqueue(up("a"))
	e.p.Drain(context.Background())
	if e.result(ResultFailed) != 1 {
		t.Fatalf("results = %v", e.results)
	}
	if !strings.Contains(e.logs.String(), secure.ErrBlockedAddress.Error()) {
		t.Fatalf("log lacks the dial refusal: %s", e.logs.String())
	}
}
