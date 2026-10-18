package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/jobpush"
	"github.com/sarvinshrivastava/pipeline-runner/internal/metrics"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

const pushToken = "Tok3nSecretXYZ"

type fakePusher struct {
	mu      sync.Mutex
	pushes  []jobpush.Push
	forgets []string
}

func (f *fakePusher) Enqueue(x jobpush.Push) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushes = append(f.pushes, x)
}

func (f *fakePusher) Forget(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgets = append(f.forgets, jobID)
}

func (f *fakePusher) all() []jobpush.Push {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]jobpush.Push(nil), f.pushes...)
}

func (f *fakePusher) forgot(jobID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.forgets {
		if id == jobID {
			return true
		}
	}
	return false
}

func pushPolicy(p *model.PolicyIn) *model.PolicyIn {
	if p == nil {
		p = &model.PolicyIn{}
	}
	p.PushURLRef = ptr("Svc/KUMA")
	return p
}

func TestPushOnSuccess(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	j := h.create(cronJob("tick", "* * * * *", pushPolicy(nil)))
	first := h.runs(j.ID)[0]
	h.advance(time.Minute)
	h.waitStatus(first.ID, model.StatusSuccess)
	ps := fp.all()
	if len(ps) != 1 {
		t.Fatalf("pushes = %+v", ps)
	}
	if p := ps[0]; !p.Up || p.Status != "up" || p.PingMs < 0 || p.JobID != j.ID || p.JobName != "tick" ||
		p.ClientID != h.owner.ClientID || p.RunID != first.ID || p.Ref != "Svc/KUMA" {
		t.Fatalf("push = %+v", p)
	}
}

func TestNoPushWhenRefUnset(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	j := h.create(invokeJob("f", nil))
	h.waitStatus(invoke(t, h, j.ID).ID, model.StatusSuccess)
	if ps := fp.all(); len(ps) != 0 {
		t.Fatalf("pushes = %+v", ps)
	}
}

func TestNoPushWhileRetryPending(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	h.exec.set(failAmbiguous)
	j := h.create(invokeJob("f", pushPolicy(&model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(2), BackoffSec: ptr(int64(30))}})))
	root := invoke(t, h, j.ID)
	h.waitStatus(root.ID, model.StatusFailed)
	if ps := fp.all(); len(ps) != 0 {
		t.Fatalf("a failure with a retry pending must not push: %+v", ps)
	}
	h.advance(30 * time.Second)
	retry := h.runs(j.ID)[1]
	h.waitStatus(retry.ID, model.StatusFailed)
	ps := fp.all()
	if len(ps) != 1 || ps[0].Up || ps[0].Status != "failed" || ps[0].PingMs >= 0 || ps[0].RunID != retry.ID {
		t.Fatalf("final failure push = %+v", ps)
	}
}

func TestNoDownWhenPushOnFailureFalse(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	h.exec.set(failAmbiguous)
	j := h.create(invokeJob("f", pushPolicy(&model.PolicyIn{PushOnFailure: ptr(false)})))
	h.waitStatus(invoke(t, h, j.ID).ID, model.StatusFailed)
	if ps := fp.all(); len(ps) != 0 {
		t.Fatalf("pushes = %+v", ps)
	}
	h.exec.set(func(context.Context, target.ExecInput) target.Result { return okResult() })
	h.waitStatus(invoke(t, h, j.ID).ID, model.StatusSuccess)
	if ps := fp.all(); len(ps) != 1 || !ps[0].Up {
		t.Fatalf("success must still push up: %+v", ps)
	}
}

func TestNoPushForSkippedCancelledMissed(t *testing.T) {
	t.Run("missed", func(t *testing.T) {
		fp := &fakePusher{}
		h := newHarness(t, withPusher(fp))
		j := h.create(cronJob("tick", "* * * * *", pushPolicy(&model.PolicyIn{MaxLateSec: ptr(int64(30))})))
		first := h.runs(j.ID)[0]
		h.clk.Set(t0.Add(time.Hour))
		h.sync()
		if r := h.run(first.ID); r.Status != model.StatusMissed {
			t.Fatalf("status = %s", r.Status)
		}
		h.sync()
		// The outage collapses to the current occurrence, which may run and
		// push up; the missed run itself must push nothing.
		for _, p := range fp.all() {
			if p.RunID == first.ID || !p.Up {
				t.Fatalf("pushes = %+v", fp.all())
			}
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		fp := &fakePusher{}
		h := newHarness(t, withPusher(fp))
		g := newGate()
		defer g.open()
		h.exec.set(g.behaviour(okResult()))
		j := h.create(invokeJob("f", pushPolicy(nil)))
		r := invoke(t, h, j.ID)
		h.waitStatus(r.ID, model.StatusRunning)
		if _, _, err := h.e.Cancel(context.Background(), r.ID); err != nil {
			t.Fatal(err)
		}
		h.waitStatus(r.ID, model.StatusCancelled)
		if ps := fp.all(); len(ps) != 0 {
			t.Fatalf("pushes = %+v", ps)
		}
	})
	t.Run("skipped", func(t *testing.T) {
		fp := &fakePusher{}
		h := newHarness(t, withPusher(fp))
		g := newGate()
		h.exec.set(g.behaviour(okResult()))
		j := h.create(cronJob("slow", "* * * * *", pushPolicy(&model.PolicyIn{TimeoutSec: ptr(int64(3600))})))
		first := h.runs(j.ID)[0]
		h.advance(time.Minute)
		h.waitStatus(first.ID, model.StatusRunning)
		h.advance(time.Minute)
		if r := h.runs(j.ID)[1]; r.Status != model.StatusSkipped {
			t.Fatalf("second run = %s", r.Status)
		}
		if ps := fp.all(); len(ps) != 0 {
			t.Fatalf("pushes = %+v", ps)
		}
		g.open()
		h.waitStatus(first.ID, model.StatusSuccess)
		if ps := fp.all(); len(ps) != 1 || !ps[0].Up {
			t.Fatalf("the running run pushes when it finishes: %+v", ps)
		}
	})
}

func TestTimeoutAndInterruptedPushDown(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		return target.Result{Status: model.StatusTimeout, Error: "timed out"}
	})
	j := h.create(invokeJob("f", pushPolicy(nil)))
	h.waitStatus(invoke(t, h, j.ID).ID, model.StatusTimeout)

	h.exec.set(failAmbiguous)
	amo := h.create(invokeJob("amo", pushPolicy(&model.PolicyIn{Delivery: ptr(model.AtMostOnce)})))
	h.waitStatus(invoke(t, h, amo.ID).ID, model.StatusInterrupted)

	ps := fp.all()
	if len(ps) != 2 || ps[0].Up || ps[0].Status != "timeout" || ps[1].Up || ps[1].Status != "interrupted" {
		t.Fatalf("pushes = %+v", ps)
	}
}

func TestDeletedJobNoPush(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	g := newGate()
	defer g.open()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", pushPolicy(nil)))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusRunning)
	if err := h.e.DeleteJob(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(r.ID, model.StatusCancelled)
	if ps := fp.all(); len(ps) != 0 {
		t.Fatalf("pushes = %+v", ps)
	}
	if !fp.forgot(j.ID) {
		t.Fatal("delete must Forget the job's pending push")
	}
}

func TestPushAfterUnflushedReplay(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	ctx := context.Background()
	if _, err := h.st.W.ExecContext(ctx, `CREATE TRIGGER boom BEFORE UPDATE OF status ON runs
		WHEN NEW.status = 'success' BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	j := h.create(invokeJob("f", pushPolicy(nil)))
	r := invoke(t, h, j.ID)
	h.waitFor("unflushed", func() bool { return h.e.Unflushed() == 1 })
	if ps := fp.all(); len(ps) != 0 {
		t.Fatalf("no push for an unsaved result: %+v", ps)
	}
	if _, err := h.st.W.ExecContext(ctx, `DROP TRIGGER boom`); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Second)
	h.waitStatus(r.ID, model.StatusSuccess)
	if ps := fp.all(); len(ps) != 1 || !ps[0].Up {
		t.Fatalf("pushes after replay = %+v", ps)
	}
}

func fieldErrOf(t *testing.T, err error) *model.FieldError {
	t.Helper()
	fe, ok := model.AsFieldError(err)
	if !ok {
		t.Fatalf("not a FieldError: %v", err)
	}
	return fe
}

func TestCreateRefMissing400(t *testing.T) {
	h := newHarness(t, withSecrets(&mapSecrets{vals: map[string]string{}}))
	_, err := h.e.CreateJob(context.Background(), h.owner, invokeJob("f", pushPolicy(nil)), nil)
	if fe := fieldErrOf(t, err); fe.Field != "policy.push_url_ref" || fe.Code != "secret_ref_missing" {
		t.Fatalf("err = %+v", fe)
	}
}

func TestCreateRefNotPermitted400(t *testing.T) {
	h := newHarness(t, withSecrets(&mapSecrets{vals: map[string]string{"Other/KUMA": "x"}}))
	_, err := h.e.CreateJob(context.Background(), h.owner, invokeJob("f", &model.PolicyIn{PushURLRef: ptr("Other/KUMA")}), nil)
	if fe := fieldErrOf(t, err); fe.Field != "policy.push_url_ref" || fe.Code != "secret_ref_not_permitted" {
		t.Fatalf("err = %+v", fe)
	}
}

func checksOf(t *testing.T, h *harness, ref string) map[string]Check {
	t.Helper()
	j, err := model.ValidateJobCreate(invokeJob("f", &model.PolicyIn{PushURLRef: ptr(ref)}), h.e.vctx(h.owner))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Check{}
	for _, c := range h.e.CheckJob(context.Background(), j, h.owner) {
		out[c.Name] = c
	}
	return out
}

func TestCheckJobPushChecks(t *testing.T) {
	sec := &mapSecrets{vals: map[string]string{
		"Svc/KUMA": " https://status.example.com/api/push/" + pushToken + "?status=up&msg=OK&ping=\n",
		"Svc/BAD":  "https://status.example.com/api/v1/monitors",
	}}
	h := newHarness(t, withSecrets(sec), withPushHosts([]string{"status.example.com"}))

	c := checksOf(t, h, "Svc/KUMA")
	if !c["push_url_ref"].OK || !c["push_url"].OK {
		t.Fatalf("ok case: %+v", c)
	}
	if _, ok := c["push_dns"]; ok {
		t.Fatal("no push_dns row without a callback policy")
	}

	c = checksOf(t, h, "Svc/NOPE")
	if r := c["push_url_ref"]; r.OK || !strings.Contains(r.Detail, "does not exist") {
		t.Fatalf("missing: %+v", c)
	}
	if _, ok := c["push_url"]; ok {
		t.Fatal("no push_url row when the ref is missing")
	}

	c = checksOf(t, h, "Svc/BAD")
	if r := c["push_url"]; r.OK || r.Detail != "path must be /api/push/<token>" {
		t.Fatalf("bad path: %+v", c)
	}
}

func TestCheckJobPushVaultDown(t *testing.T) {
	sec := &mapSecrets{vals: map[string]string{}}
	h := newHarness(t, withSecrets(sec), withPushHosts([]string{"status.example.com"}))
	sec.mu.Lock()
	sec.err = errors.New("vault unreachable")
	sec.mu.Unlock()
	c := checksOf(t, h, "Svc/KUMA")
	if r := c["push_url_ref"]; r.OK || r.Detail != ErrVaultDown.Error() {
		t.Fatalf("vault down: %+v", c)
	}
	if _, ok := c["push_url"]; ok {
		t.Fatal("no push_url row when the vault is down")
	}
}

func TestCheckJobPushDetailNoToken(t *testing.T) {
	sec := &mapSecrets{vals: map[string]string{
		"Svc/PORT": "https://status.example.com:8443/api/push/" + pushToken,
		"Svc/LOOP": "https://127.0.0.1/api/push/" + pushToken,
	}}
	h := newHarness(t, withSecrets(sec), withPushHosts([]string{"status.example.com", "127.0.0.1"}),
		withCallbackPolicy(secure.IPAllowed))

	c := checksOf(t, h, "Svc/PORT")
	if r := c["push_url"]; r.OK || r.Detail != jobpush.ErrURLPort.Error() {
		t.Fatalf("port: %+v", c)
	}
	c2 := checksOf(t, h, "Svc/LOOP")
	if r := c2["push_dns"]; r.OK || !strings.HasPrefix(r.Detail, "127.0.0.1 resolves to ") {
		t.Fatalf("dns: %+v", c2)
	}
	for _, cs := range []map[string]Check{c, c2} {
		for _, r := range cs {
			if strings.Contains(r.Detail, pushToken) {
				t.Fatalf("detail has the token: %+v", r)
			}
		}
	}
}

func TestPatchToAtWithKeptRefRefused(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("tick", "* * * * *", pushPolicy(nil)))
	_, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Trigger: &model.TriggerIn{Type: model.TriggerDelay, DelaySec: ptr(int64(60))}})
	if fe := fieldErrOf(t, err); fe.Field != "policy.push_url_ref" || fe.Code != "invalid" {
		t.Fatalf("err = %+v", fe)
	}
}

func TestReconcileForgetsDeletedJob(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	j := h.create(invokeJob("f", pushPolicy(nil)))
	if err := store.DeleteJob(context.Background(), h.st.W, j.ID); err != nil {
		t.Fatal(err)
	}
	h.advance(61 * time.Second) // past reconcileEvery
	if !fp.forgot(j.ID) {
		t.Fatal("reconcile must Forget a job deleted behind the loop's back")
	}
}

func TestDeleteForgetsWithNilMetrics(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	if h.metrics != nil {
		t.Fatal("this test needs nil metrics")
	}
	j := h.create(invokeJob("f", pushPolicy(nil)))
	if err := h.e.DeleteJob(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()
	if !fp.forgot(j.ID) {
		t.Fatal("delete must Forget even without metrics")
	}
}

func pushSeries(t *testing.T, m *metrics.Metrics, job string) int {
	t.Helper()
	fams, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range fams {
		if f.GetName() != "plr_job_push_total" {
			continue
		}
		for _, mt := range f.GetMetric() {
			for _, l := range mt.GetLabel() {
				if l.GetName() == "job" && l.GetValue() == job {
					n++
				}
			}
		}
	}
	return n
}

func TestJobPushSeriesDeletedAfterInflightCompletes(t *testing.T) {
	got, release := make(chan struct{}, 4), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	u, _ := url.Parse(srv.URL)
	sec := &mapSecrets{vals: map[string]string{"Svc/KUMA": "http://localhost:" + u.Port() + "/api/push/" + pushToken}}

	m := metrics.New(func() float64 { return 0 })
	var owner model.Principal
	p := jobpush.New(jobpush.Config{
		Secrets: sec,
		Clients: func(context.Context, string) (model.Principal, error) { return owner, nil },
		Hosts:   []string{"localhost"},
		HTTP:    secure.NewHTTPClient(time.Second, func(string, netip.Addr) bool { return true }),
		Timeout: 5 * time.Second,
		Count:   m.AddJobPush,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	h := newHarness(t, withPusher(p), withSecrets(sec), func(h *harness) { h.metrics = m })
	owner = h.owner
	pctx, stop := context.WithCancel(context.Background())
	pdone := make(chan struct{})
	go func() { defer close(pdone); p.Run(pctx) }()
	defer func() { stop(); <-pdone }()

	j := h.create(invokeJob("tick", pushPolicy(nil)))
	h.waitStatus(invoke(t, h, j.ID).ID, model.StatusSuccess)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("push never reached the server")
	}
	if err := h.e.DeleteJob(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()
	close(release)
	h.waitFor("push counted after delete", func() bool { return pushSeries(t, m, "tick") == 1 })
	h.advance(61 * time.Second) // past reconcileEvery
	if n := pushSeries(t, m, "tick"); n != 0 {
		t.Fatalf("%d plr_job_push_total series left for a deleted job", n)
	}
}
