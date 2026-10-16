package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

func code(n int) *int { return &n }

func failAmbiguous(context.Context, target.ExecInput) target.Result {
	return target.Result{Status: model.StatusFailed, HTTPStatus: code(503), Error: "HTTP 503", Ambiguous: true, Delivered: true}
}

func invoke(t *testing.T, h *harness, jobID string) model.Run {
	t.Helper()
	r, err := h.e.Invoke(context.Background(), jobID, json.RawMessage(`{"x":1}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRetryBackoffSchedule(t *testing.T) {
	h := newHarness(t)
	h.exec.set(failAmbiguous)
	j := h.create(invokeJob("f", &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(3), BackoffSec: ptr(int64(30))}}))
	root := invoke(t, h, j.ID)
	h.waitStatus(root.ID, model.StatusFailed)
	rs := h.runs(j.ID)
	if len(rs) != 2 || rs[1].Kind != model.KindRetry || rs[1].Attempt != 2 || !rs[1].ScheduledFor.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("first retry: %+v", rs)
	}
	h.advance(30 * time.Second)
	h.waitStatus(rs[1].ID, model.StatusFailed)
	rs = h.runs(j.ID)
	if len(rs) != 3 || !rs[2].ScheduledFor.Equal(t0.Add(30*time.Second+60*time.Second)) || rs[2].ChainRootID != root.ID {
		t.Fatalf("second retry (backoff doubles): %+v", rs[2])
	}
	h.advance(60 * time.Second)
	h.waitStatus(rs[2].ID, model.StatusFailed)
	if len(h.runs(j.ID)) != 3 {
		t.Fatal("max_attempts exceeded")
	}
	if h.pager.count("failed") != 1 {
		t.Fatalf("only the final failure pages: %v", h.pager.titles())
	}
}

func TestAtMostOnceAmbiguousNeedsHumanUnsentRetries(t *testing.T) {
	h := newHarness(t)
	h.exec.set(failAmbiguous)
	retry := &model.RetryIn{MaxAttempts: ptr(3), BackoffSec: ptr(int64(1))}
	j := h.create(invokeJob("send", &model.PolicyIn{Delivery: ptr(model.AtMostOnce), Retry: retry}))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusInterrupted)
	if len(h.runs(j.ID)) != 1 || h.pager.count("NEEDS A HUMAN") != 1 {
		t.Fatalf("ambiguous at_most_once must not retry and must page: %v", h.pager.titles())
	}
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		return target.Result{Status: model.StatusFailed, Error: "connection refused", Unsent: true}
	})
	r2 := invoke(t, h, j.ID)
	h.waitStatus(r2.ID, model.StatusFailed)
	found := false
	for _, x := range h.runs(j.ID) {
		if x.ParentRunID == r2.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("provably unsent at_most_once must retry")
	}
}

func TestNoRetryFor4xx(t *testing.T) {
	h := newHarness(t)
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		return target.Result{Status: model.StatusFailed, HTTPStatus: code(400), NoRetry: true}
	})
	j := h.create(invokeJob("f", &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(5)}}))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusFailed)
	if len(h.runs(j.ID)) != 1 {
		t.Fatal("4xx must not retry")
	}
}

func TestDeferCapAndPast(t *testing.T) {
	h := newHarness(t)
	until := t0.Add(2 * time.Hour)
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		return target.Result{DeferUntil: &until, HTTPStatus: code(200)}
	})
	j := h.create(invokeJob("f", &model.PolicyIn{MaxDeferSec: ptr(int64(3600))}))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusFailed)
	if h.pager.count("defer cap") != 1 {
		t.Fatal("a defer past max_defer_sec must fail and page")
	}

	past := t0.Add(-time.Hour)
	calls := 0
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		calls++
		if calls == 1 {
			return target.Result{DeferUntil: &past, HTTPStatus: code(200)}
		}
		return okResult()
	})
	r2 := invoke(t, h, j.ID)
	h.waitStatus(r2.ID, model.StatusDeferred)
	var succ model.Run
	h.waitFor("defer successor runs", func() bool {
		for _, x := range h.runs(j.ID) {
			if x.ParentRunID == r2.ID {
				succ = x
				return x.Status == model.StatusSuccess
			}
		}
		return false
	})
	if succ.Kind != model.KindDefer || succ.Attempt != 1 {
		t.Fatalf("successor: %+v", succ)
	}
}

func asyncJob(name string) model.JobCreate {
	in := invokeJob(name, &model.PolicyIn{TimeoutSec: ptr(int64(600))})
	in.Target.HTTP.Mode = "async"
	in.Target.HTTP.LeaseSec = ptr(int64(60))
	return in
}

func waiting(context.Context, target.ExecInput) target.Result {
	return target.Result{Waiting: true, HTTPStatus: code(202), Delivered: true}
}

func TestAsyncCompleteAndLeaseExpiry(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	j := h.create(asyncJob("pub"))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusWaiting)
	_, _, inflight := h.counts()
	if inflight[poolHTTP] != 0 {
		t.Fatal("waiting must release the HTTP pool slot")
	}
	got, _, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success", Output: json.RawMessage(`{"media":1}`)})
	if err != nil || got.Status != model.StatusSuccess {
		t.Fatalf("complete: %v %+v", err, got)
	}

	r2 := invoke(t, h, j.ID)
	h.waitStatus(r2.ID, model.StatusWaiting)
	h.advance(61 * time.Second)
	h.waitStatus(r2.ID, model.StatusInterrupted)
	if _, _, err := h.e.Complete(context.Background(), r2.ID, model.CompleteReq{Status: "success"}); err != ErrNotWaiting { //nolint:errorlint
		t.Fatalf("late complete: %v", err)
	}
	h.waitFor("late-completion page", func() bool { return h.pager.count("late completion") == 1 })
}

func TestHeartbeatCappedByDeadline(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	in := asyncJob("pub")
	in.Policy = &model.PolicyIn{TimeoutSec: ptr(int64(90))}
	j := h.create(in)
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusWaiting)
	h.advance(50 * time.Second)
	_, lease, err := h.e.Heartbeat(context.Background(), r.ID)
	if err != nil || lease == nil || !lease.Equal(t0.Add(90*time.Second)) {
		t.Fatalf("lease must be capped at the deadline: %v %v", lease, err)
	}
	h.advance(41 * time.Second)
	h.waitStatus(r.ID, model.StatusTimeout)
}

// An expiry already queued when a heartbeat re-arms the lease must not
// interrupt the run, whether its generation is stale or the stored lease moved.
func TestStaleLeaseExpiryIgnoredAfterHeartbeat(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	j := h.create(asyncJob("pub"))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusWaiting)
	var before uint64
	h.onLoop(func() { before = h.e.leaseGen[r.ID] })
	h.advance(30 * time.Second)
	if _, _, err := h.e.Heartbeat(context.Background(), r.ID); err != nil {
		t.Fatal(err)
	}
	var current uint64
	h.onLoop(func() {
		current = h.e.leaseGen[r.ID]
		h.e.onLeaseExpired(r.ID, before) // stale generation
	})
	if current == before {
		t.Fatal("re-arm reused the lease generation")
	}
	h.onLoop(func() { h.e.onLeaseExpired(r.ID, current) }) // lease still valid in the DB
	if got := h.run(r.ID); got.Status != model.StatusWaiting {
		t.Fatalf("heartbeated run became %s", got.Status)
	}
}

func TestEarlyCompleteBeforeWaiting(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		<-release
		return target.Result{Waiting: true, HTTPStatus: code(202), Delivered: true}
	})
	j := h.create(asyncJob("pub"))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusRunning)
	_, accepted, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success"})
	if err != nil || !accepted {
		t.Fatalf("early complete: %v %v", accepted, err)
	}
	if _, accepted, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success"}); err != nil || !accepted {
		t.Fatal("identical duplicate must be accepted")
	}
	if _, _, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "failed"}); err != ErrConflict { //nolint:errorlint
		t.Fatalf("different duplicate: %v", err)
	}
	close(release)
	h.waitStatus(r.ID, model.StatusSuccess)
}

func TestEarlyCompleteThen5xxStillSucceeds(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	h.exec.set(func(ctx context.Context, in target.ExecInput) target.Result {
		<-release
		return failAmbiguous(ctx, in)
	})
	in := asyncJob("pub")
	in.Policy = &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(3)}}
	j := h.create(in)
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusRunning)
	if _, _, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	h.waitStatus(r.ID, model.StatusSuccess)
	if len(h.runs(j.ID)) != 1 {
		t.Fatal("the target's early success must win over a later 5xx (no retry)")
	}
}

func TestCancelPerState(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusRunning)
	_, accepted, err := h.e.Cancel(context.Background(), r.ID)
	if err != nil || !accepted {
		t.Fatalf("running cancel: %v %v", accepted, err)
	}
	h.waitStatus(r.ID, model.StatusCancelled)
	if _, _, err := h.e.Cancel(context.Background(), r.ID); err != ErrRunTerminal { //nolint:errorlint
		t.Fatalf("terminal cancel: %v", err)
	}
	if _, _, err := h.e.Cancel(context.Background(), "nope"); err != ErrNotFound { //nolint:errorlint
		t.Fatalf("unknown cancel: %v", err)
	}
	g.open()
}

func TestCancelVsFinishRace(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", &model.PolicyIn{ConcurrencyKey: ptr("k"), Retry: &model.RetryIn{MaxAttempts: ptr(3)}}))
	h.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-ctx.Done():
			return target.Result{Status: model.StatusFailed, Ambiguous: true}
		case <-time.After(time.Duration(time.Now().UnixNano()%3) * time.Millisecond):
			return okResult()
		}
	})
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		r := invoke(t, h, j.ID)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = h.e.Cancel(context.Background(), r.ID)
		}()
	}
	wg.Wait()
	h.waitFor("all runs terminal", func() bool {
		for _, r := range h.runs(j.ID) {
			if !r.Status.Terminal() {
				return false
			}
		}
		return true
	})
	for _, r := range h.runs(j.ID) {
		if r.Kind == model.KindRetry && r.ParentRunID != "" {
			if p := h.run(r.ParentRunID); p.Status == model.StatusCancelled {
				t.Fatalf("cancelled run %s got a retry successor", p.ID)
			}
		}
	}
	jb, kb, inf := h.counts()
	if len(jb)+len(kb)+len(inf) != 0 {
		t.Fatalf("slots leaked or negative: %v %v %v", jb, kb, inf)
	}
}

func TestLeaseExpiryVsCompleteSameInstant(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	j := h.create(asyncJob("pub"))
	for i := 0; i < 50; i++ {
		r := invoke(t, h, j.ID)
		h.waitStatus(r.ID, model.StatusWaiting)
		done := make(chan struct{})
		go func() {
			_, _, _ = h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success"})
			close(done)
		}()
		h.clk.Advance(61 * time.Second)
		<-done
		h.waitFor("terminal", func() bool { return h.run(r.ID).Status.Terminal() })
	}
	_, kb, inf := h.counts()
	if len(kb)+len(inf) != 0 {
		t.Fatalf("leaked: %v %v", kb, inf)
	}
}

func TestDeleteJobCancelsEverything(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", &model.PolicyIn{Overlap: ptr(model.OverlapQueue)}))
	running := invoke(t, h, j.ID)
	h.waitStatus(running.ID, model.StatusRunning)
	queued := invoke(t, h, j.ID)
	h.sync()
	if err := h.e.DeleteJob(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(queued.ID, model.StatusCancelled)
	h.waitStatus(running.ID, model.StatusCancelled)
	jb, _, inf := h.counts()
	if len(jb)+len(inf) != 0 {
		t.Fatalf("leaked: %v %v", jb, inf)
	}
	if _, err := store.JobRevision(context.Background(), h.st.W, j.ID, 1); err != nil {
		t.Fatal("history must keep the revision")
	}
}

func TestInvokeDisabledRejected(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	if _, err := h.e.SetEnabled(context.Background(), j.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.Invoke(context.Background(), j.ID, nil, nil, nil); err != ErrJobDisabled { //nolint:errorlint
		t.Fatalf("err = %v", err)
	}
}

func TestAwait(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusSuccess)
	got, done := h.e.Await(context.Background(), r.ID, time.Second) // already finished
	if !done || got.Status != model.StatusSuccess {
		t.Fatalf("await finished run: %v %+v", done, got)
	}

	calls := 0
	h.exec.set(func(context.Context, target.ExecInput) target.Result {
		calls++
		if calls == 1 {
			return target.Result{Status: model.StatusFailed, Unsent: true}
		}
		return okResult()
	})
	j2 := h.create(invokeJob("g", &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(2), BackoffSec: ptr(int64(5))}}))
	r2 := invoke(t, h, j2.ID)
	res := make(chan model.Run, 1)
	go func() {
		got, _ := h.e.Await(context.Background(), r2.ID, 5*time.Second)
		res <- got
	}()
	h.waitStatus(r2.ID, model.StatusFailed)
	h.advance(5 * time.Second)
	got = <-res
	if got.Status != model.StatusSuccess || got.Kind != model.KindRetry {
		t.Fatalf("await must follow the retry chain: %+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	r3 := invoke(t, h, j.ID)
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	if _, done := h.e.Await(ctx, r3.ID, 5*time.Second); done {
		t.Fatal("cancelled wait reported done")
	}
	h.sync()
	if h.run(r3.ID).Status.Terminal() {
		t.Fatal("a disconnected waiter must not cancel the run")
	}
	g.open()
	h.waitStatus(r3.ID, model.StatusSuccess)
}

// ---------------------------------------------------------------- recovery

func insertRunning(t *testing.T, h *harness, j model.Job, kind string, startedAt time.Time, early *model.CompleteReq) model.Run {
	t.Helper()
	id := newID()
	r := model.Run{ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: kind, Attempt: 1, ChainRootID: id,
		Status: model.StatusRunning, ScheduledFor: startedAt, OriginFor: startedAt, ContextFor: startedAt, LatenessRef: startedAt,
		Rev: j.Rev, StartedAt: &startedAt, EarlyResult: early, CreatedAt: startedAt}
	if kind == model.KindSchedule {
		n := startedAt
		r.NominalFor = &n
	}
	if err := store.InsertRun(context.Background(), h.st.W, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRecovery(t *testing.T) {
	h := newHarness(t)
	alo := h.create(cronJob("alo", "0 * * * *", nil)) // has a materialised successor at 10:00
	amo := h.create(invokeJob("amo", &model.PolicyIn{Delivery: ptr(model.AtMostOnce)}))
	asy := h.create(asyncJob("asy"))
	gone := h.create(invokeJob("gone", nil))
	h.stop()

	rALO := insertRunning(t, h, alo, model.KindSchedule, t0.Add(-time.Minute), nil)
	rAMO := insertRunning(t, h, amo, model.KindInvoke, t0, nil)
	rEarly := insertRunning(t, h, asy, model.KindInvoke, t0, &model.CompleteReq{Status: "success"})
	rGone := insertRunning(t, h, gone, model.KindInvoke, t0, nil)
	if err := store.DeleteJob(context.Background(), h.st.W, gone.ID); err != nil {
		t.Fatal(err)
	}
	h.start()

	h.waitStatus(rALO.ID, model.StatusSuccess) // redispatched with the same id
	if calls := h.exec.snapshot(); calls[len(calls)-1].Run.ID != rALO.ID && !calledWith(h, rALO.ID) {
		t.Fatal("at_least_once run must be re-sent with the same run id")
	}
	if h.run(rAMO.ID).Status != model.StatusInterrupted || h.pager.count("interrupted") < 1 {
		t.Fatal("at_most_once must be interrupted and paged")
	}
	if h.run(rEarly.ID).Status != model.StatusSuccess || calledWith(h, rEarly.ID) {
		t.Fatal("a run with an early result is finalized from it, never re-sent")
	}
	if r := h.run(rGone.ID); r.Status != model.StatusInterrupted || calledWith(h, rGone.ID) {
		t.Fatalf("deleted job's run: %+v", r)
	}
	jb, _, inf := h.counts()
	if len(jb)+len(inf) != 0 {
		t.Fatalf("held after recovery: %v %v", jb, inf)
	}
}

func calledWith(h *harness, runID string) bool {
	for _, c := range h.exec.snapshot() {
		if c.Run.ID == runID {
			return true
		}
	}
	return false
}

func TestBootWaitingPastDeadlineTimesOut(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	in := asyncJob("pub")
	in.Policy = &model.PolicyIn{TimeoutSec: ptr(int64(60))}
	j := h.create(in)
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusWaiting)
	h.stop()
	h.clk.Advance(2 * time.Minute)
	h.start()
	h.waitStatus(r.ID, model.StatusTimeout)
}

func TestBootWaitingLeaseGraceOnce(t *testing.T) {
	h := newHarness(t)
	h.exec.set(waiting)
	in := asyncJob("pub")
	in.Policy = &model.PolicyIn{TimeoutSec: ptr(int64(3600))}
	j := h.create(in)
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusWaiting)
	h.stop()
	h.clk.Advance(2 * time.Minute) // lease (60 s) expired while down
	h.start()
	h.sync()
	got := h.run(r.ID)
	if got.Status != model.StatusWaiting || !got.LeaseGraceUsed {
		t.Fatalf("expected one grace lease: %+v", got)
	}
	if _, _, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success"}); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------- liveness

func TestLoopNeverSendsToSelf(t *testing.T) {
	h := newHarness(t)
	h.stop()
	ctx := context.Background()
	for i := 0; i < 1100; i++ {
		id := newID()
		j := model.Job{ID: id, ClientID: h.owner.ClientID, Name: fmt.Sprintf("j%04d", i), State: model.JobActive, Rev: 1,
			Trigger: model.Trigger{Type: model.TriggerCron, Cron: "* * * * *", Timezone: "UTC"},
			Target:  model.Target{Type: model.TargetHTTP, HTTP: &model.HTTPTarget{Method: "POST", URL: "http://svc/", Mode: "sync"}},
			Policy:  model.DefaultPolicy(model.TriggerCron, model.Target{}), CreatedAt: t0, UpdatedAt: t0}
		j.Policy.MaxLateSec = 30
		if err := store.CreateJob(ctx, h.st.W, j); err != nil {
			t.Fatal(err)
		}
	}
	h.start() // materialises 1100 jobs without the loop
	h.clk.Set(t0.Add(time.Hour))
	start := time.Now()
	for i := 0; i < 5; i++ {
		h.sync()
	}
	if time.Since(start) > 30*time.Second {
		t.Fatal("loop stalled")
	}
	if age := h.e.LoopAge(); age > time.Second {
		t.Fatalf("loop age %s", age)
	}
}

func TestFinalizeDBFailureDoesNotStallLoop(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.st.W.ExecContext(ctx, `CREATE TRIGGER boom BEFORE UPDATE OF status ON runs
		WHEN NEW.status = 'success' BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitFor("unflushed", func() bool { return h.e.Unflushed() == 1 })
	if !h.e.Healthy() || h.pager.count("db write failing") != 1 {
		t.Fatal("db failure must page but keep /healthz up")
	}
	_, _, inf := h.counts()
	if inf[poolHTTP] != 1 {
		t.Fatal("slots stay held until the flush")
	}
	if _, err := h.st.W.ExecContext(ctx, `DROP TRIGGER boom`); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Second)
	h.waitStatus(r.ID, model.StatusSuccess)
	h.waitFor("flushed", func() bool { return h.e.Unflushed() == 0 })
	_, _, inf = h.counts()
	if len(inf) != 0 {
		t.Fatalf("slot not released after flush: %v", inf)
	}
}

func TestRetentionPrunes(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusSuccess)
	h.clk.Advance(61 * 24 * time.Hour)
	if err := h.e.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRun(context.Background(), h.st.W, r.ID); err != store.ErrNotFound { //nolint:errorlint
		t.Fatalf("old run kept: %v", err)
	}
}

// With no sub-minute jobs the 7-day prune must not run for every job.
func TestRetentionKeepsRecentRunsWithoutFastJobs(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusSuccess)
	h.clk.Advance(10 * 24 * time.Hour)
	if err := h.e.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRun(context.Background(), h.st.W, r.ID); err != nil {
		t.Fatalf("10-day-old run pruned: %v", err)
	}
}

func TestShutdownIgnoresResultsAfterDrain(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	h.waitStatus(r.ID, model.StatusRunning)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.e.Stop(ctx, h.clk.Now()) // drain deadline already reached
	g.open()
	time.Sleep(20 * time.Millisecond)
	if got := h.run(r.ID); got.Status != model.StatusRunning {
		t.Fatalf("after drainBy the run is left for boot recovery: %s", got.Status)
	}
	h.e = nil
}

// Draining finishes in-flight work but admits nothing new, even when a slot
// frees up before drainBy.
func TestShutdownDrainsInflightWithoutNewDispatch(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", &model.PolicyIn{Overlap: ptr(model.OverlapQueue)}))
	running := invoke(t, h, j.ID)
	h.waitStatus(running.ID, model.StatusRunning)
	queued := invoke(t, h, j.ID)
	h.sync()
	stopped := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.e.Stop(ctx, h.clk.Now().Add(20*time.Second))
		close(stopped)
	}()
	time.Sleep(20 * time.Millisecond)
	g.open()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish after the in-flight run completed")
	}
	if got := h.run(running.ID); got.Status != model.StatusSuccess {
		t.Fatalf("in-flight run: %s", got.Status)
	}
	if got := h.run(queued.ID); got.Status != model.StatusPending {
		t.Fatalf("queued run dispatched during drain: %s", got.Status)
	}
	h.e = nil
}

// TestHealthStall is acceptance #8: a loop that stops iterating reads as
// unhealthy within 60 s.
func TestHealthStall(t *testing.T) {
	h := newHarness(t)
	if !h.e.Healthy() {
		t.Fatal("fresh engine unhealthy")
	}
	release := make(chan struct{})
	go func() { _ = h.e.send(context.Background(), evCall{f: func() { <-release }, done: make(chan struct{})}) }()
	time.Sleep(50 * time.Millisecond) // the loop is now blocked
	h.clk.Advance(61 * time.Second)
	if h.e.Healthy() {
		t.Fatal("stalled loop still healthy")
	}
	close(release)
	h.waitFor("healthy again", h.e.Healthy)
}

// A deleted or disabled job's future runs stay in the heap as ghosts until
// due; they must leave the pending count at once (load-test finding).
func TestRemovedJobRunsLeavePendingCount(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	del := h.create(cronJob("del", "0 0 1 1 *", nil))
	dis := h.create(cronJob("dis", "0 0 1 1 *", nil))
	pending := func() (n int) {
		h.onLoop(func() { n = len(h.e.inHeap) + len(h.e.parked) })
		return
	}
	if n := pending(); n != 2 {
		t.Fatalf("pending = %d, want 2", n)
	}
	if err := h.e.DeleteJob(ctx, del.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.SetEnabled(ctx, dis.ID, false); err != nil {
		t.Fatal(err)
	}
	h.sync()
	if n := pending(); n != 0 {
		t.Fatalf("pending = %d after delete and disable, want 0", n)
	}

	// A PATCH replaces the pending schedule run; a cancel removes it.
	upd := h.create(cronJob("upd", "0 0 1 1 *", nil))
	if _, err := h.e.UpdateJob(ctx, upd.ID, model.JobPatch{Trigger: &model.TriggerIn{Type: model.TriggerCron, Cron: "0 0 2 1 *"}}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	if n := pending(); n != 1 {
		t.Fatalf("pending = %d after patch, want 1", n)
	}
	runs, err := store.PendingRunsForJob(ctx, h.st.W, upd.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("pending runs: %v %v", runs, err)
	}
	if _, _, err := h.e.Cancel(ctx, runs[0].ID); err != nil {
		t.Fatal(err)
	}
	var counted bool
	h.onLoop(func() { _, counted = h.e.inHeap[runs[0].ID] })
	if counted {
		t.Fatal("cancelled run still counted as pending")
	}
}

func TestStoppingClosesOnStop(t *testing.T) {
	h := newHarness(t)
	select {
	case <-h.e.Stopping():
		t.Fatal("closed before Stop")
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.e.Stop(ctx, time.Now())
	select {
	case <-h.e.Stopping():
	default:
		t.Fatal("Stopping not closed after Stop")
	}
}
