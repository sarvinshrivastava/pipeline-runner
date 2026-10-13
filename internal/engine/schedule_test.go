package engine

import (
	"context"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

func TestCronFiresAndMaterialisesNext(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("tick", "* * * * *", nil))
	rs := h.runs(j.ID)
	if len(rs) != 1 || rs[0].Status != model.StatusPending || !rs[0].ScheduledFor.Equal(t0.Add(time.Minute)) {
		t.Fatalf("first pending: %+v", rs)
	}
	h.advance(time.Minute)
	h.waitStatus(rs[0].ID, model.StatusSuccess)
	rs = h.runs(j.ID)
	if len(rs) != 2 || rs[1].Status != model.StatusPending || !rs[1].ScheduledFor.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("next occurrence not materialised: %+v", rs)
	}
	in := h.exec.calls[0]
	if in.RunToken == "" || in.Run.ID != rs[0].ID {
		t.Fatalf("exec input: %+v", in)
	}
}

func TestMissedPastWindowPagesOnce(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("tick", "* * * * *", &model.PolicyIn{MaxLateSec: ptr(int64(30))}))
	first := h.runs(j.ID)[0]
	h.clk.Set(t0.Add(time.Hour)) // forward jump
	h.sync()
	if r := h.run(first.ID); r.Status != model.StatusMissed {
		t.Fatalf("status = %s", r.Status)
	}
	if n := h.pager.count("missed"); n != 1 {
		t.Fatalf("missed pages = %d (%v)", n, h.pager.titles())
	}
	rs := h.runs(j.ID)
	last := rs[len(rs)-1]
	if last.NominalFor.Before(t0.Add(time.Hour)) {
		t.Fatalf("the outage must collapse to the current occurrence: %+v", last)
	}
	for _, c := range h.exec.calls {
		if c.Run.ID == first.ID {
			t.Fatal("a missed run must never execute late")
		}
	}
}

func TestOverlapSkipWhileRunningAndPagesAfterThree(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(cronJob("slow", "* * * * *", &model.PolicyIn{TimeoutSec: ptr(int64(3600))}))
	first := h.runs(j.ID)[0]
	h.advance(time.Minute)
	h.waitStatus(first.ID, model.StatusRunning)
	for i := 0; i < 3; i++ {
		h.advance(time.Minute)
	}
	skipped := 0
	for _, r := range h.runs(j.ID) {
		if r.Status == model.StatusSkipped {
			skipped++
		}
	}
	if skipped != 3 {
		t.Fatalf("skipped = %d, runs %+v", skipped, h.runs(j.ID))
	}
	if n := h.pager.count("skipped 3 consecutive"); n != 1 {
		t.Fatalf("skip pages = %d", n)
	}
	g.open()
	h.waitStatus(first.ID, model.StatusSuccess)
}

func TestOverlapQueueKeepsBacklogOfOne(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(cronJob("q", "* * * * *", &model.PolicyIn{Overlap: ptr(model.OverlapQueue), MaxLateSec: ptr(int64(600)), TimeoutSec: ptr(int64(3600))}))
	first := h.runs(j.ID)[0]
	h.advance(time.Minute)
	h.waitStatus(first.ID, model.StatusRunning)
	h.advance(time.Minute)
	h.advance(time.Minute)
	pending := 0
	for _, r := range h.runs(j.ID) {
		if r.Status == model.StatusPending {
			pending++
		}
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want 1", pending)
	}
	g.open()
	h.waitStatus(first.ID, model.StatusSuccess)
	second := h.runs(j.ID)[1]
	h.waitStatus(second.ID, model.StatusSuccess)
}

func TestParkedRunGoesMissed(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(cronJob("q", "* * * * *", &model.PolicyIn{Overlap: ptr(model.OverlapQueue), MaxLateSec: ptr(int64(30)), TimeoutSec: ptr(int64(3600))}))
	first := h.runs(j.ID)[0]
	h.advance(time.Minute)
	h.waitStatus(first.ID, model.StatusRunning)
	h.advance(time.Minute) // second becomes due and parks behind the first
	second := h.runs(j.ID)[1]
	h.advance(31 * time.Second)
	if r := h.run(second.ID); r.Status != model.StatusMissed {
		t.Fatalf("parked run status = %s", r.Status)
	}
	g.open()
}

func TestNamespacedKeysAndCrossJobWake(t *testing.T) {
	h := newHarness(t)
	other := h.addClient("other")
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	key := &model.PolicyIn{ConcurrencyKey: ptr("ig:1"), TimeoutSec: ptr(int64(3600))}
	a := h.create(invokeJob("a", key))
	b := h.create(invokeJob("b", key))
	c, err := h.e.CreateJob(context.Background(), other, invokeJob("c", key), nil)
	if err != nil {
		t.Fatal(err)
	}
	ra, _ := h.e.Invoke(context.Background(), a.ID, nil, nil, nil)
	rb, _ := h.e.Invoke(context.Background(), b.ID, nil, nil, nil)
	rc, _ := h.e.Invoke(context.Background(), c.ID, nil, nil, nil)
	h.waitStatus(ra.ID, model.StatusRunning)
	h.waitStatus(rc.ID, model.StatusRunning) // other client's same key: independent
	if r := h.run(rb.ID); r.Status != model.StatusPending {
		t.Fatalf("b must wait for the shared key, status = %s", r.Status)
	}
	g.open()
	h.waitStatus(rb.ID, model.StatusSuccess) // woken when a released the key
}

func TestPoolSaturationFIFO(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", nil))
	var ids []string
	for i := 0; i < 3; i++ {
		r, err := h.e.Invoke(context.Background(), j.ID, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	h.waitStatus(ids[0], model.StatusRunning)
	if h.run(ids[1]).Status != model.StatusPending || h.run(ids[2]).Status != model.StatusPending {
		t.Fatal("pool of 1 must park the rest")
	}
	_, _, inflight := h.counts()
	if inflight[poolHTTP] != 1 {
		t.Fatalf("inflight = %v", inflight)
	}
	g.open()
	for _, id := range ids {
		h.waitStatus(id, model.StatusSuccess)
	}
	_, _, inflight = h.counts()
	if len(inflight) != 0 {
		t.Fatalf("slots leaked: %v", inflight)
	}
}

func TestDisableEnableWithinIntervalStillFires(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("hourly", "30 * * * *", nil))
	first := h.runs(j.ID)[0] // 09:30
	h.advance(10 * time.Minute)
	if _, err := h.e.SetEnabled(context.Background(), j.ID, false); err != nil {
		t.Fatal(err)
	}
	if h.run(first.ID).Status != model.StatusCancelled {
		t.Fatal("disable must cancel the pending run")
	}
	h.advance(2 * time.Minute)
	if _, err := h.e.SetEnabled(context.Background(), j.ID, true); err != nil {
		t.Fatal(err)
	}
	h.sync()
	rs := h.runs(j.ID)
	last := rs[len(rs)-1]
	if last.Status != model.StatusPending || !last.NominalFor.Equal(t0.Add(30*time.Minute)) {
		t.Fatalf("09:30 must be re-created: %+v", last)
	}
	h.advance(18 * time.Minute)
	h.waitStatus(last.ID, model.StatusSuccess)
}

func TestCancelPendingScheduleRunMaterialisesNext(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("hourly", "30 * * * *", nil))
	first := h.runs(j.ID)[0]
	if _, _, err := h.e.Cancel(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()
	rs := h.runs(j.ID)
	last := rs[len(rs)-1]
	if last.Status != model.StatusPending || !last.NominalFor.Equal(t0.Add(90*time.Minute)) {
		t.Fatalf("user cancel skips this occurrence only: %+v", rs)
	}
}

func TestPatchPolicyKeepsNextOccurrence(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("hourly", "30 * * * *", nil))
	h.advance(10 * time.Minute)
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Policy: &model.PolicyIn{TimeoutSec: ptr(int64(60))}}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	rs := h.runs(j.ID)
	last := rs[len(rs)-1]
	if !last.NominalFor.Equal(t0.Add(30*time.Minute)) || last.Rev != 2 {
		t.Fatalf("09:30 must survive a policy patch with the new revision: %+v", last)
	}
}

func TestPatchCronEarlierTodayStillFires(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("daily", "30 9 * * *", nil))
	h.advance(10 * time.Minute) // 09:10
	tr := model.TriggerIn{Type: model.TriggerCron, Cron: "20 9 * * *"}
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Trigger: &tr}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	rs := h.runs(j.ID)
	if !rs[len(rs)-1].NominalFor.Equal(t0.Add(20 * time.Minute)) {
		t.Fatalf("today's 09:20 must fire: %+v", rs[len(rs)-1])
	}
}

func TestPatchToDelayZeroFires(t *testing.T) {
	h := newHarness(t)
	j := h.create(model.JobCreate{Name: "once", Trigger: model.TriggerIn{Type: model.TriggerAt, At: ptr(t0.Add(7 * 24 * time.Hour))}, Target: httpTarget()})
	h.advance(time.Minute)
	tr := model.TriggerIn{Type: model.TriggerDelay, DelaySec: ptr(int64(0))}
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Trigger: &tr}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	rs := h.runs(j.ID)
	h.waitStatus(rs[len(rs)-1].ID, model.StatusSuccess)
	got, _ := store.GetJob(context.Background(), h.st.W, j.ID)
	if got.State != model.JobDone {
		t.Fatalf("one-shot must be done, state = %s", got.State)
	}
}

func TestPatchDisabledJobDoesNotFire(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("m", "* * * * *", nil))
	if _, err := h.e.SetEnabled(context.Background(), j.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Policy: &model.PolicyIn{TimeoutSec: ptr(int64(5))}}); err != nil {
		t.Fatal(err)
	}
	h.advance(5 * time.Minute)
	if h.exec.callCount() != 0 {
		t.Fatal("disabled job fired")
	}
}

func TestEnableAtAfterInstantDoesNotFireLate(t *testing.T) {
	h := newHarness(t)
	j := h.create(model.JobCreate{Name: "once", Trigger: model.TriggerIn{Type: model.TriggerAt, At: ptr(t0.Add(time.Hour))}, Target: httpTarget()})
	h.advance(50 * time.Minute)
	if _, err := h.e.SetEnabled(context.Background(), j.ID, false); err != nil {
		t.Fatal(err)
	}
	h.advance(12 * time.Minute) // 10:02, the instant passed while disabled
	if _, err := h.e.SetEnabled(context.Background(), j.ID, true); err != nil {
		t.Fatal(err)
	}
	h.advance(61 * time.Second) // past a reconcile tick
	if h.exec.callCount() != 0 {
		t.Fatal("must not fire late after enable (P14)")
	}
	got, _ := store.GetJob(context.Background(), h.st.W, j.ID)
	if got.State != model.JobDone {
		t.Fatalf("state = %s, want done", got.State)
	}
}

func TestAtJobDoneOnlyAfterRetryChain(t *testing.T) {
	h := newHarness(t)
	calls := 0
	h.exec.set(func(context.Context, targetInput) targetResult {
		calls++
		if calls == 1 {
			code := 503
			return targetResult{Status: model.StatusFailed, HTTPStatus: &code, Ambiguous: true}
		}
		return okResult()
	})
	j := h.create(model.JobCreate{Name: "once", Trigger: model.TriggerIn{Type: model.TriggerAt, At: ptr(t0.Add(time.Minute))}, Target: httpTarget(),
		Policy: &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(2), BackoffSec: ptr(int64(30))}}})
	h.advance(time.Minute)
	first := h.runs(j.ID)[0]
	h.waitStatus(first.ID, model.StatusFailed)
	got, _ := store.GetJob(context.Background(), h.st.W, j.ID)
	if got.State != model.JobActive {
		t.Fatalf("job done while a retry is pending: %s", got.State)
	}
	h.advance(30 * time.Second)
	retry := h.runs(j.ID)[1]
	h.waitStatus(retry.ID, model.StatusSuccess)
	got, _ = store.GetJob(context.Background(), h.st.W, j.ID)
	if got.State != model.JobDone {
		t.Fatalf("state = %s", got.State)
	}
}

func TestBackwardsClockStepNoDoubleDispatch(t *testing.T) {
	h := newHarness(t)
	j := h.create(cronJob("m", "* * * * *", nil))
	h.advance(time.Minute)
	first := h.runs(j.ID)[0]
	h.waitStatus(first.ID, model.StatusSuccess)
	h.clk.Set(t0) // step back a minute
	h.sync()
	h.clk.Set(t0.Add(time.Minute))
	h.sync()
	if n := h.exec.callCount(); n != 1 {
		t.Fatalf("calls = %d", n)
	}
}

func TestReconcileRepairsDroppedInsert(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	// Insert a pending run behind the loop's back (as if its event was dropped).
	id := newID()
	now := h.clk.Now()
	r := model.Run{ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: model.KindInvoke, Attempt: 1, ChainRootID: id,
		Status: model.StatusPending, ScheduledFor: now, OriginFor: now, ContextFor: now, LatenessRef: now, Rev: 1, CreatedAt: now}
	if err := store.InsertRun(context.Background(), h.st.W, r); err != nil {
		t.Fatal(err)
	}
	h.advance(61 * time.Second)
	h.waitStatus(id, model.StatusSuccess)
}
