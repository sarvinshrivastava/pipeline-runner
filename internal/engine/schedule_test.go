package engine

import (
	"context"
	"strings"
	"sync"
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
	in := h.exec.snapshot()[0]
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
	for _, c := range h.exec.snapshot() {
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

// coalescedSeries reads plr_coalesced_occurrences_total as "job|client|reason" → value.
func coalescedSeries(t *testing.T, h *harness) map[string]float64 {
	t.Helper()
	mfs, err := h.metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "plr_coalesced_occurrences_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			out[l["job"]+"|"+l["client"]+"|"+l["reason"]] = m.GetCounter().GetValue()
		}
	}
	return out
}

// phased hands each exec call the gate current at its start (nil: succeed).
type phased struct {
	mu sync.Mutex
	g  *gate
}

func (p *phased) set(g *gate) {
	p.mu.Lock()
	p.g = g
	p.mu.Unlock()
}

func (p *phased) behaviour(ctx context.Context, in targetInput) targetResult {
	p.mu.Lock()
	g := p.g
	p.mu.Unlock()
	if g == nil {
		return okResult()
	}
	return g.behaviour(okResult())(ctx, in)
}

func secQueuePolicy() *model.PolicyIn {
	return &model.PolicyIn{Overlap: ptr(model.OverlapQueue), MaxLateSec: ptr(int64(600)), JitterSec: ptr(int64(0)), TimeoutSec: ptr(int64(3600))}
}

// queueBacklog runs an every-second overlap=queue job whose first run (T)
// blocks for 5 s: T+1 parks, and its claim at T+5 coalesces T+2..T+4 into
// T+5. Exec calls from then on use next.
func queueBacklog(t *testing.T, next *gate) (*harness, *phased, model.Job) {
	t.Helper()
	h := newHarness(t, withMetrics())
	ph := &phased{}
	g := newGate()
	ph.set(g)
	h.exec.set(ph.behaviour)
	j := h.create(cronJob("sec", "* * * * * *", secQueuePolicy()))
	first := h.runs(j.ID)[0]
	h.advance(time.Second) // T: claimed
	h.waitStatus(first.ID, model.StatusRunning)
	h.advance(time.Second) // T+1: parks behind T
	h.advance(4 * time.Second)
	ph.set(next)
	g.open()
	return h, ph, j
}

func TestBacklogCoalescedCountedAndPaged(t *testing.T) {
	h, _, j := queueBacklog(t, nil)
	h.waitFor("coalesced page", func() bool { return len(h.pager.bodies("coalesced")) == 1 })
	if body := h.pager.bodies("coalesced")[0]; !strings.Contains(body, "3 occurrences were coalesced") {
		t.Fatalf("page body: %q", body)
	}
	got := coalescedSeries(t, h)
	if v := got["sec|"+j.ClientID+"|backlog"]; v != 3 {
		t.Fatalf("backlog = %v, want 3 (%v)", v, got)
	}
	if _, ok := got["sec|"+j.ClientID+"|expired"]; ok {
		t.Fatalf("nothing expired: %v", got)
	}
}

func TestCoalescedPagesRateLimited(t *testing.T) {
	g2 := newGate()
	h, ph, j := queueBacklog(t, g2)
	h.waitFor("first page", func() bool { return len(h.pager.bodies("coalesced")) == 1 })
	// T+1 now runs blocked on g2 and T+5 parks behind it: a second backlog.
	h.advance(5 * time.Second)
	ph.set(nil)
	g2.open()
	waitCount(h, 0, model.StatusRunning)
	if n := len(h.pager.bodies("coalesced")); n != 1 {
		t.Fatalf("a second backlog within 10 min must not page: %d pages", n)
	}
	if v := coalescedSeries(t, h)["sec|"+j.ClientID+"|backlog"]; v != 7 {
		t.Fatalf("backlog = %v, want 3 + 4", v)
	}
	if _, err := h.e.SetEnabled(context.Background(), j.ID, false); err != nil { // stop new occurrences
		t.Fatal(err)
	}
	h.advance(10*time.Minute + time.Second)
	h.waitFor("held page", func() bool { return len(h.pager.bodies("coalesced")) == 2 })
	if body := h.pager.bodies("coalesced")[1]; !strings.Contains(body, "4 occurrences were coalesced") {
		t.Fatalf("held page body: %q", body)
	}
}

func TestOutageExpiredCountCapped(t *testing.T) {
	h := newHarness(t, withMetrics())
	j := h.create(cronJob("sec", "* * * * * *", &model.PolicyIn{MaxLateSec: ptr(int64(60))}))
	h.clk.Set(t0.Add(3 * time.Hour))
	h.sync()
	h.waitFor("outage page", func() bool { return len(h.pager.bodies("missed")) == 1 })
	body := h.pager.bodies("missed")[0]
	if !strings.Contains(body, "At least 1000 occurrences expired") || !strings.Contains(body, "1 run missed") {
		t.Fatalf("page body: %q", body)
	}
	got := coalescedSeries(t, h)
	if v := got["sec|"+j.ClientID+"|expired"]; v != maxExpiredCount {
		t.Fatalf("expired = %v, want %d", v, maxExpiredCount)
	}
	if v := got["sec|"+j.ClientID+"|backlog"]; v != 59 {
		t.Fatalf("backlog = %v, want 59 (the max_late window)", v)
	}
}

func TestDeletedJobDropsCountsAndSeries(t *testing.T) {
	h := newHarness(t, withMetrics())
	ctx := context.Background()
	other := h.addClient("other")
	p := &model.PolicyIn{MaxLateSec: ptr(int64(60))}
	a := h.create(cronJob("dup", "* * * * * *", p))
	b, err := h.e.CreateJob(ctx, other, cronJob("dup", "* * * * * *", p), nil)
	if err != nil {
		t.Fatal(err)
	}
	h.sync()
	h.advance(3 * time.Minute) // outage: both page at once
	h.waitFor("first pages", func() bool { return h.pager.count("dup: missed") == 2 })
	h.advance(2 * time.Minute) // a second outage within 10 min: held
	var heldA, heldB bool
	h.onLoop(func() { heldA, heldB = h.e.pageBatch[a.ID] != nil, h.e.pageBatch[b.ID] != nil })
	if !heldA || !heldB {
		t.Fatalf("held counts: a %v, b %v", heldA, heldB)
	}
	if err := h.e.DeleteJob(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()
	for k := range coalescedSeries(t, h) {
		if strings.HasPrefix(k, "dup|"+a.ClientID+"|") {
			t.Fatalf("deleted job's series kept: %s", k)
		}
	}
	if _, ok := coalescedSeries(t, h)["dup|"+b.ClientID+"|expired"]; !ok {
		t.Fatal("the other client's same-name job lost its series")
	}
	h.advance(11 * time.Minute)
	h.waitFor("b's held page", func() bool { return h.pager.count("dup: missed") == 3 })
	h.sync()
	if n := h.pager.count("dup: missed"); n != 3 {
		t.Fatalf("pages = %d: the deleted job paged", n)
	}
}

func TestPatchCoalescesThroughAPI(t *testing.T) {
	h := newHarness(t, withMetrics())
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(cronJob("sec", "* * * * * *", secQueuePolicy()))
	first := h.runs(j.ID)[0]
	h.advance(time.Second) // T: claimed
	h.waitStatus(first.ID, model.StatusRunning)
	h.advance(time.Second) // T+1 parks
	h.advance(4 * time.Second)
	p := secQueuePolicy()
	p.TimeoutSec = ptr(int64(1800))
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Policy: p}); err != nil {
		t.Fatal(err)
	}
	// The PATCH re-creates T+1 at T+5, collapsing T+1..T+4.
	if v := coalescedSeries(t, h)["sec|"+j.ClientID+"|backlog"]; v != 4 {
		t.Fatalf("backlog = %v, want 4", v)
	}
	h.waitFor("patch page", func() bool { return len(h.pager.bodies("coalesced")) == 1 })
	if body := h.pager.bodies("coalesced")[0]; !strings.Contains(body, "4 occurrences were coalesced") {
		t.Fatalf("page body: %q", body)
	}
	g.open()
}
