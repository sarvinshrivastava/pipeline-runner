package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// runGates blocks each run's exec until its own gate opens; runs listed in
// pass return at once.
type runGates struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
	pass  map[string]bool // job IDs whose runs never block
}

func newRunGates(h *harness) *runGates {
	g := &runGates{gates: map[string]chan struct{}{}, pass: map[string]bool{}}
	h.exec.set(func(ctx context.Context, in target.ExecInput) target.Result {
		g.mu.Lock()
		if g.pass[in.Run.JobID] {
			g.mu.Unlock()
			return okResult()
		}
		ch := g.chanFor(in.Run.ID)
		g.mu.Unlock()
		select {
		case <-ch:
			return okResult()
		case <-ctx.Done():
			return target.Result{Status: model.StatusFailed, Error: "context done", Ambiguous: true}
		}
	})
	return g
}

func (g *runGates) chanFor(id string) chan struct{} {
	ch, ok := g.gates[id]
	if !ok {
		ch = make(chan struct{})
		g.gates[id] = ch
	}
	return ch
}

func (g *runGates) open(id string) {
	g.mu.Lock()
	close(g.chanFor(id))
	g.mu.Unlock()
}

func (g *runGates) passJob(jobID string) {
	g.mu.Lock()
	g.pass[jobID] = true
	g.mu.Unlock()
}

func prioJob(name, prio string) model.JobCreate {
	return invokeJob(name, &model.PolicyIn{Priority: ptr(prio)})
}

// callOrder lists the run IDs in the order they were executed.
func callOrder(h *harness) []string {
	var out []string
	for _, c := range h.exec.snapshot() {
		out = append(out, c.Run.ID)
	}
	return out
}

func wantOrder(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("call order %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("call order %v, want %v", got, want)
		}
	}
}

func (h *harness) jobAs(owner model.Principal, in model.JobCreate) model.Job {
	h.t.Helper()
	j, err := h.e.CreateJob(context.Background(), owner, in, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.sync()
	return j
}

func TestPriorityOrdersWaiters(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	g := newRunGates(h)
	hold := invoke(t, h, h.create(invokeJob("hold", nil)).ID)
	waitJobRun(h, hold.JobID, hold.ID, model.StatusRunning)
	low, norm, high := h.create(prioJob("low", "low")), h.create(prioJob("norm", "normal")), h.create(prioJob("high", "high"))
	for _, j := range []model.Job{low, norm, high} {
		g.passJob(j.ID)
	}
	rl, rn, rh := invoke(t, h, low.ID), invoke(t, h, norm.ID), invoke(t, h, high.ID)
	h.sync()
	g.open(hold.ID)
	waitCount(h, 4, model.StatusSuccess)
	wantOrder(t, callOrder(h), hold.ID, rh.ID, rn.ID, rl.ID)
}

func TestPriorityAgingBoundsWait(t *testing.T) {
	for _, tc := range []struct {
		wait     time.Duration
		lowFirst bool
	}{{61 * time.Second, true}, {59 * time.Second, false}} {
		t.Run(tc.wait.String(), func(t *testing.T) {
			h := newHarness(t, withMaxHTTP(1))
			g := newRunGates(h)
			hold := invoke(t, h, h.create(invokeJob("hold", nil)).ID)
			waitJobRun(h, hold.JobID, hold.ID, model.StatusRunning)
			low, high := h.create(prioJob("low", "low")), h.create(prioJob("high", "high"))
			g.passJob(low.ID)
			g.passJob(high.ID)
			rl := invoke(t, h, low.ID)
			h.sync() // parked now: its wait starts here
			h.advance(tc.wait)
			rh := invoke(t, h, high.ID)
			h.sync()
			g.open(hold.ID)
			waitCount(h, 3, model.StatusSuccess)
			if tc.lowFirst {
				wantOrder(t, callOrder(h), hold.ID, rl.ID, rh.ID)
			} else {
				wantOrder(t, callOrder(h), hold.ID, rh.ID, rl.ID)
			}
		})
	}
}

func TestClientCapLeavesSlotsForOthers(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4), withPerClient(2))
	newRunGates(h)
	a := h.create(invokeJob("a", nil))
	b := h.jobAs(h.addClient("b"), invokeJob("b", nil))
	for i := 0; i < 4; i++ {
		invoke(t, h, a.ID)
	}
	waitCount(h, 2, model.StatusRunning)
	b1, b2 := invoke(t, h, b.ID), invoke(t, h, b.ID)
	waitJobRun(h, b.ID, b1.ID, model.StatusRunning)
	waitJobRun(h, b.ID, b2.ID, model.StatusRunning)
	var busy map[string]int
	h.onLoop(func() { busy = clone(h.e.clientBusy) })
	if busy[a.ClientID] != 2 || busy[b.ClientID] != 2 {
		t.Fatalf("clientBusy = %v, want 2 each", busy)
	}
}

func TestCappedRunKeepsItsPlace(t *testing.T) {
	h := newHarness(t, withMaxHTTP(2), withPerClient(1))
	g := newRunGates(h)
	a := h.create(invokeJob("a", nil))
	b := h.jobAs(h.addClient("b"), invokeJob("b", nil))
	a1 := invoke(t, h, a.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusRunning)
	a2 := invoke(t, h, a.ID) // capped while the pool has room
	b1 := invoke(t, h, b.ID)
	waitJobRun(h, b.ID, b1.ID, model.StatusRunning)
	b2 := invoke(t, h, b.ID) // pool full; ranks behind a2
	h.sync()
	var list []string
	h.onLoop(func() { list = append(list, h.e.waits["pool:"+poolHTTP]...) })
	wantOrder(t, list, a2.ID, b2.ID)

	g.open(b1.ID)
	waitJobRun(h, b.ID, b2.ID, model.StatusRunning)
	h.onLoop(func() { list = append(list[:0], h.e.waits["pool:"+poolHTTP]...) })
	wantOrder(t, list, a2.ID)

	g.open(a1.ID)
	waitJobRun(h, a.ID, a2.ID, model.StatusRunning)
	g.open(a2.ID)
	g.open(b2.ID)
	waitCount(h, 4, model.StatusSuccess)
}

func TestAsyncWaitingFreesClientShare(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4), withPerClient(1))
	h.exec.set(waiting)
	j := h.create(asyncJob("pub"))
	r1, r2 := invoke(t, h, j.ID), invoke(t, h, j.ID)
	h.waitStatus(r1.ID, model.StatusWaiting)
	h.waitStatus(r2.ID, model.StatusWaiting)
	var busy map[string]int
	h.onLoop(func() { busy = clone(h.e.clientBusy) })
	if len(busy) != 0 {
		t.Fatalf("waiting runs must hold no client share: %v", busy)
	}
}

func TestNoClientCapWhenZero(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4))
	newRunGates(h)
	a := h.create(invokeJob("a", nil))
	for i := 0; i < 4; i++ {
		invoke(t, h, a.ID)
	}
	waitCount(h, 4, model.StatusRunning)
}

func TestRebuildCountsClientShare(t *testing.T) {
	h := newHarness(t, withMaxHTTP(8), withPerClient(2))
	newRunGates(h)
	a := h.create(invokeJob("a", nil))
	invoke(t, h, a.ID)
	invoke(t, h, a.ID)
	waitCount(h, 2, model.StatusRunning)
	var busy map[string]int
	h.onLoop(func() {
		h.e.clientBusy = map[string]int{} // simulate drift
		if err := h.e.rebuildHeld(h.e.life); err != nil {
			t.Error(err)
		}
		busy = clone(h.e.clientBusy)
	})
	if busy[a.ClientID] != 2 {
		t.Fatalf("rebuilt clientBusy = %v, want 2", busy)
	}
	r3 := invoke(t, h, a.ID)
	h.sync()
	var parked string
	h.onLoop(func() { parked = h.e.parked[r3.ID] })
	if parked != "pool:"+poolHTTP {
		t.Fatalf("a run over the client's share must park on the pool list, got %q", parked)
	}
}

func TestCancelCappedRunDropsItsMeta(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4), withPerClient(1))
	g := newRunGates(h)
	a := h.create(invokeJob("a", nil))
	a1 := invoke(t, h, a.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusRunning)
	a2 := invoke(t, h, a.ID)
	h.sync()
	if _, _, err := h.e.Cancel(context.Background(), a2.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()
	var parked, meta int
	h.onLoop(func() { parked, meta = len(h.e.parked), len(h.e.meta) })
	if parked != 0 || meta != 0 {
		t.Fatalf("parked %d, meta %d after cancel; want 0", parked, meta)
	}
	g.open(a1.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusSuccess)
}

// A release scans past more capped runs than the row cache holds without
// reading any of them.
func TestCappedScanReadsNoRows(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 2100 runs")
	}
	h := newHarness(t, withMaxHTTP(2), withPerClient(1))
	g := newRunGates(h)
	a := h.create(invokeJob("a", nil))
	b := h.jobAs(h.addClient("b"), invokeJob("b", nil))
	a1 := invoke(t, h, a.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusRunning)
	ctx := context.Background()
	for i := 0; i < rowCap+52; i++ {
		if _, err := h.e.Invoke(ctx, a.ID, json.RawMessage(`{}`), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	b1 := invoke(t, h, b.ID)
	waitJobRun(h, b.ID, b1.ID, model.StatusRunning)
	b2 := invoke(t, h, b.ID)
	h.sync()
	c := countGetRun(h)
	g.open(b1.ID)
	waitJobRun(h, b.ID, b2.ID, model.StatusRunning)
	// b2's own admission may read its uncached row; the capped runs must not.
	if n := c.r.Load() + c.w.Load(); n > 4 {
		t.Fatalf("GetRun calls during the release = %d; the capped runs were read", n)
	}
}

// overlap=queue keeps a job's own queue FIFO even when its revisions differ
// in priority.
func TestJobQueueStaysFIFOAcrossPriorities(t *testing.T) {
	h := newHarness(t)
	g := newRunGates(h)
	j := h.create(invokeJob("q", &model.PolicyIn{Overlap: ptr(model.OverlapQueue), Priority: ptr("low")}))
	r1 := invoke(t, h, j.ID)
	waitJobRun(h, j.ID, r1.ID, model.StatusRunning)
	r2 := invoke(t, h, j.ID)
	if _, err := h.e.UpdateJob(context.Background(), j.ID, model.JobPatch{Policy: &model.PolicyIn{Overlap: ptr(model.OverlapQueue), Priority: ptr("high")}}); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Second)
	r3 := invoke(t, h, j.ID)
	h.sync()
	g.passJob(j.ID)
	g.open(r1.ID)
	waitCount(h, 3, model.StatusSuccess)
	wantOrder(t, callOrder(h), r1.ID, r2.ID, r3.ID)
}

// A run moved between lists by a wake keeps its one late-deadline entry and
// is missed exactly once.
func TestMovedRunPushesNoHeapEntry(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	g := newRunGates(h)
	x := h.create(asyncJobWith("x", &model.PolicyIn{Overlap: ptr(model.OverlapQueue), MaxLateSec: ptr(int64(30)), TimeoutSec: ptr(int64(600))}))
	hold := h.create(invokeJob("hold", nil))
	h.exec.set(func(ctx context.Context, in target.ExecInput) target.Result {
		if in.Run.JobID == x.ID {
			return waiting(ctx, in)
		}
		g.mu.Lock()
		ch := g.chanFor(in.Run.ID)
		g.mu.Unlock()
		<-ch
		return okResult()
	})
	r1 := invoke(t, h, x.ID)
	h.waitStatus(r1.ID, model.StatusWaiting) // holds job x, not the pool
	hr := invoke(t, h, hold.ID)
	waitJobRun(h, hold.ID, hr.ID, model.StatusRunning) // pool full
	r2 := invoke(t, h, x.ID)                           // parks on job:x
	h.sync()
	var before, after int
	var list string
	h.onLoop(func() { before, list = len(h.e.heap), h.e.parked[r2.ID] })
	if list != "job:"+x.ID {
		t.Fatalf("setup: r2 parked on %q", list)
	}
	if _, _, err := h.e.Complete(context.Background(), r1.ID, model.CompleteReq{Status: "success"}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	h.onLoop(func() { after, list = len(h.e.heap), h.e.parked[r2.ID] })
	if list != "pool:"+poolHTTP {
		t.Fatalf("r2 should have moved to the pool list, is on %q", list)
	}
	if after > before {
		t.Fatalf("heap grew from %d to %d on a move", before, after)
	}
	h.advance(31 * time.Second)
	h.waitStatus(r2.ID, model.StatusMissed)
	h.waitFor("missed count", func() bool {
		n := 0
		for _, r := range h.runs(x.ID) {
			if r.Status == model.StatusMissed {
				n++
			}
		}
		return n == 1
	})
	g.open(hr.ID)
}

func asyncJobWith(name string, p *model.PolicyIn) model.JobCreate {
	in := invokeJob(name, p)
	in.Target.HTTP.Mode = "async"
	in.Target.HTTP.LeaseSec = ptr(int64(60))
	return in
}

func TestKeyListOrdersByPriority(t *testing.T) {
	h := newHarness(t)
	g := newRunGates(h)
	keyed := func(name, prio string) model.JobCreate {
		return invokeJob(name, &model.PolicyIn{ConcurrencyKey: ptr("k"), Priority: ptr(prio)})
	}
	hold := invoke(t, h, h.create(keyed("hold", "normal")).ID)
	waitJobRun(h, hold.JobID, hold.ID, model.StatusRunning)
	low, norm, high := h.create(keyed("low", "low")), h.create(keyed("norm", "normal")), h.create(keyed("high", "high"))
	for _, j := range []model.Job{low, norm, high} {
		g.passJob(j.ID)
	}
	rl, rn, rh := invoke(t, h, low.ID), invoke(t, h, norm.ID), invoke(t, h, high.ID)
	h.sync()
	var list string
	h.onLoop(func() { list = h.e.parked[rl.ID] })
	if list == "" || list[:4] != "key:" {
		t.Fatalf("setup: low parked on %q, want a key list", list)
	}
	g.open(hold.ID)
	waitCount(h, 4, model.StatusSuccess)
	wantOrder(t, callOrder(h), hold.ID, rh.ID, rn.ID, rl.ID)
}

func TestDeleteJobDropsCappedRunMeta(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4), withPerClient(1))
	g := newRunGates(h)
	a := h.create(invokeJob("a", nil))
	other := h.create(invokeJob("other", nil))
	a1 := invoke(t, h, a.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusRunning)
	invoke(t, h, other.ID) // same client: capped, parks
	h.sync()
	var parked, meta int
	h.onLoop(func() { parked, meta = len(h.e.parked), len(h.e.meta) })
	if parked != 1 || meta != 1 {
		t.Fatalf("setup: parked %d, meta %d", parked, meta)
	}
	if err := h.e.DeleteJob(context.Background(), other.ID); err != nil {
		t.Fatal(err)
	}
	g.open(a1.ID)
	waitJobRun(h, a.ID, a1.ID, model.StatusSuccess)
	h.onLoop(func() { parked, meta = len(h.e.parked), len(h.e.meta) })
	if parked != 0 || meta != 0 {
		t.Fatalf("parked %d, meta %d after delete; want 0", parked, meta)
	}
}

// A backdated due time ranks from when the run started waiting.
func TestBackdatedRunCannotJumpAging(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	g := newRunGates(h)
	hold := invoke(t, h, h.create(invokeJob("hold", nil)).ID)
	waitJobRun(h, hold.JobID, hold.ID, model.StatusRunning)
	norm := h.create(prioJob("norm", "normal"))
	high := h.create(invokeJob("high", &model.PolicyIn{Priority: ptr("high"), MaxLateSec: ptr(int64(7 * 86400))}))
	g.passJob(norm.ID)
	g.passJob(high.ID)
	rn := invoke(t, h, norm.ID)
	h.sync()                    // parked now: its wait starts here
	h.advance(61 * time.Second) // normal has now aged past a fresh high
	old := pendingInvoke(high, h.clk.Now().Add(-6*24*time.Hour), nil)
	old.ScheduledFor = h.clk.Now()
	if err := store.InsertRun(context.Background(), h.st.W, old); err != nil {
		t.Fatal(err)
	}
	h.onLoop(func() { h.e.pushRun(old) })
	h.sync()
	g.open(hold.ID)
	waitCount(h, 3, model.StatusSuccess)
	wantOrder(t, callOrder(h), hold.ID, rn.ID, old.ID)
}

func TestPendingLimitRefusesInvoke(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	newRunGates(h)
	h.e.d.MaxPendingPerClient = 3
	j := h.create(invokeJob("f", nil))
	invoke(t, h, j.ID) // runs: in flight counts too
	h.sync()
	for i := 0; i < 2; i++ {
		invoke(t, h, j.ID) // pending
	}
	if _, err := h.e.Invoke(context.Background(), j.ID, nil, nil, nil); !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("4th invoke in flight: %v, want ErrPendingLimit", err)
	}
	other := h.addClient("other")
	spec := model.AdhocReq{Client: "other", Target: httpTarget()}
	if _, err := h.e.Adhoc(context.Background(), other, spec, nil); err != nil {
		t.Fatalf("another client must not be limited: %v", err)
	}
}

// Retries of failing on-demand runs count against the limit.
func TestPendingLimitCountsRetries(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4))
	h.e.d.MaxPendingPerClient = 2
	h.exec.set(failAmbiguous)
	j := h.create(invokeJob("f", &model.PolicyIn{Retry: &model.RetryIn{MaxAttempts: ptr(10), BackoffSec: ptr(int64(3600))}}))
	invoke(t, h, j.ID)
	invoke(t, h, j.ID)
	waitCount(h, 2, model.StatusFailed)
	if _, err := h.e.Invoke(context.Background(), j.ID, nil, nil, nil); !errors.Is(err, ErrPendingLimit) {
		t.Fatalf("invoke with 2 retries pending: %v, want ErrPendingLimit", err)
	}
}
