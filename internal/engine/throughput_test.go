package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

// getRunCounter counts engine GetRun calls by exact SQL text, per pool. The
// harness polls with h.run (GetRun on W), so tests that count wait with
// h.sync and read through h.runs or countStatus instead.
type getRunCounter struct{ w, r atomic.Int64 }

func countGetRun(h *harness) *getRunCounter {
	h.t.Helper()
	var text string
	h.st.R.SetQueryHook(func(q string) { text = q })
	_, _ = store.GetRun(context.Background(), h.st.R, "probe")
	h.st.R.SetQueryHook(nil)
	c := &getRunCounter{}
	h.st.W.SetQueryHook(func(q string) {
		if q == text {
			c.w.Add(1)
		}
	})
	h.st.R.SetQueryHook(func(q string) {
		if q == text {
			c.r.Add(1)
		}
	})
	h.t.Cleanup(func() { h.st.W.SetQueryHook(nil); h.st.R.SetQueryHook(nil) })
	return c
}

func countStatus(h *harness, statuses ...model.Status) int {
	h.t.Helper()
	n := 0
	for _, s := range statuses {
		var c int
		if err := h.st.R.QueryRowContext(context.Background(), `SELECT count(*) FROM runs WHERE status = ?`, string(s)).Scan(&c); err != nil {
			h.t.Fatal(err)
		}
		n += c
	}
	return n
}

// waitCount waits, without GetRun, until want runs have one of statuses.
func waitCount(h *harness, want int, statuses ...model.Status) {
	h.t.Helper()
	h.waitFor(fmt.Sprintf("%d runs %v", want, statuses), func() bool { return countStatus(h, statuses...) == want })
	h.sync() // the statuses commit before the handlers' post-commit work
}

// waitJobRun waits, without GetRun, until the job's run id has status want.
func waitJobRun(h *harness, jobID, id string, want model.Status) model.Run {
	h.t.Helper()
	var got model.Run
	h.waitFor("run "+id+" → "+string(want), func() bool {
		for _, r := range h.runs(jobID) {
			if r.ID == id {
				got = r
				return r.Status == want
			}
		}
		return false
	})
	h.sync()
	return got
}

func pendingInvoke(j model.Job, at time.Time, payload json.RawMessage) model.Run {
	id := newID()
	return model.Run{ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: model.KindInvoke, Attempt: 1, ChainRootID: id,
		Status: model.StatusPending, ScheduledFor: at, OriginFor: at, ContextFor: at, LatenessRef: at, Rev: j.Rev, Payload: payload, CreatedAt: at}
}

func (h *harness) cached(id string) (ok bool) {
	h.onLoop(func() { _, ok = h.e.rows[id] })
	return ok
}

func TestInvokedRunsReadNoRows(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	c := countGetRun(h)
	for i := 0; i < 50; i++ {
		invoke(t, h, j.ID)
	}
	waitCount(h, 50, model.StatusSuccess)
	if w, r := c.w.Load(), c.r.Load(); w+r != 0 {
		t.Fatalf("engine GetRun: W %d, R %d; want 0", w, r)
	}
}

func TestSameInstantBurstAdmitsParkedWithoutReads(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	for i := 0; i < 100; i++ {
		h.create(cronJob(fmt.Sprintf("tick-%03d", i), "* * * * *", nil)) // first run 60 s ahead: not cached
	}
	c := countGetRun(h)
	h.advance(time.Minute)
	var parked, inflight, rows int
	h.onLoop(func() { parked, inflight, rows = len(h.e.parked), h.e.inflight[poolHTTP], len(h.e.rows) })
	if parked != 36 || inflight != 64 || rows != 36 {
		t.Fatalf("parked %d, inflight %d, rows %d; want 36, 64, 36", parked, inflight, rows)
	}
	g.open()
	waitCount(h, 100, model.StatusSuccess)
	if r, w := c.r.Load(), c.w.Load(); r != 100 || w != 0 {
		t.Fatalf("GetRun: R %d (first dispatch fallback, want 100), W %d (want 0)", r, w)
	}
	_, _, fl := h.counts()
	if fl[poolHTTP] != 0 {
		t.Fatalf("inflight = %v", fl)
	}
}

func TestStaleCachedRowNeverExecutes(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	ctx := context.Background()
	r := pendingInvoke(j, h.clk.Now().Add(2*time.Second), nil)
	if err := store.InsertRun(ctx, h.st.W, r); err != nil {
		t.Fatal(err)
	}
	if err := h.e.send(ctx, evRunInserted{runs: []model.Run{r}}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	if !h.cached(r.ID) {
		t.Fatal("a run due within the horizon must be cached at push")
	}
	// Cancelled behind the loop's back: no event.
	if ok, err := store.Transition(ctx, h.st.W, r.ID, []model.Status{model.StatusPending}, model.StatusCancelled, store.Fields{"finished_at": h.clk.Now()}); err != nil || !ok {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	h.advance(2 * time.Second)
	h.sync()
	if n := h.exec.callCount(); n != 0 {
		t.Fatalf("executor calls = %d", n)
	}
	if h.cached(r.ID) {
		t.Fatal("the stale row must be dropped")
	}
	if got := h.run(r.ID); got.Status != model.StatusCancelled {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestEarlyResultAppliedFromHeldRow(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	h.exec.set(func(context.Context, targetInput) targetResult {
		<-release
		return targetResult{Waiting: true, HTTPStatus: code(202), Delivered: true}
	})
	j := h.create(asyncJob("pub"))
	r := invoke(t, h, j.ID)
	waitJobRun(h, j.ID, r.ID, model.StatusRunning)
	if _, accepted, err := h.e.Complete(context.Background(), r.ID, model.CompleteReq{Status: "success", Output: json.RawMessage(`{"early":1}`)}); err != nil || !accepted {
		t.Fatalf("early complete: %v %v", accepted, err)
	}
	c := countGetRun(h) // Complete itself reads; count from here
	close(release)
	got := waitJobRun(h, j.ID, r.ID, model.StatusSuccess)
	if w, rr := c.w.Load(), c.r.Load(); w+rr != 0 {
		t.Fatalf("GetRun after complete: W %d, R %d", w, rr)
	}
	if !bytes.Equal(compactJSON(got.Output), []byte(`{"early":1}`)) {
		t.Fatalf("output = %s, want the early result's", got.Output)
	}
}

func TestLostInsertDispatchedThroughReader(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	r := pendingInvoke(j, h.clk.Now().Add(70*time.Second), nil)
	if err := store.InsertRun(context.Background(), h.st.W, r); err != nil { // its event was lost
		t.Fatal(err)
	}
	c := countGetRun(h)
	h.advance(61 * time.Second)
	h.sync() // reconcile ran after the advance's pass
	var inHeap int
	h.onLoop(func() { inHeap = h.e.inHeap[r.ID] })
	if inHeap != 1 || h.cached(r.ID) {
		t.Fatalf("reconcile must re-push without caching (9 s away): inHeap %d, cached %v", inHeap, h.cached(r.ID))
	}
	h.advance(9 * time.Second)
	waitJobRun(h, j.ID, r.ID, model.StatusSuccess)
	if rr, w := c.r.Load(), c.w.Load(); rr != 1 || w != 0 {
		t.Fatalf("GetRun: R %d (want 1), W %d (want 0)", rr, w)
	}
}

func TestExecDoneWithoutHeldRowFallsBack(t *testing.T) {
	h := newHarness(t)
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", nil))
	r := invoke(t, h, j.ID)
	waitJobRun(h, j.ID, r.ID, model.StatusRunning)
	h.onLoop(func() {
		hd := h.e.held[r.ID]
		hd.run = model.Run{}
		h.e.held[r.ID] = hd
	})
	c := countGetRun(h)
	g.open()
	waitJobRun(h, j.ID, r.ID, model.StatusSuccess)
	if w := c.w.Load(); w != 1 {
		t.Fatalf("fallback GetRun on W = %d, want 1", w)
	}
	_, _, inflight := h.counts()
	if len(inflight) != 0 {
		t.Fatalf("slots leaked: %v", inflight)
	}
}

func TestRunPushedTwiceExecutesOnce(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	ctx := context.Background()
	r := pendingInvoke(j, h.clk.Now().Add(120*time.Second), nil)
	if err := store.InsertRun(ctx, h.st.W, r); err != nil {
		t.Fatal(err)
	}
	h.advance(61 * time.Second)
	h.sync() // reconcile re-push
	h.advance(57 * time.Second)
	if err := h.e.send(ctx, evRunInserted{runs: []model.Run{r}}); err != nil { // the late event
		t.Fatal(err)
	}
	h.sync()
	var inHeap int
	h.onLoop(func() { inHeap = h.e.inHeap[r.ID] })
	if inHeap != 2 || !h.cached(r.ID) {
		t.Fatalf("inHeap %d, cached %v; want 2, true", inHeap, h.cached(r.ID))
	}
	h.advance(2 * time.Second)
	waitJobRun(h, j.ID, r.ID, model.StatusSuccess)
	if n := h.exec.callCount(); n != 1 {
		t.Fatalf("executor calls = %d, want 1", n)
	}
}

func TestRowCacheIsBounded(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", nil))
	big := json.RawMessage(`"` + string(bytes.Repeat([]byte{'x'}, 200<<10)) + `"`)
	small := json.RawMessage(`"` + string(bytes.Repeat([]byte{'x'}, 1<<10)) + `"`)
	var far, due int
	h.onLoop(func() {
		at := h.e.now().Add(time.Hour)
		for i := 0; i < 10_000; i++ {
			h.e.pushRun(pendingInvoke(j, at, big))
		}
		far = len(h.e.rows)
		now := h.e.now()
		for i := 0; i < 3_000; i++ {
			h.e.pushRun(pendingInvoke(j, now, small))
		}
		due = len(h.e.rows)
	})
	if far != 0 {
		t.Fatalf("1 h ahead with 200 KiB payloads: rows = %d, want 0", far)
	}
	if due > rowCap {
		t.Fatalf("rows = %d, cap %d", due, rowCap)
	}
}

func TestRowCacheDoesNotLeak(t *testing.T) {
	h := newHarness(t, withMaxHTTP(4))
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	ctx := context.Background()
	a := h.create(invokeJob("a", nil))
	b := h.create(invokeJob("b", nil))
	c := h.create(invokeJob("c", nil))
	var aRuns []model.Run
	for i := 0; i < 1000; i++ {
		jobID := []string{a.ID, b.ID, c.ID}[i%3]
		r, err := h.e.Invoke(ctx, jobID, json.RawMessage(`{"i":1}`), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if jobID == a.ID {
			aRuns = append(aRuns, r)
		}
	}
	h.sync()
	cancelled := 0
	for _, r := range aRuns[len(aRuns)-50:] { // parked: still pending
		if _, _, err := h.e.Cancel(ctx, r.ID); err == nil {
			cancelled++
		}
	}
	if cancelled != 50 {
		t.Fatalf("cancelled %d of 50", cancelled)
	}
	if err := h.e.DeleteJob(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	h.sync()

	check := func(when string) {
		t.Helper()
		var orphans []string
		var held, live int
		h.onLoop(func() {
			for id := range h.e.rows {
				if h.e.inHeap[id] == 0 && h.e.parked[id] == "" {
					orphans = append(orphans, id)
				}
			}
			held = len(h.e.held)
			live = countStatus(h, model.StatusRunning, model.StatusWaiting)
		})
		if len(orphans) > 0 {
			t.Fatalf("%s: %d rows outside heap and wait lists, e.g. %s", when, len(orphans), orphans[0])
		}
		if held != live {
			t.Fatalf("%s: held %d, running+waiting in the DB %d", when, held, live)
		}
	}
	check("parked")
	h.advance(61 * time.Second)
	h.sync() // reconcile
	check("after reconcile")
	g.open()
	waitCount(h, 0, model.StatusPending, model.StatusRunning, model.StatusWaiting)
	check("drained")
	var rows int
	h.onLoop(func() { rows = len(h.e.rows) })
	if rows != 0 {
		t.Fatalf("rows left after drain: %d", rows)
	}
}

func TestWakeKeepsHeadOnTransientReadError(t *testing.T) {
	h := newHarness(t, withMaxHTTP(1))
	g := newGate()
	h.exec.set(g.behaviour(okResult()))
	j := h.create(invokeJob("f", nil))
	a := invoke(t, h, j.ID) // holds the only slot
	waitJobRun(h, j.ID, a.ID, model.StatusRunning)

	big := json.RawMessage(`{"pad":"` + string(bytes.Repeat([]byte("x"), 9<<10)) + `"}`) // too big to cache
	b := pendingInvoke(j, h.clk.Now(), big)
	if err := store.InsertRun(context.Background(), h.st.W, b); err != nil {
		t.Fatal(err)
	}
	h.onLoop(func() { h.e.pushRun(b) })
	h.sync()
	var parked bool
	h.onLoop(func() { _, parked = h.e.parked[b.ID] })
	if !parked || h.cached(b.ID) {
		t.Fatalf("setup: parked %v, cached %v; want parked and uncached", parked, h.cached(b.ID))
	}

	var getRun string
	h.st.R.SetQueryHook(func(q string) { getRun = q })
	_, _ = store.GetRun(context.Background(), h.st.R, "probe")
	h.st.R.SetQueryHook(nil)
	h.st.R.SetFaultHook(func(q string) error {
		if q == getRun {
			return errors.New("transient read failure")
		}
		return nil
	})
	g.open() // a finishes, its release wakes the list, the head's read fails
	waitJobRun(h, j.ID, a.ID, model.StatusSuccess)
	h.onLoop(func() { _, parked = h.e.parked[b.ID] })
	if !parked {
		t.Fatal("a transient read error must leave the head parked, not drop it")
	}

	h.st.R.SetFaultHook(nil)
	h.onLoop(func() { h.e.wake("pool:" + poolHTTP) })
	waitJobRun(h, j.ID, b.ID, model.StatusSuccess)
}

// A stale cached row past max_late loses its finalize and pages nothing.
func TestStaleRowPastMaxLateIsNotPagedMissed(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("f", &model.PolicyIn{MaxLateSec: ptr(int64(1))}))
	ctx := context.Background()
	r := pendingInvoke(j, h.clk.Now().Add(2*time.Second), nil)
	if err := store.InsertRun(ctx, h.st.W, r); err != nil {
		t.Fatal(err)
	}
	if err := h.e.send(ctx, evRunInserted{runs: []model.Run{r}}); err != nil {
		t.Fatal(err)
	}
	h.sync()
	if ok, err := store.Transition(ctx, h.st.W, r.ID, []model.Status{model.StatusPending}, model.StatusCancelled, store.Fields{"finished_at": h.clk.Now()}); err != nil || !ok {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	h.advance(4 * time.Second) // past max_late: the missed branch, on a stale row
	h.sync()
	if n := h.pager.count("missed"); n != 0 {
		t.Fatalf("missed pages = %d (%v)", n, h.pager.titles())
	}
	if got := h.run(r.ID); got.Status != model.StatusCancelled {
		t.Fatalf("status = %s", got.Status)
	}
}
