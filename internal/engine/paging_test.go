package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// PLR-1: per-job failure-page collapse.

type execFn = func(context.Context, target.ExecInput) target.Result

// boom returns an exec that fails without retry with "boom-<n>", n counting
// from 1 across every call of the returned func.
func boom() execFn {
	var mu sync.Mutex
	n := 0
	return func(context.Context, target.ExecInput) target.Result {
		mu.Lock()
		n++
		k := n
		mu.Unlock()
		return target.Result{Status: model.StatusFailed, HTTPStatus: code(500), Error: fmt.Sprintf("boom-%d", k), NoRetry: true}
	}
}

// matching returns copies of the pages whose title contains substr.
func (p *fakePager) matching(substr string) []notify.Msg {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []notify.Msg
	for _, m := range p.msgs {
		if strings.Contains(m.Title, substr) {
			out = append(out, m)
		}
	}
	return out
}

// runOnce invokes jobID with exec f (nil: success) and waits for want.
func (h *harness) runOnce(jobID string, f execFn, want model.Status) model.Run {
	h.t.Helper()
	h.exec.set(f)
	return h.waitStatus(invoke(h.t, h, jobID).ID, want)
}

func (h *harness) failState(jobID string) (fs failState, ok bool) {
	h.onLoop(func() {
		if p := h.e.failStates[jobID]; p != nil {
			fs, ok = *p, true
		}
	})
	return
}

func (h *harness) batch(jobID string) (pc pageCount, ok bool) {
	h.onLoop(func() {
		if p := h.e.pageBatch[jobID]; p != nil {
			pc, ok = *p, true
		}
	})
	return
}

func TestFailurePagesOnceThenSummary(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := boom()
	for range 10 {
		h.runOnce(j.ID, f, model.StatusFailed)
	}
	if n := h.pager.count("j: failed"); n != 1 {
		t.Fatalf("failure pages = %d, want 1: %v", n, h.pager.titles())
	}
	h.advance(missPageEvery + time.Second)
	got := h.pager.matching("j: failed")
	if len(got) != 2 {
		t.Fatalf("pages after window = %v", h.pager.titles())
	}
	if b := got[1].Body; !strings.Contains(b, "9 more runs failed") || !strings.Contains(b, "boom-10") {
		t.Fatalf("summary body = %q", b)
	}
	for _, m := range got {
		if m.Level != notify.Fail {
			t.Fatalf("level = %v", m.Level)
		}
	}
}

func TestRecoveredOnceAfterPagedFailure(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	h.runOnce(j.ID, boom(), model.StatusFailed)
	h.runOnce(j.ID, nil, model.StatusSuccess)
	got := h.pager.matching("j: recovered")
	if len(got) != 1 || got[0].Level != notify.Fail || !slices.Equal(got[0].Tags, []string{"white_check_mark"}) {
		t.Fatalf("recovered = %+v", got)
	}
	h.runOnce(j.ID, nil, model.StatusSuccess)
	if n := len(h.pager.titles()); n != 2 {
		t.Fatalf("a second success must not page: %v", h.pager.titles())
	}
}

func TestRecoveredCarriesHeldFailures(t *testing.T) {
	h := newHarness(t) // the harness client has no ntfy_topic
	j := h.create(invokeJob("j", nil))
	f := boom()
	for range 3 {
		h.runOnce(j.ID, f, model.StatusFailed)
	}
	h.runOnce(j.ID, nil, model.StatusSuccess)
	if ts := h.pager.titles(); len(ts) != 2 || ts[0] != "j: failed" || ts[1] != "j: recovered" {
		t.Fatalf("pages = %v", ts)
	}
	m := h.pager.matching("j: recovered")[0]
	if m.Level != notify.Fail || !slices.Equal(m.Tags, []string{"white_check_mark"}) || m.Topic != "" ||
		!strings.Contains(m.Body, "2 more runs failed") || !strings.Contains(m.Body, "boom-3") {
		t.Fatalf("recovered = %+v", m)
	}
	if _, ok := h.batch(j.ID); ok {
		t.Fatal("the recovered page must take the held failures")
	}
	h.advance(missPageEvery + time.Second)
	if n := len(h.pager.titles()); n != 2 {
		t.Fatalf("nothing is left to flush: %v", h.pager.titles())
	}
}

func TestNotifyOnSuccessReplacedByRecovered(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", &model.PolicyIn{NotifyOnSuccess: ptr(true)}))
	h.runOnce(j.ID, boom(), model.StatusFailed)
	h.runOnce(j.ID, nil, model.StatusSuccess)
	if h.pager.count("j: recovered") != 1 || h.pager.count("j: succeeded") != 0 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
	h.runOnce(j.ID, nil, model.StatusSuccess)
	if h.pager.count("j: succeeded") != 1 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
}

func TestTwoJobsCollapseIndependently(t *testing.T) {
	h := newHarness(t)
	a := h.create(invokeJob("a", nil))
	b := h.create(invokeJob("b", nil))
	f := boom()
	for range 4 {
		h.runOnce(a.ID, f, model.StatusFailed)
		h.runOnce(b.ID, f, model.StatusFailed)
	}
	if h.pager.count("a: failed") != 1 || h.pager.count("b: failed") != 1 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
	h.advance(missPageEvery + time.Second)
	for _, name := range []string{"a: failed", "b: failed"} {
		got := h.pager.bodies(name)
		if len(got) != 2 || !strings.Contains(got[1], "3 more runs failed") {
			t.Fatalf("%s bodies = %q", name, got)
		}
	}
}

func TestAdhocFailuresStillPagePerRun(t *testing.T) {
	h := newHarness(t)
	h.exec.set(boom())
	for range 3 {
		r, err := h.e.Adhoc(context.Background(), h.owner, model.AdhocReq{Target: httpTarget()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		h.waitStatus(r.ID, model.StatusFailed)
	}
	if n := h.pager.count("failed"); n != 3 {
		t.Fatalf("ad hoc pages = %d: %v", n, h.pager.titles())
	}
	var n int
	h.onLoop(func() { n = len(h.e.failStates) })
	if n != 0 {
		t.Fatalf("failStates = %d", n)
	}
}

func TestNeedsHumanAndDeferCapStayPerRun(t *testing.T) {
	h := newHarness(t)
	amo := h.create(invokeJob("amo", &model.PolicyIn{Delivery: ptr(model.AtMostOnce)}))
	for range 2 {
		h.runOnce(amo.ID, failAmbiguous, model.StatusInterrupted)
	}
	until := t0.Add(2 * time.Hour)
	deferFar := func(context.Context, target.ExecInput) target.Result {
		return target.Result{DeferUntil: &until, HTTPStatus: code(200)}
	}
	dc := h.create(invokeJob("dc", &model.PolicyIn{MaxDeferSec: ptr(int64(3600))}))
	for range 2 {
		h.runOnce(dc.ID, deferFar, model.StatusFailed)
	}
	if h.pager.count("NEEDS A HUMAN") != 2 || h.pager.count("defer cap") != 2 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
	for _, id := range []string{amo.ID, dc.ID} {
		if _, ok := h.failState(id); ok {
			t.Fatal("a per-run class must not create collapse state")
		}
	}
}

func TestCollapsedFailuresStillPushDown(t *testing.T) {
	fp := &fakePusher{}
	h := newHarness(t, withPusher(fp))
	j := h.create(invokeJob("j", pushPolicy(&model.PolicyIn{PushOnFailure: ptr(true)})))
	f := boom()
	for range 5 {
		h.runOnce(j.ID, f, model.StatusFailed)
	}
	down := 0
	for _, p := range fp.all() {
		if !p.Up {
			down++
		}
	}
	if down != 5 || h.pager.count("j: failed") != 1 {
		t.Fatalf("down pushes = %d, pages = %v", down, h.pager.titles())
	}
	h.runOnce(j.ID, nil, model.StatusSuccess)
	all := fp.all()
	if len(all) != 6 || !all[5].Up || h.pager.count("j: recovered") != 1 {
		t.Fatalf("pushes = %+v, pages = %v", all, h.pager.titles())
	}
}

func TestCronJobFailuresCollapse(t *testing.T) {
	h := newHarness(t)
	h.exec.set(boom())
	j := h.create(cronJob("c", "* * * * *", nil))
	pendingSchedule := func() string {
		for _, r := range h.runs(j.ID) {
			if r.Kind == model.KindSchedule && r.Status == model.StatusPending {
				return r.ID
			}
		}
		return ""
	}
	for range 5 {
		id := pendingSchedule()
		if id == "" {
			t.Fatal("no pending schedule run")
		}
		h.advance(time.Minute)
		h.waitStatus(id, model.StatusFailed)
	}
	pc, _ := h.batch(j.ID)
	if h.pager.count("c: failed") != 1 || pc.failed != 4 || pendingSchedule() == "" {
		t.Fatalf("pages = %v, held = %d", h.pager.titles(), pc.failed)
	}
}

func TestRecoveryAfterClockStepBack(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	h.runOnce(j.ID, boom(), model.StatusFailed)
	h.clk.Set(t0.Add(-time.Hour))
	h.runOnce(j.ID, nil, model.StatusSuccess)
	if h.pager.count("j: recovered") != 1 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
}

func TestRetriedFailureNotCounted(t *testing.T) {
	h := newHarness(t)
	h.exec.set(failAmbiguous)
	retry := &model.RetryIn{MaxAttempts: ptr(3), BackoffSec: ptr(int64(1))}
	j := h.create(invokeJob("j", &model.PolicyIn{Retry: retry}))
	invoke(t, h, j.ID)
	h.waitFor("chain ends after 3 attempts", func() bool {
		rs := h.runs(j.ID)
		if len(rs) == 3 && rs[2].Status == model.StatusFailed {
			return true
		}
		h.clk.Advance(time.Second)
		return false
	})
	h.sync()
	if h.pager.count("j: failed") != 1 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
	if _, ok := h.batch(j.ID); ok {
		t.Fatal("retried attempts must not be counted")
	}
}

// pageLog records when each page went out, on the engine clock.
type pageLog struct {
	h    *harness
	seen int
	at   map[string][]time.Time // title → times
	body map[string][]string
}

func newPageLog(h *harness) *pageLog {
	return &pageLog{h: h, at: map[string][]time.Time{}, body: map[string][]string{}}
}

func (l *pageLog) record() {
	l.h.pager.mu.Lock()
	defer l.h.pager.mu.Unlock()
	for _, m := range l.h.pager.msgs[l.seen:] {
		l.at[m.Title] = append(l.at[m.Title], l.h.clk.Now())
		l.body[m.Title] = append(l.body[m.Title], m.Body)
	}
	l.seen = len(l.h.pager.msgs)
}

// pattern runs n invokes 15 s apart; run k fails when fails(k).
func (l *pageLog) pattern(jobID string, n int, fails func(k int) bool) {
	f := boom()
	for k := range n {
		if k > 0 {
			l.h.advance(15 * time.Second)
			l.record()
		}
		if fails(k) {
			l.h.runOnce(jobID, f, model.StatusFailed)
		} else {
			l.h.runOnce(jobID, nil, model.StatusSuccess)
		}
		l.record()
	}
}

// perWindow counts times in fixed missPageEvery windows anchored at start.
func perWindow(times []time.Time, start time.Time) map[int]int {
	out := map[int]int{}
	for _, at := range times {
		out[int(at.Sub(start)/missPageEvery)]++
	}
	return out
}

func TestFlappingJobDamped(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	l := newPageLog(h)
	l.pattern(j.ID, 100, func(k int) bool { return k%2 == 0 }) // 25 min; t600 and t1200 fail
	want := []time.Time{t0, t0.Add(missPageEvery), t0.Add(2 * missPageEvery)}
	if got := l.at["j: failed"]; !slices.Equal(got, want) {
		t.Fatalf("failed pages at %v, want %v", got, want)
	}
	// The job heals after every failure, so the summaries do not re-arm (P2):
	// one recovered notice for the whole flapping incident.
	if got := l.at["j: recovered"]; !slices.Equal(got, []time.Time{t0.Add(15 * time.Second)}) {
		t.Fatalf("recovered pages at %v", got)
	}
	for i, b := range l.body["j: failed"] {
		if (i == 0) == strings.Contains(b, "more run") {
			t.Fatalf("failed page %d body = %q (only the first is immediate)", i, b)
		}
		if i > 0 && !strings.Contains(b, "The job has succeeded since.") {
			t.Fatalf("summary %d body = %q", i, b)
		}
	}
}

func TestFailFailSuccessPattern(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	l := newPageLog(h)
	l.pattern(j.ID, 100, func(k int) bool { return k%3 != 2 })
	if b := l.body["j: failed"]; len(b) == 0 || strings.Contains(b[0], "more run") {
		t.Fatalf("first failure page must be immediate: %q", b)
	}
	for _, title := range []string{"j: failed", "j: recovered"} {
		for w, n := range perWindow(l.at[title], t0) {
			if n > 1 {
				t.Fatalf("%d %q pages in window %d: %v", n, title, w, l.at[title])
			}
		}
	}
	if len(l.at["j: recovered"]) < 2 {
		t.Fatalf("each arming is followed by a recovery: %v", l.at["j: recovered"])
	}
}

func TestHealedJobNoLateRearm(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := boom()
	h.runOnce(j.ID, f, model.StatusFailed)
	h.runOnce(j.ID, nil, model.StatusSuccess) // recovered
	h.runOnce(j.ID, f, model.StatusFailed)    // held: inside the window
	h.runOnce(j.ID, nil, model.StatusSuccess) // healed
	h.runOnce(j.ID, nil, model.StatusSuccess)
	h.advance(missPageEvery + time.Second)
	got := h.pager.bodies("j: failed")
	if len(got) != 2 || !strings.HasSuffix(got[1], "The job has succeeded since.") {
		t.Fatalf("failed bodies = %q", got)
	}
	for range 3 {
		h.runOnce(j.ID, nil, model.StatusSuccess)
	}
	if n := h.pager.count("j: recovered"); n != 1 {
		t.Fatalf("recovered = %d: %v", n, h.pager.titles())
	}
}

func TestRecoveryLeavesHeldMissed(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := boom()
	h.runOnce(j.ID, f, model.StatusFailed)
	h.runOnce(j.ID, f, model.StatusFailed)
	h.onLoop(func() { h.e.pageCountFor(j.ID).missed++ })
	h.runOnce(j.ID, nil, model.StatusSuccess)
	b := h.pager.bodies("j: recovered")
	if len(b) != 1 || !strings.Contains(b[0], "1 more run failed") || strings.Contains(b[0], "missed") {
		t.Fatalf("recovered bodies = %q", b)
	}
	if pc, _ := h.batch(j.ID); pc.missed != 1 || pc.failed != 0 {
		t.Fatalf("held = %+v", pc)
	}
	h.advance(missPageEvery + time.Second)
	if h.pager.count("j: missed") != 1 {
		t.Fatalf("pages = %v", h.pager.titles())
	}
}

func TestFailurePageCarriesHeldOccurrences(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	h.onLoop(func() {
		h.e.pageBatch[j.ID] = &pageCount{coalesced: 3}
		h.e.lastMissPage[j.ID] = h.e.now()
	})
	h.runOnce(j.ID, boom(), model.StatusFailed)
	b := h.pager.bodies("j: failed")
	if len(b) != 1 || !strings.Contains(b[0], "3 occurrences were coalesced") {
		t.Fatalf("failed bodies = %q", b)
	}
	if _, ok := h.batch(j.ID); ok {
		t.Fatal("the immediate page must take the held counts")
	}
}

func TestClockStepBackReopensImmediatePage(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := boom()
	h.runOnce(j.ID, f, model.StatusFailed)
	h.runOnce(j.ID, nil, model.StatusSuccess)
	h.clk.Set(t0.Add(-time.Hour))
	h.runOnce(j.ID, f, model.StatusFailed)
	b := h.pager.bodies("j: failed")
	if len(b) != 2 || strings.Contains(b[1], "more run") {
		t.Fatalf("failed bodies = %q", b)
	}
}

func TestStaleSuccessReplayDoesNotRecover(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	r := insertRunning(t, h, j, model.KindInvoke, t0, nil)
	var paged bool
	h.onLoop(func() {
		now := h.e.now()
		h.e.failStates[j.ID] = &failState{paged: true, lastFailPage: now}
		h.e.afterFinalize(r, outcome{status: model.StatusSuccess, finishedAt: now.Add(-time.Second)},
			false, -1, []model.Status{model.StatusRunning})
		paged = h.e.failStates[j.ID].paged
	})
	if h.pager.count("recovered") != 0 || !paged {
		t.Fatalf("pages = %v, paged = %v", h.pager.titles(), paged)
	}
}

func TestPageCountText(t *testing.T) {
	long := strings.Repeat("é", 400) // 800 bytes: the 512-byte cut splits a rune
	tests := []struct {
		name  string
		pc    pageCount
		title string
		body  string
	}{
		{"one failure", pageCount{failed: 1, lastStatus: model.StatusFailed, lastErr: "boom"}, "failed",
			"1 more run failed since the last page (last: failed); last error: boom."},
		{"many failures", pageCount{failed: 4, lastStatus: model.StatusTimeout, lastErr: "slow"}, "failed",
			"4 more runs failed since the last page (last: timeout); last error: slow."},
		{"empty error", pageCount{failed: 2, lastStatus: model.StatusFailed}, "failed",
			"2 more runs failed since the last page (last: failed); last error: (none)."},
		{"healed", pageCount{failed: 1, healed: true, lastStatus: model.StatusFailed, lastErr: "x"}, "failed",
			"1 more run failed since the last page (last: failed); last error: x. The job has succeeded since."},
		{"failures and missed", pageCount{failed: 1, missed: 2, lastStatus: model.StatusFailed, lastErr: "x"}, "failed",
			"1 more run failed since the last page (last: failed); last error: x. 2 runs missed their max_late window and were not run."},
		{"missed", pageCount{missed: 1, coalesced: 1}, "missed",
			"1 run missed its max_late window and was not run. 1 occurrence was coalesced into a later run because dispatch was behind schedule."},
		{"coalesced", pageCount{coalesced: 2, expired: 1}, "coalesced",
			"2 occurrences were coalesced into one run because dispatch was behind schedule. 1 occurrence expired while the runner was down or the job was blocked."},
		{"expired", pageCount{expired: 1000, capped: true}, "expired",
			"At least 1000 occurrences expired while the runner was down or the job was blocked."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := summaryTitle(&tc.pc); got != tc.title {
				t.Fatalf("title = %q, want %q", got, tc.title)
			}
			if got := tc.pc.body(); got != tc.body {
				t.Fatalf("body = %q\nwant   %q", got, tc.body)
			}
		})
	}

	// The held error is cut to maxSummaryErrBytes without a split rune.
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := func(context.Context, target.ExecInput) target.Result {
		return target.Result{Status: model.StatusFailed, Error: long, NoRetry: true}
	}
	h.runOnce(j.ID, f, model.StatusFailed)
	h.runOnce(j.ID, f, model.StatusFailed)
	pc, _ := h.batch(j.ID)
	if len(pc.lastErr) > maxSummaryErrBytes || len(pc.lastErr) < maxSummaryErrBytes-1 || !strings.HasPrefix(long, pc.lastErr) {
		t.Fatalf("held error is %d bytes", len(pc.lastErr))
	}
}

func TestFailureSummaryJoinsMissedCounts(t *testing.T) {
	h := newHarness(t)
	j := h.create(invokeJob("j", nil))
	f := boom()
	h.runOnce(j.ID, f, model.StatusFailed)
	h.runOnce(j.ID, f, model.StatusFailed)
	h.onLoop(func() { h.e.pageCountFor(j.ID).missed += 2 })
	h.advance(missPageEvery + time.Second)
	b := h.pager.bodies("j: failed")
	if len(b) != 2 || !strings.Contains(b[1], "1 more run failed") || !strings.Contains(b[1], "2 runs missed their max_late window") ||
		h.pager.count("j: missed") != 0 {
		t.Fatalf("pages = %v, bodies = %q", h.pager.titles(), b)
	}
}
