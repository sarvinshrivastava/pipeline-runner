package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

const (
	maxEventsPerIteration = 256
	maxFiresPerIteration  = 512
	reconcileEvery        = 60 * time.Second
)

type event interface{ isEvent() }

type evRunInserted struct{ runs []model.Run }
type evJobChanged struct {
	jobID    string
	deleted  bool
	inserted []model.Run
	released []string // waiting runs cancelled in the write's transaction
	dropped  []string // pending runs cancelled in the write's transaction
	cancel   []string // running runs to cancel
}
type evExecDone struct {
	runID string
	res   target.Result
}
type evLeaseExpired struct {
	runID string
	gen   uint64
}
type evCancel struct {
	runID string
	reply chan cancelReply
}
type evComplete struct {
	runID string
	req   model.CompleteReq
	reply chan completeReply
}
type evHeartbeat struct {
	runID string
	reply chan hbReply
}
type evAwait struct {
	chainRoot string
	ch        chan model.Run
}
type evAwaitDrop struct {
	chainRoot string
	ch        chan model.Run
}
type evShutdown struct {
	drainBy time.Time
	done    chan struct{}
}
type evSync struct{ done chan struct{} }

// evCall runs f on the loop (tests inspect loop-owned state with it).
type evCall struct {
	f    func()
	done chan struct{}
}

func (evRunInserted) isEvent()  {}
func (evJobChanged) isEvent()   {}
func (evExecDone) isEvent()     {}
func (evLeaseExpired) isEvent() {}
func (evCancel) isEvent()       {}
func (evComplete) isEvent()     {}
func (evHeartbeat) isEvent()    {}
func (evAwait) isEvent()        {}
func (evAwaitDrop) isEvent()    {}
func (evShutdown) isEvent()     {}
func (evSync) isEvent()         {}
func (evCall) isEvent()         {}

type cancelReply struct {
	run      model.Run
	accepted bool // a running run is being cancelled (202)
	err      error
}
type completeReply struct {
	run      model.Run
	accepted bool // stored as an early result (202)
	err      error
}
type hbReply struct {
	run   model.Run
	lease *time.Time
	err   error
}

func (e *Engine) loop() {
	defer close(e.done)
	timer := e.d.Clock.NewTimer(time.Second)
	defer timer.Stop()
	for {
		now := e.now()
		e.loopAt.Store(now.UnixMilli())
		if e.draining && (e.inflight[poolHTTP] == 0 || !now.Before(e.drainBy)) {
			e.accepting.Store(false)
			close(e.drainDone)
			return
		}
		sleep := time.Second
		if next, ok := e.heap.peek(); ok && !e.draining {
			if w := next.at.Sub(now); w < sleep {
				sleep = max(w, 0)
			}
		}
		if e.draining {
			sleep = min(sleep, max(e.drainBy.Sub(now), 0))
		}
		timer.Stop()
		timer.Reset(sleep)

		select {
		case <-e.life.Done():
			return
		case ev := <-e.events:
			e.handle(ev)
			for i := 1; i < maxEventsPerIteration; i++ {
				select {
				case ev := <-e.events:
					e.handle(ev)
				default:
					i = maxEventsPerIteration
				}
			}
		case <-timer.C():
		}
		now = e.now()
		e.loopAt.Store(now.UnixMilli())
		if !e.draining {
			e.fireDue(now)
		}
		e.retryUnflushed(now)
		if now.Sub(e.lastReconcile) >= reconcileEvery || now.Before(e.lastReconcile) {
			e.reconcile(now)
		}
		e.updateGauges()
	}
}

func (e *Engine) handle(ev event) {
	switch v := ev.(type) {
	case evRunInserted:
		for _, r := range v.runs {
			e.push(r.ID, r.ScheduledFor)
		}
	case evJobChanged:
		e.onJobChanged(v)
	case evExecDone:
		e.onExecDone(v.runID, v.res)
	case evLeaseExpired:
		e.onLeaseExpired(v.runID, v.gen)
	case evCancel:
		r, accepted, err := e.cancelRun(v.runID)
		reply(v.reply, cancelReply{run: r, accepted: accepted, err: err})
	case evComplete:
		r, accepted, err := e.completeRun(v.runID, v.req)
		reply(v.reply, completeReply{run: r, accepted: accepted, err: err})
	case evHeartbeat:
		r, lease, err := e.heartbeat(v.runID)
		reply(v.reply, hbReply{run: r, lease: lease, err: err})
	case evAwait:
		e.awaiters[v.chainRoot] = append(e.awaiters[v.chainRoot], v.ch)
		if r, err := store.ChainLatest(e.life, e.st.W, v.chainRoot); err == nil && r.Status.Terminal() {
			e.notifyAwaiters(r)
		}
	case evAwaitDrop:
		list := e.awaiters[v.chainRoot]
		for i, ch := range list {
			if ch == v.ch {
				e.awaiters[v.chainRoot] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(e.awaiters[v.chainRoot]) == 0 {
			delete(e.awaiters, v.chainRoot)
		}
	case evShutdown:
		e.draining, e.drainBy, e.drainDone = true, v.drainBy, v.done
	case evSync:
		e.fireDue(e.now())
		close(v.done)
	case evCall:
		v.f()
		close(v.done)
	}
}

// reply never blocks: channels have capacity 1 and the reader may be gone.
func reply[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

func (e *Engine) push(runID string, at time.Time) {
	e.heap.push(entry{runID: runID, at: at, kind: entryDue})
	e.inHeap[runID]++
}

func (e *Engine) fireDue(now time.Time) {
	for i := 0; i < maxFiresPerIteration; i++ {
		next, ok := e.heap.peek()
		if !ok || next.at.After(now) {
			break
		}
		ent := e.heap.pop()
		if ent.kind == entryDue {
			if e.inHeap[ent.runID]--; e.inHeap[ent.runID] <= 0 {
				delete(e.inHeap, ent.runID)
			}
			if _, isParked := e.parked[ent.runID]; isParked {
				continue // its wait list or late deadline owns it now
			}
		} else {
			if _, isParked := e.parked[ent.runID]; !isParked {
				continue
			}
			e.unpark(ent.runID)
		}
		e.dispatch(ent.runID, now)
	}
	e.flushMissedPages()
}

// spec returns the snapshot a run executes with.
func (e *Engine) spec(r model.Run) (model.RunSpec, error) {
	if r.AdhocSpec != nil {
		return *r.AdhocSpec, nil
	}
	k := revKey{r.JobID, r.Rev}
	if s, ok := e.revs[k]; ok {
		return s, nil
	}
	s, err := store.JobRevision(e.life, e.st.W, r.JobID, r.Rev)
	if err != nil {
		return model.RunSpec{}, err
	}
	e.revs[k] = s
	return s, nil
}

// blocker names the wait list that blocks r, or "" when it can run.
// skip reports that r is a schedule run blocked only by its own job under
// overlap=skip.
func (e *Engine) blocker(r model.Run, spec model.RunSpec) (list string, skip bool) {
	if r.JobID != "" && spec.Policy.Overlap != model.OverlapAllow && e.jobBusy[r.JobID] > 0 {
		if spec.Policy.Overlap == model.OverlapSkip && r.Kind == model.KindSchedule {
			return "", true
		}
		return "job:" + r.JobID, false
	}
	if ns := nsKeyOf(r, spec); ns != "" && e.keyBusy[ns] >= e.keyLimit(ns, spec.Policy.ConcurrencyLimit) {
		return "key:" + ns, false
	}
	pool := poolOf(spec.Target.Type)
	if e.inflight[pool] >= e.poolCap(pool) {
		return "pool:" + pool, false
	}
	return "", false
}

// keyLimit is the minimum limit across the client's jobs using the key.
func (e *Engine) keyLimit(ns string, own int) int {
	limit := max(own, 1)
	for _, je := range e.jobs {
		if je.job.NSKey == ns && je.job.Policy.ConcurrencyLimit > 0 && je.job.Policy.ConcurrencyLimit < limit {
			limit = je.job.Policy.ConcurrencyLimit
		}
	}
	return limit
}

func (e *Engine) dispatch(runID string, now time.Time) {
	ctx := e.life
	r, err := store.GetRun(ctx, e.st.W, runID)
	if err != nil || r.Status != model.StatusPending {
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			e.log.Error("load run for dispatch", "run", runID, "err", err)
			e.push(runID, now.Add(time.Second))
		}
		return // ghost: deleted or no longer pending
	}
	spec, err := e.spec(r)
	if err != nil {
		e.log.Error("load run spec", "run", runID, "err", err)
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusFailed, err: "run specification missing", page: true})
		return
	}
	var je *jobEntry
	if r.JobID != "" {
		je = e.jobs[r.JobID]
		if je == nil && r.Kind != model.KindAdhoc {
			e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusCancelled, err: "job deleted"})
			return
		}
	}

	late := now.Sub(r.LatenessRef)
	if late > time.Duration(spec.Policy.MaxLateSec)*time.Second {
		lateMs := late.Milliseconds()
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusMissed, lateMs: &lateMs,
			err: fmt.Sprintf("not dispatched within max_late_sec (%ds)", spec.Policy.MaxLateSec)})
		if r.JobID != "" {
			e.missedBatch[r.JobID]++
		}
		return
	}

	list, skip := e.blocker(r, spec)
	if skip {
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusSkipped, err: "previous run still in progress"})
		return
	}
	if list != "" {
		e.park(r, spec, list)
		return
	}

	late = max(late, 0)
	lateMs := late.Milliseconds()
	deadline := now.Add(time.Duration(spec.Policy.TimeoutSec) * time.Second)
	ok, err := store.Transition(ctx, e.st.W, r.ID, []model.Status{model.StatusPending}, model.StatusRunning, store.Fields{
		"started_at": now, "late_ms": lateMs, "deadline_at": deadline,
	})
	if err != nil {
		e.log.Error("claim run", "run", r.ID, "err", err)
		e.push(r.ID, now.Add(time.Second))
		return
	}
	if !ok {
		return
	}
	r.Status, r.StartedAt, r.LateMs, r.DeadlineAt = model.StatusRunning, &now, &lateMs, &deadline

	h := held{jobID: r.JobID, ns: nsKeyOf(r, spec), pool: poolOf(spec.Target.Type)}
	e.reserve(r.ID, h)

	jobLabel := r.JobName
	if e.d.Metrics != nil {
		e.d.Metrics.FireDelay.WithLabelValues(spec.Target.Type).Observe(max(now.Sub(r.ScheduledFor), 0).Seconds())
		e.d.Metrics.Late.WithLabelValues(jobLabel).Observe(late.Seconds())
	}
	if late > time.Duration(spec.Policy.LateWarnSec)*time.Second {
		e.page(r, notify.Fail, "fired late", fmt.Sprintf("Run %s fired %s late (late_warn_sec %d).", r.ID, late.Round(time.Millisecond), spec.Policy.LateWarnSec))
	}
	// Cron jobs get their next occurrence now, so overlap=skip can see it come
	// due while this run is still going. A one-shot has nothing to add, and
	// its job must not be marked done while a retry may still follow.
	if r.Kind == model.KindSchedule && je != nil && je.job.Trigger.Type == model.TriggerCron {
		e.materialise(je)
	}
	e.startExec(r, spec, deadline)
}

func (e *Engine) reserve(runID string, h held) {
	e.held[runID] = h
	if h.jobID != "" {
		e.jobBusy[h.jobID]++
	}
	if h.ns != "" {
		e.keyBusy[h.ns]++
	}
	if h.pool != "" {
		e.inflight[h.pool]++
	}
}

// release frees what a run holds. Without a held entry it is a no-op.
func (e *Engine) release(runID string) {
	h, ok := e.held[runID]
	if !ok {
		return
	}
	delete(e.held, runID)
	dec := func(m map[string]int, k string) {
		if k == "" {
			return
		}
		if m[k]--; m[k] <= 0 {
			if m[k] < 0 {
				e.log.Error("gate count went negative", "key", k)
			}
			delete(m, k)
		}
	}
	dec(e.jobBusy, h.jobID)
	dec(e.keyBusy, h.ns)
	dec(e.inflight, h.pool)
	e.wakeFor(h)
}

// releasePool frees only the pool slot (running → waiting).
func (e *Engine) releasePool(runID string) {
	h, ok := e.held[runID]
	if !ok || h.pool == "" {
		return
	}
	pool := h.pool
	h.pool = ""
	e.held[runID] = h
	if e.inflight[pool]--; e.inflight[pool] <= 0 {
		delete(e.inflight, pool)
	}
	e.wake("pool:" + pool)
}

func (e *Engine) wakeFor(h held) {
	if h.jobID != "" {
		e.wake("job:" + h.jobID)
	}
	if h.ns != "" {
		e.wake("key:" + h.ns)
	}
	if h.pool != "" {
		e.wake("pool:" + h.pool)
	}
}

func (e *Engine) park(r model.Run, spec model.RunSpec, list string) {
	if _, already := e.parked[r.ID]; already {
		return
	}
	e.parked[r.ID] = list
	e.waits[list] = append(e.waits[list], r.ID)
	e.heap.push(entry{runID: r.ID, at: r.LatenessRef.Add(time.Duration(spec.Policy.MaxLateSec)*time.Second + time.Millisecond), kind: entryLateDeadline})
}

func (e *Engine) unpark(runID string) {
	list, ok := e.parked[runID]
	if !ok {
		return
	}
	delete(e.parked, runID)
	ids := e.waits[list]
	for i, id := range ids {
		if id == runID {
			e.waits[list] = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	if len(e.waits[list]) == 0 {
		delete(e.waits, list)
	}
}

// wake admits runs from the head of a wait list while they can run. Ghost
// heads are popped in a loop so they never use up a wake.
func (e *Engine) wake(list string) {
	now := e.now()
	for {
		ids := e.waits[list]
		if len(ids) == 0 {
			delete(e.waits, list)
			return
		}
		head := ids[0]
		r, err := store.GetRun(e.life, e.st.W, head)
		if err != nil || r.Status != model.StatusPending {
			e.unpark(head)
			continue
		}
		spec, err := e.spec(r)
		if err != nil {
			e.unpark(head)
			e.dispatch(head, now) // fails it with a clear error
			continue
		}
		if blockedBy, skip := e.blocker(r, spec); blockedBy != "" && !skip {
			if blockedBy != list {
				e.unpark(head)
				e.park(r, spec, blockedBy)
				continue
			}
			return
		}
		e.unpark(head)
		e.dispatch(head, now)
	}
}

func (e *Engine) flushMissedPages() {
	for jobID, n := range e.missedBatch {
		delete(e.missedBatch, jobID)
		je := e.jobs[jobID]
		if je == nil {
			continue
		}
		body := "1 run missed its max_late window and was not run."
		if n > 1 {
			body = fmt.Sprintf("%d runs missed their max_late window and were not run.", n)
		}
		e.pageJob(je.job, notify.Fail, "missed", body)
	}
}

// startExec runs the target off the loop.
func (e *Engine) startExec(r model.Run, spec model.RunSpec, deadline time.Time) {
	// The deadline is on the engine clock; the context needs a duration.
	ctx, cancel := context.WithTimeout(e.life, max(deadline.Sub(e.now()), 0))
	e.cancelFns[r.ID] = cancel
	go func() {
		defer cancel()
		res := e.execute(ctx, r, spec)
		_ = e.send(e.life, evExecDone{runID: r.ID, res: res})
	}()
}

func (e *Engine) updateGauges() {
	m := e.d.Metrics
	if m == nil {
		return
	}
	m.PendingRuns.Set(float64(len(e.inHeap) + len(e.parked)))
	for _, p := range []string{poolHTTP, poolContainer} {
		m.Inflight.WithLabelValues(p).Set(float64(e.inflight[p]))
		m.QueueDepth.WithLabelValues(p).Set(float64(len(e.waits["pool:"+p])))
	}
	m.WaitingRuns.Set(float64(len(e.leaseTimers)))
	m.UnflushedWrites.Set(float64(len(e.unflushed)))
}

// Sync processes queued events and fires everything due. Tests use it.
func (e *Engine) Sync(ctx context.Context) error {
	done := make(chan struct{})
	if err := e.send(ctx, evSync{done: done}); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unflushed reports how many terminal results are held in memory.
func (e *Engine) Unflushed() int {
	// Read without the loop: a racy snapshot is fine for status reporting.
	return int(e.unflushedCount.Load())
}
