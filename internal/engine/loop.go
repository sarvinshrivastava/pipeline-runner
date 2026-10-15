package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
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
	missPageEvery         = 10 * time.Minute // per job
)

type event interface{ isEvent() }

type evRunInserted struct{ runs []model.Run }
type evJobChanged struct {
	jobID     string
	deleted   bool
	inserted  []model.Run
	coalesced int      // occurrences the write's materialise collapsed (backlog)
	expired   int      // occurrences older than max_late it skipped
	released  []string // waiting runs cancelled in the write's transaction
	dropped   []string // pending runs cancelled in the write's transaction
	cancel    []string // running runs to cancel
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
			if n := len(e.unflushed); n > 0 {
				e.log.Error("shutting down with unsaved run results; boot recovery will resolve them", "count", n)
			}
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
		e.flushMissedPages(now)
	}
}

func (e *Engine) handle(ev event) {
	switch v := ev.(type) {
	case evRunInserted:
		for _, r := range v.runs {
			e.pushRun(r)
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
		now := e.now()
		e.fireDue(now)
		e.flushMissedPages(now)
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

// pushAt queues a run by ID only; any cached row is kept.
func (e *Engine) pushAt(runID string, at time.Time) {
	e.heap.push(entry{runID: runID, at: at, kind: entryDue})
	e.inHeap[runID]++
}

// pushRun queues a run and caches its row when it is due within rowHorizon.
func (e *Engine) pushRun(r model.Run) {
	if r.ScheduledFor.Sub(e.now()) <= rowHorizon {
		e.cacheRow(r)
	}
	e.pushAt(r.ID, r.ScheduledFor)
}

// cacheRow stores a pending run's row within the payload and count bounds.
// The times are cut to milliseconds, as the store would return them.
func (e *Engine) cacheRow(r model.Run) {
	if r.Status != model.StatusPending || len(r.Payload) > rowMaxPayload {
		return
	}
	if _, ok := e.rows[r.ID]; !ok && len(e.rows) >= rowCap {
		return
	}
	if r.AdhocSpec != nil {
		if b, err := json.Marshal(r.AdhocSpec); err != nil || len(b) > rowMaxPayload {
			return
		}
	}
	r.ScheduledFor, r.OriginFor, r.ContextFor = msUTC(r.ScheduledFor), msUTC(r.OriginFor), msUTC(r.ContextFor)
	r.LatenessRef, r.CreatedAt = msUTC(r.LatenessRef), msUTC(r.CreatedAt)
	if r.NominalFor != nil {
		n := msUTC(*r.NominalFor)
		r.NominalFor = &n
	}
	e.rows[r.ID] = r
}

func msUTC(t time.Time) time.Time { return time.UnixMilli(t.UnixMilli()).UTC() }

// pendingRow returns a pending run's row from the cache, else from the
// reader pool.
func (e *Engine) pendingRow(ctx context.Context, runID string) (model.Run, error) {
	if r, ok := e.rows[runID]; ok {
		return r, nil
	}
	return store.GetRun(ctx, e.st.R, runID)
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
	if e.inflight[pool] >= e.poolCap(pool) || e.clientCapped(pool, r.ClientID) {
		// A capped run waits on the pool list too, keeping its rank there.
		return "pool:" + pool, false
	}
	return "", false
}

// clientCapped reports that client already holds its share of the pool.
func (e *Engine) clientCapped(pool, client string) bool {
	return pool == poolHTTP && e.d.MaxHTTPPerClient > 0 && e.clientBusy[client] >= e.d.MaxHTTPPerClient
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
	// The cached row leaves on every exit except a park or a transient re-push.
	keepRow := false
	defer func() {
		if !keepRow {
			delete(e.rows, runID)
		}
	}()
	retry := func() {
		keepRow = true
		e.pushAt(runID, now.Add(time.Second))
	}
	r, err := e.pendingRow(ctx, runID)
	if err != nil || r.Status != model.StatusPending {
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			e.log.Error("load run for dispatch", "run", runID, "err", err)
			retry()
		}
		return // ghost: deleted or no longer pending
	}
	spec, err := e.spec(r)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		e.log.Error("load run spec; retrying", "run", runID, "err", err)
		retry()
		return
	}
	if err != nil {
		e.log.Error("load run spec", "run", runID, "err", err)
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusFailed, err: "run specification missing", page: true})
		return
	}
	var je *jobEntry
	if r.JobID != "" {
		je = e.jobs[r.JobID]
		if je == nil {
			// The cache misses a job whose change event was lost; reconcile
			// would add it within a minute, so check the store before cancelling.
			switch j, err := store.GetJob(ctx, e.st.W, r.JobID); {
			case err == nil:
				je = newJobEntry(j, now)
				e.jobs[j.ID] = je
			case !errors.Is(err, store.ErrNotFound):
				retry()
				return
			}
		}
		if je == nil && r.Kind != model.KindAdhoc {
			e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusCancelled, err: "job deleted"})
			return
		}
	}

	late := now.Sub(r.LatenessRef)
	if late > time.Duration(spec.Policy.MaxLateSec)*time.Second {
		lateMs := late.Milliseconds()
		won := e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusMissed, lateMs: &lateMs,
			err: fmt.Sprintf("not dispatched within max_late_sec (%ds)", spec.Policy.MaxLateSec)})
		if won && r.JobID != "" { // a stale row's lost finalize missed nothing
			e.pageCountFor(r.JobID).missed++
		}
		return
	}

	list, skip := e.blocker(r, spec)
	if skip {
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusSkipped, err: "previous run still in progress"})
		return
	}
	if list != "" {
		keepRow = true
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
		retry()
		return
	}
	if !ok {
		return // stale row: the run left pending behind the loop's back
	}
	r.Status, r.StartedAt, r.LateMs, r.DeadlineAt = model.StatusRunning, &now, &lateMs, &deadline

	h := held{jobID: r.JobID, ns: nsKeyOf(r, spec), pool: poolOf(spec.Target.Type), run: r}
	if h.pool == poolHTTP {
		h.client = r.ClientID
	}
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
	if h.client != "" {
		e.clientBusy[h.client]++
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
	dec(e.clientBusy, h.client)
	e.wakeFor(h)
}

// releasePool frees only the pool slot (running → waiting).
func (e *Engine) releasePool(runID string) {
	h, ok := e.held[runID]
	if !ok || h.pool == "" {
		return
	}
	pool, client := h.pool, h.client
	h.pool, h.client = "", ""
	e.held[runID] = h
	if e.inflight[pool]--; e.inflight[pool] <= 0 {
		delete(e.inflight, pool)
	}
	if client != "" {
		if e.clientBusy[client]--; e.clientBusy[client] <= 0 {
			delete(e.clientBusy, client)
		}
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

// parkMeta is what a parked run is ranked by, kept so a wait-list scan can
// order and skip runs without loading them.
type parkMeta struct {
	latRef int64 // LatenessRef, unix ms
	offset int64 // priority offset, ms
	seq    uint64
	client string
	http   bool
}

// rankFor orders a list: a job's own queue stays FIFO by due time; key and
// pool lists add the priority offset, so a waiting run ages past fresh
// higher-priority arrivals.
func rankFor(list string, m parkMeta) int64 {
	if strings.HasPrefix(list, "job:") {
		return m.latRef
	}
	return m.latRef + m.offset
}

func (e *Engine) park(r model.Run, spec model.RunSpec, list string) {
	if _, already := e.parked[r.ID]; already {
		return
	}
	e.parkSeq++
	m := parkMeta{
		latRef: r.LatenessRef.UnixMilli(), offset: model.PriorityOffset(spec.Policy.Priority).Milliseconds(),
		seq: e.parkSeq, client: r.ClientID, http: spec.Target.Type == model.TargetHTTP,
	}
	e.meta[r.ID] = m
	e.insert(r.ID, list, m)
	e.cacheRow(r) // a parked run is due: its admission reads nothing
	e.heap.push(entry{runID: r.ID, at: r.LatenessRef.Add(time.Duration(spec.Policy.MaxLateSec)*time.Second + time.Millisecond), kind: entryLateDeadline})
}

// insert places a run in list by (rank, seq).
func (e *Engine) insert(runID, list string, m parkMeta) {
	e.parked[runID] = list
	ids := e.waits[list]
	rank := rankFor(list, m)
	i := sort.Search(len(ids), func(i int) bool {
		o := e.meta[ids[i]]
		ro := rankFor(list, o)
		return ro > rank || (ro == rank && o.seq > m.seq)
	})
	e.waits[list] = slices.Insert(ids, i, runID)
}

// move re-lists a parked run. It keeps its rank inputs and pushes no heap
// entry: the run's late-deadline entry still finds it parked.
func (e *Engine) move(runID, list string) {
	m := e.meta[runID]
	e.detach(runID)
	e.insert(runID, list, m)
}

func (e *Engine) unpark(runID string) {
	if e.detach(runID) {
		delete(e.meta, runID)
	}
}

// detach removes a run from its wait list, keeping its meta.
func (e *Engine) detach(runID string) bool {
	list, ok := e.parked[runID]
	if !ok {
		return false
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
	return true
}

// wake admits runs from a wait list, best rank first, while they can run.
// On the HTTP pool list it steps over runs whose client holds its full
// share (from their meta, with no I/O); they keep their place. Ghosts are
// dropped in the loop so they never use up a wake.
func (e *Engine) wake(list string) {
	if e.draining {
		return // no new dispatch during shutdown; boot recovery takes them
	}
	now := e.now()
	pool, isPool := strings.CutPrefix(list, "pool:")
	capScan := isPool && pool == poolHTTP && e.d.MaxHTTPPerClient > 0
	for i := 0; ; {
		if isPool && e.inflight[pool] >= e.poolCap(pool) {
			return
		}
		ids := e.waits[list] // re-read: unpark and move rewrite it in place
		if i >= len(ids) {
			return
		}
		id := ids[i]
		if m := e.meta[id]; capScan && m.http && e.clientBusy[m.client] >= e.d.MaxHTTPPerClient {
			i++
			continue
		}
		r, err := e.pendingRow(e.life, id)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			// Transient: unparking would orphan the run (its late-deadline
			// entry is skipped once it leaves the list). It keeps its place;
			// the next release or reconcile wakes the list again.
			e.log.Error("load parked run; leaving it parked", "run", id, "err", err)
			return
		}
		if err != nil || r.Status != model.StatusPending {
			e.unpark(id)
			continue
		}
		spec, err := e.spec(r)
		if err != nil {
			e.unpark(id)
			e.dispatch(id, now) // fails it with a clear error
			continue
		}
		if blockedBy, skip := e.blocker(r, spec); blockedBy != "" && !skip {
			if blockedBy != list {
				e.move(id, blockedBy)
				continue
			}
			return
		}
		e.unpark(id)
		e.dispatch(id, now)
	}
}

// flushMissedPages pages each job's accumulated counts, at most once per job
// per missPageEvery; in between they keep accumulating. It runs at the end
// of every loop pass, so held counts go out once the window lapses.
func (e *Engine) flushMissedPages(now time.Time) {
	for jobID, pc := range e.pageBatch {
		je := e.jobs[jobID]
		if je == nil {
			delete(e.pageBatch, jobID)
			delete(e.lastMissPage, jobID)
			continue
		}
		if last, ok := e.lastMissPage[jobID]; ok && now.Sub(last) < missPageEvery && !now.Before(last) {
			continue
		}
		delete(e.pageBatch, jobID)
		e.lastMissPage[jobID] = now
		title := "coalesced"
		if pc.missed > 0 {
			title = "missed"
		}
		e.pageJob(je.job, notify.Fail, title, pc.body())
	}
}

func (e *Engine) pageCountFor(jobID string) *pageCount {
	pc := e.pageBatch[jobID]
	if pc == nil {
		pc = &pageCount{}
		e.pageBatch[jobID] = pc
	}
	return pc
}

// addPageCounts adds occurrences a materialise dropped to the job's next page.
func (e *Engine) addPageCounts(jobID string, coalesced, expired int) {
	if coalesced <= 0 && expired <= 0 {
		return
	}
	pc := e.pageCountFor(jobID)
	pc.coalesced += coalesced
	pc.expired += expired
	if expired >= maxExpiredCount {
		pc.capped = true
	}
}

func (pc *pageCount) body() string {
	var s []string
	switch {
	case pc.missed == 1:
		s = append(s, "1 run missed its max_late window and was not run.")
	case pc.missed > 1:
		s = append(s, fmt.Sprintf("%d runs missed their max_late window and were not run.", pc.missed))
	}
	switch {
	case pc.coalesced == 1:
		s = append(s, "1 occurrence was coalesced into a later run because dispatch was behind schedule.")
	case pc.coalesced > 1:
		s = append(s, fmt.Sprintf("%d occurrences were coalesced into one run because dispatch was behind schedule.", pc.coalesced))
	}
	if pc.expired > 0 {
		n := fmt.Sprintf("%d occurrences", pc.expired)
		switch {
		case pc.capped:
			n = "At least " + n
		case pc.expired == 1:
			n = "1 occurrence"
		}
		s = append(s, n+" expired while the runner was down or the job was blocked.")
	}
	return strings.Join(s, " ")
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
