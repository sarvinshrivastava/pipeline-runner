package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/sarvinshrivastava/pipeline-runner/internal/jobpush"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/schedule"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

const maxErrorBytes = 4 << 10

// Per-run failure classes (PLR-1): pages with these errors are never
// collapsed and never arm recovery. decide, onLeaseExpired, recover.go and the
// boot derivation share them.
const (
	maxSummaryErrBytes = 512
	errSpecMissing     = "run specification missing"                      // runtime; boot appends " at boot"
	errNeedsHuman      = "at_most_once request may have been processed: " // decide, prefix
	errLeaseExpired    = "lease expired without heartbeat or completion"  // onLeaseExpired → NEEDS A HUMAN
	errDeferCap        = "target asked to defer to "                      // decide, prefix
)

// perRunError reports whether a failure's error marks a class that pages per
// run (never collapsed, never arms recovery).
func perRunError(err string) bool {
	for _, p := range []string{errSpecMissing, errNeedsHuman, errLeaseExpired, errDeferCap} {
		if strings.HasPrefix(err, p) {
			return true
		}
	}
	return false
}

// maxExpiredCount caps the expired-occurrence count (and the Next calls it
// takes) per materialise; a page then says "at least".
const maxExpiredCount = 1_000

// outcome is a decided terminal transition.
type outcome struct {
	status          model.Status
	err             string
	httpStatus      *int
	exitCode        *int
	output          json.RawMessage
	outputTruncated bool
	outputError     string
	lateMs          *int64
	delivered       bool
	successor       *model.Run
	page            bool
	pageTitle       string
	finishedAt      time.Time // set by finalize; afterFinalize may run later (unflushed replay)
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// decide turns an executor result into an outcome, including any retry or
// defer successor.
func (e *Engine) decide(r model.Run, spec model.RunSpec, res target.Result, now time.Time) outcome {
	o := outcome{
		status: res.Status, err: res.Error, httpStatus: res.HTTPStatus, exitCode: res.ExitCode, output: res.Output,
		outputTruncated: res.OutputTruncated, outputError: res.OutputError, delivered: res.Delivered,
	}
	p := spec.Policy

	if res.DeferUntil != nil {
		until := *res.DeferUntil
		if until.Before(now) {
			until = now
		}
		if until.Sub(r.OriginFor) > time.Duration(p.MaxDeferSec)*time.Second {
			o.status, o.page, o.pageTitle = model.StatusFailed, true, "defer cap exceeded"
			o.err = fmt.Sprintf(errDeferCap+"%s, beyond max_defer_sec (%ds) from the original schedule", until.Format(time.RFC3339), p.MaxDeferSec)
			return o
		}
		o.status = model.StatusDeferred
		s := e.successor(r, model.KindDefer, r.Attempt, until, now)
		o.successor = &s
		return o
	}
	if o.status == model.StatusSuccess {
		return o
	}
	if o.status == "" {
		o.status = model.StatusFailed
	}

	canRetry := !res.NoRetry && r.Attempt < p.Retry.MaxAttempts
	if p.Delivery == model.AtMostOnce && !res.Unsent {
		canRetry = false
		if res.Ambiguous {
			o.status, o.page, o.pageTitle = model.StatusInterrupted, true, "NEEDS A HUMAN"
			o.err = errNeedsHuman + o.err
			return o
		}
	}
	if canRetry {
		backoff := time.Duration(p.Retry.BackoffSec) * time.Second << (r.Attempt - 1)
		if limit := time.Duration(p.Retry.MaxBackoffSec) * time.Second; backoff > limit || backoff <= 0 {
			backoff = limit
		}
		s := e.successor(r, model.KindRetry, r.Attempt+1, now.Add(backoff), now)
		o.successor = &s
		return o
	}
	o.page, o.pageTitle = true, string(o.status)
	return o
}

func (e *Engine) successor(r model.Run, kind string, attempt int, at, now time.Time) model.Run {
	return model.Run{
		ID: newID(), JobID: r.JobID, JobName: r.JobName, ClientID: r.ClientID, Kind: kind, Attempt: attempt,
		ParentRunID: r.ID, ChainRootID: r.ChainRootID, Status: model.StatusPending,
		ScheduledFor: at, OriginFor: r.OriginFor, ContextFor: r.ContextFor, LatenessRef: at,
		Rev: r.Rev, AdhocSpec: r.AdhocSpec, Payload: r.Payload, CreatedAt: now,
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// finalize writes a terminal transition (plus successor) atomically. When the
// guarded transition loses, nothing else happens. It returns whether it won.
func (e *Engine) finalize(r model.Run, from []model.Status, o outcome) bool {
	now := e.now()
	o.finishedAt = now
	var je *jobEntry
	if r.JobID != "" {
		je = e.jobs[r.JobID]
	}
	if o.status == model.StatusSuccess && je != nil && je.minGap > 0 && je.minGap < time.Minute {
		o.output = nil // sub-minute jobs keep output only for non-success runs (P2)
	}
	fields := store.Fields{"finished_at": now, "error": truncate(o.err, maxErrorBytes)}
	if o.httpStatus != nil {
		fields["http_status"] = *o.httpStatus
	}
	if o.exitCode != nil {
		fields["exit_code"] = *o.exitCode
	}
	if len(o.output) > 0 {
		fields["output"] = o.output
	}
	if o.outputTruncated {
		fields["output_truncated"] = true
	}
	if o.outputError != "" {
		fields["output_error"] = o.outputError
	}
	if o.lateMs != nil {
		fields["late_ms"] = *o.lateMs
	}
	if o.delivered {
		fields["delivery_attempted"] = true
	}

	jobDone := false
	skips := -1
	if je != nil {
		oneShotEnds := je.job.Trigger.Type == model.TriggerAt && o.successor == nil &&
			(r.Kind == model.KindSchedule || r.Kind == model.KindRetry || r.Kind == model.KindDefer)
		jobDone = oneShotEnds && je.job.State == model.JobActive
		if r.Kind == model.KindSchedule {
			switch {
			case o.status == model.StatusSkipped:
				skips = je.job.ConsecutiveSkips + 1
			case je.job.ConsecutiveSkips > 0:
				skips = 0
			}
		}
	}

	won := false
	write := func(ctx context.Context) error {
		return e.st.Tx(ctx, func(tx *store.Tx) error {
			ok, err := store.Transition(ctx, tx, r.ID, from, o.status, fields)
			if err != nil {
				return err
			}
			if !ok {
				return errStaleTransition
			}
			if o.successor != nil {
				if err := store.InsertRun(ctx, tx, *o.successor); err != nil {
					return err
				}
			}
			if jobDone {
				// The job may have been deleted since; its run still finalizes.
				if err := store.SetJobState(ctx, tx, r.JobID, model.JobDone, now); err != nil && !errors.Is(err, store.ErrNotFound) {
					return err
				}
			}
			if skips >= 0 {
				return store.SetConsecutiveSkips(ctx, tx, r.JobID, skips)
			}
			return nil
		})
	}
	err := write(e.life)
	switch {
	case err == nil:
		won = true
	case errors.Is(err, errStaleTransition):
		return false
	default:
		e.log.Error("finalize write failed; holding result in memory", "run", r.ID, "status", o.status, "outcome", fmt.Sprintf("%+v", o), "err", err)
		if e.isUnflushed(r.ID) {
			return false // already held; the first result wins
		}
		if len(e.unflushed) == 0 { // page once per outage, not per run
			e.page(r, notify.Fail, "db write failing", "A run result could not be saved: "+err.Error())
		}
		e.addUnflushed(r, o.status, write, func() { e.afterFinalize(r, o, jobDone, skips, from) })
		return false
	}
	e.afterFinalize(r, o, jobDone, skips, from)
	return won
}

// afterFinalize runs once a terminal write has committed.
func (e *Engine) afterFinalize(r model.Run, o outcome, jobDone bool, skips int, from []model.Status) {
	if o.status == model.StatusMissed && r.JobID != "" {
		e.pageCountFor(r.JobID).missed++
	}
	e.release(r.ID)
	e.stopLease(r.ID)
	delete(e.cancelFns, r.ID)
	delete(e.cancelRequested, r.ID)
	r.Status = o.status
	r.Error = o.err

	// The run's end is finalize's clock, not now: an unflushed replay can run
	// afterFinalize long after the run finished.
	finished := o.finishedAt

	je := e.jobs[r.JobID]
	if je != nil {
		if jobDone {
			je.job.State = model.JobDone
		}
		if skips >= 0 {
			je.job.ConsecutiveSkips = skips
		}
	}
	if o.successor != nil {
		e.pushRun(*o.successor)
	} else {
		e.notifyAwaiters(r)
	}

	if m := e.d.Metrics; m != nil {
		m.RunsTotal.WithLabelValues(r.JobName, r.ClientID, string(o.status)).Inc()
		if r.StartedAt != nil {
			m.RunDuration.WithLabelValues(r.JobName, r.ClientID).Observe(finished.Sub(*r.StartedAt).Seconds())
		}
		if o.status == model.StatusSkipped {
			m.SkippedTotal.WithLabelValues(r.JobName).Inc()
		}
	}

	fs := e.failStates[r.JobID]
	// An old success replayed late does not recover; a future lastFailPage
	// (clock stepped back) never blocks recovery.
	recovered := o.status == model.StatusSuccess && r.AdhocSpec == nil && je != nil && fs != nil && fs.paged &&
		(!o.finishedAt.Before(fs.lastFailPage) || e.now().Before(fs.lastFailPage))
	switch {
	case e.collapsible(r, o, je):
		e.pageFailure(r, o)
	case o.page:
		e.page(r, notify.Fail, o.pageTitle, o.err)
	case recovered:
		e.pageRecovered(r, fs) // replaces notify_on_success's "succeeded" for this run
	case o.status == model.StatusSuccess && r.AdhocSpec == nil && je != nil && je.job.Policy.NotifyOnSuccess:
		e.page(r, notify.OK, "succeeded", "Run "+r.ID+" succeeded.")
	}
	// P2: a success while !paged marks held failures healed (this runs after
	// the switch, so it never sees the batch pageRecovered just cleared).
	if o.status == model.StatusSuccess && r.AdhocSpec == nil && je != nil && !recovered {
		if pc := e.pageBatch[r.JobID]; pc != nil && pc.failed > 0 {
			pc.healed = true
		}
	}
	if je != nil && e.d.Pusher != nil && je.job.Policy.PushURLRef != "" {
		// The current policy, not the run's revision: the ref names an
		// operator monitor, so a change applies from the next terminal write.
		pol := je.job.Policy
		x := jobpush.Push{JobID: je.job.ID, JobName: r.JobName, ClientID: r.ClientID, RunID: r.ID, Ref: pol.PushURLRef, PingMs: -1}
		switch {
		case o.status == model.StatusSuccess:
			if r.StartedAt != nil {
				x.PingMs = finished.Sub(*r.StartedAt).Milliseconds()
			}
			x.Up, x.Status = true, "up"
			e.d.Pusher.Enqueue(x)
		case o.successor == nil && pol.PushOnFailure &&
			(o.status == model.StatusFailed || o.status == model.StatusTimeout || o.status == model.StatusInterrupted):
			x.Status = string(o.status)
			e.d.Pusher.Enqueue(x)
		}
	}
	if skips == 3 && je != nil {
		e.pageJob(je.job, notify.Fail, "skipped 3 consecutive runs", "Its runtime exceeds its interval; overlap=skip is dropping occurrences.")
	}

	// A schedule run leaving pending (missed, skipped, cancelled) materialises
	// the next occurrence; claims materialise in dispatch.
	if r.Kind == model.KindSchedule && je != nil && containsStatus(from, model.StatusPending) {
		e.materialise(je)
	}
}

func containsStatus(list []model.Status, s model.Status) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (e *Engine) notifyAwaiters(r model.Run) {
	for _, ch := range e.awaiters[r.ChainRootID] {
		reply(ch, r)
	}
	delete(e.awaiters, r.ChainRootID)
}

func (e *Engine) addUnflushed(r model.Run, to model.Status, write func(ctx context.Context) error, after func()) {
	e.unflushed = append(e.unflushed, &unflushed{
		op: write, after: after, runID: r.ID, desc: string(to), nextTry: e.now().Add(100 * time.Millisecond), backoff: 100 * time.Millisecond,
	})
	e.unflushedCount.Store(int64(len(e.unflushed)))
}

// retryUnflushed retries held results from the loop timer, never sleeping.
// Slots stay held until the write lands, then finalize's post-commit work
// runs. A stale write means another path finalized the run and owns that work.
func (e *Engine) retryUnflushed(now time.Time) {
	kept := e.unflushed[:0]
	for _, u := range e.unflushed {
		if now.Before(u.nextTry) {
			kept = append(kept, u)
			continue
		}
		err := u.op(e.life)
		switch {
		case err == nil:
			u.after()
			continue
		case errors.Is(err, errStaleTransition):
			continue
		}
		u.backoff = min(u.backoff*2, 30*time.Second)
		u.nextTry = now.Add(u.backoff)
		kept = append(kept, u)
	}
	e.unflushed = kept
	e.unflushedCount.Store(int64(len(e.unflushed)))
}

func (e *Engine) isUnflushed(runID string) bool {
	for _, u := range e.unflushed {
		if u.runID == runID {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- materialise

// materialise inserts the job's next schedule run on the loop.
func (e *Engine) materialise(je *jobEntry) {
	inserted, collapsed, expired, err := insertNext(e.life, e.st.W, &je.job, e.now(), e.d.Rand)
	if err != nil {
		e.log.Error("materialise", "job", je.job.ID, "err", err)
		return
	}
	e.countCoalesced(je.job, collapsed, expired)
	e.addPageCounts(je.job.ID, collapsed, expired)
	if inserted != nil {
		e.pushRun(*inserted)
	}
}

// countCoalesced adds a materialise's dropped occurrences to the metric. It
// is safe off the loop (API transactions call it after commit).
func (e *Engine) countCoalesced(j model.Job, collapsed, expired int) {
	if m := e.d.Metrics; m != nil {
		m.AddCoalesced(j.Name, j.ClientID, "backlog", collapsed)
		m.AddCoalesced(j.Name, j.ClientID, "expired", expired)
	}
}

// nextNominal applies the cursor rule. Cron: Next(max(cursor, now − max_late)),
// with a NULL cursor meaning "from now" (a new job never fires the past).
// When several occurrences are already due, they collapse into the latest
// one; collapsed is how many earlier ones were dropped. expired counts the
// occurrences after the cursor that are older than max_late (up to
// maxExpiredCount). At: the instant if it is after max(cursor, now −
// max_late). Zero means none.
func nextNominal(j model.Job, now time.Time) (nominal time.Time, collapsed, expired int) {
	window := now.Add(-time.Duration(j.Policy.MaxLateSec) * time.Second)
	switch j.Trigger.Type {
	case model.TriggerCron:
		s, err := schedule.Parse(j.Trigger.Cron, j.Trigger.Timezone)
		if err != nil {
			return time.Time{}, 0, 0
		}
		base := now
		if j.LastNominalFor != nil {
			base = window
			if j.LastNominalFor.After(window) {
				base = *j.LastNominalFor
			} else {
				for t := s.Next(*j.LastNominalFor); !t.IsZero() && !t.After(window) && expired < maxExpiredCount; t = s.Next(t) {
					expired++
				}
			}
		}
		nominal = s.Next(base)
		for !nominal.IsZero() && !nominal.After(now) {
			next := s.Next(nominal)
			if next.IsZero() || next.After(now) {
				break
			}
			nominal = next
			collapsed++
		}
		return nominal, collapsed, expired
	case model.TriggerAt:
		base := window
		if j.LastNominalFor != nil && j.LastNominalFor.After(base) {
			base = *j.LastNominalFor
		}
		if j.Trigger.At != nil && j.Trigger.At.After(base) {
			return *j.Trigger.At, 0, 0
		}
	}
	return time.Time{}, 0, 0
}

// insertNext materialises j's next schedule run through q (the loop's writer
// or an API transaction). It updates j's cursor (and state, for a one-shot
// with nothing left) in place. A runs_one_pending conflict inserts nothing
// and reports no counts. It returns nextNominal's collapsed and expired.
func insertNext(ctx context.Context, q store.Querier, j *model.Job, now time.Time, rnd func(int64) int64) (*model.Run, int, int, error) {
	if j.State != model.JobActive || (j.Trigger.Type != model.TriggerCron && j.Trigger.Type != model.TriggerAt) {
		return nil, 0, 0, nil
	}
	nominal, collapsed, expired := nextNominal(*j, now)
	if nominal.IsZero() {
		if j.Trigger.Type == model.TriggerAt {
			j.State = model.JobDone
			return nil, 0, 0, store.SetJobState(ctx, q, j.ID, model.JobDone, now)
		}
		return nil, 0, 0, nil
	}
	fire := nominal
	if j.Policy.JitterSec > 0 {
		fire = fire.Add(time.Duration(rnd(j.Policy.JitterSec*1000+1)) * time.Millisecond)
	}
	id := newID()
	r := model.Run{
		ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: model.KindSchedule, Attempt: 1,
		ChainRootID: id, Status: model.StatusPending, NominalFor: &nominal, ScheduledFor: fire, OriginFor: fire,
		ContextFor: fire, LatenessRef: fire, Rev: j.Rev, CreatedAt: now,
	}
	ok, err := store.InsertScheduleRun(ctx, q, r)
	if err != nil || !ok {
		return nil, 0, 0, err
	}
	if err := store.SetJobCursor(ctx, q, j.ID, &nominal); err != nil {
		return nil, 0, 0, err
	}
	j.LastNominalFor = &nominal
	return &r, collapsed, expired, nil
}

// ---------------------------------------------------------------- paging

// collapsible reports whether a paging outcome is an exhausted plain-status
// failure of an active job's run, which PLR-1 collapses per job.
func (e *Engine) collapsible(r model.Run, o outcome, je *jobEntry) bool {
	switch o.status {
	case model.StatusFailed, model.StatusTimeout, model.StatusInterrupted:
	default:
		return false
	}
	return o.page && o.successor == nil && o.pageTitle == string(o.status) && !perRunError(o.err) &&
		r.AdhocSpec == nil && je != nil && je.job.State == model.JobActive
}

func (e *Engine) failStateFor(jobID string) *failState {
	fs := e.failStates[jobID]
	if fs == nil {
		fs = &failState{}
		e.failStates[jobID] = fs
	}
	return fs
}

// lapsed reports whether a failure-page window that began at t is over (zero
// or a future t after a clock step back count as over).
func lapsed(t, now time.Time) bool {
	return t.IsZero() || now.Before(t) || now.Sub(t) >= missPageEvery
}

// pageFailure pages a job's first failure at once, with any held
// missed/coalesced/expired counts, and counts later ones for the summary.
func (e *Engine) pageFailure(r model.Run, o outcome) {
	now := e.now() // the page goes out now; o.finishedAt can be old on an unflushed replay
	fs := e.failStateFor(r.JobID)
	if fs.paged || !lapsed(fs.lastFailPage, now) {
		pc := e.pageCountFor(r.JobID)
		pc.failed++
		pc.healed = false
		pc.lastStatus, pc.lastErr = o.status, strings.ToValidUTF8(truncate(o.err, maxSummaryErrBytes), "")
		return
	}
	body := o.err
	if pc := e.pageBatch[r.JobID]; pc != nil {
		// Held occurrences, plus failures held across a disable/re-enable.
		if s := pc.body(); s != "" {
			body += "\n" + s
		}
		delete(e.pageBatch, r.JobID)
	}
	e.page(r, notify.Fail, o.pageTitle, body)
	fs.paged, fs.lastFailPage = true, now
	e.lastMissPage[r.JobID] = now
}

// pageRecovered sends the one recovered notice; held failures ride in its
// body. It goes at Fail level so it routes to the failure pages' topic; its
// tag marks it as good news. Occurrence counts and lastMissPage are left to
// their own window; lastFailPage is unchanged (the immediate page or the
// flush already stamped it), so a failure within 10 min is counted.
func (e *Engine) pageRecovered(r model.Run, fs *failState) {
	body := "Run " + r.ID + " succeeded."
	if pc := e.pageBatch[r.JobID]; pc != nil && pc.failed > 0 {
		pc.healed = false // this page is the recovery; no "succeeded since" suffix
		body += " Before it, " + pc.failedSentence()
		pc.failed, pc.lastStatus, pc.lastErr = 0, "", ""
		if pc.empty() {
			delete(e.pageBatch, r.JobID)
		}
	}
	fs.paged = false
	e.pageTagged(r, notify.Fail, []string{"white_check_mark"}, "recovered", body)
}

func (e *Engine) page(r model.Run, level notify.Level, title, body string) {
	e.pageTagged(r, level, nil, title, body)
}

// pageTagged is page with explicit ntfy tags (nil: the level's default).
func (e *Engine) pageTagged(r model.Run, level notify.Level, tags []string, title, body string) {
	if e.d.Pager == nil {
		return
	}
	topic := ""
	if c, err := store.GetClient(e.life, e.st.W, r.ClientID); err == nil {
		topic = c.NtfyTopic
	}
	e.d.Pager.Send(notify.Msg{Level: level, Topic: topic, Tags: tags, Title: r.JobName + ": " + title,
		Body: fmt.Sprintf("%s\nrun %s (attempt %d, %s)", body, r.ID, r.Attempt, r.Kind)})
}

func (e *Engine) pageJob(j model.Job, level notify.Level, title, body string) {
	if e.d.Pager == nil {
		return
	}
	topic := ""
	if c, err := store.GetClient(e.life, e.st.W, j.ClientID); err == nil {
		topic = c.NtfyTopic
	}
	e.d.Pager.Send(notify.Msg{Level: level, Topic: topic, Title: j.Name + ": " + title, Body: body})
}
