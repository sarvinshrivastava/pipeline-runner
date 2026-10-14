package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/schedule"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

const maxErrorBytes = 4 << 10

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
			o.err = fmt.Sprintf("target asked to defer to %s, beyond max_defer_sec (%ds) from the original schedule", until.Format(time.RFC3339), p.MaxDeferSec)
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
			o.err = "at_most_once request may have been processed: " + o.err
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
	e.release(r.ID)
	e.stopLease(r.ID)
	delete(e.cancelFns, r.ID)
	delete(e.cancelRequested, r.ID)
	r.Status = o.status
	r.Error = o.err

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
			m.RunDuration.WithLabelValues(r.JobName, r.ClientID).Observe(e.now().Sub(*r.StartedAt).Seconds())
		}
		if o.status == model.StatusSkipped {
			m.SkippedTotal.WithLabelValues(r.JobName).Inc()
		}
	}

	switch {
	case o.page:
		e.page(r, notify.Fail, o.pageTitle, o.err)
	case o.status == model.StatusSuccess && r.AdhocSpec == nil && je != nil && je.job.Policy.NotifyOnSuccess:
		e.page(r, notify.OK, "succeeded", "Run "+r.ID+" succeeded.")
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

func (e *Engine) page(r model.Run, level notify.Level, title, body string) {
	if e.d.Pager == nil {
		return
	}
	topic := ""
	if c, err := store.GetClient(e.life, e.st.W, r.ClientID); err == nil {
		topic = c.NtfyTopic
	}
	e.d.Pager.Send(notify.Msg{Level: level, Topic: topic, Title: r.JobName + ": " + title,
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
