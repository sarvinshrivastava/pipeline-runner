package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// Start recovers state from the store and starts the loop.
//
// Boot order (plan §6.7): idempotency claims are cleared; running runs are
// finalized from an early result, interrupted, or kept running for
// redispatch; waiting runs time out, get one lease grace, or keep their lease;
// held slots are rebuilt from what is still running/waiting; every active job
// is materialised; pending runs are pushed; then the loop starts.
func (e *Engine) Start(ctx context.Context) error {
	now := e.now()
	if err := store.IdemClearInProgress(ctx, e.st.W); err != nil {
		return err
	}
	jobs, err := store.AllJobs(ctx, e.st.W)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		e.jobs[j.ID] = newJobEntry(j, now)
	}

	type redispatch struct {
		run  model.Run
		spec model.RunSpec
	}
	var again []redispatch
	var reattach []redispatch

	running, err := store.RunsByStatus(ctx, e.st.W, model.StatusRunning)
	if err != nil {
		return err
	}
	for _, r := range running {
		spec, err := e.spec(r)
		if err != nil {
			e.finalize(r, []model.Status{model.StatusRunning}, outcome{status: model.StatusInterrupted, err: "run specification missing at boot", page: true, pageTitle: "interrupted"})
			continue
		}
		switch {
		case r.EarlyResult != nil:
			e.finalize(r, []model.Status{model.StatusRunning}, e.fromComplete(r, spec, *r.EarlyResult, nil, now))
		case r.Kind != model.KindAdhoc && e.jobs[r.JobID] == nil:
			e.finalize(r, []model.Status{model.StatusRunning}, outcome{status: model.StatusInterrupted, err: "job deleted"})
		case spec.Target.Type == model.TargetHTTP:
			inWindow := now.Sub(r.LatenessRef) <= time.Duration(spec.Policy.MaxLateSec)*time.Second
			if spec.Policy.Delivery == model.AtLeastOnce && inWindow {
				deadline := now.Add(time.Duration(spec.Policy.TimeoutSec) * time.Second)
				ok, err := store.Transition(ctx, e.st.W, r.ID, []model.Status{model.StatusRunning}, model.StatusRunning,
					store.Fields{"started_at": now, "deadline_at": deadline})
				if err != nil {
					return err
				}
				if ok {
					r.StartedAt, r.DeadlineAt = &now, &deadline
					again = append(again, redispatch{r, spec})
				}
				continue
			}
			reason := "runner restarted mid-run; at_most_once is never re-sent"
			if spec.Policy.Delivery == model.AtLeastOnce {
				reason = fmt.Sprintf("runner restarted mid-run and the run is past max_late_sec (%ds)", spec.Policy.MaxLateSec)
			}
			e.finalize(r, []model.Status{model.StatusRunning}, outcome{status: model.StatusInterrupted, err: reason, page: true, pageTitle: "interrupted"})
		default:
			if e.d.Containers == nil {
				e.finalize(r, []model.Status{model.StatusRunning}, outcome{status: model.StatusInterrupted, err: "container targets are disabled", page: true, pageTitle: "interrupted"})
				continue
			}
			reattach = append(reattach, redispatch{r, spec})
		}
	}

	waiting, err := store.RunsByStatus(ctx, e.st.W, model.StatusWaiting)
	if err != nil {
		return err
	}
	type lease struct {
		id string
		at time.Time
	}
	var leases []lease
	for _, r := range waiting {
		spec, err := e.spec(r)
		if err != nil || spec.Target.HTTP == nil {
			e.finalize(r, []model.Status{model.StatusWaiting}, outcome{status: model.StatusInterrupted, err: "run specification missing at boot", page: true, pageTitle: "interrupted"})
			continue
		}
		switch {
		case r.DeadlineAt != nil && !now.Before(*r.DeadlineAt):
			e.finalize(r, []model.Status{model.StatusWaiting}, e.decide(r, spec, target.Result{
				Status: model.StatusTimeout, Error: "async run exceeded timeout_sec while the runner was down", Ambiguous: true, Delivered: true,
			}, now))
		case r.LeaseExpiresAt != nil && !now.Before(*r.LeaseExpiresAt) && !r.LeaseGraceUsed:
			at := now.Add(time.Duration(spec.Target.HTTP.LeaseSec) * time.Second)
			if r.DeadlineAt != nil && at.After(*r.DeadlineAt) {
				at = *r.DeadlineAt
			}
			if ok, err := store.Transition(ctx, e.st.W, r.ID, []model.Status{model.StatusWaiting}, model.StatusWaiting,
				store.Fields{"lease_expires_at": at, "lease_grace_used": true}); err != nil {
				return err
			} else if ok {
				leases = append(leases, lease{r.ID, at})
			}
		case r.LeaseExpiresAt != nil:
			leases = append(leases, lease{r.ID, *r.LeaseExpiresAt}) // fires now if already expired
		}
	}

	// Rebuild held slots from the rows still running/waiting.
	if err := e.rebuildHeld(ctx); err != nil {
		return err
	}
	for _, l := range leases {
		e.armLease(l.id, l.at)
	}

	pending, err := store.RunsByStatus(ctx, e.st.W, model.StatusPending)
	if err != nil {
		return err
	}
	for _, r := range pending {
		e.push(r.ID, r.ScheduledFor)
	}
	e.ensureAllMaterialised(pending)

	for _, rd := range again {
		e.startExec(rd.run, rd.spec, *rd.run.DeadlineAt)
	}
	for _, rd := range reattach {
		e.startReattach(rd.run, rd.spec)
	}

	e.lastReconcile = now
	e.loopAt.Store(now.UnixMilli())
	go e.loop()
	go e.retentionLoop()
	return nil
}

func (e *Engine) startReattach(r model.Run, spec model.RunSpec) {
	ctx, cancel := context.WithCancel(e.life)
	e.cancelFns[r.ID] = cancel
	go func() {
		defer cancel()
		for {
			if err := e.d.Containers.Ping(ctx); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
		res, err := e.d.Containers.Reattach(ctx, r, spec)
		if err != nil {
			res = target.Result{Status: model.StatusInterrupted, Error: "reattach failed: " + err.Error(), Ambiguous: true}
		}
		_ = e.send(e.life, evExecDone{runID: r.ID, res: res})
	}()
}

// rebuildHeld recomputes held slots and counts from running/waiting rows.
// Entries of runs whose result is unflushed are kept as they are.
func (e *Engine) rebuildHeld(ctx context.Context) error {
	rows, err := store.RunsByStatus(ctx, e.st.W, model.StatusRunning, model.StatusWaiting)
	if err != nil {
		return err
	}
	next := map[string]held{}
	for id, h := range e.held {
		if e.isUnflushed(id) {
			next[id] = h
		}
	}
	for _, r := range rows {
		if _, kept := next[r.ID]; kept {
			continue
		}
		spec, err := e.spec(r)
		if err != nil {
			continue
		}
		h := held{jobID: r.JobID, ns: nsKeyOf(r, spec)}
		if r.Status == model.StatusRunning {
			h.pool = poolOf(spec.Target.Type)
		}
		next[r.ID] = h
	}
	e.held, e.jobBusy, e.keyBusy, e.inflight = map[string]held{}, map[string]int{}, map[string]int{}, map[string]int{}
	for id, h := range next {
		e.reserve(id, h)
	}
	return nil
}

// ensureAllMaterialised gives every active job its pending schedule run if
// missing. pending is the current set of pending runs (one query, not one per job).
func (e *Engine) ensureAllMaterialised(pending []model.Run) {
	hasPending := make(map[string]bool, len(pending))
	for _, r := range pending {
		if r.Kind == model.KindSchedule {
			hasPending[r.JobID] = true
		}
	}
	for _, je := range e.jobs {
		if je.job.Trigger.Type == model.TriggerCron && hasPending[je.job.ID] {
			continue
		}
		e.ensureMaterialised(je)
	}
}

// ensureMaterialised gives an active job its pending schedule run if missing.
func (e *Engine) ensureMaterialised(je *jobEntry) {
	j := je.job
	if j.State != model.JobActive {
		return
	}
	switch j.Trigger.Type {
	case model.TriggerCron:
		pending, err := store.PendingRunsForJob(e.life, e.st.W, j.ID)
		if err != nil {
			return
		}
		for _, r := range pending {
			if r.Kind == model.KindSchedule {
				return
			}
		}
	case model.TriggerAt:
		if j.Trigger.At == nil {
			return
		}
		has, err := store.HasScheduleRunFor(e.life, e.st.W, j.ID, *j.Trigger.At)
		if err != nil || has {
			return
		}
	default:
		return
	}
	e.materialise(je)
}

// reconcile repairs drift between memory and the store (plan §6.0).
func (e *Engine) reconcile(now time.Time) {
	e.lastReconcile = now
	ctx := e.life
	jobs, err := store.AllJobs(ctx, e.st.R)
	if err != nil {
		e.log.Error("reconcile: load jobs", "err", err)
		return
	}
	fresh := make(map[string]*jobEntry, len(jobs))
	for _, j := range jobs {
		fresh[j.ID] = newJobEntry(j, now)
	}
	e.jobs = fresh

	pending, err := store.RunsByStatus(ctx, e.st.R, model.StatusPending)
	if err != nil {
		e.log.Error("reconcile: load pending", "err", err)
		return
	}
	for _, r := range pending {
		if e.inHeap[r.ID] == 0 && e.parked[r.ID] == "" {
			e.log.Warn("reconcile: pending run missing from heap; re-pushed", "run", r.ID)
			e.push(r.ID, r.ScheduledFor)
		}
	}
	e.ensureAllMaterialised(pending)

	before := fmt.Sprint(len(e.held), e.inflight, e.keyBusy, e.jobBusy)
	if err := e.rebuildHeld(ctx); err != nil {
		e.log.Error("reconcile: rebuild held", "err", err)
		return
	}
	if after := fmt.Sprint(len(e.held), e.inflight, e.keyBusy, e.jobBusy); after != before {
		e.log.Warn("reconcile: gate counts drifted; repaired", "before", before, "after", after)
	}
	for list := range e.waits {
		e.wake(list)
	}
}
