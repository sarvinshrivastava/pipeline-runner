package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/schedule"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// Commit lets the API store its idempotent response in the same transaction
// as the write it describes. It may be nil.
type Commit func(ctx context.Context, q store.Querier, result any) error

func (e *Engine) vctx(owner model.Principal) model.VCtx {
	return model.VCtx{Now: e.now(), Principal: owner, EnvKey: e.d.EnvKey, ContainersEnabled: e.d.ContainersEnabled}
}

// checkRefsExist enforces that every reference exists in the vault at write
// time, so a typo fails now rather than at 4 a.m.
func (e *Engine) checkRefsExist(ctx context.Context, spec model.RunSpec) error {
	for _, ref := range spec.Refs() {
		parsed, err := vault.ParseRef(ref)
		if err != nil {
			return &model.FieldError{Field: "target", Code: "invalid", Message: err.Error()}
		}
		ok, err := e.d.Secrets.Exists(ctx, parsed)
		if err != nil {
			return ErrVaultDown
		}
		if !ok {
			return &model.FieldError{Field: "target", Code: "secret_ref_missing", Message: "secret " + ref + " does not exist in the secrets manager"}
		}
	}
	return nil
}

// notifyJob tells the loop about a committed write. A failed send is logged:
// the write stands and reconcile applies it within 60 s.
func (e *Engine) notifyJob(ctx context.Context, ev evJobChanged) {
	if err := e.sendFromHandler(ctx, ev); err != nil {
		e.log.Warn("job change not delivered to the loop; reconcile will apply it", "job", ev.jobID, "err", err)
	}
}

// NextFires previews a job's next fire instants.
func NextFires(j model.Job, now time.Time, n int) []time.Time {
	switch j.Trigger.Type {
	case model.TriggerCron:
		s, err := schedule.Parse(j.Trigger.Cron, j.Trigger.Timezone)
		if err != nil {
			return nil
		}
		return s.NextN(now, n)
	case model.TriggerAt:
		if j.Trigger.At != nil {
			return []time.Time{*j.Trigger.At}
		}
	}
	return nil
}

// ValidateJob is a dry run of CreateJob: nothing is stored.
func (e *Engine) ValidateJob(ctx context.Context, owner model.Principal, in model.JobCreate) (model.Job, error) {
	j, err := model.ValidateJobCreate(in, e.vctx(owner))
	if err != nil {
		return j, err
	}
	return j, e.checkRefsExist(ctx, j.Spec())
}

// checkResolves resolves the callback host and applies the dial-time policy,
// so validate reports what the first fire would hit.
func (e *Engine) checkResolves(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	host := u.Hostname()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("%s does not resolve from the runner", host)
	}
	for _, ip := range ips {
		if !e.d.CallbackPolicy(host, ip) {
			return fmt.Errorf("%s resolves to %s, which callbacks may not reach", host, ip)
		}
	}
	return nil
}

// CreateJob validates, stores and schedules a job.
func (e *Engine) CreateJob(ctx context.Context, owner model.Principal, in model.JobCreate, commit Commit) (model.Job, error) {
	j, err := model.ValidateJobCreate(in, e.vctx(owner))
	if err != nil {
		return j, err
	}
	if err := e.checkRefsExist(ctx, j.Spec()); err != nil {
		return j, err
	}
	now := e.now()
	j.ID, j.Rev, j.CreatedAt, j.UpdatedAt = newID(), 1, now, now
	var inserted []model.Run
	err = e.st.Tx(ctx, func(tx *store.Tx) error {
		n, err := store.CountJobs(ctx, tx, owner.ClientID)
		if err != nil {
			return err
		}
		if n >= MaxJobsPerClient {
			return ErrJobLimit
		}
		if err := store.CreateJob(ctx, tx, j); err != nil {
			if store.IsUniqueViolation(err) {
				return ErrNameTaken
			}
			return err
		}
		r, _, err := insertNext(ctx, tx, &j, now, e.d.Rand)
		if err != nil {
			return err
		}
		if r != nil {
			inserted = append(inserted, *r)
		}
		if commit != nil {
			return commit(ctx, tx, j)
		}
		return nil
	})
	if err != nil {
		return j, err
	}
	e.notifyJob(ctx, evJobChanged{jobID: j.ID, inserted: inserted})
	return j, nil
}

func (e *Engine) owner(ctx context.Context, q store.Querier, clientID string) (model.Principal, error) {
	c, err := store.GetClient(ctx, q, clientID)
	if err != nil {
		return model.Principal{}, err
	}
	return model.PrincipalOf(c), nil
}

// UpdateJob applies a PATCH (plan §6.1 cursor rule).
func (e *Engine) UpdateJob(ctx context.Context, jobID string, patch model.JobPatch) (model.Job, error) {
	cur, err := store.GetJob(ctx, e.st.R, jobID)
	if errors.Is(err, store.ErrNotFound) {
		return cur, ErrNotFound
	}
	if err != nil {
		return cur, err
	}
	owner, err := e.owner(ctx, e.st.R, cur.ClientID)
	if err != nil {
		return cur, err
	}
	preview, _, err := model.ValidatePatch(cur, patch, e.vctx(owner))
	if err != nil {
		return cur, err
	}
	if err := e.checkRefsExist(ctx, preview.Spec()); err != nil {
		return cur, err
	}

	now := e.now()
	var next model.Job
	var inserted []model.Run
	var dropped []string
	err = e.st.Tx(ctx, func(tx *store.Tx) error {
		cur, err := store.GetJob(ctx, tx, jobID) // re-read inside the transaction
		if err != nil {
			return err
		}
		var triggerChanged bool
		next, triggerChanged, err = model.ValidatePatch(cur, patch, e.vctx(owner))
		if err != nil {
			return err
		}
		if specChanged(cur, next) {
			next.Rev = cur.Rev + 1
			if err := store.InsertRevision(ctx, tx, next.ID, next.Rev, next.Spec(), now); err != nil {
				return err
			}
		}
		deletedID, deleted, err := store.DeletePendingSchedule(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if deletedID != "" {
			dropped = append(dropped, deletedID)
		}
		switch {
		case triggerChanged && next.Trigger.Type == model.TriggerCron:
			next.LastNominalFor = &now
		case triggerChanged:
			next.LastNominalFor = nil
		case deleted != nil:
			c := deleted.Add(-time.Millisecond)
			if now.Before(c) {
				c = now
			}
			next.LastNominalFor = &c
		}
		if triggerChanged && cur.State == model.JobDone && next.Trigger.Type == model.TriggerAt &&
			!next.Trigger.At.Before(now.Add(-time.Duration(next.Policy.MaxLateSec)*time.Second)) {
			next.State = model.JobActive
		}
		if triggerChanged && cur.State == model.JobDone && next.Trigger.Type == model.TriggerCron {
			next.State = model.JobActive
		}
		next.UpdatedAt = now
		if err := store.UpdateJob(ctx, tx, next); err != nil {
			if store.IsUniqueViolation(err) {
				return ErrNameTaken
			}
			return err
		}
		r, _, err := insertNext(ctx, tx, &next, now, e.d.Rand)
		if err != nil {
			return err
		}
		if r != nil {
			inserted = append(inserted, *r)
		}
		return nil
	})
	if err != nil {
		return next, err
	}
	e.notifyJob(ctx, evJobChanged{jobID: jobID, inserted: inserted, dropped: dropped})
	return next, nil
}

func specChanged(a, b model.Job) bool {
	x, _ := json.Marshal(a.Spec())
	y, _ := json.Marshal(b.Spec())
	return string(x) != string(y)
}

// SetEnabled enables or disables a job.
func (e *Engine) SetEnabled(ctx context.Context, jobID string, enabled bool) (model.Job, error) {
	now := e.now()
	var j model.Job
	var inserted []model.Run
	var dropped []string
	err := e.st.Tx(ctx, func(tx *store.Tx) error {
		var err error
		j, err = store.GetJob(ctx, tx, jobID)
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if enabled {
			switch j.State {
			case model.JobDone:
				return ErrJobDone
			case model.JobActive:
				return nil
			}
			j.State = model.JobActive
			c := now
			if j.LastNominalFor != nil && j.LastNominalFor.After(now) {
				c = *j.LastNominalFor
			}
			j.LastNominalFor = &c
			if err := store.SetJobCursor(ctx, tx, j.ID, &c); err != nil {
				return err
			}
			if err := store.SetJobState(ctx, tx, j.ID, model.JobActive, now); err != nil {
				return err
			}
			r, _, err := insertNext(ctx, tx, &j, now, e.d.Rand)
			if err != nil {
				return err
			}
			if r != nil {
				inserted = append(inserted, *r)
			}
			return nil
		}
		if j.State == model.JobDisabled {
			return nil
		}
		cancelled, err := store.CancelPendingForJob(ctx, tx, j.ID, "job disabled", now, model.KindSchedule, model.KindRetry, model.KindDefer)
		if err != nil {
			return err
		}
		for _, r := range cancelled {
			dropped = append(dropped, r.ID)
			if r.Kind == model.KindSchedule && r.NominalFor != nil {
				c := r.NominalFor.Add(-time.Millisecond)
				j.LastNominalFor = &c
				if err := store.SetJobCursor(ctx, tx, j.ID, &c); err != nil {
					return err
				}
			}
		}
		j.State = model.JobDisabled
		return store.SetJobState(ctx, tx, j.ID, model.JobDisabled, now)
	})
	if err != nil {
		return j, err
	}
	e.notifyJob(ctx, evJobChanged{jobID: jobID, inserted: inserted, dropped: dropped})
	return j, nil
}

// DeleteJob cancels the job's pending and waiting runs and deletes it in one
// transaction; running runs are cancelled through the loop.
func (e *Engine) DeleteJob(ctx context.Context, jobID string) error {
	now := e.now()
	var released, running, dropped []string
	err := e.st.Tx(ctx, func(tx *store.Tx) error {
		if _, err := store.GetJob(ctx, tx, jobID); errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		cancelled, err := store.CancelPendingForJob(ctx, tx, jobID, "job deleted", now,
			model.KindSchedule, model.KindRetry, model.KindDefer, model.KindInvoke)
		if err != nil {
			return err
		}
		for _, r := range cancelled {
			dropped = append(dropped, r.ID)
		}
		live, err := runsOfJob(ctx, tx, jobID, model.StatusWaiting, model.StatusRunning)
		if err != nil {
			return err
		}
		for _, r := range live {
			if r.Status == model.StatusRunning {
				running = append(running, r.ID)
				continue
			}
			ok, err := store.Transition(ctx, tx, r.ID, []model.Status{model.StatusWaiting}, model.StatusCancelled,
				store.Fields{"finished_at": now, "error": "job deleted"})
			if err != nil {
				return err
			}
			if ok {
				released = append(released, r.ID)
			}
		}
		return store.DeleteJob(ctx, tx, jobID)
	})
	if err != nil {
		return err
	}
	e.notifyJob(ctx, evJobChanged{jobID: jobID, deleted: true, released: released, cancel: running, dropped: dropped})
	return nil
}

func runsOfJob(ctx context.Context, q store.Querier, jobID string, statuses ...model.Status) ([]model.Run, error) {
	all, err := store.RunsByStatus(ctx, q, statuses...)
	if err != nil {
		return nil, err
	}
	var out []model.Run
	for _, r := range all {
		if r.JobID == jobID {
			out = append(out, r)
		}
	}
	return out, nil
}

// Invoke creates an on-demand run of a job.
func (e *Engine) Invoke(ctx context.Context, jobID string, payload json.RawMessage, asOf *time.Time, commit Commit) (model.Run, error) {
	now := e.now()
	var r model.Run
	err := e.st.Tx(ctx, func(tx *store.Tx) error {
		j, err := store.GetJob(ctx, tx, jobID)
		if errors.Is(err, store.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if j.State == model.JobDisabled {
			return ErrJobDisabled
		}
		id := newID()
		contextFor := now
		if asOf != nil {
			contextFor = asOf.UTC()
		}
		r = model.Run{ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: model.KindInvoke, Attempt: 1,
			ChainRootID: id, Status: model.StatusPending, ScheduledFor: now, OriginFor: now, ContextFor: contextFor,
			LatenessRef: now, Rev: j.Rev, Payload: payload, CreatedAt: now}
		if err := store.InsertRun(ctx, tx, r); err != nil {
			return err
		}
		if commit != nil {
			return commit(ctx, tx, r)
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	e.notifyRun(ctx, r)
	return r, nil
}

// Adhoc creates a run that belongs to no job.
func (e *Engine) Adhoc(ctx context.Context, owner model.Principal, in model.AdhocReq, commit Commit) (model.Run, error) {
	spec, err := model.ValidateAdhoc(in, e.vctx(owner))
	if err != nil {
		return model.Run{}, err
	}
	if err := e.checkRefsExist(ctx, spec); err != nil {
		return model.Run{}, err
	}
	now := e.now()
	id := newID()
	r := model.Run{ID: id, JobName: model.AdhocJobName, ClientID: owner.ClientID, Kind: model.KindAdhoc, Attempt: 1,
		ChainRootID: id, Status: model.StatusPending, ScheduledFor: now, OriginFor: now, ContextFor: now,
		LatenessRef: now, AdhocSpec: &spec, Payload: in.Payload, CreatedAt: now}
	err = e.st.Tx(ctx, func(tx *store.Tx) error {
		if err := store.InsertRun(ctx, tx, r); err != nil {
			return err
		}
		if commit != nil {
			return commit(ctx, tx, r)
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	e.notifyRun(ctx, r)
	return r, nil
}

func (e *Engine) notifyRun(ctx context.Context, r model.Run) {
	if err := e.sendFromHandler(ctx, evRunInserted{runs: []model.Run{r}}); err != nil {
		e.log.Warn("new run not delivered to the loop; reconcile will dispatch it", "run", r.ID, "err", err)
	}
}

// Await blocks until the run chain rooted at chainRoot is terminal or d
// passes. A caller that goes away never cancels the run.
func (e *Engine) Await(ctx context.Context, chainRoot string, d time.Duration) (model.Run, bool) {
	ch := make(chan model.Run, 1)
	if err := e.sendFromHandler(ctx, evAwait{chainRoot: chainRoot, ch: ch}); err != nil {
		return model.Run{}, false
	}
	defer func() {
		select {
		case e.events <- evAwaitDrop{chainRoot: chainRoot, ch: ch}:
		default:
		}
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case r := <-ch:
		latest, err := store.ChainLatest(ctx, e.st.R, chainRoot)
		if err == nil {
			return latest, true
		}
		return r, true
	case <-timer.C:
	case <-ctx.Done():
	case <-e.stopping: // shutting down: answer 202 now rather than at drainBy
	case <-e.life.Done():
	}
	return model.Run{}, false
}

// Cancel cancels a run. accepted means a running run is being stopped (202).
func (e *Engine) Cancel(ctx context.Context, runID string) (model.Run, bool, error) {
	ch := make(chan cancelReply, 1)
	if err := e.sendFromHandler(ctx, evCancel{runID: runID, reply: ch}); err != nil {
		return model.Run{}, false, err
	}
	return waitReply(ctx, ch, func(r cancelReply) (model.Run, bool, error) { return r.run, r.accepted, r.err })
}

// Complete records an async completion.
func (e *Engine) Complete(ctx context.Context, runID string, req model.CompleteReq) (model.Run, bool, error) {
	ch := make(chan completeReply, 1)
	if err := e.sendFromHandler(ctx, evComplete{runID: runID, req: req, reply: ch}); err != nil {
		return model.Run{}, false, err
	}
	return waitReply(ctx, ch, func(r completeReply) (model.Run, bool, error) { return r.run, r.accepted, r.err })
}

// Heartbeat extends an async run's lease. lease is nil while the run is still
// running (its 202 has not been processed yet).
func (e *Engine) Heartbeat(ctx context.Context, runID string) (model.Run, *time.Time, error) {
	ch := make(chan hbReply, 1)
	if err := e.sendFromHandler(ctx, evHeartbeat{runID: runID, reply: ch}); err != nil {
		return model.Run{}, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case v := <-ch:
		return v.run, v.lease, v.err
	case <-ctx.Done():
		return model.Run{}, nil, ErrBusy
	}
}

func waitReply[T any](ctx context.Context, ch chan T, unpack func(T) (model.Run, bool, error)) (model.Run, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case v := <-ch:
		return unpack(v)
	case <-ctx.Done():
		return model.Run{}, false, ErrBusy
	}
}

// Check is one line of a validation report.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// CheckJob re-validates a stored job against its owner's current settings
// and the vault, as the run would at fire time.
func (e *Engine) CheckJob(ctx context.Context, j model.Job, owner model.Principal) []Check {
	var checks []Check
	add := func(name string, err error) {
		c := Check{Name: name, OK: err == nil}
		if err != nil {
			c.Detail = err.Error()
		}
		checks = append(checks, c)
	}
	if h := j.Target.HTTP; h != nil {
		var err error
		if u, perr := url.Parse(h.URL); perr != nil || !model.HostAllowed(u.Hostname(), owner.CallbackHosts) {
			err = errors.New("callback host is not in the client's callback_hosts")
		}
		add("callback_host", err)
		if err == nil && e.d.CallbackPolicy != nil {
			add("callback_dns", e.checkResolves(ctx, h.URL))
		}
	}
	if j.Target.Container != nil || j.Target.Inline != nil {
		var err error
		switch {
		case !e.d.ContainersEnabled:
			err = errors.New("container targets are not enabled on this runner")
		case !owner.ContainerAllowed:
			err = errors.New("the client may not run containers")
		}
		add("containers", err)
	}
	for _, ref := range j.Spec().Refs() {
		if _, err := model.CheckRef(ref, owner); err != nil {
			add("secret "+ref, err)
			continue
		}
		add("secret "+ref, e.checkRefsExist(ctx, model.RunSpec{Target: model.Target{Type: model.TargetHTTP,
			HTTP: &model.HTTPTarget{Auth: model.HTTPAuth{BearerRef: ref}}}}))
	}
	if j.Trigger.Type == model.TriggerCron || j.Trigger.Type == model.TriggerAt {
		var err error
		if len(NextFires(j, e.now(), 1)) == 0 {
			err = errors.New("the trigger has no future fire time")
		}
		add("schedule", err)
	}
	return checks
}
