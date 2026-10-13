package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// execute runs off the loop: fire-time policy re-check, secret resolution,
// then the executor.
func (e *Engine) execute(ctx context.Context, r model.Run, spec model.RunSpec) target.Result {
	fail := func(msg string, unsent bool) target.Result {
		return target.Result{Status: model.StatusFailed, Error: msg, Unsent: unsent, NoRetry: !unsent}
	}
	c, err := store.GetClient(ctx, e.st.R, r.ClientID)
	if err != nil {
		return fail("owning client no longer exists", false)
	}
	p := model.PrincipalOf(c)
	if h := spec.Target.HTTP; h != nil {
		u, err := url.Parse(h.URL)
		if err != nil || !model.HostAllowed(u.Hostname(), p.CallbackHosts) {
			return fail("callback host is no longer in this client's callback_hosts", false)
		}
	}
	if (spec.Target.Container != nil || spec.Target.Inline != nil) && (!e.d.ContainersEnabled || !p.ContainerAllowed) {
		return fail("container targets are not enabled for this client", false)
	}
	secrets := map[string]string{}
	for _, ref := range spec.Refs() {
		if _, err := model.CheckRef(ref, p); err != nil {
			return fail("secret ref not permitted: "+ref, false)
		}
		parsed, err := vault.ParseRef(ref)
		if err != nil {
			return fail(err.Error(), false)
		}
		v, err := e.d.Secrets.Get(ctx, parsed)
		if err != nil {
			// Nothing was sent, so a retry is safe in both delivery modes.
			return target.Result{Status: model.StatusFailed, Error: "could not resolve " + ref + ": " + err.Error(), Unsent: true}
		}
		secrets[ref] = v
	}
	exec, ok := e.d.Executors[spec.Target.Type]
	if !ok {
		return fail("target type "+spec.Target.Type+" is not enabled on this runner", false)
	}
	return exec.Execute(ctx, target.ExecInput{
		Run: r, Spec: spec, Secrets: secrets, RunToken: secure.RunToken(e.d.RunTokenKey, r.ID),
		LogPath: e.logPath(r.ID), EnvKey: e.d.EnvKey,
	})
}

func is2xxNot202(res target.Result) bool {
	return res.HTTPStatus != nil && *res.HTTPStatus >= 200 && *res.HTTPStatus < 300 && *res.HTTPStatus != 202
}

func (e *Engine) onExecDone(runID string, res target.Result) {
	if !e.accepting.Load() {
		return // past drainBy: boot recovery owns this run
	}
	delete(e.cancelFns, runID)
	r, err := store.GetRun(e.life, e.st.W, runID)
	if err != nil || r.Status != model.StatusRunning {
		return
	}
	spec, err := e.spec(r)
	if err != nil {
		e.log.Error("load spec on exec done", "run", runID, "err", err)
		return
	}
	running := []model.Status{model.StatusRunning}
	now := e.now()

	if e.cancelRequested[runID] {
		e.finalize(r, running, outcome{status: model.StatusCancelled, err: "cancelled", httpStatus: res.HTTPStatus,
			exitCode: res.ExitCode, delivered: res.Delivered})
		return
	}
	if r.EarlyResult != nil {
		if res.Status == model.StatusSuccess && !res.Waiting && is2xxNot202(res) {
			o := e.decide(r, spec, res, now)
			o.err = "target also reported completion early: " + r.EarlyResult.Status
			e.finalize(r, running, o)
			return
		}
		e.finalize(r, running, e.fromComplete(r, spec, *r.EarlyResult, res.HTTPStatus, now))
		return
	}
	if res.Waiting {
		lease := now.Add(time.Duration(spec.Target.HTTP.LeaseSec) * time.Second)
		if r.DeadlineAt != nil && lease.After(*r.DeadlineAt) {
			lease = *r.DeadlineAt
		}
		ok, err := store.Transition(e.life, e.st.W, r.ID, running, model.StatusWaiting, store.Fields{
			"lease_expires_at": lease, "http_status": 202, "delivery_attempted": true,
		})
		if err != nil || !ok {
			if err != nil {
				e.log.Error("mark waiting", "run", r.ID, "err", err)
			}
			return
		}
		e.releasePool(r.ID)
		e.armLease(r.ID, lease)
		return
	}
	e.finalize(r, running, e.decide(r, spec, res, now))
}

// fromComplete maps an async completion into an outcome.
func (e *Engine) fromComplete(r model.Run, spec model.RunSpec, cr model.CompleteReq, httpStatus *int, now time.Time) outcome {
	res := target.Result{HTTPStatus: httpStatus, Output: cr.Output, Error: cr.Error, Delivered: true}
	if len(res.Output) > model.MaxOutputBytes {
		res.Output, res.OutputTruncated = nil, true
	}
	if cr.Status == string(model.StatusSuccess) {
		res.Status = model.StatusSuccess
	} else {
		res.Status = model.StatusFailed
		if res.Error == "" {
			res.Error = "target reported failure"
		}
		res.Ambiguous = false // a reported failure is definite; at_least_once may retry
	}
	return e.decide(r, spec, res, now)
}

// ---------------------------------------------------------------- leases

func (e *Engine) armLease(runID string, expires time.Time) {
	e.stopLease(runID)
	gen := e.leaseGen[runID] + 1
	e.leaseGen[runID] = gen
	e.leaseTimers[runID] = e.d.Clock.AfterFunc(max(expires.Sub(e.now()), 0), func() {
		_ = e.send(e.life, evLeaseExpired{runID: runID, gen: gen})
	})
}

func (e *Engine) stopLease(runID string) {
	if t, ok := e.leaseTimers[runID]; ok {
		t.Stop()
		delete(e.leaseTimers, runID)
	}
	delete(e.leaseGen, runID)
}

func (e *Engine) onLeaseExpired(runID string, gen uint64) {
	if e.leaseGen[runID] != gen {
		return // re-armed by a heartbeat since
	}
	delete(e.leaseTimers, runID)
	r, err := store.GetRun(e.life, e.st.W, runID)
	if err != nil || r.Status != model.StatusWaiting {
		return
	}
	spec, err := e.spec(r)
	if err != nil {
		return
	}
	now := e.now()
	if r.DeadlineAt != nil && !now.Before(*r.DeadlineAt) {
		e.finalize(r, []model.Status{model.StatusWaiting}, e.decide(r, spec, target.Result{
			Status: model.StatusTimeout, Error: "async run exceeded timeout_sec", Ambiguous: true, Delivered: true,
		}, now))
		return
	}
	o := e.decide(r, spec, target.Result{Status: model.StatusInterrupted, Error: "lease expired without heartbeat or completion", Ambiguous: true, Delivered: true}, now)
	if o.status == model.StatusInterrupted {
		o.page, o.pageTitle = true, "NEEDS A HUMAN"
	}
	e.finalize(r, []model.Status{model.StatusWaiting}, o)
}

// ---------------------------------------------------------------- API ops on the loop

func (e *Engine) cancelRun(runID string) (model.Run, bool, error) {
	r, err := store.GetRun(e.life, e.st.W, runID)
	if errors.Is(err, store.ErrNotFound) {
		return r, false, ErrNotFound
	}
	if err != nil {
		return r, false, err
	}
	switch r.Status {
	case model.StatusPending:
		e.unpark(r.ID)
		e.finalize(r, []model.Status{model.StatusPending}, outcome{status: model.StatusCancelled, err: "cancelled"})
	case model.StatusWaiting:
		e.finalize(r, []model.Status{model.StatusWaiting}, outcome{status: model.StatusCancelled, err: "cancelled"})
	case model.StatusRunning:
		e.cancelRequested[r.ID] = true
		if cancel, ok := e.cancelFns[r.ID]; ok {
			cancel()
		}
		return r, true, nil
	default:
		return r, false, ErrRunTerminal
	}
	r, err = store.GetRun(e.life, e.st.W, runID)
	if err == nil && !r.Status.Terminal() {
		return r, false, ErrConflict
	}
	return r, false, err
}

func (e *Engine) isAsync(r model.Run) (model.RunSpec, bool) {
	spec, err := e.spec(r)
	return spec, err == nil && spec.Target.HTTP != nil && spec.Target.HTTP.Mode == "async"
}

func (e *Engine) completeRun(runID string, cr model.CompleteReq) (model.Run, bool, error) {
	r, err := store.GetRun(e.life, e.st.W, runID)
	if errors.Is(err, store.ErrNotFound) {
		return r, false, ErrNotFound
	}
	if err != nil {
		return r, false, err
	}
	spec, async := e.isAsync(r)
	if !async {
		return r, false, ErrNotAsync
	}
	switch r.Status {
	case model.StatusRunning:
		ok, err := store.SetEarlyResult(e.life, e.st.W, r.ID, cr)
		if err != nil {
			return r, false, err
		}
		if ok {
			return r, true, nil
		}
		if r.EarlyResult != nil && sameComplete(*r.EarlyResult, cr) {
			return r, true, nil
		}
		return r, false, ErrConflict
	case model.StatusWaiting:
		e.finalize(r, []model.Status{model.StatusWaiting}, e.fromComplete(r, spec, cr, nil, e.now()))
		r, err = store.GetRun(e.life, e.st.W, runID)
		return r, false, err
	}
	if r.Status == model.StatusInterrupted && cr.Status == string(model.StatusSuccess) {
		note := "target reported success after lease expiry"
		if _, err := e.st.W.ExecContext(e.life, `UPDATE runs SET error = substr(coalesce(error,'') || ' | ' || ?, 1, 4096) WHERE id = ?`, note, r.ID); err != nil {
			e.log.Error("note late completion", "run", r.ID, "err", err)
		}
		e.page(r, notify.Fail, "late completion", note+"; a retry may already have run.")
	}
	return r, false, ErrNotWaiting
}

func sameComplete(a, b model.CompleteReq) bool {
	return a.Status == b.Status && a.Error == b.Error && bytes.Equal(compactJSON(a.Output), compactJSON(b.Output))
}

func compactJSON(b json.RawMessage) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return b
	}
	return buf.Bytes()
}

func (e *Engine) heartbeat(runID string) (model.Run, *time.Time, error) {
	r, err := store.GetRun(e.life, e.st.W, runID)
	if errors.Is(err, store.ErrNotFound) {
		return r, nil, ErrNotFound
	}
	if err != nil {
		return r, nil, err
	}
	spec, async := e.isAsync(r)
	if !async {
		return r, nil, ErrNotAsync
	}
	switch r.Status {
	case model.StatusRunning:
		return r, nil, nil
	case model.StatusWaiting:
		lease := e.now().Add(time.Duration(spec.Target.HTTP.LeaseSec) * time.Second)
		if r.DeadlineAt != nil && lease.After(*r.DeadlineAt) {
			lease = *r.DeadlineAt
		}
		ok, err := store.Transition(e.life, e.st.W, r.ID, []model.Status{model.StatusWaiting}, model.StatusWaiting, store.Fields{"lease_expires_at": lease})
		if err != nil {
			return r, nil, err
		}
		if !ok {
			return r, nil, ErrNotWaiting
		}
		e.armLease(r.ID, lease)
		return r, &lease, nil
	}
	return r, nil, ErrNotWaiting
}

func (e *Engine) onJobChanged(v evJobChanged) {
	if v.deleted {
		delete(e.jobs, v.jobID)
	} else if j, err := store.GetJob(e.life, e.st.W, v.jobID); err == nil {
		e.jobs[j.ID] = newJobEntry(j, e.now())
	} else {
		e.log.Error("reload job", "job", v.jobID, "err", err)
	}
	for _, id := range v.released {
		e.release(id)
		e.stopLease(id)
		if r, err := store.GetRun(e.life, e.st.W, id); err == nil {
			e.notifyAwaiters(r)
		}
	}
	for _, id := range v.cancel {
		e.cancelRequested[id] = true
		if cancel, ok := e.cancelFns[id]; ok {
			cancel()
		}
	}
	for _, r := range v.inserted {
		e.push(r.ID, r.ScheduledFor)
	}
}

func newJobEntry(j model.Job, now time.Time) *jobEntry {
	return &jobEntry{job: j, minGap: j.MinGap(now)}
}

