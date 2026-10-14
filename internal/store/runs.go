package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

const runCols = `id, job_id, job_name, client_id, trigger, attempt, parent_run_id, chain_root_id, status,
	nominal_for, scheduled_for, origin_for, context_for, lateness_ref, rev, adhoc_spec_json, payload,
	started_at, finished_at, late_ms, lease_expires_at, deadline_at, lease_grace_used, early_result,
	delivery_attempted, http_status, exit_code, output, output_truncated, output_error, error,
	container_name, created_at`

func scanRun(row scanner) (model.Run, error) {
	var (
		r                                                  model.Run
		jobID, parent, adhoc, early, outErr, errStr, cname sql.NullString
		nominal, started, finished, late, lease, deadline  sql.NullInt64
		rev, httpStatus, exitCode                          sql.NullInt64
		sched, origin, contextFor, latRef, created         int64
		graceUsed, delivered, truncated                    int
		status                                             string
		payload, output                                    []byte
	)
	err := row.Scan(&r.ID, &jobID, &r.JobName, &r.ClientID, &r.Kind, &r.Attempt, &parent, &r.ChainRootID, &status,
		&nominal, &sched, &origin, &contextFor, &latRef, &rev, &adhoc, &payload,
		&started, &finished, &late, &lease, &deadline, &graceUsed, &early,
		&delivered, &httpStatus, &exitCode, &output, &truncated, &outErr, &errStr,
		&cname, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	r.JobID, r.ParentRunID, r.Status = jobID.String, parent.String, model.Status(status)
	if payload != nil {
		r.Payload = json.RawMessage(payload)
	}
	if output != nil {
		r.Output = json.RawMessage(output)
	}
	r.NominalFor = fromNullMs(nominal)
	r.ScheduledFor, r.OriginFor, r.ContextFor, r.LatenessRef = fromMs(sched), fromMs(origin), fromMs(contextFor), fromMs(latRef)
	r.Rev = int(rev.Int64)
	if adhoc.Valid {
		var spec model.RunSpec
		if err := json.Unmarshal([]byte(adhoc.String), &spec); err != nil {
			return r, err
		}
		r.AdhocSpec = &spec
	}
	r.StartedAt, r.FinishedAt = fromNullMs(started), fromNullMs(finished)
	if late.Valid {
		r.LateMs = &late.Int64
	}
	r.LeaseExpiresAt, r.DeadlineAt = fromNullMs(lease), fromNullMs(deadline)
	r.LeaseGraceUsed, r.DeliveryAttempt, r.OutputTruncated = graceUsed == 1, delivered == 1, truncated == 1
	if early.Valid {
		var cr model.CompleteReq
		if err := json.Unmarshal([]byte(early.String), &cr); err != nil {
			return r, err
		}
		r.EarlyResult = &cr
	}
	if httpStatus.Valid {
		v := int(httpStatus.Int64)
		r.HTTPStatus = &v
	}
	if exitCode.Valid {
		v := int(exitCode.Int64)
		r.ExitCode = &v
	}
	r.OutputError, r.Error, r.ContainerName = outErr.String, errStr.String, cname.String
	r.CreatedAt = fromMs(created)
	return r, nil
}

func runArgs(r model.Run) ([]any, error) {
	var adhoc any
	if r.AdhocSpec != nil {
		b, err := json.Marshal(r.AdhocSpec)
		if err != nil {
			return nil, err
		}
		adhoc = string(b)
	}
	var early any
	if r.EarlyResult != nil {
		b, err := json.Marshal(r.EarlyResult)
		if err != nil {
			return nil, err
		}
		early = string(b)
	}
	var rev any
	if r.Rev > 0 {
		rev = r.Rev
	}
	var payload any
	if len(r.Payload) > 0 {
		payload = []byte(r.Payload)
	}
	var output any
	if len(r.Output) > 0 {
		output = []byte(r.Output)
	}
	return []any{r.ID, nullStr(r.JobID), r.JobName, r.ClientID, r.Kind, r.Attempt, nullStr(r.ParentRunID), r.ChainRootID, string(r.Status),
		msPtr(r.NominalFor), ms(r.ScheduledFor), ms(r.OriginFor), ms(r.ContextFor), ms(r.LatenessRef), rev, adhoc, payload,
		msPtr(r.StartedAt), msPtr(r.FinishedAt), r.LateMs, msPtr(r.LeaseExpiresAt), msPtr(r.DeadlineAt), boolInt(r.LeaseGraceUsed), early,
		boolInt(r.DeliveryAttempt), r.HTTPStatus, r.ExitCode, output, boolInt(r.OutputTruncated), nullStr(r.OutputError), nullStr(r.Error),
		nullStr(r.ContainerName), ms(r.CreatedAt)}, nil
}

// InsertRun inserts a run.
func InsertRun(ctx context.Context, q Querier, r model.Run) error {
	args, err := runArgs(r)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO runs (`+runCols+`) VALUES (`+placeholders(33)+`)`, args...)
	return err
}

// InsertScheduleRun inserts a pending schedule run unless the job already has
// one. Only a runs_one_pending conflict is absorbed; any other constraint
// error is returned.
func InsertScheduleRun(ctx context.Context, q Querier, r model.Run) (bool, error) {
	args, err := runArgs(r)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, `INSERT INTO runs (`+runCols+`) VALUES (`+placeholders(33)+`)
		ON CONFLICT (job_id) WHERE trigger = 'schedule' AND status = 'pending' AND job_id IS NOT NULL DO NOTHING`, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// GetRun loads a run.
func GetRun(ctx context.Context, q Querier, id string) (model.Run, error) {
	return scanRun(q.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id))
}

// Fields are column updates applied with a transition. Keys must be run
// column names; they come from code, never from input.
type Fields map[string]any

var mutableRunCols = map[string]bool{
	"started_at": true, "finished_at": true, "late_ms": true, "lease_expires_at": true, "deadline_at": true,
	"lease_grace_used": true, "early_result": true, "delivery_attempted": true, "http_status": true,
	"exit_code": true, "output": true, "output_truncated": true, "output_error": true, "error": true,
	"container_name": true,
}

// Transition moves a run to `to` only if its status is one of `from`. It is
// the only way a run's status changes. It reports whether the row changed.
func Transition(ctx context.Context, q Querier, id string, from []model.Status, to model.Status, f Fields) (bool, error) {
	set := []string{"status = ?"}
	args := []any{string(to)}
	// Sorted, so one field set always yields one SQL text (one cached statement).
	for _, col := range slices.Sorted(maps.Keys(f)) {
		if !mutableRunCols[col] {
			return false, fmt.Errorf("store: column %q is not mutable", col)
		}
		set = append(set, col+" = ?")
		args = append(args, sqlValue(f[col]))
	}
	args = append(args, id)
	for _, s := range from {
		args = append(args, string(s))
	}
	res, err := q.ExecContext(ctx, `UPDATE runs SET `+strings.Join(set, ", ")+` WHERE id = ? AND status IN (`+placeholders(len(from))+`)`, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func sqlValue(v any) any {
	switch x := v.(type) {
	case time.Time:
		return ms(x)
	case *time.Time:
		return msPtr(x)
	case bool:
		return boolInt(x)
	case json.RawMessage:
		if len(x) == 0 {
			return nil
		}
		return []byte(x)
	case *model.CompleteReq:
		if x == nil {
			return nil
		}
		b, _ := json.Marshal(x)
		return string(b)
	case string:
		return nullStr(x)
	}
	return v
}

// SetEarlyResult stores an async completion that arrived while the run was
// still running. It succeeds once per run.
func SetEarlyResult(ctx context.Context, q Querier, id string, cr model.CompleteReq) (bool, error) {
	b, err := json.Marshal(cr)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, `UPDATE runs SET early_result = ? WHERE id = ? AND status = 'running' AND early_result IS NULL`, string(b), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// RunsByStatus returns runs in any of the statuses, oldest first.
func RunsByStatus(ctx context.Context, q Querier, statuses ...model.Status) ([]model.Run, error) {
	args := make([]any, len(statuses))
	for i, s := range statuses {
		args[i] = string(s)
	}
	return queryAll(ctx, q, scanRun, `SELECT `+runCols+` FROM runs WHERE status IN (`+placeholders(len(statuses))+`) ORDER BY scheduled_for, id`, args...)
}

// PendingRunsForJob returns a job's pending runs in dispatch order.
func PendingRunsForJob(ctx context.Context, q Querier, jobID string) ([]model.Run, error) {
	return queryAll(ctx, q, scanRun, `SELECT `+runCols+` FROM runs WHERE job_id = ? AND status = 'pending' ORDER BY scheduled_for, id`, jobID)
}

// ChainLatest returns the newest run of a retry/defer chain.
func ChainLatest(ctx context.Context, q Querier, rootID string) (model.Run, error) {
	return scanRun(q.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE chain_root_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`, rootID))
}

// HasScheduleRunFor reports whether a non-cancelled schedule run exists for nominal.
func HasScheduleRunFor(ctx context.Context, q Querier, jobID string, nominal time.Time) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM runs WHERE job_id = ? AND trigger = 'schedule' AND nominal_for = ? AND status <> 'cancelled'`,
		jobID, ms(nominal)).Scan(&n)
	return n > 0, err
}

// DeletePendingSchedule deletes a job's pending schedule run and returns its
// nominal instant (nil when there was none).
func DeletePendingSchedule(ctx context.Context, q Querier, jobID string) (id string, nominal *time.Time, err error) {
	var n sql.NullInt64
	err = q.QueryRowContext(ctx, `DELETE FROM runs WHERE job_id = ? AND trigger = 'schedule' AND status = 'pending' RETURNING id, nominal_for`, jobID).Scan(&id, &n)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	return id, fromNullMs(n), nil
}

// CancelPendingForJob cancels a job's pending runs of the given kinds and
// returns them.
func CancelPendingForJob(ctx context.Context, q Querier, jobID string, reason string, now time.Time, kinds ...string) ([]model.Run, error) {
	args := []any{reason, ms(now), jobID}
	for _, k := range kinds {
		args = append(args, k)
	}
	return queryAll(ctx, q, scanRun, `UPDATE runs SET status = 'cancelled', error = ?, finished_at = ?
		WHERE job_id = ? AND status = 'pending' AND trigger IN (`+placeholders(len(kinds))+`) RETURNING `+runCols, args...)
}

// RunFilter narrows ListRuns.
type RunFilter struct {
	ClientID string // "" = all clients
	JobID    string
	Status   string
	Cursor   string
	Limit    int
}

// ListRuns pages runs by id descending.
func ListRuns(ctx context.Context, q Querier, f RunFilter) ([]model.Run, error) {
	return queryAll(ctx, q, scanRun, `SELECT `+runCols+` FROM runs
		WHERE (? = '' OR client_id = ?) AND (? = '' OR job_id = ?) AND (? = '' OR status = ?) AND (? = '' OR id < ?)
		ORDER BY id DESC LIMIT ?`,
		f.ClientID, f.ClientID, f.JobID, f.JobID, f.Status, f.Status, f.Cursor, f.Cursor, f.Limit)
}

// DeleteFinishedBefore deletes at most limit terminal runs finished before
// `before`, optionally only for jobIDs, and returns their ids.
func DeleteFinishedBefore(ctx context.Context, q Querier, before time.Time, jobIDs []string, limit int) ([]string, error) {
	args := []any{ms(before)}
	jobClause := ""
	if jobIDs != nil {
		if len(jobIDs) == 0 {
			return nil, nil
		}
		q = Uncached(q) // the IN list's length comes from data
		jobClause = ` AND job_id IN (` + placeholders(len(jobIDs)) + `)`
		for _, id := range jobIDs {
			args = append(args, id)
		}
	}
	args = append(args, limit)
	return queryAll(ctx, q, scanID, `DELETE FROM runs WHERE id IN (SELECT id FROM runs WHERE finished_at IS NOT NULL AND finished_at < ?`+
		jobClause+` AND status NOT IN ('pending','running','waiting') LIMIT ?) RETURNING id`, args...)
}

func scanID(row scanner) (string, error) {
	var id string
	err := row.Scan(&id)
	return id, err
}
