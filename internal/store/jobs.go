package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

const jobCols = `id, client_id, name, state, trigger_json, target_type, target_json, policy_json,
	concurrency_key, rev, last_nominal_for, consecutive_skips, created_at, updated_at`

func scanJob(row interface{ Scan(...any) error }) (model.Job, error) {
	var (
		j                           model.Job
		trig, ttype, target, policy string
		key                         sql.NullString
		last                        sql.NullInt64
		created, updated            int64
	)
	err := row.Scan(&j.ID, &j.ClientID, &j.Name, &j.State, &trig, &ttype, &target, &policy,
		&key, &j.Rev, &last, &j.ConsecutiveSkips, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	if err != nil {
		return j, err
	}
	if err := errors.Join(json.Unmarshal([]byte(trig), &j.Trigger), json.Unmarshal([]byte(target), &j.Target),
		json.Unmarshal([]byte(policy), &j.Policy)); err != nil {
		return j, err
	}
	j.NSKey = key.String
	j.LastNominalFor = fromNullMs(last)
	j.CreatedAt, j.UpdatedAt = fromMs(created), fromMs(updated)
	return j, nil
}

func jobArgs(j model.Job) ([]any, error) {
	trig, err1 := json.Marshal(j.Trigger)
	target, err2 := json.Marshal(j.Target)
	policy, err3 := json.Marshal(j.Policy)
	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, err
	}
	return []any{j.ID, j.ClientID, j.Name, j.State, string(trig), j.Target.Type, string(target), string(policy),
		nullStr(j.NSKey), j.Rev, msPtr(j.LastNominalFor), j.ConsecutiveSkips, ms(j.CreatedAt), ms(j.UpdatedAt)}, nil
}

// CreateJob inserts a job and its first revision.
func CreateJob(ctx context.Context, q Querier, j model.Job) error {
	args, err := jobArgs(j)
	if err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO jobs (`+jobCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, args...); err != nil {
		return err
	}
	return InsertRevision(ctx, q, j.ID, j.Rev, j.Spec(), j.UpdatedAt)
}

// UpdateJob rewrites every column of a job (the caller bumps Rev and inserts
// the revision when the spec changed).
func UpdateJob(ctx context.Context, q Querier, j model.Job) error {
	args, err := jobArgs(j)
	if err != nil {
		return err
	}
	res, err := q.ExecContext(ctx, `UPDATE jobs SET client_id=?, name=?, state=?, trigger_json=?, target_type=?, target_json=?,
		policy_json=?, concurrency_key=?, rev=?, last_nominal_for=?, consecutive_skips=?, created_at=?, updated_at=? WHERE id=?`,
		append(args[1:], j.ID)...)
	return affected(res, err)
}

// GetJob loads a job by id.
func GetJob(ctx context.Context, q Querier, id string) (model.Job, error) {
	return scanJob(q.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
}

// ListJobs pages jobs by id descending. clientID "" lists all clients.
func ListJobs(ctx context.Context, q Querier, clientID, cursor string, limit int) ([]model.Job, error) {
	query := `SELECT ` + jobCols + ` FROM jobs WHERE (? = '' OR client_id = ?) AND (? = '' OR id < ?) ORDER BY id DESC LIMIT ?`
	rows, err := q.QueryContext(ctx, query, clientID, clientID, cursor, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return collectJobs(rows)
}

// AllJobs returns every job (the engine's cache).
func AllJobs(ctx context.Context, q Querier) ([]model.Job, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return collectJobs(rows)
}

func collectJobs(rows *sql.Rows) ([]model.Job, error) {
	var out []model.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// CountJobs counts a client's jobs.
func CountJobs(ctx context.Context, q Querier, clientID string) (int, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE client_id = ?`, clientID).Scan(&n)
	return n, err
}

// DeleteJob removes a job row. Revisions and runs are kept for history.
func DeleteJob(ctx context.Context, q Querier, id string) error {
	res, err := q.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	return affected(res, err)
}

// SetJobCursor updates the materialise cursor.
func SetJobCursor(ctx context.Context, q Querier, id string, cursor *time.Time) error {
	_, err := q.ExecContext(ctx, `UPDATE jobs SET last_nominal_for = ? WHERE id = ?`, msPtr(cursor), id)
	return err
}

// SetJobState updates the state.
func SetJobState(ctx context.Context, q Querier, id, state string, now time.Time) error {
	res, err := q.ExecContext(ctx, `UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?`, state, ms(now), id)
	return affected(res, err)
}

// SetConsecutiveSkips stores the skip counter.
func SetConsecutiveSkips(ctx context.Context, q Querier, id string, n int) error {
	_, err := q.ExecContext(ctx, `UPDATE jobs SET consecutive_skips = ? WHERE id = ?`, n, id)
	return err
}

// InsertRevision stores a spec snapshot.
func InsertRevision(ctx context.Context, q Querier, jobID string, rev int, spec model.RunSpec, now time.Time) error {
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO job_revisions (job_id, rev, spec_json, created_at) VALUES (?,?,?,?)`,
		jobID, rev, string(b), ms(now))
	return err
}

// JobRevision loads a spec snapshot.
func JobRevision(ctx context.Context, q Querier, jobID string, rev int) (model.RunSpec, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT spec_json FROM job_revisions WHERE job_id = ? AND rev = ?`, jobID, rev).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return model.RunSpec{}, ErrNotFound
	}
	if err != nil {
		return model.RunSpec{}, err
	}
	var spec model.RunSpec
	return spec, json.Unmarshal([]byte(raw), &spec)
}

// PruneRevisions deletes revisions that are not current and no run references.
func PruneRevisions(ctx context.Context, q Querier) (int64, error) {
	res, err := q.ExecContext(ctx, `DELETE FROM job_revisions WHERE
		NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = job_revisions.job_id AND j.rev = job_revisions.rev)
		AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = job_revisions.job_id AND r.rev = job_revisions.rev)`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
