package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// IdemRow is a stored idempotency record.
type IdemRow struct {
	BodyHash []byte
	Done     bool
	Status   int
	Response []byte
}

// IdemBegin claims (client, route, key). When a row already exists it is
// returned and nothing is claimed.
func IdemBegin(ctx context.Context, q Querier, clientID, route, key string, hash []byte, now time.Time) (*IdemRow, error) {
	res, err := q.ExecContext(ctx, `INSERT INTO idempotency (client_id, route, key, body_hash, state, created_at)
		VALUES (?,?,?,?, 'in_progress', ?) ON CONFLICT DO NOTHING`, clientID, route, key, hash, ms(now))
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil, nil
	}
	var (
		row    IdemRow
		state  string
		status sql.NullInt64
	)
	err = q.QueryRowContext(ctx, `SELECT body_hash, state, status, response FROM idempotency WHERE client_id = ? AND route = ? AND key = ?`,
		clientID, route, key).Scan(&row.BodyHash, &state, &status, &row.Response)
	if errors.Is(err, sql.ErrNoRows) {
		// Deleted between the insert and the read: treat as in progress.
		return &IdemRow{}, nil
	}
	if err != nil {
		return nil, err
	}
	row.Done, row.Status = state == "done", int(status.Int64)
	return &row, nil
}

// IdemFinish stores the response. Call it in the same transaction as the write
// the response describes.
func IdemFinish(ctx context.Context, q Querier, clientID, route, key string, status int, response []byte) error {
	_, err := q.ExecContext(ctx, `UPDATE idempotency SET state = 'done', status = ?, response = ? WHERE client_id = ? AND route = ? AND key = ?`,
		status, response, clientID, route, key)
	return err
}

// IdemAbort deletes an in-progress claim.
func IdemAbort(ctx context.Context, q Querier, clientID, route, key string) error {
	_, err := q.ExecContext(ctx, `DELETE FROM idempotency WHERE client_id = ? AND route = ? AND key = ? AND state = 'in_progress'`, clientID, route, key)
	return err
}

// IdemClearInProgress removes every in-progress claim (boot).
func IdemClearInProgress(ctx context.Context, q Querier) error {
	_, err := q.ExecContext(ctx, `DELETE FROM idempotency WHERE state = 'in_progress'`)
	return err
}

// IdemPrune deletes records older than before.
func IdemPrune(ctx context.Context, q Querier, before time.Time) (int64, error) {
	res, err := q.ExecContext(ctx, `DELETE FROM idempotency WHERE created_at < ?`, ms(before))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
