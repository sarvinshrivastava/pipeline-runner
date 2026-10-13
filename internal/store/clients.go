package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

// ErrHasJobs is returned when deleting a client that still owns jobs.
var ErrHasJobs = errors.New("client owns jobs")

const clientCols = `id, name, token_hash, prev_token_hash, prev_valid_until, scopes, callback_hosts,
	secret_ref_prefixes, repo_prefixes, container_allowed, root_allowed, ntfy_topic, created_at, updated_at`

func scanClient(row interface{ Scan(...any) error }) (model.Client, error) {
	var (
		c                              model.Client
		prevUntil                      sql.NullInt64
		scopes, hosts, prefixes, repos string
		container, root                int
		topic                          sql.NullString
		created, updated               int64
	)
	err := row.Scan(&c.ID, &c.Name, &c.TokenHash, &c.PrevTokenHash, &prevUntil, &scopes, &hosts,
		&prefixes, &repos, &container, &root, &topic, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, err
	}
	for _, f := range []struct {
		raw string
		dst *[]string
	}{{scopes, &c.Scopes}, {hosts, &c.CallbackHosts}, {prefixes, &c.SecretRefPrefixes}, {repos, &c.RepoPrefixes}} {
		if err := json.Unmarshal([]byte(f.raw), f.dst); err != nil {
			return c, err
		}
	}
	c.PrevValidUntil = fromNullMs(prevUntil)
	c.ContainerAllowed, c.RootAllowed = container == 1, root == 1
	c.NtfyTopic = topic.String
	c.CreatedAt, c.UpdatedAt = fromMs(created), fromMs(updated)
	return c, nil
}

func jsonList(v []string) string {
	if v == nil {
		v = []string{}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// CreateClient inserts a client.
func CreateClient(ctx context.Context, q Querier, c model.Client) error {
	_, err := q.ExecContext(ctx, `INSERT INTO clients (`+clientCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.ID, c.Name, c.TokenHash, c.PrevTokenHash, msPtr(c.PrevValidUntil), jsonList(c.Scopes), jsonList(c.CallbackHosts),
		jsonList(c.SecretRefPrefixes), jsonList(c.RepoPrefixes), boolInt(c.ContainerAllowed), boolInt(c.RootAllowed),
		nullStr(c.NtfyTopic), ms(c.CreatedAt), ms(c.UpdatedAt))
	return err
}

// GetClient loads a client by id.
func GetClient(ctx context.Context, q Querier, id string) (model.Client, error) {
	return scanClient(q.QueryRowContext(ctx, `SELECT `+clientCols+` FROM clients WHERE id = ?`, id))
}

// ClientByName loads a client by name.
func ClientByName(ctx context.Context, q Querier, name string) (model.Client, error) {
	return scanClient(q.QueryRowContext(ctx, `SELECT `+clientCols+` FROM clients WHERE name = ?`, name))
}

// ClientByTokenHash matches the current token hash, or the previous one while
// its rotation grace lasts.
func ClientByTokenHash(ctx context.Context, q Querier, h []byte, now time.Time) (model.Client, error) {
	return scanClient(q.QueryRowContext(ctx, `SELECT `+clientCols+` FROM clients
		WHERE token_hash = ? OR (prev_token_hash = ? AND prev_valid_until > ?) LIMIT 1`, h, h, ms(now)))
}

// ListClients returns every client ordered by name.
func ListClients(ctx context.Context, q Querier) ([]model.Client, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+clientCols+` FROM clients ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []model.Client
	for rows.Next() {
		c, err := scanClient(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpdateClient writes the mutable settings of a client.
func UpdateClient(ctx context.Context, q Querier, c model.Client) error {
	res, err := q.ExecContext(ctx, `UPDATE clients SET scopes=?, callback_hosts=?, secret_ref_prefixes=?, repo_prefixes=?,
		container_allowed=?, root_allowed=?, ntfy_topic=?, updated_at=? WHERE id=?`,
		jsonList(c.Scopes), jsonList(c.CallbackHosts), jsonList(c.SecretRefPrefixes), jsonList(c.RepoPrefixes),
		boolInt(c.ContainerAllowed), boolInt(c.RootAllowed), nullStr(c.NtfyTopic), ms(c.UpdatedAt), c.ID)
	return affected(res, err)
}

// RotateClient installs a new token hash, keeping the old one valid until
// graceUntil (nil = immediately invalid).
func RotateClient(ctx context.Context, q Querier, id string, newHash []byte, graceUntil *time.Time, now time.Time) error {
	res, err := q.ExecContext(ctx, `UPDATE clients SET prev_token_hash = CASE WHEN ? IS NULL THEN NULL ELSE token_hash END,
		prev_valid_until = ?, token_hash = ?, updated_at = ? WHERE id = ?`,
		msPtr(graceUntil), msPtr(graceUntil), newHash, ms(now), id)
	return affected(res, err)
}

// DeleteClient removes a client that owns no jobs.
func DeleteClient(ctx context.Context, q Querier, id string) error {
	res, err := q.ExecContext(ctx, `DELETE FROM clients WHERE id = ?`, id)
	if err != nil && strings.Contains(err.Error(), "FOREIGN KEY") {
		return ErrHasJobs
	}
	return affected(res, err)
}

func affected(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
