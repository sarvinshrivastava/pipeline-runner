package store

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

// maxCachedStmts bounds the cache. Every cached text comes from code, so the
// cap is only a guard against SQL built from data; past it calls run uncached.
const maxCachedStmts = 512

// DB is a *sql.DB whose Exec/Query/QueryRow reuse prepared statements, keyed
// by exact SQL text. Each DB owns its cache: database/sql refuses a Stmt from
// another *sql.DB inside a transaction.
type DB struct {
	*sql.DB
	c *stmtCache
}

type stmtCache struct {
	mu       sync.RWMutex
	m        map[string]*sql.Stmt
	prepares atomic.Int64
	begins   atomic.Int64
	full     atomic.Bool
	log      *slog.Logger

	// Test hooks; nil in production.
	queryHook  atomic.Pointer[func(query string)]
	faultHook  atomic.Pointer[func(query string) error]
	commitHook atomic.Pointer[func() error]
}

func newDB(db *sql.DB, log *slog.Logger) *DB {
	return &DB{DB: db, c: &stmtCache{m: map[string]*sql.Stmt{}, log: log}}
}

// before runs the test hooks for a statement about to run. A non-nil error is
// returned in place of running it.
func (c *stmtCache) before(query string) error {
	if h := c.queryHook.Load(); h != nil {
		(*h)(query)
	}
	if h := c.faultHook.Load(); h != nil {
		return (*h)(query)
	}
	return nil
}

func (c *stmtCache) cached(query string) *sql.Stmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[query]
}

// stmt returns the cached statement for query, preparing it on a miss. It is
// never called inside a transaction. nil means "run uncached" (cap reached or
// the prepare failed, in which case the caller's plain call reports the error).
func (c *stmtCache) stmt(ctx context.Context, db *sql.DB, query string) *sql.Stmt {
	if s := c.cached(query); s != nil {
		return s
	}
	if c.atCap() {
		return nil
	}
	p, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil
	}
	c.prepares.Add(1)
	c.mu.Lock()
	if s := c.m[query]; s != nil { // a racing caller won
		c.mu.Unlock()
		_ = p.Close()
		return s
	}
	if len(c.m) >= maxCachedStmts {
		c.mu.Unlock()
		_ = p.Close()
		c.warnFull()
		return nil
	}
	c.m[query] = p
	c.mu.Unlock()
	return p
}

func (c *stmtCache) atCap() bool {
	c.mu.RLock()
	n := len(c.m)
	c.mu.RUnlock()
	if n < maxCachedStmts {
		return false
	}
	c.warnFull()
	return true
}

func (c *stmtCache) warnFull() {
	if c.full.CompareAndSwap(false, true) {
		c.log.Warn("prepared statement cache is full; further SQL runs unprepared", "cap", maxCachedStmts)
	}
}

// ExecContext runs query through the cached statement.
func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := d.c.before(query); err != nil {
		return nil, err
	}
	if s := d.c.stmt(ctx, d.DB, query); s != nil {
		return s.ExecContext(ctx, args...)
	}
	return d.DB.ExecContext(ctx, query, args...)
}

// QueryContext runs query through the cached statement.
func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := d.c.before(query); err != nil {
		return nil, err
	}
	if s := d.c.stmt(ctx, d.DB, query); s != nil {
		return s.QueryContext(ctx, args...)
	}
	return d.DB.QueryContext(ctx, query, args...)
}

// QueryRowContext runs query through the cached statement.
func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := d.c.before(query); err != nil {
		return faultRow(d.DB.QueryRowContext, query)
	}
	if s := d.c.stmt(ctx, d.DB, query); s != nil {
		return s.QueryRowContext(ctx, args...)
	}
	return d.DB.QueryRowContext(ctx, query, args...)
}

// faultRow yields a *sql.Row whose Scan fails without touching SQLite. A Row
// can't carry an arbitrary error, so a fault-hooked QueryRow reports
// context.Canceled rather than the hook's error.
func faultRow(queryRow func(ctx context.Context, query string, args ...any) *sql.Row, query string) *sql.Row {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return queryRow(ctx, query)
}

// BeginTx starts a transaction whose statements reuse this DB's cache.
func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	d.c.begins.Add(1)
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, db: d}, nil
}

// Close closes the cached statements, then the DB.
func (d *DB) Close() error {
	d.c.mu.Lock()
	var errs []error
	for q, s := range d.c.m {
		errs = append(errs, s.Close())
		delete(d.c.m, q)
	}
	d.c.mu.Unlock()
	return errors.Join(append(errs, d.DB.Close())...)
}

// Prepares counts statements prepared into the cache (tests).
func (d *DB) Prepares() int64 { return d.c.prepares.Load() }

// CachedLen is the number of cached statements (tests).
func (d *DB) CachedLen() int {
	d.c.mu.RLock()
	defer d.c.mu.RUnlock()
	return len(d.c.m)
}

// Begins counts transactions begun on this DB (tests).
func (d *DB) Begins() int64 { return d.c.begins.Load() }

// SetQueryHook calls f with the text of every statement run through this DB
// or its transactions (tests). nil removes it.
func (d *DB) SetQueryHook(f func(query string)) {
	if f == nil {
		d.c.queryHook.Store(nil)
		return
	}
	d.c.queryHook.Store(&f)
}

// SetFaultHook returns f's non-nil error instead of running a statement, on
// the DB and transaction paths (tests). nil removes it.
func (d *DB) SetFaultHook(f func(query string) error) {
	if f == nil {
		d.c.faultHook.Store(nil)
		return
	}
	d.c.faultHook.Store(&f)
}

// SetCommitHook makes Commit roll back and return f's non-nil error (tests).
// nil removes it.
func (d *DB) SetCommitHook(f func() error) {
	if f == nil {
		d.c.commitHook.Store(nil)
		return
	}
	d.c.commitHook.Store(&f)
}

// Tx is a write transaction that runs statements through its DB's cache.
//
// It never prepares on the DB while open: on the single-connection writer
// that would wait for the connection this transaction holds. A text that is
// not cached yet runs unprepared and is prepared once the transaction ends.
type Tx struct {
	*sql.Tx
	db     *DB
	missed []string
}

func (t *Tx) stmt(ctx context.Context, query string) *sql.Stmt {
	if p := t.db.c.cached(query); p != nil {
		return t.StmtContext(ctx, p)
	}
	for _, q := range t.missed {
		if q == query {
			return nil
		}
	}
	t.missed = append(t.missed, query)
	return nil
}

// ExecContext runs query in the transaction.
func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := t.db.c.before(query); err != nil {
		return nil, err
	}
	if s := t.stmt(ctx, query); s != nil {
		return s.ExecContext(ctx, args...)
	}
	return t.Tx.ExecContext(ctx, query, args...)
}

// QueryContext runs query in the transaction.
func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := t.db.c.before(query); err != nil {
		return nil, err
	}
	if s := t.stmt(ctx, query); s != nil {
		return s.QueryContext(ctx, args...)
	}
	return t.Tx.QueryContext(ctx, query, args...)
}

// QueryRowContext runs query in the transaction.
func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := t.db.c.before(query); err != nil {
		return faultRow(t.Tx.QueryRowContext, query)
	}
	if s := t.stmt(ctx, query); s != nil {
		return s.QueryRowContext(ctx, args...)
	}
	return t.Tx.QueryRowContext(ctx, query, args...)
}

// Commit commits, then caches the texts this transaction ran unprepared.
func (t *Tx) Commit() error {
	if h := t.db.c.commitHook.Load(); h != nil {
		if err := (*h)(); err != nil {
			_ = t.Tx.Rollback()
			t.prepareMissed()
			return err
		}
	}
	err := t.Tx.Commit()
	t.prepareMissed()
	return err
}

// Rollback rolls back, then caches the texts this transaction ran unprepared.
func (t *Tx) Rollback() error {
	err := t.Tx.Rollback()
	t.prepareMissed()
	return err
}

func (t *Tx) prepareMissed() {
	missed := t.missed
	t.missed = nil
	for _, q := range missed {
		t.db.c.stmt(context.Background(), t.db.DB, q)
	}
}

// Uncached runs every statement through q without the cache, for SQL whose
// text varies with data (an IN list sized by input). Test hooks still apply.
func Uncached(q Querier) Querier {
	switch v := q.(type) {
	case *DB:
		return uncached{raw: v.DB, c: v.c}
	case *Tx:
		return uncached{raw: v.Tx, c: v.db.c}
	}
	return q
}

type uncached struct {
	raw Querier
	c   *stmtCache
}

func (u uncached) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := u.c.before(query); err != nil {
		return nil, err
	}
	return u.raw.ExecContext(ctx, query, args...)
}

func (u uncached) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := u.c.before(query); err != nil {
		return nil, err
	}
	return u.raw.QueryContext(ctx, query, args...)
}

func (u uncached) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := u.c.before(query); err != nil {
		return faultRow(u.raw.QueryRowContext, query)
	}
	return u.raw.QueryRowContext(ctx, query, args...)
}
