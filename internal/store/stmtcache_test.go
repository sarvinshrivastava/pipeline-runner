package store

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

func TestStmtCacheSameSQLPreparesOnce(t *testing.T) {
	s := open(t)
	before := s.R.Prepares()
	for i := 0; i < 100; i++ {
		var n int
		if err := s.R.QueryRowContext(bg, `SELECT count(*) FROM runs WHERE status = ?`, "pending").Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.R.Prepares() - before; got != 1 {
		t.Fatalf("prepares = %d, want 1", got)
	}
}

func TestTransitionTextIsDeterministic(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	r := schedRun(j, t0)
	if err := InsertRun(bg, s.W, r); err != nil {
		t.Fatal(err)
	}
	before := s.W.CachedLen()
	cols := []string{"started_at", "late_ms", "deadline_at", "error", "http_status", "exit_code", "container_name"}
	for i := 0; i < 20; i++ {
		f := Fields{}
		for k := range cols { // a different insertion order each time
			c := cols[(k+i)%len(cols)]
			switch c {
			case "started_at", "deadline_at":
				f[c] = t0
			case "error", "container_name":
				f[c] = "x"
			default:
				f[c] = i
			}
		}
		ok, err := Transition(bg, s.W, r.ID, []model.Status{model.StatusPending}, model.StatusPending, f)
		if err != nil || !ok {
			t.Fatalf("transition %d: %v %v", i, ok, err)
		}
	}
	if got := s.W.CachedLen() - before; got != 1 {
		t.Fatalf("cache entries added = %d, want 1", got)
	}
}

func TestStmtCacheCapRunsUncached(t *testing.T) {
	s := open(t)
	var logs bytes.Buffer
	s.R.c.log = slog.New(slog.NewTextHandler(&logs, nil))
	for i := 0; s.R.CachedLen() < maxCachedStmts; i++ {
		var n int
		if err := s.R.QueryRowContext(bg, fmt.Sprintf(`SELECT %d`, i)).Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		var n int
		if err := s.R.QueryRowContext(bg, `SELECT ? + 1000000`, i).Scan(&n); err != nil || n != i+1000000 {
			t.Fatalf("past cap: n=%d err=%v", n, err)
		}
		rows, err := s.R.QueryContext(bg, fmt.Sprintf(`SELECT %d + 2000000`, i))
		if err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	}
	if got := s.R.CachedLen(); got != maxCachedStmts {
		t.Fatalf("cached = %d, want %d", got, maxCachedStmts)
	}
	if n := strings.Count(logs.String(), "cache is full"); n != 1 {
		t.Fatalf("full warnings = %d, want 1:\n%s", n, logs.String())
	}
}

func TestTxReusesCachedParent(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	insert := func(id string) error {
		return s.Tx(bg, func(tx *Tx) error {
			r := schedRun(j, t0)
			r.Kind, r.NominalFor, r.ID, r.ChainRootID = model.KindInvoke, nil, id, id
			return InsertRun(bg, tx, r)
		})
	}
	if err := insert("warm"); err != nil { // miss: prepared after commit
		t.Fatal(err)
	}
	prepares := s.W.Prepares()
	for _, id := range []string{"a", "b"} {
		if err := insert(id); err != nil {
			t.Fatalf("tx %s: %v", id, err)
		}
	}
	if got := s.W.Prepares(); got != prepares {
		t.Fatalf("prepares grew %d → %d: the cached parent was not reused", prepares, got)
	}
	for _, id := range []string{"warm", "a", "b"} {
		if _, err := GetRun(bg, s.R, id); err != nil {
			t.Fatalf("run %s: %v", id, err)
		}
	}
	errRollback := fmt.Errorf("roll back")
	err := s.Tx(bg, func(tx *Tx) error {
		r := schedRun(j, t0)
		r.Kind, r.NominalFor, r.ID, r.ChainRootID = model.KindInvoke, nil, "rolled", "rolled"
		if err := InsertRun(bg, tx, r); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatalf("err = %v", err)
	}
	if _, err := GetRun(bg, s.R, "rolled"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back row: %v", err)
	}
}

func TestUncachedLeavesCacheAlone(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	before := s.W.CachedLen()
	if _, err := DeleteFinishedBefore(bg, s.W, t0, []string{j.ID, "other"}, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := Uncached(s.W).ExecContext(bg, `UPDATE jobs SET name = name WHERE id = ?`, j.ID); err != nil {
		t.Fatal(err)
	}
	err := s.Tx(bg, func(tx *Tx) error {
		_, err := Uncached(tx).ExecContext(bg, `UPDATE jobs SET name = name WHERE id = ? AND 1 = 1`, j.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.W.CachedLen(); got != before {
		t.Fatalf("cached %d → %d", before, got)
	}
}

func TestStmtCacheConcurrentReaders(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	r := schedRun(j, t0)
	if err := InsertRun(bg, s.W, r); err != nil {
		t.Fatal(err)
	}
	before := s.R.CachedLen()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for g := 0; g < 8; g++ {
		wg.Go(func() {
			for i := 0; i < 1000; i++ {
				got, err := GetRun(bg, s.R, r.ID)
				if err != nil || got.ID != r.ID {
					errs <- fmt.Errorf("get: %w", err)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := s.R.CachedLen() - before; got != 1 {
		t.Fatalf("cache entries added = %d, want 1", got)
	}
}

func TestPrepareErrorMatchesUncached(t *testing.T) {
	s := open(t)
	const bad = `SELECT * FROM no_such_table`
	before := s.W.CachedLen()
	_, want := s.W.DB.ExecContext(bg, bad)
	_, got := s.W.ExecContext(bg, bad)
	if want == nil || got == nil || got.Error() != want.Error() {
		t.Fatalf("got %v, want %v", got, want)
	}
	var n int
	want = s.R.DB.QueryRowContext(bg, bad).Scan(&n)
	got = s.R.QueryRowContext(bg, bad).Scan(&n)
	if want == nil || got == nil || got.Error() != want.Error() {
		t.Fatalf("row: got %v, want %v", got, want)
	}
	if s.W.CachedLen() != before {
		t.Fatal("a failed prepare must not be cached")
	}
}

func TestFirstUseInsideTxDoesNotDeadlock(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	const q = `UPDATE jobs SET updated_at = ? WHERE id = ?`
	run := func() error {
		done := make(chan error, 1)
		go func() {
			done <- s.Tx(bg, func(tx *Tx) error {
				_, err := tx.ExecContext(bg, q, ms(t0), j.ID)
				return err
			})
		}()
		select {
		case err := <-done:
			return err
		case <-time.After(time.Second):
			return fmt.Errorf("tx did not finish within 1 s (deadlock)")
		}
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if s.W.c.cached(q) == nil {
		t.Fatal("a text missed inside a tx must be cached after commit")
	}
	prepares := s.W.Prepares()
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if s.W.Prepares() != prepares {
		t.Fatal("second tx use must be a cache hit")
	}
}

func TestCachesArePerDB(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	const wq = `SELECT name FROM jobs WHERE id = ?`
	const rq = `SELECT state FROM jobs WHERE id = ?`
	var v string
	if err := s.W.QueryRowContext(bg, wq, j.ID).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if err := s.R.QueryRowContext(bg, rq, j.ID).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if s.W.c.cached(rq) != nil {
		t.Fatal("a statement cached for R leaked into W's cache")
	}
	err := s.Tx(bg, func(tx *Tx) error {
		if err := tx.QueryRowContext(bg, wq, j.ID).Scan(&v); err != nil { // W-cached
			return fmt.Errorf("w-cached: %w", err)
		}
		if err := tx.QueryRowContext(bg, rq, j.ID).Scan(&v); err != nil { // R-only: a W miss
			return fmt.Errorf("r-cached: %w", err)
		}
		if len(tx.missed) != 1 || tx.missed[0] != rq {
			return fmt.Errorf("missed = %v, want only the R text", tx.missed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.W.c.cached(rq) == nil || s.W.c.cached(rq) == s.R.c.cached(rq) {
		t.Fatal("W must prepare its own statement after the tx")
	}
}

func TestFaultAndCommitHooks(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	boom := fmt.Errorf("boom")
	var seen []string
	s.W.SetQueryHook(func(q string) { seen = append(seen, q) })
	s.W.SetFaultHook(func(q string) error {
		if strings.HasPrefix(q, "UPDATE jobs") {
			return boom
		}
		return nil
	})
	err := s.Tx(bg, func(tx *Tx) error {
		_, err := tx.ExecContext(bg, `UPDATE jobs SET name = name WHERE id = ?`, j.ID)
		return err
	})
	if !errors.Is(err, boom) {
		t.Fatalf("fault: %v", err)
	}
	s.W.SetFaultHook(nil)
	s.W.SetCommitHook(func() error { return boom })
	begins := s.W.Begins()
	err = s.Tx(bg, func(tx *Tx) error {
		_, err := tx.ExecContext(bg, `UPDATE jobs SET name = 'renamed' WHERE id = ?`, j.ID)
		return err
	})
	if !errors.Is(err, boom) || s.W.Begins() != begins+1 {
		t.Fatalf("commit hook: %v, begins %d → %d", err, begins, s.W.Begins())
	}
	s.W.SetCommitHook(nil)
	s.W.SetQueryHook(nil)
	if got, _ := GetJob(bg, s.R, j.ID); got.Name != j.Name {
		t.Fatal("a failed commit must leave nothing behind")
	}
	if len(seen) != 2 {
		t.Fatalf("query hook saw %v", seen)
	}
}
