package engine

import (
	"context"
	"os"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

const retentionBatch = 500

// retentionLoop prunes at boot and daily at 03:30 UTC. It runs in its own
// goroutine with small batches, so the single writer is never held long and
// the loop's claims keep their latency.
func (e *Engine) retentionLoop() {
	for {
		if err := e.RunRetention(e.life); err != nil && e.life.Err() == nil {
			e.log.Error("retention", "err", err)
		}
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), 3, 30, 0, 0, time.UTC)
		if !next.After(now) {
			next = next.AddDate(0, 0, 1)
		}
		select {
		case <-e.life.Done():
			return
		case <-time.After(next.Sub(now)):
		}
	}
}

// RunRetention deletes old runs, their logs, stale idempotency records and
// unreferenced revisions.
func (e *Engine) RunRetention(ctx context.Context) error {
	now := e.now()
	jobs, err := store.AllJobs(ctx, e.st.R)
	if err != nil {
		return err
	}
	var fast []string
	for _, j := range jobs {
		if g := j.MinGap(now); g > 0 && g < time.Minute {
			fast = append(fast, j.ID)
		}
	}
	if err := e.pruneRuns(ctx, now.AddDate(0, 0, -7), fast); err != nil {
		return err
	}
	if err := e.pruneRuns(ctx, now.AddDate(0, 0, -max(e.d.RetentionDays, 1)), nil); err != nil {
		return err
	}
	if _, err := store.IdemPrune(ctx, e.st.W, now.Add(-24*time.Hour)); err != nil {
		return err
	}
	_, err = store.PruneRevisions(ctx, e.st.W)
	return err
}

func (e *Engine) pruneRuns(ctx context.Context, before time.Time, jobIDs []string) error {
	for {
		ids, err := store.DeleteFinishedBefore(ctx, e.st.W, before, jobIDs, retentionBatch)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := os.Remove(e.logPath(id)); err != nil && !os.IsNotExist(err) {
				e.log.Warn("remove run log", "run", id, "err", err)
			}
		}
		if len(ids) < retentionBatch {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
