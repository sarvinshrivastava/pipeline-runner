package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

var (
	bg   = context.Background()
	t0   = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	spec = model.RunSpec{Target: model.Target{Type: model.TargetHTTP, HTTP: &model.HTTPTarget{Method: "POST", URL: "http://x/", Mode: "sync"}}}
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(bg, filepath.Join(t.TempDir(), "v2", "plr.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func seed(t *testing.T, s *Store) (model.Client, model.Job) {
	t.Helper()
	c := model.Client{ID: uuid.NewString(), Name: "reel-drop", TokenHash: []byte("h1"), Scopes: []string{"jobs:read"}, CreatedAt: t0, UpdatedAt: t0}
	if err := CreateClient(bg, s.W, c); err != nil {
		t.Fatal(err)
	}
	j := model.Job{ID: uuid.NewString(), ClientID: c.ID, Name: "tick", State: model.JobActive, Rev: 1,
		Trigger: model.Trigger{Type: model.TriggerCron, Cron: "* * * * *", Timezone: "UTC"}, Target: spec.Target, Policy: spec.Policy,
		CreatedAt: t0, UpdatedAt: t0}
	if err := CreateJob(bg, s.W, j); err != nil {
		t.Fatal(err)
	}
	return c, j
}

func schedRun(j model.Job, nominal time.Time) model.Run {
	id := uuid.NewString()
	return model.Run{ID: id, JobID: j.ID, JobName: j.Name, ClientID: j.ClientID, Kind: model.KindSchedule, Attempt: 1,
		ChainRootID: id, Status: model.StatusPending, NominalFor: &nominal, ScheduledFor: nominal, OriginFor: nominal,
		ContextFor: nominal, LatenessRef: nominal, Rev: 1, CreatedAt: t0}
}

func TestMigrateIdempotentAndNewerRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plr.db")
	s, err := Open(bg, path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s, err = Open(bg, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.W.ExecContext(bg, "PRAGMA user_version = 99"); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := Open(bg, path); !errors.Is(err, ErrNewerSchema) {
		t.Fatalf("err = %v", err)
	}
}

func TestOnePendingScheduleRun(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	ok, err := InsertScheduleRun(bg, s.W, schedRun(j, t0))
	if err != nil || !ok {
		t.Fatalf("first insert: %v %v", ok, err)
	}
	ok, err = InsertScheduleRun(bg, s.W, schedRun(j, t0.Add(time.Minute)))
	if err != nil || ok {
		t.Fatalf("second pending must be absorbed: %v %v", ok, err)
	}
}

func TestScheduleOnceExcludesCancelled(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	r := schedRun(j, t0)
	if _, err := InsertScheduleRun(bg, s.W, r); err != nil {
		t.Fatal(err)
	}
	// Same nominal while the first is non-cancelled → hard error, not absorbed.
	if ok, _ := Transition(bg, s.W, r.ID, []model.Status{model.StatusPending}, model.StatusRunning, nil); !ok {
		t.Fatal("claim failed")
	}
	if _, err := InsertScheduleRun(bg, s.W, schedRun(j, t0)); !IsUniqueViolation(err) {
		t.Fatalf("duplicate nominal err = %v", err)
	}
	if ok, _ := Transition(bg, s.W, r.ID, []model.Status{model.StatusRunning}, model.StatusCancelled, nil); !ok {
		t.Fatal("cancel failed")
	}
	if ok, err := InsertScheduleRun(bg, s.W, schedRun(j, t0)); err != nil || !ok {
		t.Fatalf("re-insert after cancel: %v %v", ok, err)
	}
}

func TestTransitionGuard(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	r := schedRun(j, t0)
	if err := InsertRun(bg, s.W, r); err != nil {
		t.Fatal(err)
	}
	ok, err := Transition(bg, s.W, r.ID, []model.Status{model.StatusRunning}, model.StatusSuccess, nil)
	if err != nil || ok {
		t.Fatalf("wrong from must not change: %v %v", ok, err)
	}
	ok, err = Transition(bg, s.W, r.ID, []model.Status{model.StatusPending}, model.StatusRunning, Fields{"started_at": t0, "late_ms": int64(12)})
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	got, _ := GetRun(bg, s.R, r.ID)
	if got.Status != model.StatusRunning || got.StartedAt == nil || *got.LateMs != 12 {
		t.Fatalf("run = %+v", got)
	}
	if _, err := Transition(bg, s.W, r.ID, []model.Status{model.StatusRunning}, model.StatusSuccess, Fields{"status": "x"}); err == nil {
		t.Fatal("status must not be settable as a field")
	}
}

func TestRevisionSurvivesJobDelete(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	if err := DeleteJob(bg, s.W, j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := JobRevision(bg, s.R, j.ID, 1); err != nil {
		t.Fatalf("revision gone: %v", err)
	}
}

func TestRotationGraceAndClientDeleteRestrict(t *testing.T) {
	s := open(t)
	c, _ := seed(t, s)
	until := t0.Add(time.Hour)
	if err := RotateClient(bg, s.W, c.ID, []byte("h2"), &until, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientByTokenHash(bg, s.R, []byte("h1"), t0.Add(time.Minute)); err != nil {
		t.Fatalf("old token inside grace: %v", err)
	}
	if _, err := ClientByTokenHash(bg, s.R, []byte("h1"), t0.Add(2*time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old token after grace: %v", err)
	}
	if err := RotateClient(bg, s.W, c.ID, []byte("h3"), nil, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientByTokenHash(bg, s.R, []byte("h2"), t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no-grace rotation kept old token: %v", err)
	}
	if err := DeleteClient(bg, s.W, c.ID); !errors.Is(err, ErrHasJobs) {
		t.Fatalf("delete with jobs: %v", err)
	}
}

func TestCursorPagination(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	for i := 0; i < 5; i++ {
		r := schedRun(j, t0.Add(time.Duration(i)*time.Minute))
		r.Kind = model.KindInvoke
		r.NominalFor = nil
		r.ID, _ = newV7()
		r.ChainRootID = r.ID
		if err := InsertRun(bg, s.W, r); err != nil {
			t.Fatal(err)
		}
	}
	page1, _ := ListRuns(bg, s.R, RunFilter{Limit: 3})
	page2, _ := ListRuns(bg, s.R, RunFilter{Limit: 3, Cursor: page1[2].ID})
	if len(page1) != 3 || len(page2) != 2 || page2[0].ID >= page1[2].ID {
		t.Fatalf("pages %d/%d", len(page1), len(page2))
	}
}

func TestIdempotency(t *testing.T) {
	s := open(t)
	row, err := IdemBegin(bg, s.W, "c", "POST /v1/jobs", "k", []byte("h"), t0)
	if err != nil || row != nil {
		t.Fatalf("first claim: %+v %v", row, err)
	}
	row, _ = IdemBegin(bg, s.W, "c", "POST /v1/jobs", "k", []byte("h"), t0)
	if row == nil || row.Done {
		t.Fatalf("second must see in_progress: %+v", row)
	}
	if err := IdemFinish(bg, s.W, "c", "POST /v1/jobs", "k", 201, []byte(`{"id":"x"}`)); err != nil {
		t.Fatal(err)
	}
	row, _ = IdemBegin(bg, s.W, "c", "POST /v1/jobs", "k", []byte("h"), t0)
	if row == nil || !row.Done || row.Status != 201 {
		t.Fatalf("replay: %+v", row)
	}
	// Other client, same key: independent.
	if row, _ := IdemBegin(bg, s.W, "c2", "POST /v1/jobs", "k", []byte("h"), t0); row != nil {
		t.Fatal("keys must be scoped by client")
	}
	if err := IdemClearInProgress(bg, s.W); err != nil {
		t.Fatal(err)
	}
	if row, _ := IdemBegin(bg, s.W, "c2", "POST /v1/jobs", "k", []byte("h"), t0); row != nil {
		t.Fatal("in_progress must be cleared at boot")
	}
}

func TestConcurrentWritersSerialise(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- s.Tx(bg, func(tx *Tx) error {
				r := schedRun(j, t0)
				r.Kind, r.NominalFor = model.KindInvoke, nil
				r.ID = fmt.Sprintf("run-%02d", i)
				r.ChainRootID = r.ID
				return InsertRun(bg, tx, r)
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeleteFinishedBefore(t *testing.T) {
	s := open(t)
	_, j := seed(t, s)
	r := schedRun(j, t0)
	fin := t0
	r.Status, r.FinishedAt = model.StatusSuccess, &fin
	if err := InsertRun(bg, s.W, r); err != nil {
		t.Fatal(err)
	}
	ids, err := DeleteFinishedBefore(bg, s.W, t0.Add(time.Hour), nil, 500)
	if err != nil || len(ids) != 1 {
		t.Fatalf("deleted %v %v", ids, err)
	}
}

func newV7() (string, error) {
	id, err := uuid.NewV7()
	return id.String(), err
}
