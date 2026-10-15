// Package engine fires runs at their instants and drives them to a terminal
// status.
//
// Concurrency model (plan §6.0): the loop goroutine is the only code that
// touches the heap, gate counts, pool counters, wait lists, awaiters, lease
// timers and the job cache. Every other goroutine talks to it through
// e.events. The loop never sends on e.events itself, reply channels have
// capacity 1 and are written without blocking, and every run status change
// goes through store.Transition so a stale actor changes nothing.
package engine

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/metrics"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/notify"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// Errors returned to the API layer.
var (
	ErrBusy            = errors.New("engine busy")
	ErrNotFound        = errors.New("not found")
	ErrJobDisabled     = errors.New("job is disabled")
	ErrJobDone         = errors.New("job is done")
	ErrRunTerminal     = errors.New("run is already terminal")
	ErrNotWaiting      = errors.New("run is not waiting")
	ErrNotAsync        = errors.New("run is not an async http run")
	ErrConflict        = errors.New("a different completion was already recorded")
	ErrJobLimit        = errors.New("client job limit reached")
	ErrVaultDown       = errors.New("secrets manager unavailable")
	ErrNameTaken       = errors.New("a job with this name already exists")
	ErrShuttingDown    = errors.New("shutting down")
	errStaleTransition = errors.New("stale transition")
)

// MaxJobsPerClient bounds metric cardinality and runaway clients.
const MaxJobsPerClient = 200

// Pools.
const (
	poolHTTP      = "http"
	poolContainer = "container"
)

// SecretSource resolves and checks vault references.
type SecretSource interface {
	Get(ctx context.Context, ref vault.Ref) (string, error)
	Exists(ctx context.Context, ref vault.Ref) (bool, error)
}

// Pager sends notifications.
type Pager interface{ Send(notify.Msg) }

// ContainerRuntime is nil when container targets are disabled (1a).
type ContainerRuntime interface {
	Reattach(ctx context.Context, run model.Run, spec model.RunSpec) (target.Result, error)
	Ping(ctx context.Context) error
}

// Deps are the engine's collaborators.
type Deps struct {
	Store             *store.Store
	Clock             Clock
	Executors         map[string]target.Executor // by target type
	Secrets           SecretSource
	Pager             Pager
	Metrics           *metrics.Metrics
	Log               *slog.Logger
	RunTokenKey       []byte
	EnvKey            []byte
	MaxHTTPInflight   int
	MaxHTTPPerClient  int // one client's share of MaxHTTPInflight; 0 is no cap
	MaxContainers     int
	DataDir           string
	RetentionDays     int
	ContainersEnabled bool
	Containers        ContainerRuntime
	Rand              func(n int64) int64 // jitter source; defaults to math/rand
	// CallbackPolicy is the dial-time IP policy; the validate checks apply
	// it to the resolved callback host. nil skips that check.
	CallbackPolicy secure.IPPolicy
}

type held struct {
	jobID string
	ns    string
	pool  string
	// client is set only together with pool == poolHTTP: only an HTTP slot
	// counts against the client's share.
	client string
	// run is the row as claimed, authoritative only while run.Status is
	// running (the loop is the only writer of running rows). It is zero for
	// waiting runs and for entries rebuilt without a row.
	run model.Run
}

// Row cache bounds (see Engine.rows).
const (
	rowHorizon    = 5 * time.Second // only rows due this soon are cached at push
	rowMaxPayload = 8 << 10         // payload (and ad hoc spec) size limit
	rowCap        = 2048            // entries; ~32 MiB worst case
)

type jobEntry struct {
	job    model.Job
	minGap time.Duration
}

// pageCount accumulates a job's missed and coalesced occurrences between
// pages (one page per job per missPageEvery).
type pageCount struct {
	missed, coalesced, expired int
	capped                     bool // expired hit maxExpiredCount: "at least"
}

type unflushed struct {
	op      func(ctx context.Context) error
	after   func() // the post-commit work finalize would have done
	runID   string
	desc    string
	nextTry time.Time
	backoff time.Duration
}

// Engine is the firing engine.
type Engine struct {
	d   Deps
	st  *store.Store
	log *slog.Logger

	events   chan event
	life     context.Context
	stopLife context.CancelFunc
	stopping chan struct{} // closed when Stop begins; releases ?wait callers
	stopOnce sync.Once
	loopAt   atomic.Int64
	done     chan struct{}

	// Loop-owned state.
	heap   fireHeap
	inHeap map[string]int
	// rows caches pending runs' rows so dispatch and wake read nothing.
	// Set only by pushRun (when due within rowHorizon) and park (any due
	// time); both skip payloads or ad hoc specs over rowMaxPayload and stop
	// at rowCap. pushAt and pop/unpark never touch it. Deleted at dispatch
	// exit (except park and transient re-push), on onJobChanged dropped, on
	// cancelRun of a pending run, and by reconcile for runs no longer
	// pending. A stale row only loses its guarded claim.
	rows            map[string]model.Run
	parked          map[string]string   // runID → wait list name
	waits           map[string][]string // wait list name → run ids by (rank, seq)
	meta            map[string]parkMeta // runID → its rank inputs; same keys as parked
	parkSeq         uint64
	clientBusy      map[string]int // client → HTTP slots held
	held            map[string]held
	jobBusy         map[string]int
	keyBusy         map[string]int
	inflight        map[string]int
	jobs            map[string]*jobEntry
	revs            map[revKey]model.RunSpec
	awaiters        map[string][]chan model.Run
	leaseGen        map[string]uint64
	leaseSeq        uint64 // never reused, so a stale expiry can't match a re-arm
	leaseTimers     map[string]Timer
	cancelFns       map[string]context.CancelFunc
	cancelRequested map[string]bool
	unflushed       []*unflushed
	pageBatch       map[string]*pageCount
	lastMissPage    map[string]time.Time
	draining        bool
	drainBy         time.Time
	drainDone       chan struct{}
	lastReconcile   time.Time
	accepting       atomic.Bool
	unflushedCount  atomic.Int64
}

type revKey struct {
	jobID string
	rev   int
}

// New builds an engine; call Start to recover and begin firing.
func New(d Deps) *Engine {
	if d.Clock == nil {
		d.Clock = RealClock{}
	}
	if d.Rand == nil {
		d.Rand = func(n int64) int64 { return rand.Int64N(n) } //nolint:gosec // jitter, not security
	}
	life, stop := context.WithCancel(context.Background())
	e := &Engine{
		d: d, st: d.Store, log: d.Log,
		events: make(chan event, 1024), life: life, stopLife: stop, done: make(chan struct{}), stopping: make(chan struct{}),
		inHeap: map[string]int{}, rows: map[string]model.Run{}, parked: map[string]string{}, waits: map[string][]string{},
		meta: map[string]parkMeta{}, clientBusy: map[string]int{},
		held: map[string]held{}, jobBusy: map[string]int{}, keyBusy: map[string]int{}, inflight: map[string]int{},
		jobs: map[string]*jobEntry{}, revs: map[revKey]model.RunSpec{}, awaiters: map[string][]chan model.Run{},
		leaseGen: map[string]uint64{}, leaseTimers: map[string]Timer{}, cancelFns: map[string]context.CancelFunc{},
		cancelRequested: map[string]bool{}, pageBatch: map[string]*pageCount{}, lastMissPage: map[string]time.Time{},
	}
	e.accepting.Store(true)
	return e
}

// LoopAge is the time since the loop last completed an iteration.
func (e *Engine) LoopAge() time.Duration {
	at := e.loopAt.Load()
	if at == 0 {
		return 0
	}
	return e.d.Clock.Now().Sub(time.UnixMilli(at))
}

// Healthy reports whether the loop is alive (completed an iteration in 60 s).
func (e *Engine) Healthy() bool { return e.LoopAge() < 60*time.Second }

// Stop drains: no new dispatch, in-flight HTTP runs get until drainBy, then
// results are ignored and boot recovery owns any run still marked running.
func (e *Engine) Stop(ctx context.Context, drainBy time.Time) {
	e.stopOnce.Do(func() { close(e.stopping) })
	done := make(chan struct{})
	if err := e.send(ctx, evShutdown{drainBy: drainBy, done: done}); err == nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	e.accepting.Store(false)
	e.stopLife()
	<-e.done
}

// send delivers an event from a non-loop goroutine.
func (e *Engine) send(ctx context.Context, ev event) error {
	select {
	case e.events <- ev:
		return nil
	case <-ctx.Done():
		return ErrBusy
	case <-e.life.Done():
		return ErrShuttingDown
	}
}

// sendFromHandler bounds the wait so a saturated loop yields a 503.
func (e *Engine) sendFromHandler(ctx context.Context, ev event) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.send(ctx, ev)
}

func (e *Engine) logPath(runID string) string {
	return filepath.Join(e.d.DataDir, "v2", "logs", runID+".log")
}

// LogPath returns where a run's log lives.
func (e *Engine) LogPath(runID string) string { return e.logPath(runID) }

func (e *Engine) now() time.Time { return e.d.Clock.Now() }

func poolOf(targetType string) string {
	if targetType == model.TargetHTTP {
		return poolHTTP
	}
	return poolContainer
}

func (e *Engine) poolCap(pool string) int {
	if pool == poolHTTP {
		return e.d.MaxHTTPInflight
	}
	return e.d.MaxContainers
}

func nsKeyOf(r model.Run, spec model.RunSpec) string {
	return model.NSKey(r.ClientID, spec.Policy.ConcurrencyKey)
}
