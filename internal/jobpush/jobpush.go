// Package jobpush pings a job's Uptime Kuma push monitor after its runs.
//
// Fail-soft, like notify: a push never changes a run's outcome and is never
// retried beyond the job's next run. The engine loop only enqueues (an O(1)
// map write); client, vault and HTTP work happen on the single worker.
// Each job has at most one push waiting, and a newer push replaces it.
//
// The push URL is a secret: it is never logged or put in an error string.
package jobpush

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// Push is one ping request, built on the engine loop (no I/O there).
type Push struct {
	JobID, JobName, ClientID, RunID string
	Ref                             string // canonical vault ref
	Up                              bool
	Status                          string // "up", or the run status word for a down push
	PingMs                          int64  // run duration; <0 omitted (always omitted on down)
}

// ErrClientGone is what Clients returns when the owning client no longer exists.
var ErrClientGone = errors.New("owning client no longer exists")

// Results counted in plr_job_push_total.
const (
	ResultOK        = "ok"
	ResultFailed    = "failed"
	ResultRefused   = "refused"
	ResultReplaced  = "replaced"
	ResultCoalesced = "coalesced"
)

const (
	defaultTimeout = 5 * time.Second
	coalesceWindow = 20 * time.Second
	sentTTL        = time.Hour
	sweepEvery     = time.Minute
	drainMinLeft   = time.Second
)

// Config wires a Pusher.
type Config struct {
	Secrets interface {
		Get(context.Context, vault.Ref) (string, error)
	}
	Clients func(ctx context.Context, clientID string) (model.Principal, error)
	Hosts   []string
	HTTP    *http.Client  // secure.NewHTTPClient(3s, callbackPolicy)
	Timeout time.Duration // per push; default 5 s
	// Count records one result. Lock order: Pusher.mu is never held while
	// it runs, and metrics never call back into the pusher. Nil-safe.
	Count func(job, client, result string)
	Log   *slog.Logger
	Now   func() time.Time
}

// Pusher delivers pushes on one worker.
type Pusher struct {
	cfg Config

	mu        sync.Mutex
	pending   map[string]Push    // jobID → latest waiting push
	order     []string           // jobIDs in the arrival order of their waiting push
	sent      map[string]attempt // jobID → last attempted state and time
	lastSweep time.Time
	wake      chan struct{} // cap 1
}

type attempt struct {
	up bool
	at time.Time
}

// New returns a pusher; call Run to start delivering.
func New(cfg Config) *Pusher {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Pusher{
		cfg:     cfg,
		pending: map[string]Push{},
		sent:    map[string]attempt{},
		wake:    make(chan struct{}, 1),
	}
}

// Enqueue queues x, replacing any push still waiting for the same job. It
// never blocks and never drops for capacity: the backlog is bounded by the
// job count. Safe on the engine loop.
func (p *Pusher) Enqueue(x Push) {
	p.mu.Lock()
	_, replaced := p.pending[x.JobID]
	p.pending[x.JobID] = x
	if !replaced {
		p.order = append(p.order, x.JobID)
	}
	p.mu.Unlock()
	if replaced {
		p.count(x, ResultReplaced)
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Forget drops a deleted job's waiting push and attempt record. order is
// cleaned lazily: a pop skips IDs no longer pending.
func (p *Pusher) Forget(jobID string) {
	p.mu.Lock()
	delete(p.pending, jobID)
	delete(p.sent, jobID)
	p.mu.Unlock()
}

// Run is the single worker. It returns once ctx ends and any push in flight
// has finished (bounded by Timeout): a push already started is not aborted.
func (p *Pusher) Run(ctx context.Context) {
	for {
		p.mu.Lock()
		if ctx.Err() != nil {
			p.mu.Unlock()
			return
		}
		x, ok := p.popLocked()
		p.mu.Unlock()
		if !ok {
			select {
			case <-p.wake:
			case <-ctx.Done():
				return
			}
			continue
		}
		p.sendOne(context.WithoutCancel(ctx), x)
	}
}

// Drain delivers what is pending until ctx ends. Call it only after Run has
// returned, so there is one consumer at a time. It stops popping when ctx
// has less than a second left; each push is bounded by ctx too.
func (p *Pusher) Drain(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < drainMinLeft {
			return
		}
		p.mu.Lock()
		x, ok := p.popLocked()
		p.mu.Unlock()
		if !ok {
			return
		}
		p.sendOne(ctx, x)
	}
}

func (p *Pusher) popLocked() (Push, bool) {
	for len(p.order) > 0 {
		id := p.order[0]
		p.order = p.order[1:]
		if x, ok := p.pending[id]; ok {
			delete(p.pending, id)
			if len(p.order) == 0 {
				p.order = nil
			}
			return x, true
		}
	}
	p.order = nil
	return Push{}, false
}

// sendOne skips a push whose state matches the job's last attempted one
// within the coalescing window; a change of state is always sent.
func (p *Pusher) sendOne(ctx context.Context, x Push) {
	now := p.cfg.Now()
	p.mu.Lock()
	if now.Sub(p.lastSweep) >= sweepEvery {
		for id, a := range p.sent {
			if now.Sub(a.at) > sentTTL {
				delete(p.sent, id)
			}
		}
		p.lastSweep = now
	}
	if a, ok := p.sent[x.JobID]; ok && a.up == x.Up && now.Sub(a.at) < coalesceWindow {
		p.mu.Unlock()
		p.count(x, ResultCoalesced)
		return
	}
	// Recorded on attempt, whatever the result.
	p.sent[x.JobID] = attempt{up: x.Up, at: now}
	p.mu.Unlock()
	p.count(x, p.deliver(ctx, x))
}

func (p *Pusher) deliver(parent context.Context, x Push) string {
	ctx, cancel := context.WithTimeout(parent, p.cfg.Timeout)
	defer cancel()
	log := p.cfg.Log.With("job", x.JobName, "run", x.RunID, "ref", x.Ref)

	pr, err := p.cfg.Clients(ctx, x.ClientID)
	if errors.Is(err, ErrClientGone) {
		log.Warn("job push refused: owning client no longer exists", "result", ResultRefused)
		return ResultRefused
	}
	if err != nil {
		log.Warn("job push failed: client lookup", "result", ResultFailed, "err", err)
		return ResultFailed
	}
	if _, err := model.CheckRef(x.Ref, pr); err != nil {
		log.Warn("job push refused: ref no longer permitted", "result", ResultRefused, "err", err)
		return ResultRefused
	}
	ref, err := vault.ParseRef(x.Ref)
	if err != nil {
		log.Warn("job push refused: invalid ref", "result", ResultRefused)
		return ResultRefused
	}
	v, err := p.cfg.Secrets.Get(ctx, ref)
	if errors.Is(err, vault.ErrMissing) {
		log.Warn("job push refused: secret does not exist", "result", ResultRefused)
		return ResultRefused
	}
	if err != nil {
		log.Warn("job push failed: vault", "result", ResultFailed, "err", err)
		return ResultFailed
	}
	u, err := CheckURL(strings.TrimSpace(v), p.cfg.Hosts)
	if err != nil {
		log.Warn("job push refused", "result", ResultRefused, "rule", err.Error())
		return ResultRefused
	}
	q := url.Values{}
	if x.Up {
		q.Set("status", "up")
		q.Set("msg", "OK")
		if x.PingMs >= 0 {
			q.Set("ping", strconv.FormatInt(x.PingMs, 10))
		}
	} else {
		q.Set("status", "down")
		q.Set("msg", x.Status)
	}
	u.RawQuery = q.Encode()
	u.Fragment = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		log.Warn("push request build failed", "result", ResultFailed)
		return ResultFailed
	}
	resp, err := p.cfg.HTTP.Do(req)
	if err != nil {
		// *url.Error carries the URL; log only the cause.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		log.Warn("job push failed", "result", ResultFailed, "err", err)
		return ResultFailed
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Debug("job push sent", "result", ResultOK, "up", x.Up)
		return ResultOK
	}
	log.Warn("job push rejected", "result", ResultFailed, "status", resp.StatusCode, "msg", kumaMsg(body))
	return ResultFailed
}

// kumaMsg extracts Kuma's {"msg": …}, cut to 200 runes.
func kumaMsg(body []byte) string {
	var r struct {
		Msg string `json:"msg"`
	}
	if json.Unmarshal(body, &r) != nil {
		return ""
	}
	if rs := []rune(r.Msg); len(rs) > 200 {
		return string(rs[:200])
	}
	return r.Msg
}

func (p *Pusher) count(x Push, result string) {
	if p.cfg.Count != nil {
		p.cfg.Count(x.JobName, x.ClientID, result)
	}
}

// CheckURL errors. They are fixed strings and never echo the URL.
var (
	ErrURLTooLong    = errors.New("push URL is longer than 2048 bytes")
	ErrURLParse      = errors.New("push URL does not parse")
	ErrURLScheme     = errors.New("push URL must be https (http only for a single-label docker host)")
	ErrURLUserinfo   = errors.New("push URL must not embed credentials")
	ErrHostsEmpty    = errors.New("PUSH_URL_HOSTS is empty")
	ErrHostNotListed = errors.New("host is not in PUSH_URL_HOSTS")
	ErrURLPort       = errors.New("port not allowed for this host")
	ErrURLPath       = errors.New("path must be /api/push/<token>")
)

var pushPathRE = regexp.MustCompile(`^/api/push/[A-Za-z0-9_-]{8,128}$`)

// CheckURL applies the push destination rules to a vault value. The dial
// policy is enforced separately, at connect time.
func CheckURL(raw string, hosts []string) (*url.URL, error) {
	if len(raw) > 2048 {
		return nil, ErrURLTooLong
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, ErrURLParse
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	singleLabel := host != "" && !strings.ContainsAny(host, ".:")
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && singleLabel:
	default:
		return nil, ErrURLScheme
	}
	if u.User != nil {
		return nil, ErrURLUserinfo
	}
	if len(hosts) == 0 {
		return nil, ErrHostsEmpty
	}
	if !slices.Contains(hosts, host) {
		return nil, ErrHostNotListed
	}
	if port := u.Port(); port != "" && !singleLabel && (u.Scheme != "https" || port != "443") {
		return nil, ErrURLPort
	}
	if !pushPathRE.MatchString(u.Path) || u.RawPath != "" {
		return nil, ErrURLPath
	}
	return u, nil
}
