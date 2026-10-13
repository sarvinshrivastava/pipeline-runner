// Package httpt is the HTTP callback target: the primary way a service's own
// logic runs on the runner's schedule.
package httpt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// Executor sends callbacks with an SSRF-safe client.
type Executor struct {
	Client *http.Client
	Now    func() time.Time
}

// New returns an executor using client.
func New(client *http.Client) *Executor {
	return &Executor{Client: client, Now: time.Now}
}

type envelope struct {
	RunID        string          `json:"run_id"`
	Job          string          `json:"job"`
	Attempt      int             `json:"attempt"`
	Trigger      string          `json:"trigger"`
	ScheduledFor string          `json:"scheduled_for"`
	Payload      json.RawMessage `json:"payload"`
}

func rfc3339ms(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// Execute implements target.Executor.
func (x *Executor) Execute(ctx context.Context, in target.ExecInput) target.Result {
	h := in.Spec.Target.HTTP
	r := in.Run
	values := make([]string, 0, len(in.Secrets))
	for _, v := range in.Secrets {
		values = append(values, v)
	}
	red := secure.NewRedactor(values)
	log := newRunLog(in.LogPath)
	defer log.close()

	payload := r.Payload
	if len(payload) == 0 {
		payload = h.Body
	}
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	body, err := json.Marshal(envelope{RunID: r.ID, Job: r.JobName, Attempt: r.Attempt, Trigger: r.Kind,
		ScheduledFor: rfc3339ms(r.ContextFor), Payload: payload})
	if err != nil {
		return target.Result{Status: model.StatusFailed, Error: "encode body: " + err.Error(), NoRetry: true}
	}

	for _, ref := range []string{h.Auth.BearerRef, h.Auth.HMACRef} {
		if ref != "" && in.Secrets[ref] == "" {
			return target.Result{Status: model.StatusFailed, Error: "secret " + ref + " resolved empty", Unsent: true, NoRetry: true}
		}
	}
	var reqBody io.Reader
	signed := body // the signature covers exactly the bytes sent
	if h.Method != http.MethodGet {
		reqBody = bytes.NewReader(body)
	} else {
		signed = nil
	}
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), h.Method, h.URL, reqBody)
	if err != nil {
		return target.Result{Status: model.StatusFailed, Error: "build request: " + err.Error(), NoRetry: true}
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-PLR-Run-Id", r.ID)
	req.Header.Set("X-PLR-Job", r.JobName)
	req.Header.Set("X-PLR-Attempt", strconv.Itoa(r.Attempt))
	req.Header.Set("X-PLR-Trigger", r.Kind)
	req.Header.Set("X-PLR-Scheduled-For", rfc3339ms(r.ContextFor))
	if h.Mode == "async" {
		req.Header.Set("X-PLR-Run-Token", in.RunToken)
	}
	if h.Auth.BearerRef != "" {
		req.Header.Set("Authorization", "Bearer "+in.Secrets[h.Auth.BearerRef])
	}
	if h.Auth.HMACRef != "" {
		req.Header.Set("X-PLR-Signature", secure.Sign([]byte(in.Secrets[h.Auth.HMACRef]), strconv.FormatInt(x.Now().Unix(), 10), signed))
	}
	log.printf("→ %s %s (attempt %d, %s)\n", h.Method, h.URL, r.Attempt, r.Kind)
	log.printf("  headers: %s\n", headerNames(req.Header))

	start := time.Now()
	resp, err := x.Client.Do(req)
	delivered := wrote.Load()
	if err != nil {
		msg := red.String(err.Error())
		log.printf("✗ %s after %s (request %s)\n", msg, time.Since(start).Round(time.Millisecond), sentWord(delivered))
		res := target.Result{Status: model.StatusFailed, Error: msg, Delivered: delivered, Unsent: !delivered, Ambiguous: delivered}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			res.Status, res.Error = model.StatusTimeout, "timed out: "+msg
		}
		return res
	}
	defer func() { _ = resp.Body.Close() }()

	// Read past the cap and redact before cutting, so a secret straddling the
	// cut can't leave its prefix behind.
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, model.MaxOutputBytes+redactMargin+1))
	truncated := len(raw) > model.MaxOutputBytes
	raw = red.Bytes(raw)
	if len(raw) > model.MaxOutputBytes {
		raw = raw[:model.MaxOutputBytes]
	}
	code := resp.StatusCode
	log.printf("← %d after %s\n", code, time.Since(start).Round(time.Millisecond))
	if n := min(len(raw), 4096); n > 0 {
		log.printf("  body: %s\n", raw[:n])
	}

	res := target.Result{HTTPStatus: &code, Delivered: true, OutputTruncated: truncated}
	if readErr != nil && !truncated {
		res.OutputError = "reading response body: " + red.String(readErr.Error())
	}

	switch {
	case code == http.StatusAccepted && h.Mode == "async":
		res.Waiting = true
		log.printf("  waiting for completion (lease %ds)\n", h.LeaseSec)
	case code >= 200 && code < 300:
		res.Status = model.StatusSuccess
		res.Output = asJSON(raw, truncated)
		if until, ok, bad := deferUntil(raw, truncated); ok {
			res.Status, res.DeferUntil = "", &until
			log.printf("  deferred to %s\n", until.Format(time.RFC3339))
		} else if bad {
			res.OutputError = "defer_until unparseable; treated as success"
		}
	case code >= 300 && code < 400:
		res.Status, res.NoRetry = model.StatusFailed, true
		res.Error = fmt.Sprintf("HTTP %d: redirects are never followed", code)
	case code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable:
		if until, ok := retryAfter(resp.Header.Get("Retry-After"), x.Now()); ok {
			res.DeferUntil = &until
			log.printf("  Retry-After → deferred to %s\n", until.Format(time.RFC3339))
			break
		}
		res.Status, res.Ambiguous = model.StatusFailed, true
		res.Error = fmt.Sprintf("HTTP %d", code)
		res.Output = asJSON(raw, truncated)
	case code >= 400 && code < 500:
		res.Status, res.NoRetry = model.StatusFailed, true
		res.Error = fmt.Sprintf("HTTP %d", code)
		res.Output = asJSON(raw, truncated)
	default:
		res.Status, res.Ambiguous = model.StatusFailed, true
		res.Error = fmt.Sprintf("HTTP %d", code)
		res.Output = asJSON(raw, truncated)
	}
	return res
}

func sentWord(delivered bool) string {
	if delivered {
		return "was sent"
	}
	return "was never sent"
}

func headerNames(h http.Header) string {
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// asJSON stores valid JSON as is and anything else as a JSON string.
func asJSON(raw []byte, truncated bool) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	if !truncated && json.Valid(raw) {
		return json.RawMessage(raw)
	}
	b, _ := json.Marshal(string(raw))
	return b
}

// deferUntil reads {"defer_until": "<RFC 3339>"}. bad reports a present but
// unparseable value.
func deferUntil(raw []byte, truncated bool) (t time.Time, ok, bad bool) {
	raw = bytes.TrimLeft(raw, " \t\r\n\ufeff")
	if truncated || len(raw) == 0 || raw[0] != '{' {
		return t, false, false
	}
	var v struct {
		DeferUntil *string `json:"defer_until"`
	}
	if json.Unmarshal(raw, &v) != nil || v.DeferUntil == nil {
		return t, false, false
	}
	t, err := time.Parse(time.RFC3339Nano, *v.DeferUntil)
	if err != nil {
		return t, false, true
	}
	return t.UTC(), true, false
}

// redactMargin is read beyond MaxOutputBytes so redaction sees whole secrets.
const redactMargin = 4096

// maxRetryAfterSec caps delta-seconds before the Duration multiply can overflow.
const maxRetryAfterSec = 366 * 86400

// retryAfter parses delta-seconds or an HTTP-date.
func retryAfter(v string, now time.Time) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		secs = min(secs, maxRetryAfterSec) // max_defer_sec rejects anything later anyway
		return now.Add(time.Duration(secs) * time.Second).UTC(), true
	}
	if t, err := http.ParseTime(v); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

// runLog writes the synthesised request/response log; failures are ignored.
type runLog struct{ f *os.File }

func newRunLog(path string) *runLog {
	if path == "" {
		return &runLog{}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // path is built from a run id
	if err != nil {
		return &runLog{}
	}
	return &runLog{f: f}
}

func (l *runLog) printf(format string, args ...any) {
	if l.f != nil {
		_, _ = fmt.Fprintf(l.f, format, args...)
	}
}

func (l *runLog) close() {
	if l.f != nil {
		_ = l.f.Close()
	}
}
