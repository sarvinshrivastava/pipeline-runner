package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

// StreamTiming tunes live log streams (GET /v1/runs/{id}/logs?follow=1).
type StreamTiming struct {
	Poll          time.Duration // how often the log file is checked for new bytes
	Status        time.Duration // how often the run's status is read
	Ping          time.Duration // keep-alive comment, under nginx's 60 s read timeout
	Reauth        time.Duration // how often the bearer token is checked again
	MaxAge        time.Duration // a stream ends with reason "timeout" after this
	WriteGrace    time.Duration // a write that takes longer ends the stream
	ShutdownGrace time.Duration // the whole drain and end at shutdown
}

func defaultStreamTiming() StreamTiming {
	return StreamTiming{Poll: 500 * time.Millisecond, Status: time.Second, Ping: 15 * time.Second, Reauth: time.Minute,
		MaxAge: time.Hour, WriteGrace: 20 * time.Second, ShutdownGrace: 2 * time.Second}
}

const (
	streamMaxPerClient = 4
	streamMaxTotal     = 128
	streamCatchUpCap   = 16 << 20  // as the snapshot: older bytes are skipped with a marker
	streamTickBudget   = 256 << 10 // bytes read per poll tick; a drain reads to EOF
	streamRetryMs      = 3000
)

// streamLimits bounds open streams per client and in total.
type streamLimits struct {
	mu               sync.Mutex
	per              map[string]int
	total            int
	maxPer, maxTotal int
}

func (l *streamLimits) acquire(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maxTotal || l.per[key] >= l.maxPer {
		return false
	}
	l.per[key]++
	l.total++
	return true
}

func (l *streamLimits) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.per[key]--; l.per[key] <= 0 {
		delete(l.per, key)
	}
	l.total--
}

// parseFollow reads ?follow=: absent, empty, 0 or false is the snapshot.
func parseFollow(r *http.Request) (follow, ok bool) {
	switch r.URL.Query().Get("follow") {
	case "", "0", "false":
		return false, true
	case "1", "true":
		return true, true
	}
	return false, false
}

// startOffset is Last-Event-ID, else ?offset=; anything unusable is 0.
func startOffset(r *http.Request) int64 {
	v := r.Header.Get("Last-Event-ID")
	if v == "" {
		v = r.URL.Query().Get("offset")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

var errTruncated = errors.New("log truncated")

// followLogs streams a run's log and status as Server-Sent Events.
func (s *Server) followLogs(w http.ResponseWriter, r *http.Request, run model.Run) {
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		return
	}
	key := principalFrom(r).ClientID
	if key == "" {
		key = "admin" // the bootstrap token belongs to no client
	}
	if !s.streams.acquire(key) {
		writeError(w, http.StatusTooManyRequests, "too_many_streams", "too many live log streams open; close one or read the snapshot")
		return
	}
	defer s.streams.release(key)
	ls := &logStream{s: s, w: w, rc: http.NewResponseController(w), r: r, run: run,
		path: s.d.Engine.LogPath(run.ID), offset: startOffset(r), status: run.Status, t: s.timing}
	defer ls.close()
	ls.serve()
}

type logStream struct {
	s    *Server
	w    http.ResponseWriter
	rc   *http.ResponseController
	r    *http.Request
	run  model.Run
	path string
	t    StreamTiming

	f      *os.File
	offset int64  // file bytes consumed; pend holds the ones not sent yet
	pend   []byte // a partial rune or a trailing \r waits for the next read
	trim   bool   // drop continuation bytes a resume offset landed inside
	status model.Status
	fixed  bool // the write deadline is fixed (shutdown); writes don't extend it
	failed bool // a write failed: the client is gone
}

func (ls *logStream) close() {
	if ls.f != nil {
		_ = ls.f.Close()
	}
}

func (ls *logStream) serve() {
	// The server's ReadTimeout would cancel the request at 30 s and its
	// WriteTimeout would cut the stream at 70 s; each write sets its own.
	_ = ls.rc.SetReadDeadline(time.Time{})
	h := ls.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("X-Accel-Buffering", "no") // nginx: pass events through unbuffered
	ls.w.WriteHeader(http.StatusOK)
	ls.write([]byte("retry: " + strconv.Itoa(streamRetryMs) + "\n\n"))
	ls.statusEvent()

	stop := ls.s.d.Engine.Stopping()
	select {
	case <-stop:
		ls.shutdown()
		return
	default:
	}
	if ls.status.Terminal() {
		ls.finished()
		return
	}
	if !ls.poll() {
		return
	}
	poll, status, ping, reauth := time.NewTicker(ls.t.Poll), time.NewTicker(ls.t.Status), time.NewTicker(ls.t.Ping), time.NewTicker(ls.t.Reauth)
	maxAge := time.NewTimer(ls.t.MaxAge)
	defer func() { poll.Stop(); status.Stop(); ping.Stop(); reauth.Stop(); maxAge.Stop() }()
	for !ls.failed {
		select {
		case <-ls.r.Context().Done():
			return
		case <-stop:
			ls.shutdown()
			return
		case <-maxAge.C:
			ls.end("timeout", true)
			return
		case <-poll.C:
			if !ls.poll() {
				return
			}
		case <-status.C:
			if done := ls.checkStatus(); done {
				return
			}
		case <-ping.C:
			ls.write([]byte(": ping\n\n"))
		case <-reauth.C:
			if !ls.authorised() {
				ls.end("unauthorized", false) // no further bytes to a revoked caller
				return
			}
		}
	}
}

// poll sends new bytes; false when the stream has ended.
func (ls *logStream) poll() bool {
	switch err := ls.pump(false); {
	case errors.Is(err, errTruncated):
		ls.end("truncated", false)
		return false
	case err != nil:
		ls.s.d.Log.Error("live log read", "run", ls.run.ID, "err", err)
		ls.end("error", false)
		return false
	}
	return !ls.failed
}

// checkStatus reports a status change and ends the stream on a terminal one.
func (ls *logStream) checkStatus() (done bool) {
	st, err := store.RunStatus(ls.r.Context(), ls.s.d.Store.R, ls.run.ID)
	if errors.Is(err, store.ErrNotFound) {
		ls.status = "unknown"
		ls.end("deleted", false)
		return true
	}
	if err != nil {
		ls.s.d.Log.Warn("live log status", "run", ls.run.ID, "err", err)
		return false
	}
	if st != ls.status {
		ls.status = st
		ls.statusEvent()
	}
	if st.Terminal() {
		ls.finished()
		return true
	}
	return ls.failed
}

// authorised re-checks the token. Only a definite rejection ends the stream;
// a storage error keeps it.
func (ls *logStream) authorised() bool {
	p, err := ls.s.authFn(ls.r)
	if errors.Is(err, errBadToken) {
		return false
	}
	if err != nil {
		return true
	}
	return p.HasScope(model.ScopeJobsRead) && (p.Admin || p.ClientID == ls.run.ClientID)
}

func (ls *logStream) finished() {
	ls.end("finished", true)
}

func (ls *logStream) shutdown() {
	_ = ls.rc.SetWriteDeadline(time.Now().Add(ls.t.ShutdownGrace))
	ls.fixed = true
	ls.end("shutdown", true)
}

// end sends the last event. drain first sends every remaining byte.
func (ls *logStream) end(reason string, drain bool) {
	if drain {
		if err := ls.pump(true); err != nil {
			reason = "error"
			if errors.Is(err, errTruncated) {
				reason = "truncated"
			}
		}
	}
	body := map[string]string{"status": string(ls.status), "reason": reason}
	if reason == "finished" {
		if next, err := store.SuccessorOf(ls.r.Context(), ls.s.d.Store.R, ls.run); err == nil && next != "" {
			body["successor_run_id"] = next
		}
	}
	b, _ := json.Marshal(body)
	ls.event("end", "", b)
}

func (ls *logStream) statusEvent() {
	b, _ := json.Marshal(map[string]string{"status": string(ls.status)})
	ls.event("status", "", b)
}

// pump reads new bytes and sends what is complete. final reads to EOF and
// sends everything, including a held partial rune.
func (ls *logStream) pump(final bool) error {
	if ls.f == nil {
		f, err := os.Open(ls.path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil // no log yet
		}
		if err != nil {
			return err
		}
		ls.f = f
		if err := ls.position(); err != nil {
			return err
		}
	}
	st, err := ls.f.Stat() // the open file: a retention unlink doesn't end the stream
	if err != nil {
		return err
	}
	size := st.Size()
	if size < ls.offset {
		return errTruncated
	}
	buf := make([]byte, 32<<10)
	var read int64
	for ls.offset < size && (final || read < streamTickBudget) {
		n, err := ls.f.ReadAt(buf[:min(int64(len(buf)), size-ls.offset)], ls.offset)
		ls.pend = append(ls.pend, buf[:n]...)
		ls.offset += int64(n)
		read += int64(n)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			break
		}
	}
	if ls.trim {
		for len(ls.pend) > 0 && !utf8.RuneStart(ls.pend[0]) {
			ls.pend = ls.pend[1:]
		}
		if len(ls.pend) > 0 {
			ls.trim = false
		}
	}
	return ls.sendPend(final)
}

// position applies the resume offset and the catch-up cap once the file exists.
func (ls *logStream) position() error {
	st, err := ls.f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	ls.offset = min(ls.offset, size)
	ls.trim = ls.offset > 0
	if start := size - streamCatchUpCap; start > ls.offset {
		ls.event("log", strconv.FormatInt(start, 10), fmt.Appendf(nil, "[%d bytes skipped]\n", start-ls.offset))
		ls.offset, ls.trim = start, true
	}
	return nil
}

func (ls *logStream) sendPend(final bool) error {
	n := len(ls.pend)
	if !final {
		n = completePrefix(ls.pend)
	}
	if n == 0 {
		return nil
	}
	id := ls.offset - int64(len(ls.pend)) + int64(n)
	ls.event("log", strconv.FormatInt(id, 10), ls.pend[:n])
	ls.pend = append(ls.pend[:0], ls.pend[n:]...)
	return nil
}

// completePrefix is how much of b can go out now: not a trailing partial
// rune (it would decode as U+FFFD) and not a trailing \r (it may be half of
// a \r\n, which would then read as two line breaks).
func completePrefix(b []byte) int {
	n := len(b)
	for i := n - 1; i >= 0 && i >= n-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:n]) {
				n = i
			}
			break
		}
	}
	if n > 0 && b[n-1] == '\r' {
		n--
	}
	return n
}

// event writes one SSE event. data is split on \r\n, \r and \n so no byte of
// the log can start a field of its own; clients see line ends as \n.
func (ls *logStream) event(name, id string, data []byte) {
	var b bytes.Buffer
	b.WriteString("event: " + name + "\n")
	if id != "" {
		b.WriteString("id: " + id + "\n")
	}
	for _, line := range splitLines(data) {
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	ls.write(b.Bytes())
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	for {
		i := bytes.IndexAny(data, "\r\n")
		if i < 0 {
			return append(out, data)
		}
		out = append(out, data[:i])
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		data = data[i+1:]
	}
}

func (ls *logStream) write(p []byte) {
	if ls.failed {
		return
	}
	if !ls.fixed {
		_ = ls.rc.SetWriteDeadline(time.Now().Add(ls.t.WriteGrace))
	}
	if _, err := ls.w.Write(p); err != nil {
		ls.failed = true
		return
	}
	if err := ls.rc.Flush(); err != nil {
		ls.failed = true
	}
}
