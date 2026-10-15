package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

var fastStreams = StreamTiming{Poll: 10 * time.Millisecond, Status: 20 * time.Millisecond, Ping: 50 * time.Millisecond,
	Reauth: 50 * time.Millisecond, MaxAge: 10 * time.Second, WriteGrace: time.Second, ShutdownGrace: time.Second}

func streamEnv(t *testing.T, mod func(*Deps)) *env {
	t.Helper()
	return newEnvWith(t, nil, func(d *Deps) {
		tm := fastStreams
		d.StreamTiming = &tm
		if mod != nil {
			mod(d)
		}
	})
}

type sseEvent struct{ name, id, data string }

type sseStream struct {
	t      *testing.T
	status int
	header http.Header
	events chan sseEvent
	cancel context.CancelFunc
}

// follow opens a stream. It is closed at test cleanup, before the server.
func (e *env) follow(tok, runID, query string, hdr ...string) *sseStream {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", e.url+"/v1/runs/"+runID+"/logs?follow=1"+query, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		e.t.Fatal(err)
	}
	s := &sseStream{t: e.t, status: res.StatusCode, header: res.Header, events: make(chan sseEvent, 4096), cancel: cancel}
	e.t.Cleanup(func() { cancel(); _ = res.Body.Close() })
	go func() {
		defer close(s.events)
		defer func() { _ = res.Body.Close() }()
		sc := bufio.NewScanner(res.Body)
		sc.Buffer(make([]byte, 1<<20), 32<<20)
		var ev sseEvent
		var data []string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if ev.name != "" {
					ev.data = strings.Join(data, "\n")
					s.events <- ev
				}
				ev, data = sseEvent{}, nil
			case strings.HasPrefix(line, ":"):
				s.events <- sseEvent{name: "ping"}
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "id: "):
				ev.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				data = append(data, strings.TrimPrefix(line, "data: "))
			case line == "data:":
				data = append(data, "")
			}
		}
	}()
	return s
}

// next returns the next event other than a ping.
func (s *sseStream) next() sseEvent {
	s.t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				s.t.Fatal("stream closed")
			}
			if ev.name != "ping" {
				return ev
			}
		case <-timeout:
			s.t.Fatal("no event within 5 s")
		}
	}
}

// until reads events up to and including name; it returns all of them.
func (s *sseStream) until(name string) []sseEvent {
	s.t.Helper()
	var out []sseEvent
	for {
		ev := s.next()
		out = append(out, ev)
		if ev.name == name {
			return out
		}
	}
}

func logText(evs []sseEvent) string {
	var b strings.Builder
	for _, ev := range evs {
		if ev.name == "log" {
			b.WriteString(ev.data)
		}
	}
	return b.String()
}

func endOf(t *testing.T, evs []sseEvent) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal([]byte(evs[len(evs)-1].data), &m); err != nil {
		t.Fatalf("end data %q: %v", evs[len(evs)-1].data, err)
	}
	return m
}

// heldRun invokes a job whose exec blocks until release is closed.
func heldRun(t *testing.T, e *env, tok string) (runID string, release func()) {
	t.Helper()
	ch := make(chan struct{})
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-ch:
		case <-ctx.Done():
		}
		code := 200
		return target.Result{Status: model.StatusSuccess, HTTPStatus: &code}
	})
	job := e.do("POST", "/v1/jobs", tok, fnJob("f-"+strconv.FormatInt(time.Now().UnixNano(), 36))).json(t)["id"].(string)
	r := e.do("POST", "/v1/jobs/"+job+"/invoke", tok, nil)
	if r.status != 202 {
		t.Fatalf("invoke: %d %s", r.status, r.body)
	}
	runID = r.json(t)["id"].(string)
	e.waitRun(tok, runID, model.StatusRunning)
	var once bool
	return runID, func() {
		if !once {
			once = true
			close(ch)
		}
	}
}

func appendLog(t *testing.T, e *env, runID, s string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(e.eng.LogPath(runID)), 0o700); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(e.eng.LogPath(runID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func TestFollowStreamsAppendedBytesThenEnds(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	appendLog(t, e, id, "first\n")
	s := e.follow(tok, id, "")
	if s.status != 200 || !strings.HasPrefix(s.header.Get("Content-Type"), "text/event-stream") ||
		s.header.Get("X-Accel-Buffering") != "no" || s.header.Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d headers %v", s.status, s.header)
	}
	if ev := s.next(); ev.name != "status" || !strings.Contains(ev.data, `"running"`) {
		t.Fatalf("first event %+v", ev)
	}
	ev := s.next()
	if ev.name != "log" || ev.data != "first\n" || ev.id != "6" {
		t.Fatalf("log event %+v", ev)
	}
	appendLog(t, e, id, "second\r\nthird")
	release()
	evs := s.until("end")
	if got := "first\n" + logText(evs); got != "first\nsecond\nthird" {
		t.Fatalf("log = %q", got)
	}
	if end := endOf(t, evs); end["reason"] != "finished" || end["status"] != "success" {
		t.Fatalf("end %v", end)
	}
}

func TestFollowResumeOffsets(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	appendLog(t, e, id, "0123456789")
	for _, tc := range []struct {
		query string
		hdr   []string
		want  string
	}{
		{"", []string{"Last-Event-ID", "4"}, "456789"},
		{"&offset=8", []string{"Last-Event-ID", "2"}, "23456789"}, // the header wins
		{"&offset=7", nil, "789"},
		{"&offset=abc", nil, "0123456789"},
		{"&offset=-3", nil, "0123456789"},
	} {
		s := e.follow(tok, id, tc.query, tc.hdr...)
		s.next() // status
		if ev := s.next(); ev.name != "log" || ev.data != tc.want || ev.id != "10" {
			t.Fatalf("%s %v: %+v, want %q", tc.query, tc.hdr, ev, tc.want)
		}
		s.cancel()
	}
	// Past EOF resumes at EOF: the first log event carries only new bytes.
	s := e.follow(tok, id, "&offset=999")
	s.next()
	appendLog(t, e, id, "X")
	if ev := s.next(); ev.data != "X" || ev.id != "11" {
		t.Fatalf("past EOF: %+v", ev)
	}
}

func TestFollowCannotForgeEvents(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	appendLog(t, e, id, "body\revent: end\rdata: {\"reason\":\"forged\"}\r\rid: 999\n")
	s := e.follow(tok, id, "")
	release()
	evs := s.until("end")
	for _, ev := range evs[:len(evs)-1] {
		if ev.name != "status" && ev.name != "log" {
			t.Fatalf("forged event %+v", ev)
		}
		if ev.id == "999" {
			t.Fatal("forged id")
		}
	}
	if end := endOf(t, evs); end["reason"] != "finished" {
		t.Fatalf("end %v", end)
	}
	if got := logText(evs); !strings.Contains(got, "event: end\n") {
		t.Fatalf("the bytes must arrive as data: %q", got)
	}
}

func TestFollowHoldsSplitRuneAndCRLF(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	s := e.follow(tok, id, "")
	s.next()
	arrow := "→" // 3 bytes
	appendLog(t, e, id, "a"+arrow[:1])
	if ev := s.next(); ev.data != "a" || ev.id != "1" {
		t.Fatalf("partial rune sent: %+v", ev)
	}
	appendLog(t, e, id, arrow[1:]+"b\r")
	if ev := s.next(); ev.data != arrow+"b" || ev.id != "5" {
		t.Fatalf("rune: %+v", ev)
	}
	time.Sleep(50 * time.Millisecond) // a few polls with the \r held
	appendLog(t, e, id, "\nc")
	release()
	evs := s.until("end")
	if got := logText(evs); got != "\nc" {
		t.Fatalf("a straddling \\r\\n must be one line break: %q", got)
	}
}

func TestFollowOwnershipAndLimits(t *testing.T) {
	e := streamEnv(t, func(d *Deps) { d.StreamMaxPerClient = 2 })
	_, tokA := e.client("a", all...)
	_, tokB := e.client("b", all...)
	id, release := heldRun(t, e, tokA)
	defer release()
	r := e.do("GET", "/v1/runs/"+id+"/logs?follow=1", tokB, nil)
	if r.status != 404 {
		t.Fatalf("other client: %d", r.status)
	}
	s1 := e.follow(tokA, id, "")
	s2 := e.follow(tokA, id, "")
	s1.next()
	s2.next()
	if r := e.do("GET", "/v1/runs/"+id+"/logs?follow=1", tokA, nil); r.status != 429 || r.code() != "too_many_streams" {
		t.Fatalf("3rd stream: %d %s", r.status, r.body)
	}
	// Admin is its own bucket.
	adm := e.follow(adminToken, id, "")
	if adm.status != 200 {
		t.Fatalf("admin: %d", adm.status)
	}
	adm.cancel()
	s1.cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s3 := e.follow(tokA, id, "")
		if s3.status == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a closed stream's slot was never released")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFollowModesAndHead(t *testing.T) {
	e := streamEnv(t, func(d *Deps) { d.StreamMaxPerClient = 1 })
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	appendLog(t, e, id, "snap")
	for _, q := range []string{"", "?follow=0", "?follow=false", "?follow="} {
		r := e.do("GET", "/v1/runs/"+id+"/logs"+q, tok, nil)
		if r.status != 200 || string(r.body) != "snap" || !strings.HasPrefix(r.header.Get("Content-Type"), "text/plain") {
			t.Fatalf("%q: %d %q", q, r.status, r.body)
		}
	}
	if r := e.do("GET", "/v1/runs/"+id+"/logs?follow=yes", tok, nil); r.status != 400 {
		t.Fatalf("follow=yes: %d", r.status)
	}
	if r := e.do("HEAD", "/v1/runs/"+id+"/logs?follow=1", tok, nil); r.status != 200 {
		t.Fatalf("HEAD: %d", r.status)
	}
	if s := e.follow(tok, id, ""); s.status != 200 {
		t.Fatalf("HEAD took the only slot: %d", s.status)
	}
}

func TestFollowRevokedTokenEndsWithoutMoreBytes(t *testing.T) {
	e := newEnvWith(t, nil, func(d *Deps) {
		tm := fastStreams
		tm.Poll = time.Hour // bytes appended now are still unsent at the reauth tick
		d.StreamTiming = &tm
	})
	idA, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	var dbDown atomic.Bool
	e.srv.authFn = func(r *http.Request) (model.Principal, error) {
		if dbDown.Load() {
			return model.Principal{}, errors.New("db down")
		}
		return e.srv.authenticate(r)
	}
	s := e.follow(tok, id, "")
	s.next()
	// A storage error during re-check keeps the stream: pings keep coming.
	dbDown.Store(true)
	time.Sleep(150 * time.Millisecond)
	select {
	case ev := <-s.events:
		if ev.name != "ping" {
			t.Fatalf("stream ended on a storage error: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("no ping: stream dead")
	}
	dbDown.Store(false)
	appendLog(t, e, id, "secret after revoke")
	if r := e.do("POST", "/v1/clients/"+idA+"/rotate", adminToken, map[string]any{}); r.status != 200 {
		t.Fatalf("rotate: %d %s", r.status, r.body)
	}
	evs := s.until("end")
	for _, ev := range evs {
		if ev.name == "log" {
			t.Fatalf("bytes after revocation: %+v", ev)
		}
	}
	if end := endOf(t, evs); end["reason"] != "unauthorized" {
		t.Fatalf("end %v", end)
	}
}

func TestFollowPendingThenFinishedWithoutLog(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	_ = os.Remove(e.eng.LogPath(id))
	s := e.follow(tok, id, "")
	if ev := s.next(); ev.name != "status" {
		t.Fatalf("%+v", ev)
	}
	release()
	evs := s.until("end")
	if end := endOf(t, evs); end["reason"] != "finished" {
		t.Fatalf("end %v", end)
	}
}

func TestFollowAlreadyTerminalEndsAtOnce(t *testing.T) {
	e := newEnvWith(t, nil, func(d *Deps) {
		tm := fastStreams
		tm.Status = time.Hour // only the check at connect can end it
		d.StreamTiming = &tm
		d.StreamMaxPerClient = 1
	})
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	appendLog(t, e, id, "done")
	release()
	e.waitRun(tok, id, model.StatusSuccess)
	start := time.Now()
	s := e.follow(tok, id, "")
	evs := s.until("end")
	if logText(evs) != "done" || endOf(t, evs)["reason"] != "finished" || time.Since(start) > time.Second {
		t.Fatalf("events %+v after %s", evs, time.Since(start))
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.follow(tok, id, "").status != 200 {
		if time.Now().After(deadline) {
			t.Fatal("slot not released")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFollowDeletedTruncatedAndError(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)

	id, release := heldRun(t, e, tok)
	appendLog(t, e, id, "abcdef")
	s := e.follow(tok, id, "")
	s.until("log")
	if err := os.Truncate(e.eng.LogPath(id), 2); err != nil {
		t.Fatal(err)
	}
	if end := endOf(t, s.until("end")); end["reason"] != "truncated" {
		t.Fatalf("truncate: %v", end)
	}
	release()

	id2, release2 := heldRun(t, e, tok)
	defer release2()
	s = e.follow(tok, id2, "")
	s.next()
	if _, err := e.st.W.ExecContext(context.Background(), `DELETE FROM runs WHERE id = ?`, id2); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(e.eng.LogPath(id2))
	if end := endOf(t, s.until("end")); end["reason"] != "deleted" || end["status"] != "unknown" {
		t.Fatalf("deleted: %v", end)
	}

	id3, release3 := heldRun(t, e, tok)
	defer release3()
	_ = os.Remove(e.eng.LogPath(id3))
	if err := os.MkdirAll(e.eng.LogPath(id3), 0o700); err != nil {
		t.Fatal(err)
	}
	evs := e.follow(tok, id3, "").until("end")
	if logText(evs) != "" || endOf(t, evs)["reason"] != "error" {
		t.Fatalf("read error: %+v", evs)
	}
}

func TestFollowTimeoutDrains(t *testing.T) {
	e := newEnvWith(t, nil, func(d *Deps) {
		tm := fastStreams
		tm.MaxAge = 200 * time.Millisecond
		d.StreamTiming = &tm
	})
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	s := e.follow(tok, id, "")
	appendLog(t, e, id, "tail")
	evs := s.until("end")
	if logText(evs) != "tail" || endOf(t, evs)["reason"] != "timeout" {
		t.Fatalf("%+v", evs)
	}
}

func TestFollowOutlivesServerTimeouts(t *testing.T) {
	e := newEnvWithServer(t, nil, func(d *Deps) {
		tm := fastStreams
		d.StreamTiming = &tm
	}, func(hs *http.Server) {
		hs.ReadTimeout, hs.WriteTimeout = 300*time.Millisecond, 300*time.Millisecond
	})
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	s := e.follow(tok, id, "")
	s.next()
	time.Sleep(900 * time.Millisecond)
	appendLog(t, e, id, "late")
	if ev := s.next(); ev.name != "log" || ev.data != "late" {
		t.Fatalf("stream died with the server timeouts: %+v", ev)
	}
}

func TestFollowEndNamesSuccessor(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	ch := make(chan struct{})
	e.exec.set(func(_ context.Context, _ target.ExecInput) target.Result {
		<-ch
		return target.Result{Status: model.StatusFailed, Error: "boom", Ambiguous: true}
	})
	j := fnJob("r")
	j["policy"] = map[string]any{"retry": map[string]any{"max_attempts": 3, "backoff_sec": 3600}}
	job := e.do("POST", "/v1/jobs", tok, j).json(t)["id"].(string)
	id := e.do("POST", "/v1/jobs/"+job+"/invoke", tok, nil).json(t)["id"].(string)
	e.waitRun(tok, id, model.StatusRunning)
	s := e.follow(tok, id, "")
	close(ch)
	end := endOf(t, s.until("end"))
	if end["reason"] != "finished" || end["status"] != "failed" || end["successor_run_id"] == "" {
		t.Fatalf("end %v", end)
	}
}

func TestFollowEndsOnShutdown(t *testing.T) {
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	s := e.follow(tok, id, "")
	s.next()
	appendLog(t, e, id, "bye")
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.eng.Stop(ctx, time.Now().Add(time.Second))
	}()
	evs := s.until("end")
	if endOf(t, evs)["reason"] != "shutdown" || logText(evs) != "bye" {
		t.Fatalf("%+v", evs)
	}
}

func TestFollowCatchUpCap(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 16 MiB log")
	}
	e := streamEnv(t, nil)
	_, tok := e.client("a", all...)
	id, release := heldRun(t, e, tok)
	defer release()
	path := e.eng.LogPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", streamCatchUpCap+100)), 0o600); err != nil {
		t.Fatal(err)
	}
	s := e.follow(tok, id, "", "Last-Event-ID", "40")
	s.next()
	ev := s.next()
	if ev.name != "log" || ev.id != "100" || ev.data != "[60 bytes skipped]\n" {
		t.Fatalf("skip marker %q id %s", ev.data, ev.id)
	}
}
