// Package logsink captures a run's output with a byte cap.
//
// It keeps the HEAD and the TAIL rather than stopping at the cap: the head
// shows how a run started, and the tail holds the error that killed it.
// Truncating either end alone discards the half you need.
package logsink

import (
	"fmt"
	"os"
)

// Sink writes the head straight to the file and buffers the tail.
type Sink struct {
	f         *os.File
	headCap   int
	tailCap   int
	headBytes int
	tail      [][]byte
	tailBytes int
	dropped   int
	closed    bool
}

// New creates (truncating) the log file at path.
func New(path string, maxBytes int64) (*Sink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // path is built from a run id
	if err != nil {
		return nil, err
	}
	head := int(maxBytes / 2)
	return &Sink{f: f, headCap: head, tailCap: int(maxBytes) - head}, nil
}

// Write implements io.Writer. It never fails the caller for being over cap.
func (s *Sink) Write(p []byte) (int, error) {
	n := len(p)
	if s.headBytes < s.headCap {
		room := min(s.headCap-s.headBytes, len(p))
		if _, err := s.f.Write(p[:room]); err != nil {
			return 0, err
		}
		s.headBytes += room
		p = p[room:]
		if len(p) == 0 {
			return n, nil
		}
	}
	s.tail = append(s.tail, append([]byte(nil), p...))
	s.tailBytes += len(p)
	for s.tailBytes > s.tailCap && len(s.tail) > 0 {
		over := s.tailBytes - s.tailCap
		if over >= len(s.tail[0]) {
			s.tailBytes -= len(s.tail[0])
			s.dropped += len(s.tail[0])
			s.tail = s.tail[1:]
			continue
		}
		s.tail[0] = s.tail[0][over:]
		s.tailBytes -= over
		s.dropped += over
	}
	return n, nil
}

// Close flushes the tail and closes the file. Safe to call twice.
func (s *Sink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.dropped > 0 {
		_, _ = fmt.Fprintf(s.f, "\n\n... %d bytes of output dropped from the middle ...\n\n", s.dropped)
	}
	for _, c := range s.tail {
		if _, err := s.f.Write(c); err != nil {
			_ = s.f.Close()
			return err
		}
	}
	return s.f.Close()
}
