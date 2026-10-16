package secure

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/url"
	"sort"
)

// Mask replaces every redacted secret.
const Mask = "[REDACTED]"

// minSecretLen keeps short values (which would mangle ordinary text) out of
// the redaction set.
const minSecretLen = 6

// Redactor removes secret values, and their common encodings, from text.
type Redactor struct {
	needles [][]byte
	maxLen  int
}

// NewRedactor builds a redactor for the given secret values.
func NewRedactor(values []string) *Redactor {
	seen := map[string]bool{}
	r := &Redactor{}
	add := func(s string) {
		if len(s) < minSecretLen || seen[s] {
			return
		}
		seen[s] = true
		r.needles = append(r.needles, []byte(s))
		if len(s) > r.maxLen {
			r.maxLen = len(s)
		}
	}
	for _, v := range values {
		add(v)
		add(base64.StdEncoding.EncodeToString([]byte(v)))
		add(base64.RawStdEncoding.EncodeToString([]byte(v)))
		add(base64.URLEncoding.EncodeToString([]byte(v)))
		add(base64.RawURLEncoding.EncodeToString([]byte(v)))
		add(url.QueryEscape(v))
		add(url.PathEscape(v))
	}
	// Longest first so an encoding containing the raw value is masked whole.
	sort.Slice(r.needles, func(i, j int) bool { return len(r.needles[i]) > len(r.needles[j]) })
	return r
}

// Bytes returns b with every secret masked.
func (r *Redactor) Bytes(b []byte) []byte {
	if r == nil || len(r.needles) == 0 {
		return b
	}
	for _, n := range r.needles {
		if bytes.Contains(b, n) {
			b = bytes.ReplaceAll(b, n, []byte(Mask))
		}
	}
	return b
}

// String returns s with every secret masked.
func (r *Redactor) String(s string) string { return string(r.Bytes([]byte(s))) }

// Writer returns a streaming redactor. It holds back maxLen-1 bytes so a
// secret split across two writes is still caught. Close flushes the rest.
func (r *Redactor) Writer(w io.Writer) io.WriteCloser {
	return &redactWriter{r: r, w: w}
}

type redactWriter struct {
	r   *Redactor
	w   io.Writer
	buf []byte
}

func (rw *redactWriter) Write(p []byte) (int, error) {
	if rw.r == nil || len(rw.r.needles) == 0 {
		return rw.w.Write(p)
	}
	rw.buf = rw.r.Bytes(append(rw.buf, p...))
	keep := rw.r.maxLen - 1
	if len(rw.buf) > keep {
		cut := len(rw.buf) - keep
		if _, err := rw.w.Write(rw.buf[:cut]); err != nil {
			return 0, err
		}
		rw.buf = append([]byte(nil), rw.buf[cut:]...)
	}
	return len(p), nil
}

func (rw *redactWriter) Close() error {
	if len(rw.buf) == 0 {
		return nil
	}
	_, err := rw.w.Write(rw.r.Bytes(rw.buf))
	rw.buf = nil
	return err
}
