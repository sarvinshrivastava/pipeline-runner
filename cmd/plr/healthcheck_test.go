package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// loopbackServer serves /healthz with status on 127.0.0.1 and returns its port.
func loopbackServer(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	_, p, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestHealthcheckExitCodes(t *testing.T) {
	closed := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_, p, _ := net.SplitHostPort(ln.Addr().String())
		_ = ln.Close()
		return p
	}
	cases := []struct {
		name string
		port func() string
		want int
	}{
		{"200 is healthy", func() string { return loopbackServer(t, http.StatusOK) }, 0},
		{"503 is unhealthy", func() string { return loopbackServer(t, http.StatusServiceUnavailable) }, 1},
		{"204 is not 200", func() string { return loopbackServer(t, http.StatusNoContent) }, 1},
		{"nothing listening", closed, 1},
		{"non-numeric PORT", func() string { return "abc" }, 1},
		{"zero PORT", func() string { return "0" }, 1},
		{"PORT out of range", func() string { return strconv.Itoa(65536) }, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("PORT", c.port())
			if got := healthcheck(); got != c.want {
				t.Fatalf("healthcheck() = %d, want %d", got, c.want)
			}
		})
	}
}

// TestHealthcheckDefaultsToPort3000: with PORT unset it probes :3000, so it
// is unhealthy unless something answers there.
func TestHealthcheckDefaultsToPort3000(t *testing.T) {
	t.Setenv("PORT", "")
	ln, err := net.Listen("tcp", "127.0.0.1:3000")
	if err != nil {
		t.Skip("port 3000 is in use on this host")
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })} //nolint:gosec // test server
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	if got := healthcheck(); got != 0 {
		t.Fatalf("healthcheck() with PORT unset = %d, want 0 from :3000", got)
	}
}
