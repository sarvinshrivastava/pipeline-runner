package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/config"
)

func TestServeLifecycle(t *testing.T) {
	cfg := config.Config{
		Port: 0, MetricsPort: 0, DataDir: t.TempDir(), APIToken: "admin-token-0123456789-0123456789",
		RunTokenKey: bytes.Repeat([]byte{1}, 32), EncryptionKey: bytes.Repeat([]byte{2}, 32),
		SecretManagerURL: "http://127.0.0.1:1", MaxHTTPInflight: 4, MaxContainers: 1, RunRetentionDays: 60,
	}
	ctx, cancel := context.WithCancel(context.Background())
	addrs := make(chan [2]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), func(a, m string) { addrs <- [2]string{a, m} })
	}()
	var a [2]string
	select {
	case a = <-addrs:
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	for path, base := range map[string]string{"/healthz": a[0], "/": a[0], "/v1/openapi.json": a[0], "/metrics": a[1]} {
		resp, err := http.Get("http://" + base + path) //nolint:noctx // test
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("shutdown took longer than 25 s")
	}
	// http.Server.Shutdown treats a just-accepted connection as active for up
	// to 5 s, so an idle shutdown can take that long; it must not take more.
	if time.Since(start) > 10*time.Second {
		t.Fatalf("idle shutdown took %s", time.Since(start))
	}
}
