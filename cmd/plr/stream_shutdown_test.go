package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/config"
)

// A live log stream whose reader never reads must not hold up shutdown.
func TestShutdownEndsStalledLogStream(t *testing.T) {
	const admin = "admin-token-0123456789-0123456789"
	cfg := config.Config{
		Port: 0, MetricsPort: 0, DataDir: t.TempDir(), APIToken: admin,
		RunTokenKey: bytes.Repeat([]byte{1}, 32), EncryptionKey: bytes.Repeat([]byte{2}, 32),
		SecretManagerURL: "http://127.0.0.1:1", MaxHTTPInflight: 4, MaxContainers: 1, RunRetentionDays: 60,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), func(a, _ string) { addrs <- a })
	}()
	var base string
	select {
	case base = <-addrs:
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	call := func(method, path, tok string, body any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(context.Background(), method, "http://"+base+path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		var m map[string]any
		_ = json.NewDecoder(res.Body).Decode(&m)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s: %d %v", method, path, res.StatusCode, m)
		}
		return m
	}
	tok := call("POST", "/v1/clients", admin, map[string]any{"name": "a", "scopes": []string{"jobs:read", "jobs:write"},
		"callback_hosts": []string{"svc"}, "secret_ref_prefixes": []string{"Svc/"}})["token"].(string)
	job := call("POST", "/v1/jobs", tok, map[string]any{"name": "later",
		"trigger": map[string]any{"type": "at", "at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)},
		"target":  map[string]any{"type": "http", "http": map[string]any{"url": "http://svc/x"}}})["id"].(string)
	items := call("GET", "/v1/runs?job="+job, tok, nil)["items"].([]any)
	runID := items[0].(map[string]any)["id"].(string)

	conn, err := net.Dial("tcp", base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096) // a reader that never reads
	}
	req := "GET /v1/runs/" + runID + "/logs?follow=1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + tok + "\r\n\r\n"
	if _, err := io.Copy(conn, strings.NewReader(req)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n') // read the status line, then never again
	if err != nil || !strings.HasPrefix(line, "HTTP/1.1 200") {
		t.Fatalf("stream did not open: %q %v", line, err)
	}
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("shutdown hung on the stream")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("shutdown took %s with a stalled stream", time.Since(start))
	}
}
