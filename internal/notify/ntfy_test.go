package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPublishJSONAndFailSoft(t *testing.T) {
	var got map[string]any
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tk" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("headers: %v", r.Header)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(status)
	}))
	defer srv.Close()
	n := New(srv.URL, "tk", "ok-topic", "fail-topic", "https://plr.example", slog.New(slog.NewTextHandler(io.Discard, nil)))

	n.Send(Msg{Level: Fail, Title: "tick: failed ❌", Body: "HTTP 500"})
	n.Drain(context.Background())
	if got["topic"] != "fail-topic" || got["title"] != "tick: failed ❌" || got["priority"] != float64(4) || got["click"] != "https://plr.example" {
		t.Fatalf("body = %v", got)
	}

	n.Send(Msg{Level: OK, Topic: "client-topic", Title: "ok"})
	n.Drain(context.Background())
	if got["topic"] != "client-topic" {
		t.Fatalf("override topic: %v", got["topic"])
	}

	status = http.StatusInternalServerError
	n.Send(Msg{Level: Fail, Title: "x"})
	n.Drain(context.Background()) // must not panic or block
}

func TestQueueFullDrops(_ *testing.T) {
	n := New("", "", "", "", "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 0; i < 200; i++ {
		n.Send(Msg{Title: "x"}) // never blocks
	}
}
