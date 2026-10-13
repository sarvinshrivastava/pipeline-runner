// Package notify publishes status pings to ntfy.
//
// Two constraints carried over from the TS service:
//  1. Messages are published as JSON to the base URL rather than through
//     X-Title/X-Tags headers, because header values must be ASCII and copy
//     routinely carries emoji and typographic punctuation.
//  2. Fail-soft: a dead status channel never fails a run. Sends are queued and
//     dropped (with a log line) when the queue is full.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Level selects the topic and priority.
type Level int

// Levels.
const (
	OK Level = iota
	Fail
)

// Msg is one notification.
type Msg struct {
	Level Level
	Topic string // override (the client's admin-set topic); "" uses the level's topic
	Title string
	Body  string
	Tags  []string
}

// Notifier sends Msgs in the background.
type Notifier struct {
	Base      string
	Token     string
	OKTopic   string
	FailTopic string
	Link      string
	HTTP      *http.Client
	Log       *slog.Logger

	queue chan Msg
}

// New returns a notifier; call Run to start delivering.
func New(base, token, okTopic, failTopic, link string, log *slog.Logger) *Notifier {
	return &Notifier{
		Base: base, Token: token, OKTopic: okTopic, FailTopic: failTopic, Link: link,
		HTTP: &http.Client{Timeout: 15 * time.Second}, Log: log, queue: make(chan Msg, 64),
	}
}

// Send queues m without blocking.
func (n *Notifier) Send(m Msg) {
	select {
	case n.queue <- m:
	default:
		n.Log.Warn("ntfy queue full; dropping notification", "title", m.Title)
	}
}

// Run delivers queued messages until ctx ends.
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-n.queue:
			n.publish(ctx, m)
		}
	}
}

// Drain delivers whatever is queued, for tests and shutdown.
func (n *Notifier) Drain(ctx context.Context) {
	for {
		select {
		case m := <-n.queue:
			n.publish(ctx, m)
		default:
			return
		}
	}
}

func (n *Notifier) publish(ctx context.Context, m Msg) {
	topic := m.Topic
	if topic == "" {
		topic = n.OKTopic
		if m.Level == Fail {
			topic = n.FailTopic
		}
	}
	if topic == "" || n.Base == "" {
		n.Log.Info("no ntfy topic configured; notification not sent", "title", m.Title)
		return
	}
	body := map[string]any{"topic": topic, "title": m.Title, "message": m.Body, "priority": 3, "tags": m.Tags}
	if m.Level == Fail {
		body["priority"] = 4
	}
	if len(m.Tags) == 0 {
		body["tags"] = []string{map[Level]string{OK: "white_check_mark", Fail: "rotating_light"}[m.Level]}
	}
	if n.Link != "" {
		body["message"] = m.Body + "\n\n" + n.Link
		body["click"] = n.Link
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.Base+"/", bytes.NewReader(data))
	if err != nil {
		n.Log.Warn("ntfy request build failed", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if n.Token != "" {
		req.Header.Set("Authorization", "Bearer "+n.Token)
	}
	resp, err := n.HTTP.Do(req)
	if err != nil {
		n.Log.Warn("ntfy publish failed", "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		n.Log.Warn("ntfy publish rejected", "status", resp.StatusCode, "body", string(msg))
	}
}
