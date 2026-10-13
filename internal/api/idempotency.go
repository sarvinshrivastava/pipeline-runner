package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

// idem is one claimed Idempotency-Key for the duration of a request.
type idem struct {
	s        *Server
	clientID string
	route    string
	key      string
	status   int
	render   func(any) any
	done     bool
}

// bodyHash canonicalises the decoded body (map keys sorted, numbers exact).
func bodyHash(body []byte) ([]byte, error) {
	var v any
	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
	}
	canon, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(canon)
	return h[:], nil
}

// beginIdem claims the key, or answers the request itself (replay or 409) and
// returns handled=true. With no key header it returns (nil, false).
func (s *Server) beginIdem(w http.ResponseWriter, r *http.Request, route string, body []byte, status int, render func(any) any) (*idem, bool) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return nil, false
	}
	if len(key) > 128 {
		writeError(w, http.StatusBadRequest, "invalid", "Idempotency-Key is longer than 128 characters")
		return nil, true
	}
	hash, err := bodyHash(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return nil, true
	}
	p := principalFrom(r)
	clientID := p.ClientID
	if isBootstrap(p) {
		clientID = "_admin"
	}
	row, err := store.IdemBegin(r.Context(), s.d.Store.W, clientID, route, key, hash, s.d.Now())
	if err != nil {
		fail(w, s.d.Log, err)
		return nil, true
	}
	if row != nil {
		switch {
		case !row.Done:
			writeError(w, http.StatusConflict, "idempotency_in_progress", "a request with this Idempotency-Key is still in progress")
		case !bytes.Equal(row.BodyHash, hash):
			writeError(w, http.StatusConflict, "idempotency_key_reused", "this Idempotency-Key was used with a different body")
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(row.Status)
			_, _ = w.Write(row.Response) //nolint:gosec // our own stored JSON, served as application/json with nosniff
		}
		return nil, true
	}
	return &idem{s: s, clientID: clientID, route: route, key: key, status: status, render: render}, false
}

// commit stores the response inside the engine's write transaction.
func (i *idem) commit() engine.Commit {
	if i == nil {
		return nil
	}
	return func(ctx context.Context, q store.Querier, result any) error {
		body, err := json.Marshal(i.render(result))
		if err != nil {
			return err
		}
		return store.IdemFinish(ctx, q, i.clientID, i.route, i.key, i.status, append(body, '\n'))
	}
}

// succeeded marks the claim as stored; call only after the write committed.
func (i *idem) succeeded() {
	if i != nil {
		i.done = true
	}
}

// release deletes an unfinished claim (any non-stored outcome).
func (i *idem) release(ctx context.Context) {
	if i == nil || i.done {
		return
	}
	if err := store.IdemAbort(context.WithoutCancel(ctx), i.s.d.Store.W, i.clientID, i.route, i.key); err != nil {
		i.s.d.Log.Error("release idempotency key", "err", err)
	}
}

func renderJob(v any) any { return jobDTO(v.(model.Job)) }
func renderRun(v any) any { return runDTO(v.(model.Run)) }
