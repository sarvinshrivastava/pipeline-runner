package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// ---------------------------------------------------------------- helpers

// readBody reads the (size-capped) body; an over-limit body is a 413.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "request body is too large")
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "invalid", "could not read body")
		return nil, false
	}
	return body, true
}

// decodeStrict rejects unknown fields so a typo never silently drops a setting.
func decodeStrict(w http.ResponseWriter, body []byte, v any) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

func (s *Server) loadJob(r *http.Request) (model.Job, error) {
	j, err := store.GetJob(r.Context(), s.d.Store.R, r.PathValue("id"))
	if err != nil {
		return j, err
	}
	if p := principalFrom(r); !p.Admin && j.ClientID != p.ClientID {
		return model.Job{}, engine.ErrNotFound // no existence leak
	}
	return j, nil
}

func (s *Server) loadRun(r *http.Request) (model.Run, error) {
	run, err := store.GetRun(r.Context(), s.d.Store.R, r.PathValue("id"))
	if err != nil {
		return run, err
	}
	if p := principalFrom(r); !p.Admin && run.ClientID != p.ClientID {
		return model.Run{}, engine.ErrNotFound
	}
	return run, nil
}

// owner resolves the client a write acts for. The bootstrap admin must name
// one; a client may only act for itself.
func (s *Server) owner(r *http.Request, clientName string) (model.Principal, error) {
	p := principalFrom(r)
	if !p.Admin || p.ClientID != "" {
		if clientName != "" && clientName != p.Name {
			return model.Principal{}, &model.FieldError{Field: "client", Code: model.CodeForbidden, Message: "a client may only act for itself"}
		}
		return p, nil
	}
	if clientName == "" {
		return model.Principal{}, &model.FieldError{Field: "client", Code: "required", Message: "the admin token must name the client a job belongs to"}
	}
	c, err := store.ClientByName(r.Context(), s.d.Store.R, clientName)
	if errors.Is(err, store.ErrNotFound) {
		return model.Principal{}, &model.FieldError{Field: "client", Code: "invalid", Message: "unknown client " + clientName}
	}
	if err != nil {
		return model.Principal{}, err
	}
	return model.PrincipalOf(c), nil
}

var waitRE = regexp.MustCompile(`^(\d{1,2})s?$`)

// parseWait reads ?wait=<1..55>[s]. ok=false means a 400 was written.
func parseWait(w http.ResponseWriter, r *http.Request) (time.Duration, bool) {
	v := r.URL.Query().Get("wait")
	if v == "" {
		return 0, true
	}
	m := waitRE.FindStringSubmatch(v)
	n := 0
	if m != nil {
		n, _ = strconv.Atoi(m[1])
	}
	if n < 1 || n > 55 {
		writeError(w, http.StatusBadRequest, "invalid_wait", "wait must be 1..55 seconds, e.g. wait=30 or wait=30s")
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

func pageParams(w http.ResponseWriter, r *http.Request) (cursor string, limit int, ok bool) {
	limit = 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "invalid", "limit must be 1..100")
			return "", 0, false
		}
		limit = n
	}
	return r.URL.Query().Get("cursor"), limit, true
}

// ---------------------------------------------------------------- health, status

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	age := s.d.Engine.LoopAge()
	status := http.StatusOK
	if !s.d.Engine.Healthy() {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": status == http.StatusOK, "loop_age_ms": age.Milliseconds()})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	vaultState := "ok"
	if err := s.d.Secrets.Ping(ctx); err != nil {
		vaultState = "unreachable"
		if errors.Is(err, vault.ErrNotConfigured) {
			vaultState = "not_configured"
		}
	}
	docker := "disabled"
	if s.d.DockerStatus != nil {
		docker = s.d.DockerStatus(ctx)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": s.d.Version,
		"engine": map[string]any{
			"healthy": s.d.Engine.Healthy(), "loop_age_ms": s.d.Engine.LoopAge().Milliseconds(),
			"unflushed_writes": s.d.Engine.Unflushed(),
		},
		"vault":  vaultState,
		"docker": docker,
	})
}

// ---------------------------------------------------------------- jobs

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	cursor, limit, ok := pageParams(w, r)
	if !ok {
		return
	}
	p := principalFrom(r)
	clientID := p.ClientID
	if p.Admin && p.ClientID == "" {
		clientID = ""
		if name := r.URL.Query().Get("client"); name != "" {
			c, err := store.ClientByName(r.Context(), s.d.Store.R, name)
			if err != nil {
				fail(w, s.d.Log, err)
				return
			}
			clientID = c.ID
		}
	}
	jobs, err := store.ListJobs(r.Context(), s.d.Store.R, clientID, cursor, limit+1)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	out := page[JobDTO]{Items: []JobDTO{}}
	for i, j := range jobs {
		if i == limit {
			out.NextCursor = jobs[limit-1].ID
			break
		}
		out.Items = append(out.Items, jobDTO(j))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in model.JobCreate
	if !decodeStrict(w, body, &in) {
		return
	}
	owner, err := s.owner(r, in.Client)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	idm, handled := s.beginIdem(w, r, "POST /v1/jobs", body, http.StatusCreated, renderJob)
	if handled {
		return
	}
	defer idm.release(r.Context())
	j, err := s.d.Engine.CreateJob(r.Context(), owner, in, idm.commit())
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	idm.succeeded()
	dto := jobDTO(j)
	dto.NextFires = engine.NextFires(j, s.d.Now(), 5)
	writeJSON(w, http.StatusCreated, dto)
}

func (s *Server) validateNewJob(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in model.JobCreate
	if !decodeStrict(w, body, &in) {
		return
	}
	owner, err := s.owner(r, in.Client)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	j, err := s.d.Engine.ValidateJob(r.Context(), owner, in)
	report := map[string]any{"ok": err == nil, "checks": []engine.Check{}}
	if err != nil {
		var fe *model.FieldError
		detail := "internal error"
		switch {
		case errors.As(err, &fe):
			detail = fe.Error()
		case errors.Is(err, engine.ErrVaultDown):
			detail = err.Error()
		default:
			s.d.Log.Error("validate job", "err", err)
		}
		report["checks"] = []engine.Check{{Name: "validation", OK: false, Detail: detail}}
	} else {
		report["checks"] = []engine.Check{{Name: "validation", OK: true}}
		report["next_fires"] = engine.NextFires(j, s.d.Now(), 5)
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.loadJob(r)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	dto := jobDTO(j)
	if j.State == model.JobActive {
		dto.NextFires = engine.NextFires(j, s.d.Now(), 5)
	}
	writeJSON(w, http.StatusOK, dto)
}

func (s *Server) patchJob(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in model.JobPatch
	if !decodeStrict(w, body, &in) {
		return
	}
	if _, err := s.loadJob(r); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	j, err := s.d.Engine.UpdateJob(r.Context(), r.PathValue("id"), in)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, jobDTO(j))
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	if _, err := s.loadJob(r); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	if err := s.d.Engine.DeleteJob(r.Context(), r.PathValue("id")); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) setEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if _, err := s.loadJob(r); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	j, err := s.d.Engine.SetEnabled(r.Context(), r.PathValue("id"), enabled)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, jobDTO(j))
}

func (s *Server) enableJob(w http.ResponseWriter, r *http.Request)  { s.setEnabled(w, r, true) }
func (s *Server) disableJob(w http.ResponseWriter, r *http.Request) { s.setEnabled(w, r, false) }

func (s *Server) validateJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.loadJob(r)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	c, err := store.GetClient(r.Context(), s.d.Store.R, j.ClientID)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	checks := s.d.Engine.CheckJob(r.Context(), j, model.PrincipalOf(c))
	ok := true
	for _, c := range checks {
		ok = ok && c.OK
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "checks": checks, "next_fires": engine.NextFires(j, s.d.Now(), 5)})
}

type invokeReq struct {
	Payload json.RawMessage `json:"payload,omitempty"`
	AsOf    *time.Time      `json:"as_of,omitempty"`
}

func (s *Server) invokeJob(w http.ResponseWriter, r *http.Request) {
	wait, ok := parseWait(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in invokeReq
	if !decodeStrict(w, body, &in) {
		return
	}
	if len(in.Payload) > model.MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", "payload is larger than 256 KiB")
		return
	}
	if _, err := s.loadJob(r); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	id := r.PathValue("id")
	idm, handled := s.beginIdem(w, r, "POST /v1/jobs/{id}/invoke:"+id, body, http.StatusAccepted, renderRun)
	if handled {
		return
	}
	defer idm.release(r.Context())
	run, err := s.d.Engine.Invoke(r.Context(), id, in.Payload, in.AsOf, idm.commit())
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	idm.succeeded()
	s.respondRun(w, r, run, wait)
}

func (s *Server) createAdhoc(w http.ResponseWriter, r *http.Request) {
	wait, ok := parseWait(w, r)
	if !ok {
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in model.AdhocReq
	if !decodeStrict(w, body, &in) {
		return
	}
	if len(in.Payload) > model.MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", "payload is larger than 256 KiB")
		return
	}
	owner, err := s.owner(r, in.Client)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	idm, handled := s.beginIdem(w, r, "POST /v1/runs", body, http.StatusAccepted, renderRun)
	if handled {
		return
	}
	defer idm.release(r.Context())
	run, err := s.d.Engine.Adhoc(r.Context(), owner, in, idm.commit())
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	idm.succeeded()
	s.respondRun(w, r, run, wait)
}

// respondRun answers 200 with the final run when ?wait saw the chain finish,
// else 202 with the newest known state. A disconnect never cancels the run.
func (s *Server) respondRun(w http.ResponseWriter, r *http.Request, run model.Run, wait time.Duration) {
	if wait > 0 {
		if final, done := s.d.Engine.Await(r.Context(), run.ID, wait); done {
			writeJSON(w, http.StatusOK, runDTO(final))
			return
		}
		if latest, err := store.ChainLatest(r.Context(), s.d.Store.R, run.ID); err == nil {
			run = latest
		}
	}
	writeJSON(w, http.StatusAccepted, runDTO(run))
}

// ---------------------------------------------------------------- runs

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	cursor, limit, ok := pageParams(w, r)
	if !ok {
		return
	}
	p := principalFrom(r)
	f := store.RunFilter{JobID: r.URL.Query().Get("job"), Status: r.URL.Query().Get("status"), Cursor: cursor, Limit: limit + 1}
	if !p.Admin || p.ClientID != "" {
		f.ClientID = p.ClientID
	}
	runs, err := store.ListRuns(r.Context(), s.d.Store.R, f)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	out := page[RunDTO]{Items: []RunDTO{}}
	for i, run := range runs {
		if i == limit {
			out.NextCursor = runs[limit-1].ID
			break
		}
		out.Items = append(out.Items, runDTO(run))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.loadRun(r)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, runDTO(run))
}

func (s *Server) runLogs(w http.ResponseWriter, r *http.Request) {
	run, err := s.loadRun(r)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	f, err := os.Open(s.d.Engine.LogPath(run.ID))
	if err != nil {
		w.WriteHeader(http.StatusOK) // no log yet (pending) is an empty log
		return
	}
	defer func() { _ = f.Close() }()
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, io.LimitReader(f, 16<<20))
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	if _, err := s.loadRun(r); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	run, accepted, err := s.d.Engine.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		failRun(w, s.d.Log, err, run.Status) // the engine's error, unchanged (B2)
		return
	}
	if accepted {
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "cancelling", "run": runDTO(run)})
		return
	}
	writeJSON(w, http.StatusOK, runDTO(run))
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	run, lease, err := s.d.Engine.Heartbeat(r.Context(), r.PathValue("id"))
	if errors.Is(err, engine.ErrNotFound) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid run token")
		return
	}
	if err != nil {
		failRun(w, s.d.Log, err, run.Status)
		return
	}
	if lease == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": run.Status})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": run.Status, "lease_expires_at": lease})
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in model.CompleteReq
	if !decodeStrict(w, body, &in) {
		return
	}
	if in.Status != string(model.StatusSuccess) && in.Status != string(model.StatusFailed) {
		writeError(w, http.StatusBadRequest, "invalid", "status must be success or failed")
		return
	}
	if len(in.Output) > model.MaxOutputBytes || len(in.Error) > 4096 {
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", "output is limited to 64 KiB and error to 4 KiB")
		return
	}
	if len(in.Output) > 0 && !json.Valid(in.Output) {
		writeError(w, http.StatusBadRequest, "invalid", "output must be JSON")
		return
	}
	run, accepted, err := s.d.Engine.Complete(r.Context(), r.PathValue("id"), in)
	if errors.Is(err, engine.ErrNotFound) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid run token")
		return
	}
	if err != nil {
		failRun(w, s.d.Log, err, run.Status)
		return
	}
	if accepted {
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted"})
		return
	}
	writeJSON(w, http.StatusOK, runDTO(run))
}

// ---------------------------------------------------------------- clients

type clientIn struct {
	Name              *string   `json:"name,omitempty"`
	Scopes            *[]string `json:"scopes,omitempty"`
	CallbackHosts     *[]string `json:"callback_hosts,omitempty"`
	SecretRefPrefixes *[]string `json:"secret_ref_prefixes,omitempty"`
	RepoPrefixes      *[]string `json:"repo_prefixes,omitempty"`
	ContainerAllowed  *bool     `json:"container_allowed,omitempty"`
	RootAllowed       *bool     `json:"root_allowed,omitempty"`
	NtfyTopic         *string   `json:"ntfy_topic,omitempty"`
}

var (
	hostRE  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)
	repoRE  = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$`)
	topicRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

func applyClient(c *model.Client, in clientIn) error {
	bad := func(field, msg string) error { return &model.FieldError{Field: field, Code: "invalid", Message: msg} }
	if in.Scopes != nil {
		for _, sc := range *in.Scopes {
			if !contains(model.AllScopes, sc) {
				return bad("scopes", "unknown scope "+sc)
			}
		}
		c.Scopes = *in.Scopes
	}
	if in.CallbackHosts != nil {
		hosts := make([]string, 0, len(*in.CallbackHosts))
		for _, h := range *in.CallbackHosts {
			h = strings.ToLower(strings.TrimSpace(h))
			if !hostRE.MatchString(h) {
				return bad("callback_hosts", "invalid hostname "+h)
			}
			hosts = append(hosts, h)
		}
		c.CallbackHosts = hosts
	}
	if in.SecretRefPrefixes != nil {
		for _, p := range *in.SecretRefPrefixes {
			if !vault.ValidPrefix(p) {
				return bad("secret_ref_prefixes", "prefix "+p+` must end in "/" or be a full Folder/KEY`)
			}
		}
		c.SecretRefPrefixes = *in.SecretRefPrefixes
	}
	if in.RepoPrefixes != nil {
		for _, p := range *in.RepoPrefixes {
			if !repoRE.MatchString(p) {
				return bad("repo_prefixes", "repo prefix "+p+" must be owner/repo")
			}
		}
		c.RepoPrefixes = *in.RepoPrefixes
	}
	if in.ContainerAllowed != nil {
		c.ContainerAllowed = *in.ContainerAllowed
	}
	if in.RootAllowed != nil {
		c.RootAllowed = *in.RootAllowed
	}
	if in.NtfyTopic != nil {
		if *in.NtfyTopic != "" && !topicRE.MatchString(*in.NtfyTopic) {
			return bad("ntfy_topic", "topic must match [A-Za-z0-9_-]{1,64}")
		}
		c.NtfyTopic = *in.NtfyTopic
	}
	return nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (s *Server) listClients(w http.ResponseWriter, r *http.Request) {
	cs, err := store.ListClients(r.Context(), s.d.Store.R)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	out := make([]ClientDTO, 0, len(cs))
	for _, c := range cs {
		out = append(out, clientDTO(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) createClient(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in clientIn
	if !decodeStrict(w, body, &in) {
		return
	}
	if in.Name == nil || !model.ClientNameRE.MatchString(*in.Name) {
		fail(w, s.d.Log, &model.FieldError{Field: "name", Code: "invalid", Message: "name must match [a-z0-9-]{1,32}"})
		return
	}
	now := s.d.Now()
	c := model.Client{ID: newID(), Name: *in.Name, CreatedAt: now, UpdatedAt: now}
	if err := applyClient(&c, in); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	tok, hash, err := secure.NewAPIToken(c.Name)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	c.TokenHash = hash
	if err := store.CreateClient(r.Context(), s.d.Store.W, c); err != nil {
		if store.IsUniqueViolation(err) {
			writeError(w, http.StatusConflict, "name_taken", "a client with this name already exists")
			return
		}
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"client": clientDTO(c), "token": tok})
}

func (s *Server) patchClient(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in clientIn
	if !decodeStrict(w, body, &in) {
		return
	}
	if in.Name != nil {
		fail(w, s.d.Log, &model.FieldError{Field: "name", Code: "invalid", Message: "a client cannot be renamed"})
		return
	}
	c, err := store.GetClient(r.Context(), s.d.Store.R, r.PathValue("id"))
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	if err := applyClient(&c, in); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	c.UpdatedAt = s.d.Now()
	if err := store.UpdateClient(r.Context(), s.d.Store.W, c); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, clientDTO(c))
}

func (s *Server) deleteClient(w http.ResponseWriter, r *http.Request) {
	if err := store.DeleteClient(r.Context(), s.d.Store.W, r.PathValue("id")); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) rotateClient(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var in struct {
		GraceSec int `json:"grace_sec"`
	}
	if !decodeStrict(w, body, &in) {
		return
	}
	if in.GraceSec < 0 || in.GraceSec > 3600 {
		fail(w, s.d.Log, &model.FieldError{Field: "grace_sec", Code: "invalid", Message: "grace_sec must be 0..3600"})
		return
	}
	c, err := store.GetClient(r.Context(), s.d.Store.R, r.PathValue("id"))
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	tok, hash, err := secure.NewAPIToken(c.Name)
	if err != nil {
		fail(w, s.d.Log, err)
		return
	}
	now := s.d.Now()
	var grace *time.Time
	if in.GraceSec > 0 {
		g := now.Add(time.Duration(in.GraceSec) * time.Second)
		grace = &g
	}
	if err := store.RotateClient(r.Context(), s.d.Store.W, c.ID, hash, grace, now); err != nil {
		fail(w, s.d.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": tok, "previous_valid_until": grace})
}

// ---------------------------------------------------------------- secrets

func (s *Server) listSecrets(w http.ResponseWriter, r *http.Request) {
	keys, err := s.d.Secrets.List(r.Context(), false)
	if err != nil {
		fail(w, s.d.Log, engine.ErrVaultDown)
		return
	}
	p := principalFrom(r)
	names := []string{}
	for _, k := range keys {
		ref, err := vault.ParseRef(k)
		if err != nil {
			continue
		}
		if (p.Admin && p.ClientID == "") || vault.Permitted(ref, p.SecretRefPrefixes) {
			names = append(names, k)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"names": names})
}
