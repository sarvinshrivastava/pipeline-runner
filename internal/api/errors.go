package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/store"
)

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
	Status  string `json:"status,omitempty"` // the run's status, for run conflicts
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]apiError{"error": {Code: code, Message: msg}})
}

var errorTable = []struct {
	err    error
	status int
	code   string
}{
	{engine.ErrNotFound, http.StatusNotFound, "not_found"},
	{store.ErrNotFound, http.StatusNotFound, "not_found"},
	{engine.ErrJobDisabled, http.StatusConflict, "job_disabled"},
	{engine.ErrJobDone, http.StatusConflict, "job_done"},
	{engine.ErrRunTerminal, http.StatusConflict, "run_terminal"},
	{engine.ErrNotWaiting, http.StatusConflict, "run_not_waiting"},
	{engine.ErrNotAsync, http.StatusConflict, "run_not_async"},
	{engine.ErrConflict, http.StatusConflict, "completion_conflict"},
	{engine.ErrJobLimit, http.StatusConflict, "job_limit"},
	{engine.ErrPendingLimit, http.StatusTooManyRequests, "pending_limit"},
	{engine.ErrNameTaken, http.StatusConflict, "name_taken"},
	{store.ErrHasJobs, http.StatusConflict, "client_has_jobs"},
	{engine.ErrVaultDown, http.StatusServiceUnavailable, "vault_unavailable"},
	{engine.ErrBusy, http.StatusServiceUnavailable, "busy"},
	{engine.ErrShuttingDown, http.StatusServiceUnavailable, "shutting_down"},
}

// fail renders err. Unknown errors become a fixed 500 and are logged, never
// echoed to the caller.
func fail(w http.ResponseWriter, log *slog.Logger, err error) {
	failRun(w, log, err, "")
}

func failRun(w http.ResponseWriter, log *slog.Logger, err error, runStatus model.Status) {
	if fe, ok := model.AsFieldError(err); ok {
		status := http.StatusBadRequest
		if fe.Code == model.CodeForbidden {
			status = http.StatusForbidden
		}
		writeJSON(w, status, map[string]apiError{"error": {Code: fe.Code, Message: fe.Message, Field: fe.Field}})
		return
	}
	for _, e := range errorTable {
		if errors.Is(err, e.err) {
			writeJSON(w, e.status, map[string]apiError{"error": {Code: e.code, Message: e.err.Error(), Status: string(runStatus)}})
			return
		}
	}
	log.Error("unhandled API error", "err", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal error")
}
