package api

import (
	"encoding/json"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

// JobDTO is a job as the API shows it. Container env shows names only.
type JobDTO struct {
	ID        string          `json:"id"`
	ClientID  string          `json:"client_id"`
	Name      string          `json:"name"`
	State     string          `json:"state"`
	Trigger   model.Trigger   `json:"trigger"`
	Target    json.RawMessage `json:"target"`
	Policy    model.Policy    `json:"policy"`
	Rev       int             `json:"rev"`
	NextFires []time.Time     `json:"next_fires,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func jobDTO(j model.Job) JobDTO {
	return JobDTO{ID: j.ID, ClientID: j.ClientID, Name: j.Name, State: j.State, Trigger: j.Trigger,
		Target: renderTarget(j.Target), Policy: j.Policy, Rev: j.Rev, CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt}
}

// renderTarget replaces each sealed env with its key list.
func renderTarget(t model.Target) json.RawMessage {
	raw, _ := json.Marshal(t)
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return raw
	}
	for _, kind := range []string{"container", "inline"} {
		sub, ok := m[kind].(map[string]any)
		if !ok {
			continue
		}
		keys := []any{}
		if env, ok := sub["env"].(map[string]any); ok {
			if k, ok := env["keys"].([]any); ok {
				keys = k
			}
		}
		delete(sub, "env")
		sub["env_keys"] = keys
	}
	out, _ := json.Marshal(m)
	return out
}

// RunDTO is a run as the API shows it.
type RunDTO struct {
	ID              string          `json:"id"`
	JobID           string          `json:"job_id,omitempty"`
	JobName         string          `json:"job_name"`
	ClientID        string          `json:"client_id"`
	Trigger         string          `json:"trigger"`
	Attempt         int             `json:"attempt"`
	ParentRunID     string          `json:"parent_run_id,omitempty"`
	ChainRootID     string          `json:"chain_root_id"`
	Status          model.Status    `json:"status"`
	NominalFor      *time.Time      `json:"nominal_for,omitempty"`
	ScheduledFor    time.Time       `json:"scheduled_for"`
	ContextFor      time.Time       `json:"context_for"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	LateMs          *int64          `json:"late_ms,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	HTTPStatus      *int            `json:"http_status,omitempty"`
	ExitCode        *int            `json:"exit_code,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	OutputTruncated bool            `json:"output_truncated,omitempty"`
	OutputError     string          `json:"output_error,omitempty"`
	Error           string          `json:"error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

func runDTO(r model.Run) RunDTO {
	return RunDTO{ID: r.ID, JobID: r.JobID, JobName: r.JobName, ClientID: r.ClientID, Trigger: r.Kind, Attempt: r.Attempt,
		ParentRunID: r.ParentRunID, ChainRootID: r.ChainRootID, Status: r.Status, NominalFor: r.NominalFor,
		ScheduledFor: r.ScheduledFor, ContextFor: r.ContextFor, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		LateMs: r.LateMs, LeaseExpiresAt: r.LeaseExpiresAt, HTTPStatus: r.HTTPStatus, ExitCode: r.ExitCode,
		Payload: r.Payload, Output: r.Output, OutputTruncated: r.OutputTruncated, OutputError: r.OutputError,
		Error: r.Error, CreatedAt: r.CreatedAt}
}

// ClientDTO never includes token hashes.
type ClientDTO struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Scopes            []string  `json:"scopes"`
	CallbackHosts     []string  `json:"callback_hosts"`
	SecretRefPrefixes []string  `json:"secret_ref_prefixes"`
	RepoPrefixes      []string  `json:"repo_prefixes"`
	ContainerAllowed  bool      `json:"container_allowed"`
	RootAllowed       bool      `json:"root_allowed"`
	NtfyTopic         string    `json:"ntfy_topic,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func clientDTO(c model.Client) ClientDTO {
	return ClientDTO{ID: c.ID, Name: c.Name, Scopes: nonNil(c.Scopes), CallbackHosts: nonNil(c.CallbackHosts),
		SecretRefPrefixes: nonNil(c.SecretRefPrefixes), RepoPrefixes: nonNil(c.RepoPrefixes), ContainerAllowed: c.ContainerAllowed,
		RootAllowed: c.RootAllowed, NtfyTopic: c.NtfyTopic, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
}

// page is a cursor page.
type page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}
