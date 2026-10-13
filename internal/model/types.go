// Package model defines jobs, runs, clients and their validation.
package model

import (
	"encoding/json"
	"slices"
	"time"
)

// Trigger types. "delay" is resolved to "at" at validation time.
const (
	TriggerCron   = "cron"
	TriggerAt     = "at"
	TriggerDelay  = "delay"
	TriggerInvoke = "invoke"
)

// Target types.
const (
	TargetHTTP      = "http"
	TargetContainer = "container"
	TargetInline    = "inline"
)

// Job states.
const (
	JobActive   = "active"
	JobDisabled = "disabled"
	JobDone     = "done"
)

// Run trigger kinds.
const (
	KindSchedule = "schedule"
	KindInvoke   = "invoke"
	KindRetry    = "retry"
	KindDefer    = "defer"
	KindAdhoc    = "adhoc"
)

// Status is a run status.
type Status string

// Run statuses.
const (
	StatusPending     Status = "pending"
	StatusRunning     Status = "running"
	StatusWaiting     Status = "waiting"
	StatusSuccess     Status = "success"
	StatusFailed      Status = "failed"
	StatusTimeout     Status = "timeout"
	StatusMissed      Status = "missed"
	StatusSkipped     Status = "skipped"
	StatusInterrupted Status = "interrupted"
	StatusCancelled   Status = "cancelled"
	StatusDeferred    Status = "deferred"
)

// Terminal reports whether no further transition is possible.
func (s Status) Terminal() bool {
	switch s {
	case StatusPending, StatusRunning, StatusWaiting:
		return false
	}
	return true
}

// Delivery modes.
const (
	AtLeastOnce = "at_least_once"
	AtMostOnce  = "at_most_once"
)

// Overlap policies.
const (
	OverlapSkip  = "skip"
	OverlapQueue = "queue"
	OverlapAllow = "allow"
)

// AdhocJobName labels runs that belong to no job.
const AdhocJobName = "_adhoc"

// Trigger is a stored, validated trigger. A "delay" input is stored as "at".
type Trigger struct {
	Type     string     `json:"type"`
	Cron     string     `json:"cron,omitempty"`
	Timezone string     `json:"timezone,omitempty"`
	At       *time.Time `json:"at,omitempty"`
}

// RetryPolicy is an opt-in retry with exponential backoff.
type RetryPolicy struct {
	MaxAttempts   int   `json:"max_attempts"`
	BackoffSec    int64 `json:"backoff_sec"`
	MaxBackoffSec int64 `json:"max_backoff_sec"`
}

// Policy is a fully defaulted job policy.
type Policy struct {
	TimeoutSec       int64       `json:"timeout_sec"`
	MaxLateSec       int64       `json:"max_late_sec"`
	LateWarnSec      int64       `json:"late_warn_sec"`
	JitterSec        int64       `json:"jitter_sec"`
	Delivery         string      `json:"delivery"`
	Retry            RetryPolicy `json:"retry"`
	ConcurrencyKey   string      `json:"concurrency_key,omitempty"`
	ConcurrencyLimit int         `json:"concurrency_limit,omitempty"`
	Overlap          string      `json:"overlap"` // skip|queue|allow
	MaxDeferSec      int64       `json:"max_defer_sec"`
	NotifyOnSuccess  bool        `json:"notify_on_success"`
}

// HTTPAuth names vault references; values are resolved at fire time.
type HTTPAuth struct {
	BearerRef string `json:"bearer_ref,omitempty"`
	HMACRef   string `json:"hmac_ref,omitempty"`
}

// HTTPTarget calls a service endpoint.
type HTTPTarget struct {
	Method   string            `json:"method"`
	URL      string            `json:"url"`
	Headers  map[string]string `json:"headers,omitempty"`
	Body     json.RawMessage   `json:"body,omitempty"`
	Mode     string            `json:"mode"` // sync|async
	LeaseSec int64             `json:"lease_sec,omitempty"`
	Auth     HTTPAuth          `json:"auth"`
}

// ContainerTarget runs a command from a git repository in a throwaway container.
type ContainerTarget struct {
	RepoURL       string            `json:"repo_url,omitempty"`
	Branch        string            `json:"branch,omitempty"`
	OutputBranch  string            `json:"output_branch,omitempty"`
	Image         string            `json:"image"`
	Command       string            `json:"command"`
	CommitResults bool              `json:"commit_results"`
	MemoryMB      int64             `json:"memory_mb"`
	CPUs          float64           `json:"cpus"`
	User          string            `json:"user"`
	Network       string            `json:"network"` // none|egress
	Env           SealedEnv         `json:"env"`
	SecretRefs    map[string]string `json:"secret_refs,omitempty"`
	GitTokenRef   string            `json:"git_token_ref,omitempty"`
}

// InlineTarget runs a script body in a throwaway container.
type InlineTarget struct {
	Image       string            `json:"image"`
	Script      string            `json:"script"`
	Interpreter string            `json:"interpreter"`
	MemoryMB    int64             `json:"memory_mb"`
	CPUs        float64           `json:"cpus"`
	User        string            `json:"user"`
	Network     string            `json:"network"`
	Env         SealedEnv         `json:"env"`
	SecretRefs  map[string]string `json:"secret_refs,omitempty"`
}

// Target is exactly one of the typed targets.
type Target struct {
	Type      string           `json:"type"`
	HTTP      *HTTPTarget      `json:"http,omitempty"`
	Container *ContainerTarget `json:"container,omitempty"`
	Inline    *InlineTarget    `json:"inline,omitempty"`
}

// RunSpec is the snapshot a run executes with (a job revision or an ad-hoc spec).
type RunSpec struct {
	Target Target `json:"target"`
	Policy Policy `json:"policy"`
}

// Job is a stored job.
type Job struct {
	ID               string
	ClientID         string
	Name             string
	State            string
	Trigger          Trigger
	Target           Target
	Policy           Policy
	NSKey            string // "<client_id>:<concurrency_key>" or ""
	Rev              int
	LastNominalFor   *time.Time
	ConsecutiveSkips int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Spec returns the job's current snapshot.
func (j Job) Spec() RunSpec { return RunSpec{Target: j.Target, Policy: j.Policy} }

// Run is one execution attempt.
type Run struct {
	ID              string
	JobID           string // "" for ad-hoc
	JobName         string
	ClientID        string
	Kind            string
	Attempt         int
	ParentRunID     string
	ChainRootID     string
	Status          Status
	NominalFor      *time.Time
	ScheduledFor    time.Time
	OriginFor       time.Time
	ContextFor      time.Time
	LatenessRef     time.Time
	Rev             int // 0 for ad-hoc
	AdhocSpec       *RunSpec
	Payload         json.RawMessage
	StartedAt       *time.Time
	FinishedAt      *time.Time
	LateMs          *int64
	LeaseExpiresAt  *time.Time
	DeadlineAt      *time.Time
	LeaseGraceUsed  bool
	EarlyResult     *CompleteReq
	DeliveryAttempt bool
	HTTPStatus      *int
	ExitCode        *int
	Output          json.RawMessage
	OutputTruncated bool
	OutputError     string
	Error           string
	ContainerName   string
	CreatedAt       time.Time
}

// CompleteReq is an async completion reported by a target.
type CompleteReq struct {
	Status string          `json:"status"` // success|failed
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Scopes.
const (
	ScopeAdmin      = "admin"
	ScopeJobsRead   = "jobs:read"
	ScopeJobsWrite  = "jobs:write"
	ScopeRunsInvoke = "runs:invoke"
	ScopeRunsAdhoc  = "runs:adhoc"
)

// AllScopes lists every grantable scope.
var AllScopes = []string{ScopeAdmin, ScopeJobsRead, ScopeJobsWrite, ScopeRunsInvoke, ScopeRunsAdhoc}

// Client is an API client.
type Client struct {
	ID                string
	Name              string
	TokenHash         []byte
	PrevTokenHash     []byte
	PrevValidUntil    *time.Time
	Scopes            []string
	CallbackHosts     []string
	SecretRefPrefixes []string
	RepoPrefixes      []string
	ContainerAllowed  bool
	RootAllowed       bool
	NtfyTopic         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Principal is the authenticated caller, or the client a job belongs to.
type Principal struct {
	ClientID          string
	Name              string
	Admin             bool
	Scopes            []string
	CallbackHosts     []string
	SecretRefPrefixes []string
	RepoPrefixes      []string
	ContainerAllowed  bool
	RootAllowed       bool
	NtfyTopic         string
}

// PrincipalOf converts a client row into the principal its jobs act as.
func PrincipalOf(c Client) Principal {
	return Principal{
		ClientID: c.ID, Name: c.Name, Scopes: c.Scopes,
		CallbackHosts: c.CallbackHosts, SecretRefPrefixes: c.SecretRefPrefixes, RepoPrefixes: c.RepoPrefixes,
		ContainerAllowed: c.ContainerAllowed, RootAllowed: c.RootAllowed, NtfyTopic: c.NtfyTopic,
		Admin: slices.Contains(c.Scopes, ScopeAdmin),
	}
}

// HasScope reports whether the principal may use scope. Admin implies all.
func (p Principal) HasScope(scope string) bool {
	return p.Admin || slices.Contains(p.Scopes, scope)
}
