// Package target defines how the engine hands a run to an executor.
package target

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

// ExecInput is everything an executor needs for one run.
type ExecInput struct {
	Run      model.Run
	Spec     model.RunSpec
	Secrets  map[string]string // canonical ref → value
	RunToken string            // sent to async HTTP targets
	LogPath  string
	EnvKey   []byte // opens SealedEnv (container targets only)
}

// Result is an executor's outcome. Exactly one of these shapes applies:
//   - Waiting: an async target accepted the run (202).
//   - DeferUntil set: the target asked to be called again later.
//   - Status set: success, failed or timeout.
//
// For failures the flags tell the engine what may be retried:
//   - Unsent: provably nothing reached the target (safe to retry for both
//     delivery modes).
//   - Ambiguous: the request may have been processed (retry only for
//     at_least_once; at_most_once becomes interrupted + page).
//   - NoRetry: a definite rejection (e.g. 4xx), never retried.
type Result struct {
	Status          model.Status
	Waiting         bool
	DeferUntil      *time.Time
	HTTPStatus      *int
	ExitCode        *int
	Output          json.RawMessage
	OutputTruncated bool
	OutputError     string
	Error           string
	Unsent          bool
	Ambiguous       bool
	NoRetry         bool
	Delivered       bool // request bytes were written at least once
}

// Executor runs one target type.
type Executor interface {
	Execute(ctx context.Context, in ExecInput) Result
}
