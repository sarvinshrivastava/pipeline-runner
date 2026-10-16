package model

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/schedule"
	"github.com/sarvinshrivastava/pipeline-runner/internal/vault"
)

// Limits shared with the API layer.
const (
	MaxPayloadBytes = 256 << 10
	MaxScriptBytes  = 64 << 10
	MaxOutputBytes  = 64 << 10
	MaxURLBytes     = 2 << 10
	MaxHeaders      = 20
	MaxEnvVars      = 50
	MaxEnvBytes     = 64 << 10
	maxDelaySec     = 10 * 365 * 24 * 3600
	gapSamples      = 50
)

// ---------------------------------------------------------------- inputs

// TriggerIn is the trigger as a client sends it.
type TriggerIn struct {
	Type     string     `json:"type"`
	Cron     string     `json:"cron,omitempty"`
	Timezone string     `json:"timezone,omitempty"`
	At       *time.Time `json:"at,omitempty"`
	DelaySec *int64     `json:"delay_sec,omitempty"`
}

// RetryIn is an optional retry policy.
type RetryIn struct {
	MaxAttempts   *int   `json:"max_attempts,omitempty"`
	BackoffSec    *int64 `json:"backoff_sec,omitempty"`
	MaxBackoffSec *int64 `json:"max_backoff_sec,omitempty"`
}

// PolicyIn is an optional policy; nil fields take defaults.
type PolicyIn struct {
	TimeoutSec       *int64   `json:"timeout_sec,omitempty"`
	MaxLateSec       *int64   `json:"max_late_sec,omitempty"`
	LateWarnSec      *int64   `json:"late_warn_sec,omitempty"`
	JitterSec        *int64   `json:"jitter_sec,omitempty"`
	Delivery         *string  `json:"delivery,omitempty"`
	Retry            *RetryIn `json:"retry,omitempty"`
	ConcurrencyKey   *string  `json:"concurrency_key,omitempty"`
	ConcurrencyLimit *int     `json:"concurrency_limit,omitempty"`
	Overlap          *string  `json:"overlap,omitempty"`
	MaxDeferSec      *int64   `json:"max_defer_sec,omitempty"`
	NotifyOnSuccess  *bool    `json:"notify_on_success,omitempty"`
	Priority         *string  `json:"priority,omitempty"`
}

// HTTPIn is an HTTP target as sent.
type HTTPIn struct {
	Method   string            `json:"method,omitempty"`
	URL      string            `json:"url"`
	Headers  map[string]string `json:"headers,omitempty"`
	Body     json.RawMessage   `json:"body,omitempty"`
	Mode     string            `json:"mode,omitempty"`
	LeaseSec *int64            `json:"lease_sec,omitempty"`
	Auth     HTTPAuth          `json:"auth"`
}

// ContainerIn is a container target as sent. Env is plaintext and never stored.
type ContainerIn struct {
	RepoURL       string            `json:"repo_url,omitempty"`
	Branch        string            `json:"branch,omitempty"`
	OutputBranch  string            `json:"output_branch,omitempty"`
	Image         string            `json:"image"`
	Command       string            `json:"command"`
	CommitResults *bool             `json:"commit_results,omitempty"`
	MemoryMB      *int64            `json:"memory_mb,omitempty"`
	CPUs          *float64          `json:"cpus,omitempty"`
	User          string            `json:"user,omitempty"`
	Network       string            `json:"network,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	SecretRefs    map[string]string `json:"secret_refs,omitempty"`
	GitTokenRef   string            `json:"git_token_ref,omitempty"`
}

// InlineIn is an inline target as sent.
type InlineIn struct {
	Image       string            `json:"image"`
	Script      string            `json:"script"`
	Interpreter string            `json:"interpreter,omitempty"`
	MemoryMB    *int64            `json:"memory_mb,omitempty"`
	CPUs        *float64          `json:"cpus,omitempty"`
	User        string            `json:"user,omitempty"`
	Network     string            `json:"network,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	SecretRefs  map[string]string `json:"secret_refs,omitempty"`
}

// TargetIn is a target as sent.
type TargetIn struct {
	Type      string       `json:"type"`
	HTTP      *HTTPIn      `json:"http,omitempty"`
	Container *ContainerIn `json:"container,omitempty"`
	Inline    *InlineIn    `json:"inline,omitempty"`
}

// JobCreate is the body of POST /v1/jobs.
type JobCreate struct {
	Client  string    `json:"client,omitempty"` // admin only: the owning client's name
	Name    string    `json:"name"`
	Trigger TriggerIn `json:"trigger"`
	Target  TargetIn  `json:"target"`
	Policy  *PolicyIn `json:"policy,omitempty"`
	Enabled *bool     `json:"enabled,omitempty"`
}

// JobPatch replaces whichever top-level objects are present.
type JobPatch struct {
	Name    *string    `json:"name,omitempty"`
	Trigger *TriggerIn `json:"trigger,omitempty"`
	Target  *TargetIn  `json:"target,omitempty"`
	Policy  *PolicyIn  `json:"policy,omitempty"`
}

// AdhocReq is the body of POST /v1/runs.
type AdhocReq struct {
	Client  string          `json:"client,omitempty"`
	Target  TargetIn        `json:"target"`
	Policy  *PolicyIn       `json:"policy,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// VCtx carries what validation needs besides the input.
type VCtx struct {
	Now               time.Time
	Principal         Principal // the owning client, never the bootstrap admin
	EnvKey            []byte
	ContainersEnabled bool
}

// ---------------------------------------------------------------- patterns

var (
	// JobNameRE and ClientNameRE are the name formats for jobs and clients.
	JobNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	// ClientNameRE is the client name format.
	ClientNameRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	imageRE       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-/:@]{0,254}$`)
	gitRefRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-/]{0,254}$`)
	envNameRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	headerNameRE  = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]{1,64}$`)
	keyRE         = regexp.MustCompile(`^[A-Za-z0-9:._-]{1,64}$`)
	userRE        = regexp.MustCompile(`^\d{1,5}:\d{1,5}$`)
	interpreterRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,63}$`)
)

var reservedHeaders = map[string]bool{
	"authorization": true, "host": true, "content-length": true, "content-type": true,
	"cookie": true, "transfer-encoding": true, "connection": true, "te": true, "upgrade": true,
}

// ---------------------------------------------------------------- entry points

// ValidateJobCreate turns a create request into a job (ID and timestamps unset).
func ValidateJobCreate(in JobCreate, c VCtx) (Job, error) {
	if !JobNameRE.MatchString(in.Name) {
		return Job{}, fieldErr("name", "invalid", "must be lowercase alphanumeric with dashes, 1-63 chars")
	}
	target, err := validateTarget(in.Target, nil, c)
	if err != nil {
		return Job{}, err
	}
	trigType := in.Trigger.Type
	policy, err := validatePolicy(in.Policy, trigType, target)
	if err != nil {
		return Job{}, err
	}
	trig, err := validateTrigger(in.Trigger, policy, target.Type, c.Now)
	if err != nil {
		return Job{}, err
	}
	state := JobActive
	if in.Enabled != nil && !*in.Enabled {
		state = JobDisabled
	}
	return Job{
		ClientID: c.Principal.ClientID, Name: in.Name, State: state,
		Trigger: trig, Target: target, Policy: policy, NSKey: NSKey(c.Principal.ClientID, policy.ConcurrencyKey),
	}, nil
}

// ValidatePatch applies a patch to cur. triggerChanged follows the plan's
// cursor rule: type, cron expression, a cron job's timezone, or the instant.
func ValidatePatch(cur Job, in JobPatch, c VCtx) (next Job, triggerChanged bool, err error) {
	next = cur
	if in.Name != nil {
		if !JobNameRE.MatchString(*in.Name) {
			return Job{}, false, fieldErr("name", "invalid", "must be lowercase alphanumeric with dashes, 1-63 chars")
		}
		next.Name = *in.Name
	}
	if in.Target != nil {
		if next.Target, err = validateTarget(*in.Target, &cur.Target, c); err != nil {
			return Job{}, false, err
		}
	}
	trigType := cur.Trigger.Type
	if in.Trigger != nil {
		trigType = in.Trigger.Type
	}
	if in.Policy != nil || in.Target != nil || in.Trigger != nil {
		// A new trigger or target can change policy defaults (overlap).
		pin := in.Policy
		if pin == nil {
			pin = policyAsInput(cur.Policy)
			// An overlap still at the old default follows the new default.
			if cur.Policy.Overlap == DefaultPolicy(cur.Trigger.Type, cur.Target).Overlap {
				pin.Overlap = nil
			}
		}
		if next.Policy, err = validatePolicy(pin, trigType, next.Target); err != nil {
			return Job{}, false, err
		}
	}
	if in.Trigger != nil {
		if next.Trigger, err = validateTrigger(*in.Trigger, next.Policy, next.Target.Type, c.Now); err != nil {
			return Job{}, false, err
		}
		triggerChanged = !sameTrigger(cur.Trigger, next.Trigger)
	} else if next.Trigger.Type == TriggerCron {
		// Re-check cron limits against a possibly new target/policy.
		if _, err = validateTrigger(TriggerIn{Type: TriggerCron, Cron: cur.Trigger.Cron, Timezone: cur.Trigger.Timezone}, next.Policy, next.Target.Type, c.Now); err != nil {
			return Job{}, false, err
		}
	} else if next.Trigger.Type == TriggerAt && next.Policy.JitterSec >= next.Policy.MaxLateSec {
		return Job{}, false, fieldErr("policy.jitter_sec", "invalid", "must be less than max_late_sec")
	}
	next.NSKey = NSKey(cur.ClientID, next.Policy.ConcurrencyKey)
	return next, triggerChanged, nil
}

// ValidateAdhoc validates an ad-hoc run's spec.
func ValidateAdhoc(in AdhocReq, c VCtx) (RunSpec, error) {
	target, err := validateTarget(in.Target, nil, c)
	if err != nil {
		return RunSpec{}, err
	}
	policy, err := validatePolicy(in.Policy, TriggerInvoke, target)
	if err != nil {
		return RunSpec{}, err
	}
	return RunSpec{Target: target, Policy: policy}, nil
}

// NSKey namespaces a concurrency key by client.
func NSKey(clientID, key string) string {
	if key == "" {
		return ""
	}
	return clientID + ":" + key
}

func sameTrigger(a, b Trigger) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TriggerCron:
		return a.Cron == b.Cron && a.Timezone == b.Timezone
	case TriggerAt:
		return a.At != nil && b.At != nil && a.At.Equal(*b.At)
	}
	return true
}

// ---------------------------------------------------------------- trigger

func validateTrigger(in TriggerIn, p Policy, targetType string, now time.Time) (Trigger, error) {
	switch in.Type {
	case TriggerInvoke:
		return Trigger{Type: TriggerInvoke}, nil

	case TriggerAt, TriggerDelay:
		var at time.Time
		if in.Type == TriggerDelay {
			if in.DelaySec == nil || *in.DelaySec < 0 || *in.DelaySec > maxDelaySec {
				return Trigger{}, fieldErr("trigger.delay_sec", "invalid", "must be between 0 and 10 years in seconds")
			}
			at = now.Add(time.Duration(*in.DelaySec) * time.Second)
		} else {
			if in.At == nil {
				return Trigger{}, fieldErr("trigger.at", "required", "is required for an at trigger")
			}
			at = *in.At
		}
		at = at.UTC().Truncate(time.Millisecond)
		if at.Before(now.Add(-time.Duration(p.MaxLateSec) * time.Second)) {
			return Trigger{}, fieldErr("trigger.at", "at_in_past", "is earlier than now minus max_late_sec")
		}
		if p.JitterSec >= p.MaxLateSec {
			return Trigger{}, fieldErr("policy.jitter_sec", "invalid", "must be less than max_late_sec")
		}
		return Trigger{Type: TriggerAt, At: &at}, nil

	case TriggerCron:
		s, err := schedule.Parse(in.Cron, in.Timezone)
		if err != nil {
			return Trigger{}, fieldErr("trigger.cron", "invalid", "%s", err.Error())
		}
		if s.Next(now).IsZero() {
			return Trigger{}, fieldErr("trigger.cron", "cron_never_fires", "has no occurrence in the next 5 years")
		}
		minGap := 60 * time.Second
		if targetType == TargetHTTP {
			minGap = time.Second
		}
		gap := s.MinGap(now, gapSamples)
		if gap != 0 && gap < minGap {
			return Trigger{}, fieldErr("trigger.cron", "too_frequent", "fires every %s; the minimum for %s targets is %s", gap, targetType, minGap)
		}
		jitter := time.Duration(p.JitterSec) * time.Second
		if p.JitterSec > 0 && ((gap != 0 && jitter >= gap) || p.JitterSec >= p.MaxLateSec) {
			return Trigger{}, fieldErr("policy.jitter_sec", "invalid", "must be less than both the schedule interval and max_late_sec")
		}
		tz := in.Timezone
		if tz == "" {
			tz = "UTC"
		}
		return Trigger{Type: TriggerCron, Cron: strings.TrimSpace(in.Cron), Timezone: tz}, nil
	}
	return Trigger{}, fieldErr("trigger.type", "invalid", "must be cron, at, delay or invoke")
}

// ---------------------------------------------------------------- policy

// DefaultPolicy is the policy used when nothing is given.
func DefaultPolicy(trigType string, t Target) Policy {
	overlap := OverlapSkip
	if trigType == TriggerInvoke {
		overlap = OverlapAllow
	}
	if t.Container != nil && t.Container.RepoURL != "" {
		overlap = OverlapQueue
	}
	return Policy{
		TimeoutSec: 900, MaxLateSec: 600, LateWarnSec: 60, Delivery: AtLeastOnce,
		Retry:   RetryPolicy{MaxAttempts: 1, BackoffSec: 30, MaxBackoffSec: 3600},
		Overlap: overlap, MaxDeferSec: 86400, Priority: PriorityNormal,
	}
}

func validatePolicy(in *PolicyIn, trigType string, t Target) (Policy, error) {
	p := DefaultPolicy(trigType, t)
	if in == nil {
		return p, nil
	}
	set64 := func(dst *int64, v *int64, field string, lo, hi int64) error {
		if v == nil {
			return nil
		}
		if *v < lo || *v > hi {
			return fieldErr("policy."+field, "invalid", "must be between %d and %d", lo, hi)
		}
		*dst = *v
		return nil
	}
	for _, f := range []struct {
		dst    *int64
		v      *int64
		name   string
		lo, hi int64
	}{
		{&p.TimeoutSec, in.TimeoutSec, "timeout_sec", 1, 86400},
		{&p.MaxLateSec, in.MaxLateSec, "max_late_sec", 1, 7 * 86400},
		{&p.LateWarnSec, in.LateWarnSec, "late_warn_sec", 1, 7 * 86400},
		{&p.JitterSec, in.JitterSec, "jitter_sec", 0, 86400},
		{&p.MaxDeferSec, in.MaxDeferSec, "max_defer_sec", 0, 30 * 86400},
	} {
		if err := set64(f.dst, f.v, f.name, f.lo, f.hi); err != nil {
			return Policy{}, err
		}
	}
	if in.Delivery != nil {
		if *in.Delivery != AtLeastOnce && *in.Delivery != AtMostOnce {
			return Policy{}, fieldErr("policy.delivery", "invalid", "must be at_least_once or at_most_once")
		}
		p.Delivery = *in.Delivery
	}
	if r := in.Retry; r != nil {
		if r.MaxAttempts != nil {
			if *r.MaxAttempts < 1 || *r.MaxAttempts > 10 {
				return Policy{}, fieldErr("policy.retry.max_attempts", "invalid", "must be between 1 and 10")
			}
			p.Retry.MaxAttempts = *r.MaxAttempts
		}
		if err := set64(&p.Retry.BackoffSec, r.BackoffSec, "retry.backoff_sec", 1, 86400); err != nil {
			return Policy{}, err
		}
		if err := set64(&p.Retry.MaxBackoffSec, r.MaxBackoffSec, "retry.max_backoff_sec", 1, 7*86400); err != nil {
			return Policy{}, err
		}
		if p.Retry.MaxBackoffSec < p.Retry.BackoffSec {
			return Policy{}, fieldErr("policy.retry.max_backoff_sec", "invalid", "must be at least backoff_sec")
		}
	}
	if in.ConcurrencyKey != nil && *in.ConcurrencyKey != "" {
		if !keyRE.MatchString(*in.ConcurrencyKey) {
			return Policy{}, fieldErr("policy.concurrency_key", "invalid", "must match [A-Za-z0-9:._-]{1,64}")
		}
		p.ConcurrencyKey, p.ConcurrencyLimit = *in.ConcurrencyKey, 1
	}
	if in.ConcurrencyLimit != nil {
		if p.ConcurrencyKey == "" {
			return Policy{}, fieldErr("policy.concurrency_limit", "invalid", "needs a concurrency_key")
		}
		if *in.ConcurrencyLimit < 1 || *in.ConcurrencyLimit > 100 {
			return Policy{}, fieldErr("policy.concurrency_limit", "invalid", "must be between 1 and 100")
		}
		p.ConcurrencyLimit = *in.ConcurrencyLimit
	}
	if in.Overlap != nil {
		switch *in.Overlap {
		case OverlapSkip, OverlapQueue, OverlapAllow:
			p.Overlap = *in.Overlap
		default:
			return Policy{}, fieldErr("policy.overlap", "invalid", "must be skip, queue or allow")
		}
	}
	if p.Overlap == OverlapAllow && t.Container != nil && t.Container.RepoURL != "" {
		return Policy{}, fieldErr("policy.overlap", "overlap_not_allowed", "container jobs with a repo_url cannot use allow")
	}
	if in.NotifyOnSuccess != nil {
		p.NotifyOnSuccess = *in.NotifyOnSuccess
	}
	if in.Priority != nil {
		switch *in.Priority {
		case PriorityHigh, PriorityNormal, PriorityLow:
			p.Priority = *in.Priority
		default:
			return Policy{}, fieldErr("policy.priority", "invalid", "must be high, normal or low")
		}
	}
	return p, nil
}

func policyAsInput(p Policy) *PolicyIn {
	r := p.Retry
	in := &PolicyIn{
		TimeoutSec: &p.TimeoutSec, MaxLateSec: &p.MaxLateSec, LateWarnSec: &p.LateWarnSec, JitterSec: &p.JitterSec,
		Delivery: &p.Delivery, Retry: &RetryIn{MaxAttempts: &r.MaxAttempts, BackoffSec: &r.BackoffSec, MaxBackoffSec: &r.MaxBackoffSec},
		Overlap: &p.Overlap, MaxDeferSec: &p.MaxDeferSec, NotifyOnSuccess: &p.NotifyOnSuccess,
	}
	if p.ConcurrencyKey != "" {
		in.ConcurrencyKey, in.ConcurrencyLimit = &p.ConcurrencyKey, &p.ConcurrencyLimit
	}
	if p.Priority != "" {
		in.Priority = &p.Priority
	}
	return in
}

// ---------------------------------------------------------------- targets

func validateTarget(in TargetIn, cur *Target, c VCtx) (Target, error) {
	switch in.Type {
	case TargetHTTP:
		if in.HTTP == nil {
			return Target{}, fieldErr("target.http", "required", "is required for an http target")
		}
		h, err := validateHTTP(*in.HTTP, c.Principal)
		return Target{Type: TargetHTTP, HTTP: h}, err

	case TargetContainer, TargetInline:
		if !c.ContainersEnabled {
			return Target{}, fieldErr("target.type", CodeTargetNotEnabled, "container and inline targets are not enabled on this runner")
		}
		if !c.Principal.ContainerAllowed {
			return Target{}, fieldErr("target.type", CodeForbidden, "this client may not run containers")
		}
		var curEnv *SealedEnv
		if cur != nil && cur.Type == in.Type {
			if cur.Container != nil {
				curEnv = &cur.Container.Env
			}
			if cur.Inline != nil {
				curEnv = &cur.Inline.Env
			}
		}
		if in.Type == TargetContainer {
			if in.Container == nil {
				return Target{}, fieldErr("target.container", "required", "is required for a container target")
			}
			ct, err := validateContainer(*in.Container, curEnv, c)
			return Target{Type: TargetContainer, Container: ct}, err
		}
		if in.Inline == nil {
			return Target{}, fieldErr("target.inline", "required", "is required for an inline target")
		}
		it, err := validateInline(*in.Inline, curEnv, c)
		return Target{Type: TargetInline, Inline: it}, err
	}
	return Target{}, fieldErr("target.type", "invalid", "must be http, container or inline")
}

func validateHTTP(in HTTPIn, p Principal) (*HTTPTarget, error) {
	out := &HTTPTarget{Method: strings.ToUpper(in.Method), URL: strings.TrimSpace(in.URL), Mode: in.Mode, Auth: in.Auth}
	if out.Method == "" {
		out.Method = "POST"
	}
	switch out.Method {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
	default:
		return nil, fieldErr("target.http.method", "invalid", "must be GET, POST, PUT, PATCH or DELETE")
	}
	if len(out.URL) > MaxURLBytes {
		return nil, fieldErr("target.http.url", "invalid", "is longer than %d bytes", MaxURLBytes)
	}
	u, err := url.Parse(out.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fieldErr("target.http.url", "invalid", "must be an absolute http or https URL")
	}
	if u.User != nil {
		return nil, fieldErr("target.http.url", "invalid", "must not embed credentials; use auth.bearer_ref")
	}
	if !HostAllowed(u.Hostname(), p.CallbackHosts) {
		return nil, fieldErr("target.http.url", "host_not_allowed", "host %q is not in this client's callback_hosts", u.Hostname())
	}
	if u.Scheme == "http" && strings.Contains(u.Hostname(), ".") && (in.Auth.BearerRef != "" || in.Auth.HMACRef != "") {
		// Docker service names stay on the host's networks; a dotted name may
		// cross the internet with the secret in cleartext.
		return nil, fieldErr("target.http.url", "invalid", "must be https when auth is set and the host is not a docker service name")
	}
	if len(in.Headers) > MaxHeaders {
		return nil, fieldErr("target.http.headers", "invalid", "at most %d headers", MaxHeaders)
	}
	seen := make(map[string]bool, len(in.Headers))
	for name, v := range in.Headers {
		lower := strings.ToLower(name)
		if !headerNameRE.MatchString(name) || strings.ContainsAny(v, "\r\n") {
			return nil, fieldErr("target.http.headers", "invalid", "header %q is not a valid header", name)
		}
		if seen[lower] { // map order would decide which value is sent
			return nil, fieldErr("target.http.headers", "invalid", "header %q is given twice in different cases", name)
		}
		seen[lower] = true
		if reservedHeaders[lower] || strings.HasPrefix(lower, "proxy-") || strings.HasPrefix(lower, "x-plr-") {
			return nil, fieldErr("target.http.headers", "header_reserved", "header %q is reserved; send credentials through auth refs", name)
		}
	}
	if len(in.Headers) > 0 {
		out.Headers = in.Headers
	}
	if len(in.Body) > 0 {
		if len(in.Body) > MaxPayloadBytes || !json.Valid(in.Body) {
			return nil, fieldErr("target.http.body", "invalid", "must be JSON of at most %d bytes", MaxPayloadBytes)
		}
		out.Body = in.Body
	}
	switch out.Mode {
	case "", "sync":
		out.Mode = "sync"
		if in.LeaseSec != nil {
			return nil, fieldErr("target.http.lease_sec", "invalid", "only applies to async mode")
		}
	case "async":
		out.LeaseSec = 300
		if in.LeaseSec != nil {
			if *in.LeaseSec < 10 || *in.LeaseSec > 86400 {
				return nil, fieldErr("target.http.lease_sec", "invalid", "must be between 10 and 86400")
			}
			out.LeaseSec = *in.LeaseSec
		}
	default:
		return nil, fieldErr("target.http.mode", "invalid", "must be sync or async")
	}
	for field, ref := range map[string]string{"bearer_ref": in.Auth.BearerRef, "hmac_ref": in.Auth.HMACRef} {
		if ref == "" {
			continue
		}
		canon, err := checkRef(ref, p)
		if err != nil {
			return nil, withField(err, "target.http.auth."+field)
		}
		if field == "bearer_ref" {
			out.Auth.BearerRef = canon
		} else {
			out.Auth.HMACRef = canon
		}
	}
	return out, nil
}

// HostAllowed reports whether host exactly matches an allowlisted host.
func HostAllowed(host string, allowed []string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, a := range allowed {
		if host == strings.ToLower(a) {
			return true
		}
	}
	return false
}

// CheckRef parses a secret reference and enforces the client's prefixes. It
// returns the canonical "Folder/KEY" form.
func CheckRef(ref string, p Principal) (string, error) { return checkRef(ref, p) }

func checkRef(ref string, p Principal) (string, error) {
	r, err := vault.ParseRef(ref)
	if err != nil {
		return "", fieldErr("", "invalid", "%s", err.Error())
	}
	if strings.HasPrefix(r.Key, "PIPELINE_RUNNER_") {
		// The runner's own credentials, whatever prefixes a client holds.
		return "", fieldErr("", "secret_ref_not_permitted", "secret %q belongs to the runner itself", r.String())
	}
	if !vault.Permitted(r, p.SecretRefPrefixes) {
		return "", fieldErr("", "secret_ref_not_permitted", "secret %q is outside this client's secret_ref_prefixes", r.String())
	}
	return r.String(), nil
}

func withField(err error, field string) error {
	if fe, ok := AsFieldError(err); ok {
		return &FieldError{Field: field, Code: fe.Code, Message: fe.Message}
	}
	return err
}

type runtimeIn struct {
	memory  *int64
	cpus    *float64
	user    string
	network string
	env     map[string]string
	refs    map[string]string
}

type runtimeOut struct {
	memory  int64
	cpus    float64
	user    string
	network string
	env     SealedEnv
	refs    map[string]string
}

func validateRuntime(in runtimeIn, prefix string, curEnv *SealedEnv, c VCtx) (runtimeOut, error) {
	out := runtimeOut{memory: 512, cpus: 1.0, user: "1000:1000", network: "none"}
	if in.memory != nil {
		if *in.memory < 64 || *in.memory > 8192 {
			return out, fieldErr(prefix+".memory_mb", "invalid", "must be between 64 and 8192")
		}
		out.memory = *in.memory
	}
	if in.cpus != nil {
		if *in.cpus < 0.1 || *in.cpus > 4 {
			return out, fieldErr(prefix+".cpus", "invalid", "must be between 0.1 and 4")
		}
		out.cpus = *in.cpus
	}
	if in.user != "" {
		switch {
		case in.user == "root":
			if !c.Principal.RootAllowed {
				return out, fieldErr(prefix+".user", CodeForbidden, "this client may not run containers as root")
			}
		case !userRE.MatchString(in.user):
			return out, fieldErr(prefix+".user", "invalid", "must be uid:gid or root")
		}
		out.user = in.user
	}
	switch in.network {
	case "", "none":
	case "egress":
		out.network = "egress"
	default:
		return out, fieldErr(prefix+".network", "invalid", "must be none or egress")
	}
	if in.env == nil && curEnv != nil {
		out.env = *curEnv // PATCH without env keeps the stored ciphertext
	} else {
		if len(in.env) > MaxEnvVars {
			return out, fieldErr(prefix+".env", "invalid", "at most %d variables", MaxEnvVars)
		}
		size := 0
		for k, v := range in.env {
			if !envNameRE.MatchString(k) || strings.HasPrefix(k, "PLR_") {
				return out, fieldErr(prefix+".env", "invalid", "variable %q has an invalid or reserved name", k)
			}
			size += len(k) + len(v)
		}
		if size > MaxEnvBytes {
			return out, fieldErr(prefix+".env", "invalid", "is larger than %d bytes", MaxEnvBytes)
		}
		sealed, err := SealEnv(c.EnvKey, in.env)
		if err != nil {
			return out, err
		}
		out.env = sealed
	}
	if len(in.refs) > MaxEnvVars {
		return out, fieldErr(prefix+".secret_refs", "invalid", "at most %d refs", MaxEnvVars)
	}
	for name, ref := range in.refs {
		if !envNameRE.MatchString(name) || strings.HasPrefix(name, "PLR_") {
			return out, fieldErr(prefix+".secret_refs", "invalid", "variable %q has an invalid or reserved name", name)
		}
		if _, dup := in.env[name]; dup {
			return out, fieldErr(prefix+".secret_refs", "invalid", "variable %q is also set in env", name)
		}
		canon, err := checkRef(ref, c.Principal)
		if err != nil {
			return out, withField(err, prefix+".secret_refs."+name)
		}
		if out.refs == nil {
			out.refs = map[string]string{}
		}
		out.refs[name] = canon
	}
	return out, nil
}

func validateImage(image, field string) error {
	if !imageRE.MatchString(image) || strings.Count(image, "@") > 1 {
		return fieldErr(field, "invalid", "must start with an alphanumeric and contain only [A-Za-z0-9._-/:@]")
	}
	return nil
}

func validateContainer(in ContainerIn, curEnv *SealedEnv, c VCtx) (*ContainerTarget, error) {
	if err := validateImage(in.Image, "target.container.image"); err != nil {
		return nil, err
	}
	cmd := strings.TrimSpace(in.Command)
	if cmd == "" || len(cmd) > 4096 {
		return nil, fieldErr("target.container.command", "invalid", "is required and at most 4096 bytes")
	}
	out := &ContainerTarget{Image: in.Image, Command: cmd}
	if in.RepoURL != "" {
		u, err := url.Parse(in.RepoURL)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return nil, fieldErr("target.container.repo_url", "invalid", "must be an https URL")
		}
		if u.User != nil {
			return nil, fieldErr("target.container.repo_url", "invalid", "must not embed credentials")
		}
		out.RepoURL = in.RepoURL
		out.Branch, out.OutputBranch = in.Branch, in.OutputBranch
		if out.Branch == "" {
			out.Branch = "main"
		}
		if out.OutputBranch == "" {
			out.OutputBranch = out.Branch
		}
		for field, ref := range map[string]string{"branch": out.Branch, "output_branch": out.OutputBranch} {
			if !gitRefRE.MatchString(ref) || strings.Contains(ref, "..") {
				return nil, fieldErr("target.container."+field, "invalid", "is not a valid branch name")
			}
		}
		out.CommitResults = true
		if in.CommitResults != nil {
			out.CommitResults = *in.CommitResults
		}
		if in.GitTokenRef != "" {
			canon, err := checkRef(in.GitTokenRef, c.Principal)
			if err != nil {
				return nil, withField(err, "target.container.git_token_ref")
			}
			out.GitTokenRef = canon
		}
	} else if in.Branch != "" || in.OutputBranch != "" || in.GitTokenRef != "" || (in.CommitResults != nil && *in.CommitResults) {
		return nil, fieldErr("target.container.repo_url", "required", "branch, commit and git token settings need a repo_url")
	}
	rt, err := validateRuntime(runtimeIn{in.MemoryMB, in.CPUs, in.User, in.Network, in.Env, in.SecretRefs}, "target.container", curEnv, c)
	if err != nil {
		return nil, err
	}
	out.MemoryMB, out.CPUs, out.User, out.Network, out.Env, out.SecretRefs = rt.memory, rt.cpus, rt.user, rt.network, rt.env, rt.refs
	return out, nil
}

func validateInline(in InlineIn, curEnv *SealedEnv, c VCtx) (*InlineTarget, error) {
	if err := validateImage(in.Image, "target.inline.image"); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Script) == "" || len(in.Script) > MaxScriptBytes {
		return nil, fieldErr("target.inline.script", "invalid", "is required and at most %d bytes", MaxScriptBytes)
	}
	out := &InlineTarget{Image: in.Image, Script: in.Script, Interpreter: in.Interpreter}
	if out.Interpreter == "" {
		out.Interpreter = "sh"
	}
	if !interpreterRE.MatchString(out.Interpreter) || strings.Contains(out.Interpreter, "..") {
		return nil, fieldErr("target.inline.interpreter", "invalid", "must be a command name or path")
	}
	rt, err := validateRuntime(runtimeIn{in.MemoryMB, in.CPUs, in.User, in.Network, in.Env, in.SecretRefs}, "target.inline", curEnv, c)
	if err != nil {
		return nil, err
	}
	out.MemoryMB, out.CPUs, out.User, out.Network, out.Env, out.SecretRefs = rt.memory, rt.cpus, rt.user, rt.network, rt.env, rt.refs
	return out, nil
}

// Refs lists every vault reference a spec uses, for existence checks and
// fire-time resolution.
func (s RunSpec) Refs() []string {
	var refs []string
	switch {
	case s.Target.HTTP != nil:
		for _, r := range []string{s.Target.HTTP.Auth.BearerRef, s.Target.HTTP.Auth.HMACRef} {
			if r != "" {
				refs = append(refs, r)
			}
		}
	case s.Target.Container != nil:
		for _, r := range s.Target.Container.SecretRefs {
			refs = append(refs, r)
		}
		if s.Target.Container.GitTokenRef != "" {
			refs = append(refs, s.Target.Container.GitTokenRef)
		}
	case s.Target.Inline != nil:
		for _, r := range s.Target.Inline.SecretRefs {
			refs = append(refs, r)
		}
	}
	return refs
}

// MinGap returns the job's minimum interval, or 0 for non-cron jobs.
func (j Job) MinGap(now time.Time) time.Duration {
	if j.Trigger.Type != TriggerCron {
		return 0
	}
	s, err := schedule.Parse(j.Trigger.Cron, j.Trigger.Timezone)
	if err != nil {
		return 0
	}
	return s.MinGap(now, gapSamples)
}
