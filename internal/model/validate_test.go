package model

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var (
	testNow = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	testKey = bytes.Repeat([]byte{7}, 32)
)

func ctx() VCtx {
	return VCtx{
		Now: testNow, EnvKey: testKey, ContainersEnabled: true,
		Principal: Principal{
			ClientID: "c1", Name: "reel-drop", CallbackHosts: []string{"reel-drop"},
			SecretRefPrefixes: []string{"ReelDrop/"}, ContainerAllowed: true,
		},
	}
}

func httpJob() JobCreate {
	return JobCreate{
		Name:    "publish-tick",
		Trigger: TriggerIn{Type: TriggerCron, Cron: "*/15 * * * * *", Timezone: "Asia/Kolkata"},
		Target:  TargetIn{Type: TargetHTTP, HTTP: &HTTPIn{URL: "http://reel-drop:3000/internal/tick", Auth: HTTPAuth{BearerRef: "ReelDrop/PLR_TOKEN"}}},
	}
}

func ptr[T any](v T) *T { return &v }

func code(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	fe, ok := AsFieldError(err)
	if !ok {
		t.Fatalf("not a FieldError: %v", err)
	}
	return fe.Code
}

func TestValidHTTPJobDefaults(t *testing.T) {
	j, err := ValidateJobCreate(httpJob(), ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.Target.HTTP.Method != "POST" || j.Target.HTTP.Mode != "sync" || j.Policy.Overlap != OverlapSkip || j.Policy.TimeoutSec != 900 {
		t.Fatalf("defaults not applied: %+v %+v", j.Target.HTTP, j.Policy)
	}
	if j.State != JobActive || j.Trigger.Timezone != "Asia/Kolkata" {
		t.Fatalf("unexpected job: %+v", j)
	}
}

func TestInvokeDefaultsToAllow(t *testing.T) {
	in := httpJob()
	in.Trigger = TriggerIn{Type: TriggerInvoke}
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.Policy.Overlap != OverlapAllow {
		t.Fatalf("overlap = %s", j.Policy.Overlap)
	}
}

func TestHostAndRefControls(t *testing.T) {
	in := httpJob()
	in.Target.HTTP.URL = "http://secret-manager:8000/x"
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "host_not_allowed" {
		t.Fatalf("code = %s", c)
	}
	in = httpJob()
	in.Target.HTTP.Auth.BearerRef = "Root/PIPELINE_RUNNER_API_TOKEN"
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "secret_ref_not_permitted" {
		t.Fatalf("code = %s", c)
	}
	in = httpJob()
	in.Target.HTTP.Auth.BearerRef = "ReelDropX/KEY" // prefix must match on the folder boundary
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "secret_ref_not_permitted" {
		t.Fatalf("code = %s", c)
	}
}

func TestReservedHeaders(t *testing.T) {
	for _, h := range []string{"Authorization", "x-plr-run-token", "X-PLR-Run-Id", "Host", "Proxy-Authorization", "Cookie"} {
		in := httpJob()
		in.Target.HTTP.Headers = map[string]string{h: "x"}
		if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "header_reserved" {
			t.Fatalf("%s: code = %s", h, c)
		}
	}
	in := httpJob()
	in.Target.HTTP.Headers = map[string]string{"X-Trace": "a\r\nInjected: 1"}
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "invalid" {
		t.Fatalf("code = %s", c)
	}
}

func TestAtWindowAndDelay(t *testing.T) {
	in := httpJob()
	in.Trigger = TriggerIn{Type: TriggerAt, At: ptr(testNow.Add(-11 * time.Minute))}
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "at_in_past" {
		t.Fatalf("code = %s", c)
	}
	in.Trigger.At = ptr(testNow.Add(-5 * time.Minute)) // inside max_late: fires immediately
	if _, err := ValidateJobCreate(in, ctx()); err != nil {
		t.Fatal(err)
	}
	in.Trigger = TriggerIn{Type: TriggerDelay, DelaySec: ptr(int64(0))}
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.Trigger.Type != TriggerAt || !j.Trigger.At.Equal(testNow) {
		t.Fatalf("delay not resolved to at: %+v", j.Trigger)
	}
	in.Trigger.DelaySec = ptr(int64(-1))
	code(t, mustErr(ValidateJobCreate(in, ctx())))
}

func TestCronRules(t *testing.T) {
	in := httpJob()
	in.Trigger.Cron = "0 0 30 2 *"
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "cron_never_fires" {
		t.Fatalf("code = %s", c)
	}
	in = httpJob()
	in.Policy = &PolicyIn{JitterSec: ptr(int64(15))}
	code(t, mustErr(ValidateJobCreate(in, ctx()))) // jitter >= 15 s interval

	c := ctx()
	in = JobCreate{Name: "probe", Trigger: TriggerIn{Type: TriggerCron, Cron: "*/30 * * * * *"},
		Target: TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3", Script: "echo hi"}}}
	code(t, mustErr(ValidateJobCreate(in, c))) // seconds cron only for http
	in.Trigger.Cron = "* * * * *"
	if _, err := ValidateJobCreate(in, c); err != nil { // 60 s is allowed
		t.Fatal(err)
	}
}

func TestContainerGates(t *testing.T) {
	in := JobCreate{Name: "backup", Trigger: TriggerIn{Type: TriggerInvoke},
		Target: TargetIn{Type: TargetContainer, Container: &ContainerIn{Image: "alpine:3", Command: "true"}}}
	c := ctx()
	c.ContainersEnabled = false
	if got := code(t, mustErr(ValidateJobCreate(in, c))); got != CodeTargetNotEnabled {
		t.Fatalf("code = %s", got)
	}
	c = ctx()
	c.Principal.ContainerAllowed = false
	if got := code(t, mustErr(ValidateJobCreate(in, c))); got != CodeForbidden {
		t.Fatalf("code = %s", got)
	}
	in.Target.Container.User = "root"
	if got := code(t, mustErr(ValidateJobCreate(in, ctx()))); got != CodeForbidden {
		t.Fatalf("code = %s", got)
	}
	in.Target.Container.User = ""
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.Target.Container.Network != "none" || j.Target.Container.User != "1000:1000" {
		t.Fatalf("defaults: %+v", j.Target.Container)
	}
	in.Target.Container.Image = "--privileged"
	code(t, mustErr(ValidateJobCreate(in, ctx())))
}

func TestRepoContainerOverlap(t *testing.T) {
	in := JobCreate{Name: "monitor", Trigger: TriggerIn{Type: TriggerInvoke},
		Target: TargetIn{Type: TargetContainer, Container: &ContainerIn{Image: "alpine:3", Command: "true", RepoURL: "https://github.com/x/y"}}}
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.Policy.Overlap != OverlapQueue || !j.Target.Container.CommitResults || j.Target.Container.Branch != "main" {
		t.Fatalf("repo defaults: %+v %+v", j.Policy, j.Target.Container)
	}
	in.Policy = &PolicyIn{Overlap: ptr(OverlapAllow)}
	if c := code(t, mustErr(ValidateJobCreate(in, ctx()))); c != "overlap_not_allowed" {
		t.Fatalf("code = %s", c)
	}
}

func TestSealedEnvNeverMarshalsPlaintext(t *testing.T) {
	in := JobCreate{Name: "job", Trigger: TriggerIn{Type: TriggerInvoke},
		Target: TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3", Script: "env", Env: map[string]string{"API_KEY": "s3cr3t-value"}}}}
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(j.Spec())
	if strings.Contains(string(data), "s3cr3t-value") || !strings.Contains(string(data), "API_KEY") {
		t.Fatalf("marshalled spec leaks or lacks keys: %s", data)
	}
	var back RunSpec
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	env, err := back.Target.Inline.Env.Open(testKey)
	if err != nil || env["API_KEY"] != "s3cr3t-value" {
		t.Fatalf("round trip: %v %v", env, err)
	}
}

func TestPatchKeepsEnvWhenAbsent(t *testing.T) {
	in := JobCreate{Name: "job", Trigger: TriggerIn{Type: TriggerInvoke},
		Target: TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3", Script: "env", Env: map[string]string{"A": "1"}}}}
	cur, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := ValidatePatch(cur, JobPatch{Target: &TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3.20", Script: "env"}}}, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if got := next.Target.Inline.Env.Keys(); len(got) != 1 || got[0] != "A" {
		t.Fatalf("env keys = %v", got)
	}
	next, _, err = ValidatePatch(cur, JobPatch{Target: &TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3", Script: "env", Env: map[string]string{}}}}, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Target.Inline.Env.Keys()) != 0 {
		t.Fatal("explicit empty env must clear")
	}
}

func TestPatchTriggerChanged(t *testing.T) {
	cur, err := ValidateJobCreate(httpJob(), ctx())
	if err != nil {
		t.Fatal(err)
	}
	_, changed, err := ValidatePatch(cur, JobPatch{Policy: &PolicyIn{TimeoutSec: ptr(int64(30))}}, ctx())
	if err != nil || changed {
		t.Fatalf("policy-only patch: changed=%v err=%v", changed, err)
	}
	tz := TriggerIn{Type: TriggerCron, Cron: cur.Trigger.Cron, Timezone: "UTC"}
	if _, changed, _ = ValidatePatch(cur, JobPatch{Trigger: &tz}, ctx()); !changed {
		t.Fatal("timezone change on cron must count as a trigger change")
	}
	same := TriggerIn{Type: TriggerCron, Cron: cur.Trigger.Cron, Timezone: cur.Trigger.Timezone}
	if _, changed, _ = ValidatePatch(cur, JobPatch{Trigger: &same}, ctx()); changed {
		t.Fatal("identical trigger reported as changed")
	}
}

func TestPatchTriggerTypeRederivesDefaultOverlap(t *testing.T) {
	cur, err := ValidateJobCreate(httpJob(), ctx()) // cron: default overlap skip
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := ValidatePatch(cur, JobPatch{Trigger: &TriggerIn{Type: TriggerInvoke}}, ctx())
	if err != nil || next.Policy.Overlap != OverlapAllow {
		t.Fatalf("cron->invoke keeps default overlap: %v %v", next.Policy.Overlap, err)
	}
	in := httpJob()
	in.Policy = &PolicyIn{Overlap: ptr(OverlapQueue)}
	explicit, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	next, _, err = ValidatePatch(explicit, JobPatch{Trigger: &TriggerIn{Type: TriggerInvoke}}, ctx())
	if err != nil || next.Policy.Overlap != OverlapQueue {
		t.Fatalf("explicit overlap must survive a trigger change: %v %v", next.Policy.Overlap, err)
	}
}

func TestPatchAtPolicyRechecksJitter(t *testing.T) {
	in := httpJob()
	at := time.Now().Add(time.Hour)
	in.Trigger = TriggerIn{Type: TriggerAt, At: &at}
	cur, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ValidatePatch(cur, JobPatch{Policy: &PolicyIn{JitterSec: ptr(int64(30)), MaxLateSec: ptr(int64(30))}}, ctx())
	if err == nil {
		t.Fatal("jitter >= max_late_sec accepted on an at job")
	}
}

func TestRunnerSecretsAndCleartextAuthRefused(t *testing.T) {
	c := ctx()
	c.Principal.SecretRefPrefixes = []string{"Root/"}
	if _, err := CheckRef("Root/PIPELINE_RUNNER_API_TOKEN", c.Principal); err == nil {
		t.Fatal("runner's own secret accepted")
	}
	in := httpJob()
	c = ctx()
	c.Principal.CallbackHosts = []string{"api.example.com"}
	in.Target.HTTP.URL = "http://api.example.com/hook"
	if _, err := ValidateJobCreate(in, c); err == nil {
		t.Fatal("bearer over cleartext http to a public host accepted")
	}
	in.Target.HTTP.URL = "https://api.example.com/hook"
	if _, err := ValidateJobCreate(in, c); err != nil {
		t.Fatalf("https: %v", err)
	}
}

func TestNSKey(t *testing.T) {
	in := httpJob()
	in.Policy = &PolicyIn{ConcurrencyKey: ptr("ig:123")}
	j, err := ValidateJobCreate(in, ctx())
	if err != nil {
		t.Fatal(err)
	}
	if j.NSKey != "c1:ig:123" || j.Policy.ConcurrencyLimit != 1 {
		t.Fatalf("nskey = %q limit = %d", j.NSKey, j.Policy.ConcurrencyLimit)
	}
}

func TestAdhocAppliesClientLimits(t *testing.T) {
	c := ctx()
	c.Principal.ContainerAllowed = false
	_, err := ValidateAdhoc(AdhocReq{Target: TargetIn{Type: TargetInline, Inline: &InlineIn{Image: "alpine:3", Script: "id"}}}, c)
	if got := code(t, err); got != CodeForbidden {
		t.Fatalf("code = %s", got)
	}
	_, err = ValidateAdhoc(AdhocReq{Target: TargetIn{Type: TargetHTTP, HTTP: &HTTPIn{URL: "https://evil.example/"}}}, ctx())
	if got := code(t, err); got != "host_not_allowed" {
		t.Fatalf("code = %s", got)
	}
}

func mustErr[T any](_ T, err error) error { return err }

func TestPriorityDefaultsAndValidation(t *testing.T) {
	j, err := ValidateJobCreate(httpJob(), ctx())
	if err != nil || j.Policy.Priority != PriorityNormal {
		t.Fatalf("default priority = %q, err %v", j.Policy.Priority, err)
	}
	in := httpJob()
	in.Policy = &PolicyIn{Priority: ptr("urgent")}
	if _, err := ValidateJobCreate(in, ctx()); code(t, err) != "invalid" {
		t.Fatalf("invalid priority accepted: %v", err)
	}
	in.Policy = &PolicyIn{Priority: ptr(PriorityHigh)}
	j, err = ValidateJobCreate(in, ctx())
	if err != nil || j.Policy.Priority != PriorityHigh {
		t.Fatalf("priority = %q, err %v", j.Policy.Priority, err)
	}
	// A target patch keeps the stored priority; a policy patch replaces it.
	next, _, err := ValidatePatch(j, JobPatch{Target: ptr(httpJob().Target)}, ctx())
	if err != nil || next.Policy.Priority != PriorityHigh {
		t.Fatalf("target patch: priority %q, err %v", next.Policy.Priority, err)
	}
	next, _, err = ValidatePatch(j, JobPatch{Policy: &PolicyIn{TimeoutSec: ptr(int64(30))}}, ctx())
	if err != nil || next.Policy.Priority != PriorityNormal {
		t.Fatalf("policy patch: priority %q, err %v", next.Policy.Priority, err)
	}
}

// Policies stored before priority existed have no "priority" key.
func TestLegacyPolicyReadsAsNormalAndPatches(t *testing.T) {
	cur, err := ValidateJobCreate(httpJob(), ctx())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(cur.Policy)
	legacy := strings.Replace(string(b), `,"priority":"normal"`, "", 1)
	if strings.Contains(legacy, "priority") {
		t.Fatalf("setup: %s", legacy)
	}
	var p Policy
	if err := json.Unmarshal([]byte(legacy), &p); err != nil || p.Priority != PriorityNormal || p.TimeoutSec != 900 {
		t.Fatalf("decoded %+v, err %v", p, err)
	}
	var spec RunSpec
	if err := json.Unmarshal([]byte(`{"target":{"type":"http"},"policy":`+legacy+`}`), &spec); err != nil || spec.Policy.Priority != PriorityNormal {
		t.Fatalf("RunSpec policy priority %q, err %v", spec.Policy.Priority, err)
	}
	// A Policy built without the field (as an older binary wrote it).
	cur.Policy.Priority = ""
	tz := TriggerIn{Type: TriggerCron, Cron: cur.Trigger.Cron, Timezone: "UTC"}
	name := "renamed"
	for _, patch := range []JobPatch{{Target: ptr(httpJob().Target)}, {Trigger: &tz}, {Name: &name}} {
		if _, _, err := ValidatePatch(cur, patch, ctx()); err != nil {
			t.Fatalf("legacy patch %+v: %v", patch, err)
		}
	}
}
