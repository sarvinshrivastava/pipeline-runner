package api

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/sarvinshrivastava/pipeline-runner/internal/engine"
	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
	"github.com/sarvinshrivastava/pipeline-runner/internal/secure"
	"github.com/sarvinshrivastava/pipeline-runner/internal/target"
)

// ---------------------------------------------------------------- helpers

func loadDoc(t *testing.T) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(openapiYAML, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// resolve follows "#/components/..." refs until it reaches a node without one.
func resolve(t *testing.T, doc map[string]any, node any) map[string]any {
	t.Helper()
	m, _ := node.(map[string]any)
	for i := 0; m != nil && i < 10; i++ {
		ref, ok := m["$ref"].(string)
		if !ok {
			return m
		}
		var cur any = doc
		for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
			cm, _ := cur.(map[string]any)
			cur = cm[part]
		}
		if m, _ = cur.(map[string]any); m == nil {
			t.Fatalf("unresolvable $ref %s", ref)
		}
	}
	return m
}

type opRef struct {
	method, path string
	op           map[string]any
}

func (o opRef) id() string { s, _ := o.op["operationId"].(string); return s }

var httpMethods = []string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}

// ops lists every operation, sorted by path then method. It walks only the
// HTTP-method keys of each path item, so `parameters` and the like are skipped.
func ops(t *testing.T, doc map[string]any) []opRef {
	t.Helper()
	paths, _ := doc["paths"].(map[string]any)
	var out []opRef
	for p, item := range paths {
		im, _ := item.(map[string]any)
		for k, v := range im {
			if !slices.Contains(httpMethods, k) {
				continue
			}
			op, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%s %s is not a map", k, p)
			}
			out = append(out, opRef{strings.ToUpper(k), p, op})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].path != out[j].path {
			return out[i].path < out[j].path
		}
		return out[i].method < out[j].method
	})
	return out
}

func opByID(t *testing.T, doc map[string]any, id string) opRef {
	t.Helper()
	for _, o := range ops(t, doc) {
		if o.id() == id {
			return o
		}
	}
	t.Fatalf("no operation %s", id)
	return opRef{}
}

func wantSecurity(rt route) string {
	switch rt.auth {
	case authNone:
		return "none"
	case authRunToken:
		return "runToken"
	}
	return "bearer:" + rt.scope
}

func docSecurity(op map[string]any) string {
	sec, ok := op["security"].([]any)
	if !ok {
		return "MISSING"
	}
	if len(sec) == 0 {
		return "none"
	}
	if len(sec) != 1 {
		return "MULTIPLE"
	}
	req, _ := sec[0].(map[string]any)
	if _, ok := req["runToken"]; ok && len(req) == 1 {
		return "runToken"
	}
	if scopes, ok := req["bearer"].([]any); ok && len(req) == 1 {
		var s []string
		for _, sc := range scopes {
			s = append(s, sc.(string))
		}
		return "bearer:" + strings.Join(s, ",")
	}
	return "UNKNOWN"
}

func setOf[K comparable](keys ...K) map[K]bool {
	m := map[K]bool{}
	for _, k := range keys {
		m[k] = true
	}
	return m
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diff returns the sorted keys of a that are not in b.
func diff(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func toJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// requestExamples returns an operation's named JSON request examples, resolved.
func requestExamples(t *testing.T, doc map[string]any, op map[string]any) map[string]any {
	t.Helper()
	rb := resolve(t, doc, op["requestBody"])
	content, _ := rb["content"].(map[string]any)
	media := resolve(t, doc, content["application/json"])
	exs, _ := media["examples"].(map[string]any)
	out := map[string]any{}
	for name, ex := range exs {
		out[name] = resolve(t, doc, ex)["value"]
	}
	return out
}

// ---------------------------------------------------------------- shape of the document

var authSentenceRE = regexp.MustCompile("Requires scope `([a-z:]+)`\\.|Requires any valid bearer token\\.|Auth: run token \\(X-PLR-Run-Token\\)\\.|No auth\\.")

// nonErrorResponses are >= 400 responses that are not Error envelopes.
var nonErrorResponses = map[string]string{
	"GET /healthz 503": "healthz reports a stalled loop with its Health body",
}

var typedErrorResponses = setOf(
	"#/components/responses/BadRequest", "#/components/responses/Unauthorized", "#/components/responses/Forbidden",
	"#/components/responses/NotFound", "#/components/responses/Conflict", "#/components/responses/TooLarge",
	"#/components/responses/RateLimited", "#/components/responses/Unavailable",
)

func TestOpenAPIOperationsComplete(t *testing.T) {
	doc := loadDoc(t)
	idRE := regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	declared := map[string]bool{}
	for _, tg := range doc["tags"].([]any) {
		declared[tg.(map[string]any)["name"].(string)] = true
	}
	seen := map[string]bool{}
	for _, o := range ops(t, doc) {
		name := o.method + " " + o.path
		id := o.id()
		if !idRE.MatchString(id) || seen[id] {
			t.Errorf("%s: bad or duplicate operationId %q", name, id)
		}
		seen[id] = true
		tags, _ := o.op["tags"].([]any)
		if len(tags) != 1 || !declared[tags[0].(string)] {
			t.Errorf("%s: want exactly one declared tag, got %v", name, tags)
		}
		summary, _ := o.op["summary"].(string)
		if summary == "" || utf8.RuneCountInString(summary) > 80 || strings.HasSuffix(summary, ".") {
			t.Errorf("%s: summary %q must be 1..80 runes with no trailing period", name, summary)
		}
		desc, _ := o.op["description"].(string)
		if n := utf8.RuneCountInString(desc); n < 40 || n > 1024 {
			t.Errorf("%s: description is %d runes, want 40..1024", name, n)
		}
		if len(tags) == 1 && tags[0] == "callbacks" && !strings.HasPrefix(desc, "Called by the target") {
			t.Errorf("%s: a callbacks description must start with %q", name, "Called by the target")
		}
		auth := authSentenceRE.FindAllStringSubmatch(desc, -1)
		if len(auth) != 1 {
			t.Errorf("%s: want exactly one auth sentence, found %d", name, len(auth))
		} else {
			want := map[string]string{"none": "No auth.", "runToken": "Auth: run token (X-PLR-Run-Token).", "bearer:": "Requires any valid bearer token."}
			sec := docSecurity(o.op)
			w, ok := want[sec]
			if !ok {
				w = "Requires scope `" + strings.TrimPrefix(sec, "bearer:") + "`."
			}
			if auth[0][0] != w {
				t.Errorf("%s: auth sentence %q does not match security %s (want %q)", name, auth[0][0], sec, w)
			}
		}
		responses, _ := o.op["responses"].(map[string]any)
		for code, r := range responses {
			status, err := strconv.Atoi(code)
			if err != nil {
				t.Errorf("%s: response key %q", name, code)
				continue
			}
			rm, _ := r.(map[string]any)
			resolved := resolve(t, doc, rm)
			content, _ := resolved["content"].(map[string]any)
			if status >= 200 && status < 300 {
				if js, ok := content["application/json"]; ok && resolve(t, doc, js)["schema"] == nil {
					t.Errorf("%s %d: JSON response without a schema", name, status)
				}
				continue
			}
			if status < 400 || nonErrorResponses[name+" "+code] != "" {
				continue
			}
			if ref, ok := rm["$ref"].(string); ok {
				if !typedErrorResponses[ref] {
					t.Errorf("%s %d: $ref %s is not a typed error response", name, status, ref)
				}
				continue
			}
			js, _ := content["application/json"].(map[string]any)
			schema, _ := js["schema"].(map[string]any)
			if rm["description"] == nil || schema["$ref"] != "#/components/schemas/Error" {
				t.Errorf("%s %d: an inline error needs a description and the Error schema", name, status)
			}
		}
	}
}

func TestOpenAPIOperationIDsStable(t *testing.T) {
	doc := loadDoc(t)
	have := map[string]bool{}
	for _, o := range ops(t, doc) {
		have[o.id()] = true
	}
	for _, id := range []string{"healthz", "openapi", "status", "listJobs", "createJob", "validateNewJob", "getJob",
		"patchJob", "deleteJob", "invokeJob", "enableJob", "disableJob", "validateJob", "listRuns", "createAdhocRun",
		"getRun", "getRunLogs", "cancelRun", "heartbeatRun", "completeRun", "listClients", "createClient", "patchClient",
		"deleteClient", "rotateClient", "listSecrets"} {
		if !have[id] {
			t.Errorf("operationId %s was renamed or removed", id)
		}
	}
}

func TestOpenAPIInfoSections(t *testing.T) {
	desc := loadDoc(t)["info"].(map[string]any)["description"].(string)
	at := 0
	for _, h := range []string{"## Quick rules", "## Authentication", "## Errors", "## Rate limits and back-pressure",
		"## Idempotency", "## Pagination", "## Async callbacks", "## Live logs"} {
		i := strings.Index(desc[at:], "\n"+h+"\n")
		if i < 0 {
			t.Fatalf("info.description lacks %q after offset %d (or it is out of order)", h, at)
		}
		at += i + 1
	}
	_, rest, _ := strings.Cut(desc, "\n## Quick rules\n")
	quick, _, _ := strings.Cut(rest, "\n## ")
	if strings.TrimSpace(quick) == "" {
		t.Fatal("## Quick rules is empty")
	}
	for _, line := range strings.Split(quick, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			t.Errorf("## Quick rules contains a heading: %q", line)
		}
	}
}

// ---------------------------------------------------------------- examples

var bodyTypes = map[string]func() any{
	"createJob":      func() any { return new(model.JobCreate) },
	"validateNewJob": func() any { return new(model.JobCreate) },
	"patchJob":       func() any { return new(model.JobPatch) },
	"invokeJob":      func() any { return new(invokeReq) },
	"createAdhocRun": func() any { return new(model.AdhocReq) },
	"completeRun":    func() any { return new(model.CompleteReq) },
	"createClient":   func() any { return new(clientIn) },
	"patchClient":    func() any { return new(clientIn) },
	"rotateClient":   func() any { return new(rotateReq) },
}

func TestOpenAPIExamplesDecode(t *testing.T) {
	doc := loadDoc(t)
	for _, o := range ops(t, doc) {
		if o.op["requestBody"] == nil {
			continue
		}
		id := o.id()
		mk, ok := bodyTypes[id]
		if !ok {
			t.Errorf("%s has a JSON body but no entry in bodyTypes", id)
			continue
		}
		exs := requestExamples(t, doc, o.op)
		if len(exs) == 0 {
			t.Errorf("%s: no request examples", id)
		}
		for name, v := range exs {
			dec := json.NewDecoder(bytes.NewReader(toJSON(t, v)))
			dec.DisallowUnknownFields()
			if err := dec.Decode(mk()); err != nil {
				t.Errorf("%s example %s does not decode: %v", id, name, err)
			}
		}
	}
}

// docVault holds every secret the documentation examples reference.
func docVault(d *engine.Deps) {
	d.Secrets = fakeVault{keys: []string{"Svc/TOKEN", "Other/TOKEN", "ReelDrop/TICK_TOKEN", "ReelDrop/KUMA_PUSH"}}
}

// docClient creates the client the examples name.
func (e *env) docClient() {
	e.t.Helper()
	r := e.do("POST", "/v1/clients", adminToken, map[string]any{
		"name": "svc", "scopes": all, "callback_hosts": []string{"reel-drop"}, "secret_ref_prefixes": []string{"ReelDrop/"},
	})
	if r.status != 201 {
		e.t.Fatalf("create svc: %d %s", r.status, r.body)
	}
}

func minimalJob(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	v := resolve(t, doc, map[string]any{"$ref": "#/components/examples/MinimalJob"})["value"]
	var out map[string]any
	if err := json.Unmarshal(toJSON(t, v), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOpenAPIExampleJobValidates(t *testing.T) {
	doc := loadDoc(t)
	e := newEnvWith(t, docVault, nil)
	e.docClient()
	r := e.do("POST", "/v1/jobs/validate", adminToken, minimalJob(t, doc))
	if r.status != 200 || r.json(t)["ok"] != true {
		t.Fatalf("MinimalJob does not validate: %d %s", r.status, r.body)
	}
}

func TestOpenAPIExamplesAccepted(t *testing.T) {
	doc := loadDoc(t)
	e := newEnvWith(t, docVault, nil)
	e.docClient()
	sent := map[string]bool{}
	send := func(opID, example, path, token string, want ...int) resp {
		t.Helper()
		o := opByID(t, doc, opID)
		v, ok := requestExamples(t, doc, o.op)[example]
		if !ok {
			t.Fatalf("%s has no example %s", opID, example)
		}
		sent[opID+"/"+example] = true
		r := e.do(o.method, path, token, string(toJSON(t, v)))
		if !slices.Contains(want, r.status) {
			t.Fatalf("%s example %s: %d %s (want %v)", opID, example, r.status, r.body, want)
		}
		return r
	}

	job := send("createJob", "minimal", "/v1/jobs", adminToken, 201).json(t)["id"].(string)
	sent["validateNewJob/minimal"] = true // TestOpenAPIExampleJobValidates sends it

	pushRef := func(id string) any {
		return e.do("GET", "/v1/jobs/"+id, adminToken, nil).json(t)["policy"].(map[string]any)["push_url_ref"]
	}
	// Order A: plain policy, then the push field.
	send("patchJob", "lateAndSkip", "/v1/jobs/"+job, adminToken, 200)
	send("patchJob", "withKumaPush", "/v1/jobs/"+job, adminToken, 200)
	if got := pushRef(job); got != "ReelDrop/KUMA_PUSH" {
		t.Fatalf("order A: push_url_ref = %v", got)
	}
	// Order B: the push field, then a policy that omits it (kept).
	second := minimalJob(t, doc)
	second["name"] = "publish-tick-2"
	job2 := e.do("POST", "/v1/jobs", adminToken, second).json(t)["id"].(string)
	send("patchJob", "withKumaPush", "/v1/jobs/"+job2, adminToken, 200)
	send("patchJob", "lateAndSkip", "/v1/jobs/"+job2, adminToken, 200)
	if got := pushRef(job2); got != "ReelDrop/KUMA_PUSH" {
		t.Fatalf("order B: push_url_ref = %v", got)
	}

	send("invokeJob", "manual", "/v1/jobs/"+job+"/invoke", adminToken, 202)
	send("createAdhocRun", "tick", "/v1/runs", adminToken, 200, 202)
	cid := send("createClient", "reelDrop", "/v1/clients", adminToken, 201).json(t)["client"].(map[string]any)["id"].(string)
	send("patchClient", "addWorker", "/v1/clients/"+cid, adminToken, 200)
	send("rotateClient", "fiveMinutes", "/v1/clients/"+cid+"/rotate", adminToken, 200)

	// completeRun needs a waiting async run and its run token.
	e.exec.set(func(context.Context, target.ExecInput) target.Result {
		code := 202
		return target.Result{Waiting: true, HTTPStatus: &code, Delivered: true}
	})
	async := minimalJob(t, doc)
	async["name"] = "async-tick"
	async["trigger"] = map[string]any{"type": "invoke"}
	async["target"].(map[string]any)["http"].(map[string]any)["mode"] = "async"
	aj := e.do("POST", "/v1/jobs", adminToken, async)
	if aj.status != 201 {
		t.Fatalf("async job: %d %s", aj.status, aj.body)
	}
	run := e.do("POST", "/v1/jobs/"+aj.json(t)["id"].(string)+"/invoke", adminToken, nil).json(t)["id"].(string)
	e.waitRun(adminToken, run, model.StatusWaiting)
	o := opByID(t, doc, "completeRun")
	body := toJSON(t, requestExamples(t, doc, o.op)["success"])
	sent["completeRun/success"] = true
	if r := e.do("POST", "/v1/runs/"+run+"/complete", "", string(body), "X-PLR-Run-Token", secure.RunToken([]byte("run-key"), run)); r.status != 200 && r.status != 202 {
		t.Fatalf("completeRun example: %d %s", r.status, r.body)
	}

	// Every documented request example was sent.
	for _, op := range ops(t, doc) {
		if op.op["requestBody"] == nil {
			continue
		}
		for name := range requestExamples(t, doc, op.op) {
			if !sent[op.id()+"/"+name] {
				t.Errorf("example %s/%s is never sent through its handler", op.id(), name)
			}
		}
	}
}

// ---------------------------------------------------------------- error codes

func TestErrorCodesDocumented(t *testing.T) {
	codes := map[string]bool{}
	for _, e := range errorTable {
		codes[e.code] = true
	}
	res := []*regexp.Regexp{
		regexp.MustCompile(`writeError\(w, http\.Status\w+, "([a-z_]+)"`),
		regexp.MustCompile(`FieldError\{[^}]*Code: "([a-z_]+)"`),
		regexp.MustCompile(`fieldErr\([^,]+, "([a-z_]+)"`),
		regexp.MustCompile(`Code\w+\s*=\s*"([a-z_]+)"`),
	}
	err := filepath.WalkDir("..", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, re := range res {
			for _, m := range re.FindAllSubmatch(src, -1) {
				codes[string(m[1])] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, floor := range []string{"secret_ref_missing", "secret_ref_not_permitted", "not_found", "unauthorized"} {
		if !codes[floor] {
			t.Fatalf("the source scan found no %s: the scan is broken", floor)
		}
	}
	doc := loadDoc(t)
	e := resolve(t, doc, map[string]any{"$ref": "#/components/schemas/Error"})
	desc := e["properties"].(map[string]any)["error"].(map[string]any)["properties"].(map[string]any)["code"].(map[string]any)["description"].(string)
	for _, c := range keysOf(codes) {
		if !strings.Contains(desc, "`"+c+"`") {
			t.Errorf("error code %s is emitted but not listed in Error.error.code", c)
		}
	}
}

// ---------------------------------------------------------------- response keys

// checkKeys asserts that every key of body is a property of schema and every
// required property is present, recursing into nested objects and items[0].
func checkKeys(t *testing.T, doc map[string]any, where string, schema map[string]any, body any) {
	t.Helper()
	schema = resolve(t, doc, schema)
	switch b := body.(type) {
	case map[string]any:
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			return // an untyped object (target, payload): nothing to compare
		}
		for k, v := range b {
			ps, ok := props[k]
			if !ok {
				t.Errorf("%s: response key %q is not in the schema", where, k)
				continue
			}
			checkKeys(t, doc, where+"."+k, resolve(t, doc, ps), v)
		}
		req, _ := schema["required"].([]any)
		for _, r := range req {
			if _, ok := b[r.(string)]; !ok {
				t.Errorf("%s: required key %q is missing from the response", where, r)
			}
		}
	case []any:
		if items, ok := schema["items"]; ok && len(b) > 0 {
			checkKeys(t, doc, where+"[0]", resolve(t, doc, items), b[0])
		}
	}
}

// expectShape checks a JSON response against the document's schema for that
// operation and status.
func expectShape(t *testing.T, doc map[string]any, opID string, r resp, status int) map[string]any {
	t.Helper()
	if r.status != status {
		t.Fatalf("%s: status %d, want %d: %s", opID, r.status, status, r.body)
	}
	o := opByID(t, doc, opID)
	rs := resolve(t, doc, o.op["responses"].(map[string]any)[strconv.Itoa(status)])
	js, _ := rs["content"].(map[string]any)["application/json"].(map[string]any)
	if js == nil {
		t.Fatalf("%s %d: no JSON content documented", opID, status)
	}
	m := r.json(t)
	checkKeys(t, doc, opID+" "+strconv.Itoa(status), resolve(t, doc, js["schema"]), m)
	return m
}

func TestResponseKeysDeclared(t *testing.T) {
	doc := loadDoc(t)
	e := newEnv(t)
	_, tok := e.client("a", all...)
	shape := func(opID string, r resp, status int) map[string]any {
		t.Helper()
		return expectShape(t, doc, opID, r, status)
	}

	shape("healthz", e.do("GET", "/healthz", "", nil), 200)
	shape("status", e.do("GET", "/v1/status", adminToken, nil), 200)
	job := shape("createJob", e.do("POST", "/v1/jobs", tok, fnJob("f")), 201)["id"].(string)
	shape("listJobs", e.do("GET", "/v1/jobs", tok, nil), 200)
	shape("getJob", e.do("GET", "/v1/jobs/"+job, tok, nil), 200)
	shape("patchJob", e.do("PATCH", "/v1/jobs/"+job, tok, map[string]any{"name": "f2"}), 200)
	shape("disableJob", e.do("POST", "/v1/jobs/"+job+"/disable", tok, nil), 200)
	shape("enableJob", e.do("POST", "/v1/jobs/"+job+"/enable", tok, nil), 200)
	shape("validateJob", e.do("POST", "/v1/jobs/"+job+"/validate", tok, nil), 200)
	shape("validateNewJob", e.do("POST", "/v1/jobs/validate", tok, fnJob("g")), 200)
	run := shape("invokeJob", e.do("POST", "/v1/jobs/"+job+"/invoke", tok, nil), 202)["id"].(string)
	shape("invokeJob", e.do("POST", "/v1/jobs/"+job+"/invoke?wait=30s", tok, map[string]any{"payload": 1}), 200)
	shape("listRuns", e.do("GET", "/v1/runs", tok, nil), 200)
	shape("getRun", e.do("GET", "/v1/runs/"+run, tok, nil), 200)
	adhoc := map[string]any{"target": fnJob("x")["target"], "payload": map[string]any{"n": 1}}
	shape("createAdhocRun", e.do("POST", "/v1/runs", tok, adhoc), 202)
	shape("createAdhocRun", e.do("POST", "/v1/runs?wait=30", tok, adhoc), 200)

	// cancelRun on a pending run: a one-shot job an hour out.
	at := fnJob("later")
	at["trigger"] = map[string]any{"type": "at", "at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}
	atID := shape("createJob", e.do("POST", "/v1/jobs", tok, at), 201)["id"].(string)
	page := e.do("GET", "/v1/runs?job="+atID, tok, nil).json(t)["items"].([]any)
	if len(page) != 1 {
		t.Fatalf("pending run of the at job: %v", page)
	}
	shape("cancelRun", e.do("POST", "/v1/runs/"+page[0].(map[string]any)["id"].(string)+"/cancel", tok, nil), 200)

	// cancelRun on a running run.
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		<-ctx.Done()
		return target.Result{Status: model.StatusFailed, Ambiguous: true}
	})
	running := e.do("POST", "/v1/jobs/"+job+"/invoke", tok, nil).json(t)["id"].(string)
	e.waitRun(tok, running, model.StatusRunning)
	shape("cancelRun", e.do("POST", "/v1/runs/"+running+"/cancel", tok, nil), 202)
	e.waitRun(tok, running, model.StatusCancelled)

	// Callbacks: heartbeat and complete while the target is still being called
	// (no lease; the completion is accepted early), then on a waiting run.
	release := make(chan struct{})
	e.exec.set(func(ctx context.Context, _ target.ExecInput) target.Result {
		select {
		case <-release:
		case <-ctx.Done():
		}
		code := 202
		return target.Result{Waiting: true, HTTPStatus: &code, Delivered: true}
	})
	aj := fnJob("pub")
	aj["target"].(map[string]any)["http"].(map[string]any)["mode"] = "async"
	ajID := e.do("POST", "/v1/jobs", tok, aj).json(t)["id"].(string)
	early := e.do("POST", "/v1/jobs/"+ajID+"/invoke", tok, nil).json(t)["id"].(string)
	e.waitRun(tok, early, model.StatusRunning)
	earlyTok := secure.RunToken([]byte("run-key"), early)
	if m := shape("heartbeatRun", e.do("POST", "/v1/runs/"+early+"/heartbeat", "", nil, "X-PLR-Run-Token", earlyTok), 200); m["lease_expires_at"] != nil {
		t.Errorf("heartbeat while running has a lease: %v", m)
	}
	shape("completeRun", e.do("POST", "/v1/runs/"+early+"/complete", "", `{"status":"success"}`, "X-PLR-Run-Token", earlyTok), 202)
	close(release)
	e.waitRun(tok, early, model.StatusSuccess)
	late := e.do("POST", "/v1/jobs/"+ajID+"/invoke", tok, nil).json(t)["id"].(string)
	e.waitRun(tok, late, model.StatusWaiting)
	lateTok := secure.RunToken([]byte("run-key"), late)
	if m := shape("heartbeatRun", e.do("POST", "/v1/runs/"+late+"/heartbeat", "", nil, "X-PLR-Run-Token", lateTok), 200); m["lease_expires_at"] == nil {
		t.Errorf("heartbeat on a waiting run has no lease: %v", m)
	}
	shape("completeRun", e.do("POST", "/v1/runs/"+late+"/complete", "", `{"status":"success","output":{"n":1}}`, "X-PLR-Run-Token", lateTok), 200)

	// Clients and secrets.
	shape("listClients", e.do("GET", "/v1/clients", adminToken, nil), 200)
	cid := shape("createClient", e.do("POST", "/v1/clients", adminToken, map[string]any{"name": "c2", "scopes": []string{"jobs:read"}}), 201)["client"].(map[string]any)["id"].(string)
	shape("patchClient", e.do("PATCH", "/v1/clients/"+cid, adminToken, map[string]any{"ntfy_topic": "plr"}), 200)
	shape("rotateClient", e.do("POST", "/v1/clients/"+cid+"/rotate", adminToken, map[string]any{"grace_sec": 60}), 200)
	if m := shape("rotateClient", e.do("POST", "/v1/clients/"+cid+"/rotate", adminToken, nil), 200); m["previous_valid_until"] != nil {
		t.Errorf("rotate without grace: previous_valid_until = %v", m["previous_valid_until"])
	}
	shape("listSecrets", e.do("GET", "/v1/secrets", tok, nil), 200)

	t.Run("healthz 503", healthzStalledShape)
}

// healthzStalledShape: a stopped loop past 60 s answers 503 with the Health body.
func healthzStalledShape(t *testing.T) {
	doc := loadDoc(t)
	fc := engine.NewFakeClock(time.Date(2026, 10, 22, 6, 0, 0, 0, time.UTC))
	e := newEnvWith(t, func(d *engine.Deps) { d.Clock = fc }, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.eng.Stop(ctx, fc.Now())
	fc.Advance(61 * time.Second)
	if m := expectShape(t, doc, "healthz", e.do("GET", "/healthz", "", nil), 503); m["ok"] != false {
		t.Fatalf("stalled healthz: %v", m)
	}
}

// ---------------------------------------------------------------- schema examples

// jsonTags returns the JSON field names of a struct type as encoding/json
// sees them: the tag up to its first comma, or the Go name when the tag has no
// name; "-" and unexported fields are skipped; untagged embedded structs are
// flattened.
func jsonTags(t reflect.Type) map[string]bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	out := map[string]bool{}
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if f.Anonymous && name == "" {
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				for k := range jsonTags(ft) {
					out[k] = true
				}
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		out[name] = true
	}
	return out
}

func TestOpenAPISchemaExamplesMatchDTOs(t *testing.T) {
	doc := loadDoc(t)
	schemas := doc["components"].(map[string]any)["schemas"].(map[string]any)
	check := func(name string, schema map[string]any, example any, typ reflect.Type) {
		t.Helper()
		ex, ok := example.(map[string]any)
		if !ok {
			t.Fatalf("%s: no example object", name)
		}
		if extra := diff(setOf(keysOf(ex)...), jsonTags(typ)); len(extra) > 0 {
			t.Errorf("%s example has keys the DTO lacks: %v", name, extra)
		}
		req, _ := schema["required"].([]any)
		for _, r := range req {
			if _, ok := ex[r.(string)]; !ok {
				t.Errorf("%s example lacks required key %s", name, r)
			}
		}
	}
	job := schemas["Job"].(map[string]any)
	check("Job", job, job["example"], reflect.TypeFor[JobDTO]())
	run := schemas["Run"].(map[string]any)
	check("Run", run, run["example"], reflect.TypeFor[RunDTO]())
	errS := schemas["Error"].(map[string]any)
	ex, _ := errS["example"].(map[string]any)
	if _, ok := ex["error"]; !ok {
		t.Fatal("Error example lacks error")
	}
	check("Error.error", errS["properties"].(map[string]any)["error"].(map[string]any), ex["error"], reflect.TypeFor[apiError]())
}

// ---------------------------------------------------------------- schema properties vs DTOs

// schemaProps returns the property keys of components.schemas.<name>, or of a
// nested object reached by path. $ref is resolved and allOf merged. A node
// without properties is fatal, never an empty set.
func schemaProps(t *testing.T, doc map[string]any, name string, path ...string) map[string]bool {
	t.Helper()
	node := schemaNode(t, doc, name, path...)
	props, ok := node["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema %s %v has no properties", name, path)
	}
	return setOf(keysOf(props)...)
}

func schemaNode(t *testing.T, doc map[string]any, name string, path ...string) map[string]any {
	t.Helper()
	node := resolve(t, doc, doc["components"].(map[string]any)["schemas"].(map[string]any)[name])
	if node == nil {
		t.Fatalf("no schema %s", name)
	}
	for _, p := range path {
		next := resolve(t, doc, node[p])
		if next == nil {
			t.Fatalf("schema %s: no %q in path %v", name, p, path)
		}
		node = next
	}
	if all, ok := node["allOf"].([]any); ok {
		merged := map[string]any{}
		for k, v := range node {
			merged[k] = v
		}
		props := map[string]any{}
		for _, part := range all {
			pm, _ := resolve(t, doc, part)["properties"].(map[string]any)
			for k, v := range pm {
				props[k] = v
			}
		}
		if own, ok := node["properties"].(map[string]any); ok {
			for k, v := range own {
				props[k] = v
			}
		}
		merged["properties"] = props
		node = merged
	}
	return node
}

var schemaDTOs = []struct {
	schema string
	path   []string       // nested object inside the schema, nil for the top level
	types  []reflect.Type // the union of their tags must equal the property set
}{
	{"Policy", nil, []reflect.Type{reflect.TypeFor[model.Policy](), reflect.TypeFor[model.PolicyIn]()}},
	{"Policy", []string{"properties", "retry"}, []reflect.Type{reflect.TypeFor[model.RetryPolicy](), reflect.TypeFor[model.RetryIn]()}},
	{"Job", nil, []reflect.Type{reflect.TypeFor[JobDTO]()}},
	{"Run", nil, []reflect.Type{reflect.TypeFor[RunDTO]()}},
	{"Client", nil, []reflect.Type{reflect.TypeFor[ClientDTO]()}},
	{"ClientIn", nil, []reflect.Type{reflect.TypeFor[clientIn]()}},
	{"JobCreate", nil, []reflect.Type{reflect.TypeFor[model.JobCreate]()}},
	{"JobPatch", nil, []reflect.Type{reflect.TypeFor[model.JobPatch]()}},
	{"Trigger", nil, []reflect.Type{reflect.TypeFor[model.Trigger](), reflect.TypeFor[model.TriggerIn]()}},
	{"Target", nil, []reflect.Type{reflect.TypeFor[model.TargetIn]()}},
	{"InvokeRequest", nil, []reflect.Type{reflect.TypeFor[invokeReq]()}},
	{"AdhocRequest", nil, []reflect.Type{reflect.TypeFor[model.AdhocReq]()}},
	{"CompleteRequest", nil, []reflect.Type{reflect.TypeFor[model.CompleteReq]()}},
	{"RotateRequest", nil, []reflect.Type{reflect.TypeFor[rotateReq]()}},
	{"HTTPTarget", nil, []reflect.Type{reflect.TypeFor[model.HTTPIn]()}},
	{"HTTPTarget", []string{"properties", "auth"}, []reflect.Type{reflect.TypeFor[model.HTTPAuth]()}},
	{"JobPage", nil, []reflect.Type{reflect.TypeFor[page[JobDTO]]()}},
	{"RunPage", nil, []reflect.Type{reflect.TypeFor[page[RunDTO]]()}},
	{"Report", []string{"properties", "checks", "items"}, []reflect.Type{reflect.TypeFor[engine.Check]()}},
	{"Error", []string{"properties", "error"}, []reflect.Type{reflect.TypeFor[apiError]()}},
}

// schemaDTOExempt lists schemas built as maps or anonymous structs in the
// handlers. TestResponseKeysDeclared guards them one way only: a key a handler
// sends must be documented.
var schemaDTOExempt = map[string]string{
	"Health":           "map in healthz",
	"Status":           "map in status",
	"ClientList":       "map in listClients",
	"ClientCreated":    "map in createClient",
	"TokenRotated":     "map in rotateClient",
	"SecretNames":      "map in listSecrets",
	"CancelAccepted":   "map in cancelRun",
	"HeartbeatResult":  "map in heartbeat",
	"CompleteAccepted": "map in complete",
	"Report":           "top level is a map in validateNewJob/validateJob; checks.items is paired with engine.Check",
}

// schemaPropsExempt names schema keys (by "Schema.key") that deliberately
// differ from the Go side. Empty today.
var schemaPropsExempt = map[string]string{}

// ioAsymmetric names keys present on only one side of an output/input pair.
var ioAsymmetric = map[string]string{
	"Trigger.delay_sec": "input-only: resolved to at on create",
}

// unpairedNested are nested objects documented without properties.
var unpairedNested = map[string]string{
	"Target.container": "ContainerIn / InlineIn (validate.go) are not enabled in prod; Delivery 1b (2026-10-18) dropped them",
	"Target.inline":    "ContainerIn / InlineIn (validate.go) are not enabled in prod; Delivery 1b (2026-10-18) dropped them",
}

var marshalerType = reflect.TypeFor[json.Marshaler]()

func TestOpenAPISchemaPropertiesMatchDTOs(t *testing.T) {
	doc := loadDoc(t)
	covered := map[string]bool{}
	for _, p := range schemaDTOs {
		covered[p.schema] = true
		label := p.schema
		if len(p.path) > 0 {
			label += "." + strings.Join(p.path, ".")
		}
		props := schemaProps(t, doc, p.schema, p.path...)
		union := map[string]bool{}
		for _, typ := range p.types {
			if typ.Implements(marshalerType) || reflect.PointerTo(typ).Implements(marshalerType) {
				t.Errorf("%s: %s implements json.Marshaler; compare marshalled keys instead", label, typ)
			}
			for k := range jsonTags(typ) {
				union[k] = true
			}
		}
		for k, reason := range schemaPropsExempt {
			name, key, _ := strings.Cut(k, ".")
			if name != p.schema || len(p.path) > 0 {
				continue
			}
			if props[key] == union[key] {
				t.Errorf("schemaPropsExempt %s (%s) no longer differs: remove the stale exemption", k, reason)
			}
			delete(props, key)
			delete(union, key)
		}
		if missing, extra := diff(union, props), diff(props, union); len(missing) > 0 || len(extra) > 0 {
			t.Errorf("%s: missing in schema: %v; no Go field: %v", label, missing, extra)
		}
		if len(p.types) == 2 {
			out, in := jsonTags(p.types[0]), jsonTags(p.types[1])
			for _, k := range append(diff(out, in), diff(in, out)...) {
				if ioAsymmetric[p.schema+"."+k] == "" {
					t.Errorf("%s: %q is on only one of %s / %s; add it to the other or to ioAsymmetric", label, k, p.types[0], p.types[1])
				}
			}
			for k, reason := range ioAsymmetric {
				name, key, _ := strings.Cut(k, ".")
				if name == p.schema && len(p.path) == 0 && out[key] == in[key] {
					t.Errorf("ioAsymmetric %s (%s) no longer differs: remove the stale exemption", k, reason)
				}
			}
		}
	}
	for name := range doc["components"].(map[string]any)["schemas"].(map[string]any) {
		if !covered[name] && schemaDTOExempt[name] == "" {
			t.Errorf("schema %s is neither paired in schemaDTOs nor listed in schemaDTOExempt", name)
		}
	}
	for k := range schemaDTOExempt {
		if covered[k] && k != "Report" {
			t.Errorf("schemaDTOExempt %s is also paired: remove the exemption", k)
		}
	}
	for k, reason := range unpairedNested {
		name, key, _ := strings.Cut(k, ".")
		node := schemaNode(t, doc, name, "properties", key)
		if node["type"] != "object" || node["properties"] != nil {
			t.Errorf("unpairedNested %s (%s) is no longer a property-less object: pair it", k, reason)
		}
	}
}

// ---------------------------------------------------------------- enums

// constSets parses ../model/types.go and returns string-literal const values
// grouped by key: a typed spec by its type name (for example "Status"), an
// untyped block by its doc comment's first line. Non-string values are skipped.
func constSets(t *testing.T) map[string][]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "model", "types.go"), nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		docKey := ""
		if gd.Doc != nil {
			docKey, _, _ = strings.Cut(gd.Doc.Text(), "\n")
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			key := docKey
			if id, ok := vs.Type.(*ast.Ident); ok {
				key = id.Name
			}
			for _, v := range vs.Values {
				lit, ok := v.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING || key == "" {
					continue
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				out[key] = append(out[key], s)
			}
		}
	}
	return out
}

var enumPairs = []struct {
	schema   string   // components.schemas name
	path     []string // to the property holding enum
	constKey string   // key (or key prefix) in constSets
}{
	{"Run", []string{"properties", "status"}, "Status"},
	{"Run", []string{"properties", "trigger"}, "Run trigger kinds."},
	{"Job", []string{"properties", "state"}, "Job states."},
	{"Policy", []string{"properties", "overlap"}, "Overlap policies."},
	{"Policy", []string{"properties", "priority"}, "Priorities"},
	{"Policy", []string{"properties", "delivery"}, "Delivery modes."},
}

func enumOf(t *testing.T, doc map[string]any, name string, path ...string) map[string]bool {
	t.Helper()
	vals, ok := schemaNode(t, doc, name, path...)["enum"].([]any)
	if !ok {
		t.Fatalf("schema %s %v has no enum", name, path)
	}
	out := map[string]bool{}
	for _, v := range vals {
		out[v.(string)] = true
	}
	return out
}

func TestOpenAPIEnumsMatchCode(t *testing.T) {
	doc := loadDoc(t)
	sets := constSets(t)
	for _, p := range enumPairs {
		var keys []string
		for k := range sets {
			if k == p.constKey || strings.HasPrefix(k, p.constKey) {
				keys = append(keys, k)
			}
		}
		if len(keys) != 1 {
			t.Errorf("const key %q matches %v, want exactly one", p.constKey, keys)
			continue
		}
		code := setOf(sets[keys[0]]...)
		if len(code) == 0 {
			t.Errorf("const set %q is empty: the parser found nothing", p.constKey)
			continue
		}
		schema := enumOf(t, doc, p.schema, p.path...)
		if missing, extra := diff(code, schema), diff(schema, code); len(missing) > 0 || len(extra) > 0 {
			t.Errorf("%s.%s enum: missing %v, not in code %v", p.schema, p.path[len(p.path)-1], missing, extra)
		}
	}

	code := setOf(model.AllScopes...)
	schema := enumOf(t, doc, "Client", "properties", "scopes", "items")
	if missing, extra := diff(code, schema), diff(schema, code); len(missing) > 0 || len(extra) > 0 {
		t.Errorf("Client.scopes enum: missing %v, not in code %v", missing, extra)
	}
	// Every scope has a row in the Authentication table, and each route is
	// listed in the row of the scope it requires.
	desc := doc["info"].(map[string]any)["description"].(string)
	rows := map[string]string{}
	for _, line := range strings.Split(desc, "\n") {
		for _, sc := range model.AllScopes {
			if strings.HasPrefix(line, "| `"+sc+"` |") {
				rows[sc] = line
			}
		}
	}
	for _, sc := range model.AllScopes {
		if rows[sc] == "" {
			t.Errorf("scope %s has no row in the ## Authentication table", sc)
		}
	}
	ids := map[string]string{}
	for _, o := range ops(t, doc) {
		ids[o.method+" "+o.path] = o.id()
	}
	opRE := regexp.MustCompile(`[A-Za-z]+`)
	for _, rt := range (&Server{}).routeTable() {
		if rt.static || rt.scope == "" {
			continue
		}
		id := ids[rt.method+" "+rt.pattern]
		if !slices.Contains(opRE.FindAllString(rows[rt.scope], -1), id) {
			t.Errorf("%s (%s) is not listed in the %s row of the scope table", id, rt.method+" "+rt.pattern, rt.scope)
		}
	}
}
