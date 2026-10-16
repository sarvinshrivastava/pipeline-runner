package httpt

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sarvinshrivastava/pipeline-runner/internal/model"
)

func TestNonJSONBodyStoredAsString(t *testing.T) {
	res := exec().Execute(context.Background(), input(t, serve(t, reply(200, "plain <b>text</b>")), "sync"))
	if res.Status != model.StatusSuccess || res.OutputTruncated {
		t.Fatalf("res = %+v", res)
	}
	var s string
	if err := json.Unmarshal(res.Output, &s); err != nil || s != "plain <b>text</b>" {
		t.Fatalf("output %s is not the body as a JSON string: %v", res.Output, err)
	}
	// Valid JSON is kept as is, an empty body as nothing.
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, `[1,{"a":"b"}]`)), "sync"))
	if string(res.Output) != `[1,{"a":"b"}]` {
		t.Fatalf("json output = %s", res.Output)
	}
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, "")), "sync"))
	if res.Output != nil || res.Status != model.StatusSuccess {
		t.Fatalf("empty body: %+v", res)
	}
	// Failures keep non-JSON bodies the same way.
	res = exec().Execute(context.Background(), input(t, serve(t, reply(500, "boom")), "sync"))
	if string(res.Output) != `"boom"` || res.Error != "HTTP 500" {
		t.Fatalf("500 body: %+v", res)
	}
}

func TestOversizedBodyTruncatedToCap(t *testing.T) {
	// Even valid JSON past the cap becomes a capped JSON string, never cut JSON.
	big := `{"k":"` + strings.Repeat("y", model.MaxOutputBytes) + `"}`
	res := exec().Execute(context.Background(), input(t, serve(t, reply(200, big)), "sync"))
	if res.Status != model.StatusSuccess || !res.OutputTruncated {
		t.Fatalf("res: truncated=%v status=%s", res.OutputTruncated, res.Status)
	}
	var s string
	if err := json.Unmarshal(res.Output, &s); err != nil {
		t.Fatalf("truncated output is not a JSON string: %v", err)
	}
	if len(s) != model.MaxOutputBytes || s != big[:model.MaxOutputBytes] {
		t.Fatalf("stored %d bytes, want the first %d", len(s), model.MaxOutputBytes)
	}
	// A body exactly at the cap is not truncated.
	exact := strings.Repeat("z", model.MaxOutputBytes)
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, exact)), "sync"))
	if res.OutputTruncated {
		t.Fatal("body at the cap marked truncated")
	}
	// defer_until in a truncated body is ignored (the JSON is incomplete).
	deferBig := `{"defer_until":"2026-10-13T10:00:00Z","pad":"` + strings.Repeat("p", model.MaxOutputBytes) + `"}`
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, deferBig)), "sync"))
	if res.DeferUntil != nil || res.Status != model.StatusSuccess {
		t.Fatalf("truncated defer_until honoured: %+v", res.DeferUntil)
	}
	// A secret straddling the cut is masked, not left half-visible.
	secret := "bearer-secret-value"
	cut := strings.Repeat("a", model.MaxOutputBytes-5) + secret + strings.Repeat("b", 100)
	res = exec().Execute(context.Background(), input(t, serve(t, reply(200, cut)), "sync"))
	if strings.Contains(string(res.Output), secret[:5]) {
		t.Fatalf("secret prefix survived the cut: …%s", res.Output[len(res.Output)-20:])
	}
}

func TestRetryAfterParsing(t *testing.T) {
	cases := []struct {
		name, header string
		code         int
		want         *time.Time
	}{
		{"zero seconds", "0", 429, ptrTime(now)},
		{"padded seconds", " 30 ", 503, ptrTime(now.Add(30 * time.Second))},
		{"huge seconds capped", "999999999999", 429, ptrTime(now.Add(366 * 24 * time.Hour))},
		{"RFC 1123 date", "Tue, 13 Oct 2026 10:00:00 GMT", 429, ptrTime(now.Add(time.Hour))},
		{"RFC 850 date", "Tuesday, 13-Oct-26 09:01:00 GMT", 503, ptrTime(now.Add(time.Minute))},
		{"asctime date", "Tue Oct 13 09:02:00 2026", 429, ptrTime(now.Add(2 * time.Minute))},
		{"negative", "-5", 429, nil},
		{"fraction", "1.5", 429, nil},
		{"garbage", "soon", 503, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := exec().Execute(context.Background(), input(t, serve(t, reply(c.code, "", "Retry-After", c.header)), "sync"))
			if c.want == nil {
				if res.DeferUntil != nil || res.Status != model.StatusFailed || !res.Ambiguous {
					t.Fatalf("unparseable Retry-After must fail ambiguously: %+v", res)
				}
				return
			}
			if res.DeferUntil == nil || !res.DeferUntil.Equal(*c.want) || res.Status != "" {
				t.Fatalf("DeferUntil = %v, want %v (res %+v)", res.DeferUntil, c.want, res)
			}
			if res.DeferUntil.Location() != time.UTC {
				t.Fatalf("DeferUntil not UTC: %v", res.DeferUntil)
			}
		})
	}
	// Retry-After on other codes is ignored.
	res := exec().Execute(context.Background(), input(t, serve(t, reply(500, "", "Retry-After", "30")), "sync"))
	if res.DeferUntil != nil {
		t.Fatalf("500 with Retry-After deferred: %+v", res)
	}
}

func TestDeferUntilVariants(t *testing.T) {
	cases := []struct {
		name, body string
		want       *time.Time
		bad        bool
	}{
		{"offset normalised to UTC", `{"defer_until":"2026-10-13T15:30:00+05:30"}`, ptrTime(now.Add(time.Hour)), false},
		{"fractional seconds", `{"defer_until":"2026-10-13T10:00:00.250Z"}`, ptrTime(now.Add(time.Hour + 250*time.Millisecond)), false},
		{"leading whitespace and BOM", "\ufeff \n{\"defer_until\":\"2026-10-13T10:00:00Z\"}", ptrTime(now.Add(time.Hour)), false},
		{"null", `{"defer_until":null}`, nil, false},
		{"absent", `{"other":1}`, nil, false},
		{"not an object", `["defer_until"]`, nil, false},
		{"number", `{"defer_until":123}`, nil, false},
		{"unparseable string", `{"defer_until":"next tuesday"}`, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := exec().Execute(context.Background(), input(t, serve(t, reply(200, c.body)), "sync"))
			if c.want != nil {
				if res.DeferUntil == nil || !res.DeferUntil.Equal(*c.want) || res.DeferUntil.Location() != time.UTC || res.Status != "" {
					t.Fatalf("DeferUntil = %v, want %v (status %q)", res.DeferUntil, c.want, res.Status)
				}
				return
			}
			if res.DeferUntil != nil || res.Status != model.StatusSuccess {
				t.Fatalf("must be plain success: %+v", res)
			}
			if c.bad != (res.OutputError != "") {
				t.Fatalf("OutputError = %q, bad = %v", res.OutputError, c.bad)
			}
		})
	}
	// defer_until is only read on 2xx.
	res := exec().Execute(context.Background(), input(t, serve(t, reply(500, `{"defer_until":"2026-10-13T10:00:00Z"}`)), "sync"))
	if res.DeferUntil != nil {
		t.Fatal("defer_until honoured on a 500")
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
