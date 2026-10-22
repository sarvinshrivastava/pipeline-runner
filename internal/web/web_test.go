package web

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// TestStaticNoInline keeps our pages compatible with default-src 'self': no
// inline scripts, inline handlers or inline styles, and no innerHTML. The
// vendored Swagger bundle is exempt: it is not our code, and its sha256 is
// pinned by TestDocsVendorPinned.
func TestStaticNoInline(t *testing.T) {
	for _, page := range []string{"index.html", "apidocs/index.html"} {
		html, err := fs.ReadFile(Files(), page)
		if err != nil {
			t.Fatal(err)
		}
		for name, re := range map[string]*regexp.Regexp{
			"inline script":  regexp.MustCompile(`<script(?:\s[^>]*)?>\s*[^<\s]`),
			"inline handler": regexp.MustCompile(`\son[a-z]+\s*=`),
			"inline style":   regexp.MustCompile(`\sstyle\s*=|<style`),
			"data/js URL":    regexp.MustCompile(`(?i)(javascript:|data:)`),
		} {
			if re.Match(html) {
				t.Errorf("%s contains %s", page, name)
			}
		}
	}
	for _, script := range []string{"app.js", "apidocs/init.js"} {
		js, _ := fs.ReadFile(Files(), script)
		if strings.Contains(string(js), "innerHTML") || strings.Contains(string(js), "insertAdjacentHTML") {
			t.Errorf("%s must use textContent, not HTML injection", script)
		}
	}
	// The docs link opens a new context without the dashboard's sessionStorage.
	html, _ := fs.ReadFile(Files(), "index.html")
	link := regexp.MustCompile(`<a [^>]*href="/docs/"[^>]*>`).Find(html)
	if link == nil || !strings.Contains(string(link), `target="_blank"`) || !strings.Contains(string(link), `rel="noopener noreferrer"`) {
		t.Errorf("dashboard docs link must carry target=_blank and rel=\"noopener noreferrer\": %s", link)
	}
}

func get(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestServesAssets(t *testing.T) {
	h := Handler()
	for p, ct := range map[string]string{
		"/":                          "text/html; charset=utf-8",
		"/app.js":                    "text/javascript; charset=utf-8",
		"/style.css":                 "text/css; charset=utf-8",
		"/docs/":                     "text/html; charset=utf-8",
		"/docs/init.js":              "text/javascript; charset=utf-8",
		"/docs/swagger-ui.css":       "text/css; charset=utf-8",
		"/docs/swagger-ui-bundle.js": "text/javascript; charset=utf-8",
	} {
		rec := get(h, http.MethodGet, p)
		if rec.Code != 200 || rec.Body.Len() == 0 {
			t.Errorf("%s: %d", p, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != ct {
			t.Errorf("%s: Content-Type %q, want %q", p, got, ct)
		}
	}
}

func TestDocsCacheHeaders(t *testing.T) {
	h := Handler()
	const immutable = "public, max-age=31536000, immutable"
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for target, want := range map[string]string{
			"/docs/swagger-ui-bundle.js?v=" + DocsVersion(): immutable,
			"/docs/swagger-ui.css?v=" + DocsVersion():       immutable,
			"/docs/swagger-ui-bundle.js":                    "no-cache",
			"/docs/swagger-ui.css":                          "no-cache",
			"/docs/swagger-ui-bundle.js?v=0.0.0":            "no-cache",
			"/docs/swagger-ui.css?v=0.0.0":                  "no-cache",
			"/docs/":                                        "no-cache",
			"/docs/init.js":                                 "no-cache",
			"/docs/init.js?v=" + DocsVersion():              "no-cache",
			"/":                                             "no-cache",
		} {
			rec := get(h, method, target)
			if rec.Code != 200 {
				t.Errorf("%s %s: %d", method, target, rec.Code)
			}
			if got := rec.Header().Get("Cache-Control"); got != want {
				t.Errorf("%s %s: Cache-Control %q, want %q", method, target, got, want)
			}
		}
	}
}

// vendorPin records what a human checked when vendoring a Swagger UI version.
type vendorPin struct {
	bundle, css     string // sha256 printed by scripts/vendor-swagger-ui.sh
	sanitizeCalls   int    // ".sanitize(" call sites in the bundle
	dompurifyPrefix string // the bundled DOMPurify version starts with this
}

// docsVendorSHA256 is keyed by DocsVersion(). Re-vendoring needs a new entry.
var docsVendorSHA256 = map[string]vendorPin{
	"5.33.1": {
		bundle:          "050bc415ee7048dcd881682678f720264e7da5e373f7461d7c58c755305255f7",
		css:             "1ac324f7dcd27e4b9386b4bd6421271ec147e922a22c05ba24b11515e9aa6321",
		sanitizeCalls:   1,
		dompurifyPrefix: "3.4.",
	},
}

func TestDocsVendorPinned(t *testing.T) {
	v := DocsVersion()
	pin, ok := docsVendorSHA256[v]
	if !ok {
		t.Fatalf("no docsVendorSHA256 entry for %s: run scripts/vendor-swagger-ui.sh and record its sha256 values, the sanitize count and the DOMPurify version", v)
	}
	read := func(name string) []byte {
		t.Helper()
		b, err := fs.ReadFile(Files(), "apidocs/"+name)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	sum := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	bundle := read("swagger-ui-bundle.js")
	if got := sum(bundle); got != pin.bundle {
		t.Errorf("swagger-ui-bundle.js sha256 %s, pinned %s", got, pin.bundle)
	}
	if got := sum(read("swagger-ui.css")); got != pin.css {
		t.Errorf("swagger-ui.css sha256 %s, pinned %s", got, pin.css)
	}
	read("LICENSE")
	read("NOTICE")

	// DOMPurify's known advisories need IN_PLACE: an upgrade that starts using
	// it, or sanitizes in new places, gets a human look first.
	if !strings.Contains(string(bundle), `DOMPurify.version="`+pin.dompurifyPrefix) {
		t.Errorf("bundle does not carry DOMPurify %s*", pin.dompurifyPrefix)
	}
	if n := strings.Count(string(bundle), ".sanitize("); n != pin.sanitizeCalls {
		t.Errorf("bundle has %d .sanitize( calls, pinned %d", n, pin.sanitizeCalls)
	}
	if regexp.MustCompile(`IN_PLACE\s*:\s*(!0|true|[A-Za-z_$])`).Match(bundle) {
		t.Error("bundle sets DOMPurify IN_PLACE: check the DOMPurify advisories before vendoring")
	}

	html := string(read("index.html"))
	for _, ref := range []string{"swagger-ui-bundle.js?v=" + v, "swagger-ui.css?v=" + v} {
		if !strings.Contains(html, ref) {
			t.Errorf("apidocs/index.html does not reference %s", ref)
		}
	}
	initJS := string(read("init.js"))
	for _, want := range []string{`url: "/v1/openapi.json"`, "validatorUrl: null", "persistAuthorization: false",
		"queryConfigEnabled: false", `supportedSubmitMethods: ["get", "head"]`, "requestInterceptor: guardRequest", "follow"} {
		if !strings.Contains(initJS, want) {
			t.Errorf("init.js lacks %s", want)
		}
	}

	_ = fs.WalkDir(Files(), "apidocs", func(p string, _ fs.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, ".map") {
			t.Errorf("source map %s is vendored", p)
		}
		return err
	})
	if _, err := fs.Stat(Files(), "docs"); err == nil {
		t.Error("static/docs exists: .git/info/exclude ignores docs/, so it would never be committed")
	}
}
