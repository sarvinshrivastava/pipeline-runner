package web

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// TestStaticNoInline keeps the dashboard compatible with default-src 'self':
// no inline scripts, inline handlers or inline styles, and no innerHTML.
func TestStaticNoInline(t *testing.T) {
	html, err := fs.ReadFile(Files(), "index.html")
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
			t.Errorf("index.html contains %s", name)
		}
	}
	js, _ := fs.ReadFile(Files(), "app.js")
	if strings.Contains(string(js), "innerHTML") || strings.Contains(string(js), "insertAdjacentHTML") {
		t.Error("app.js must use textContent, not HTML injection")
	}
}

func TestServesAssets(t *testing.T) {
	h := Handler()
	for _, p := range []string{"/", "/app.js", "/style.css"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != 200 || rec.Body.Len() == 0 {
			t.Errorf("%s: %d", p, rec.Code)
		}
	}
}
