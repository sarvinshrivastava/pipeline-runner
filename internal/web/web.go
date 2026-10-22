// Package web embeds the dashboard and the API docs page. It has no build
// step: plain HTML, CSS and scripts, all served from this origin so the strict
// CSP holds.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed static
var static embed.FS

//go:embed static/apidocs/VERSION
var docsVersion string

// DocsVersion is the vendored Swagger UI version, used as the ?v= cache key.
func DocsVersion() string { return strings.TrimSpace(docsVersion) }

// versioned lists the files referenced with ?v=<DocsVersion>.
var versioned = map[string]bool{"/docs/swagger-ui-bundle.js": true, "/docs/swagger-ui.css": true}

// Files is the embedded static tree, rooted at static/.
func Files() fs.FS {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // the embed directive guarantees the directory exists
	}
	return sub
}

// Handler serves the dashboard ("/", "/app.js", "/style.css") and the API docs
// under "/docs/" from static/apidocs/. The directory is not named docs/: the
// local .git/info/exclude has an unanchored `docs/` that would silently drop it
// from git. /apidocs/* has no route.
func Handler() http.Handler {
	fsrv := http.FileServerFS(Files())
	sub, err := fs.Sub(Files(), "apidocs")
	if err != nil {
		panic(err) // embedded at build time; cannot fail at runtime
	}
	docsSrv := http.StripPrefix("/docs", http.FileServerFS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// Explicit types: FileServer keeps a preset Content-Type, and this
		// removes the dependence on the host's mime table.
		switch path.Ext(p) {
		case ".js":
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		case ".css":
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		}
		// Immutable only when the request carries the current version.
		if versioned[p] && r.URL.Query().Get("v") == DocsVersion() {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		if strings.HasPrefix(p, "/docs/") {
			docsSrv.ServeHTTP(w, r)
			return
		}
		fsrv.ServeHTTP(w, r)
	})
}
