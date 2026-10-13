// Package web embeds the dashboard. It has no build step: plain HTML, CSS and
// one script, all served from this origin so the strict CSP holds.
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var static embed.FS

// Files is the embedded static tree, rooted at static/.
func Files() fs.FS {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // the embed directive guarantees the directory exists
	}
	return sub
}

// Handler serves "/" (index.html), "/app.js" and "/style.css".
func Handler() http.Handler {
	fsrv := http.FileServerFS(Files())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		fsrv.ServeHTTP(w, r)
	})
}
