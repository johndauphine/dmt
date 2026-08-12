package webui

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/version"
)

// staticFS holds the embedded front-end. Everything under static/ is compiled
// into the dmt binary, so there are no runtime asset dependencies — dmt ships
// as a single self-contained binary. The #582 SPA build writes its production
// bundle here; the foundation ships a placeholder index.html.
//
//go:embed all:static
var staticFS embed.FS

// manifestPath is served with an explicit Content-Type: Go's builtin mime
// table (mime/type.go) knows .json and .svg but not .webmanifest, so
// http.FileServer would otherwise guess (or, on a system with no OS mime
// database, serve none at all), and some browsers require the manifest's
// media type to recognize an installable PWA.
const manifestPath = "manifest.webmanifest"

// swPath is the service worker. It is intercepted (rather than served
// directly by http.FileServer) so __DMT_VERSION__ can be substituted with the
// running build's version — see sw.js's own comment for why: it keys the
// cache name, so a dmt upgrade invalidates the previous cache instead of a
// browser serving yesterday's app.js indefinitely.
const swPath = "sw.js"

// staticHandler serves the embedded assets with an SPA fallback: any path that
// doesn't resolve to a real file is served index.html so client-side routing
// (added in #582) works on deep links and refreshes.
func (s *Server) staticHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		// The embed directive guarantees static/ exists; a failure here is a
		// build-time bug, not a runtime condition.
		panic(fmt.Sprintf("webui: embedded static assets missing: %v", err))
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if upath == "" {
			upath = "index.html"
		}
		if upath == swPath {
			serveServiceWorker(w, sub)
			return
		}
		if _, statErr := fs.Stat(sub, upath); statErr != nil {
			// Not a real asset → serve the SPA shell at "/".
			shell := r.Clone(r.Context())
			shell.URL.Path = "/"
			fileServer.ServeHTTP(w, shell)
			return
		}
		if upath == manifestPath {
			w.Header().Set("Content-Type", "application/manifest+json")
		}
		fileServer.ServeHTTP(w, r)
	})
}

// serveServiceWorker serves sw.js with __DMT_VERSION__ replaced by the
// running build's version.
func serveServiceWorker(w http.ResponseWriter, sub fs.FS) {
	data, err := fs.ReadFile(sub, swPath)
	if err != nil {
		// The embed directive guarantees this file exists; a failure here is
		// a build-time bug, not a runtime condition.
		panic(fmt.Sprintf("webui: embedded %s missing: %v", swPath, err))
	}
	data = bytes.ReplaceAll(data, []byte("__DMT_VERSION__"), []byte(version.Version))
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	// Service workers must never be served from a stale HTTP cache: a browser
	// that cached this response would keep re-registering an old worker even
	// after the version placeholder above changes on the next dmt build.
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}
