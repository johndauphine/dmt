package webui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// staticFS holds the embedded front-end. Everything under static/ is compiled
// into the dmt binary, so there are no runtime asset dependencies — dmt ships
// as a single self-contained binary. The #582 SPA build writes its production
// bundle here; the foundation ships a placeholder index.html.
//
//go:embed all:static
var staticFS embed.FS

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
		if _, statErr := fs.Stat(sub, upath); statErr != nil {
			// Not a real asset → serve the SPA shell at "/".
			shell := r.Clone(r.Context())
			shell.URL.Path = "/"
			fileServer.ServeHTTP(w, shell)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}
