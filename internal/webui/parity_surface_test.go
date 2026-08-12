package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/command"
)

// webRoutes maps each WebSupported command path (command.WebSurface) to a
// representative authenticated API route that operates it. A WebSupported path
// with no mapping fails the parity test — the registry and this table move
// together, the WebUI analogue of the TUI's slashFor map.
var webRoutes = map[string][]string{
	"run":              {"POST /api/run"},
	"resume":           {"POST /api/resume"},
	"status":           {"GET /api/run", "GET /api/status"},
	"validate":         {"POST /api/validate"},
	"diagnose":         {"POST /api/diagnose"},
	"history":          {"GET /api/history"},
	"profile":          {"GET /api/profiles"},
	"preflight":        {"POST /api/preflight"},
	"analyze":          {"POST /api/analyze"},
	"ai":               {"POST /api/ai/config-review"},
	"init-secrets":     {"POST /api/init-secrets"},
	"setup":            {"POST /api/setup/start", "POST /api/setup/input"},
	"cache":            {"POST /api/cache/clear"},
	"profile save":     {"POST /api/profiles/save"},
	"profile list":     {"GET /api/profiles"},
	"profile delete":   {"DELETE /api/profiles/x"},
	"profile export":   {"POST /api/profiles/x/export"},
	"ai config-review": {"POST /api/ai/config-review"},
	"cache clear":      {"POST /api/cache/clear"},
}

// TestWebSurfaceCoversRegistry: every production command declares a WebUI
// disposition, and WebSurface carries no stale entries.
func TestWebSurfaceCoversRegistry(t *testing.T) {
	all := append(append([]command.CommandSpec{}, command.Registry...), command.Subcommands...)
	inRegistry := map[string]bool{}
	for _, spec := range all {
		inRegistry[spec.Path] = true
		if _, ok := command.WebSupportFor(spec.Path); !ok {
			t.Errorf("command %q has no command.WebSurface entry — declare its WebUI disposition (#583)", spec.Path)
		}
	}
	for path := range command.WebSurface {
		if !inRegistry[path] {
			t.Errorf("command.WebSurface declares %q but the registry no longer defines it", path)
		}
	}
}

// TestWebSupportedCommandsAreRouted: every WebSupported command maps to at
// least one API route, and each such route is actually wired in the server
// (probing it returns something other than 404). Non-supported commands must
// not claim a route.
func TestWebSupportedCommandsAreRouted(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()

	for path, status := range command.WebSurface {
		routes, mapped := webRoutes[path]
		if status == command.WebSupported {
			if !mapped {
				t.Errorf("WebSupported command %q has no webRoutes mapping", path)
				continue
			}
			for _, r := range routes {
				// Every mapped route is behind requireAuth, so an
				// unauthenticated probe of a correctly-wired route returns
				// exactly 401. A wrong method yields 405 (mux) and an
				// unregistered path 404 — both fail here, catching typos.
				if code := probe(h, r); code != http.StatusUnauthorized {
					t.Errorf("route %q for command %q not reachable (got %d, want 401)", r, path, code)
				}
			}
		} else if mapped {
			t.Errorf("command %q is %s but claims WebUI routes %v", path, status, routes)
		}
	}

	// Every webRoutes key must be a WebSupported command — no stale/typo keys.
	for path := range webRoutes {
		if command.WebSurface[path] != command.WebSupported {
			t.Errorf("webRoutes maps %q, which is not a WebSupported command", path)
		}
	}
}

// probe sends an unauthenticated request for "METHOD /path" and returns the
// status. A correctly-wired authenticated route returns 401; a wrong method
// returns 405; an unregistered path returns 404.
func probe(h http.Handler, methodPath string) int {
	parts := strings.SplitN(methodPath, " ", 2)
	req := httptest.NewRequest(parts[0], "http://localhost"+parts[1], strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}
