package webui

import (
	"encoding/json"
	"net/http"

	"github.com/johndauphine/dmt/internal/version"
)

// buildHandler wires the full request tree: security headers on everything,
// an authenticated JSON API under /api/, and the embedded SPA everywhere else.
// Later WebUI issues register their handlers in registerAPI.
func (s *Server) buildHandler() http.Handler {
	api := http.NewServeMux()
	s.registerAPI(api)

	root := http.NewServeMux()
	// The API mux is guarded by the origin (CSRF) check; individual handlers
	// opt into auth via requireAuth so /api/login can bootstrap a session.
	root.Handle("/api/", s.originGuard(api))
	root.Handle("/", s.staticHandler())

	// hostGuard (DNS-rebinding) wraps everything; securityHeaders is
	// outermost so even rejected requests carry the hardening headers.
	return securityHeaders(s.hostGuard(root))
}

// registerAPI mounts the API endpoints. Method-based patterns (Go 1.22 mux)
// give automatic 405s. #580+ extend this with the run/progress/wizard
// surfaces.
func (s *Server) registerAPI(mux *http.ServeMux) {
	// Public — bootstraps or clears a session.
	mux.HandleFunc("POST /api/login", s.handleLogin)
	mux.HandleFunc("POST /api/logout", s.handleLogout)

	// Authenticated read/advisory surface (#579).
	authed := func(h http.HandlerFunc) http.Handler { return s.requireAuth(h) }
	mux.Handle("GET /api/health", authed(s.handleHealth))
	mux.Handle("GET /api/status", authed(s.handleStatus))
	mux.Handle("GET /api/status/{runID}", authed(s.handleStatusByID))
	mux.Handle("GET /api/history", authed(s.handleHistory))
	mux.Handle("POST /api/validate", authed(s.handleValidate))
	mux.Handle("POST /api/diagnose", authed(s.handleDiagnose))
	mux.Handle("POST /api/preflight", authed(s.handlePreflight))
	mux.Handle("POST /api/config/check", authed(s.handleConfigCheck))
	mux.Handle("POST /api/analyze", authed(s.handleAnalyze))
	mux.Handle("POST /api/ai/config-review", authed(s.handleAIConfigReview))

	// Run/resume with live progress (#580).
	mux.Handle("GET /api/run", authed(s.handleRunState))
	mux.Handle("POST /api/run", authed(s.handleRun))
	mux.Handle("POST /api/run/cancel", authed(s.handleRunCancel))
	mux.Handle("POST /api/resume", authed(s.handleResume))
	mux.Handle("GET /api/events", authed(s.handleEvents))
}

// handleHealth reports liveness plus build identity. Authenticated so it
// doubles as a quick "am I signed in?" probe for the front-end.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"name":    version.Name,
		"version": version.Version,
	})
}

// writeJSON encodes v as an application/json response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError emits the shared error envelope {"error":{code,message}} used
// across the WebUI API (the shape #579 builds on).
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
