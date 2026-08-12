package webui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
)

const testToken = "test-token-abc"

// sampleRunWithSecret is a run whose serialized Config carries a credential,
// used to prove the history DTO never serializes it.
func sampleRunWithSecret() checkpoint.Run {
	return checkpoint.Run{
		ID:           "run-123",
		Status:       "completed",
		Phase:        "complete",
		SourceSchema: "dbo",
		TargetSchema: "public",
		Config:       "source:\n  password: super-secret-password\n",
		ConfigHash:   "deadbeef",
	}
}

// writeSQLiteConfig writes a minimal, loadable sqlite→sqlite config into a
// temp dir and returns its path. status/history use the state-only
// orchestrator, so the source/target db files never need to exist — only the
// data_dir (checkpoint state) is touched.
func writeSQLiteConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf(`source:
  type: sqlite
  database: %s
target:
  type: sqlite
  database: %s
migration:
  data_dir: %s
`, filepath.Join(dir, "src.db"), filepath.Join(dir, "dst.db"), stateDir)
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath
}

// authedReq builds a bearer-authenticated request at an allowlisted host.
func authedReq(method, target, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	r.Header.Set("Authorization", "Bearer "+testToken)
	return r
}

// apiRoutes is every /api route that must be behind auth, with the method it
// answers.
var apiRoutes = []struct{ method, path string }{
	{http.MethodGet, "/api/health"},
	{http.MethodGet, "/api/configs"},
	{http.MethodGet, "/api/status"},
	{http.MethodGet, "/api/status/some-id"},
	{http.MethodGet, "/api/history"},
	{http.MethodPost, "/api/validate"},
	{http.MethodPost, "/api/diagnose"},
	{http.MethodPost, "/api/preflight"},
	{http.MethodPost, "/api/config/check"},
	{http.MethodPost, "/api/analyze"},
	{http.MethodPost, "/api/ai/config-review"},
	{http.MethodGet, "/api/run"},
	{http.MethodPost, "/api/run"},
	{http.MethodPost, "/api/run/cancel"},
	{http.MethodPost, "/api/resume"},
	{http.MethodGet, "/api/events"},
	{http.MethodPost, "/api/setup/start"},
	{http.MethodGet, "/api/setup/prompt"},
	{http.MethodPost, "/api/setup/input"},
	{http.MethodGet, "/api/profiles"},
	{http.MethodPost, "/api/profiles/save"},
	{http.MethodPost, "/api/init-secrets"},
	{http.MethodPost, "/api/cache/clear"},
	{http.MethodGet, "/api/session"},
	{http.MethodPost, "/api/session"},
}

func TestAllAPIRoutesRequireAuth(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	for _, rt := range apiRoutes {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(rt.method, "http://localhost"+rt.path, strings.NewReader("{}"))
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without auth = %d, want 401", rt.method, rt.path, rec.Code)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	// GET a POST-only route.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/validate", ""))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/validate = %d, want 405", rec.Code)
	}
}

func TestConfigErrorReturns400(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	body := `{"config":"/no/such/config.yaml"}`
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/validate", body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad config = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	var env struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if env.Error.Code != "config_error" && env.Error.Code != "io_error" {
		t.Errorf("error code = %q, want config_error/io_error", env.Error.Code)
	}
}

func TestNonexistentProfileErrors(t *testing.T) {
	// Profile resolution is wired (#581); a request for a profile that doesn't
	// exist (or with no master key configured) must fail cleanly, not 200.
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/preflight",
		`{"profile":"webui-test-nonexistent-profile"}`))
	if rec.Code < 400 {
		t.Errorf("nonexistent profile = %d, want an error status", rec.Code)
	}
}

func TestIsUnsupportedFeature(t *testing.T) {
	cases := map[string]bool{
		"analyzing config: getting tables: unsupported database type: sqlite": true,
		"validation.mode: full is not supported":                              true,
		"feature not implemented":                                             true,
		"connection refused":                                                  false,
		"row count mismatch":                                                  false,
	}
	for msg, want := range cases {
		if got := isUnsupportedFeature(fmtError(msg)); got != want {
			t.Errorf("isUnsupportedFeature(%q) = %v, want %v", msg, got, want)
		}
	}
}

func fmtError(s string) error { return &stringErr{s} }

type stringErr struct{ s string }

func (e *stringErr) Error() string { return e.s }

func TestStatusIdle(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfgPath})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/status", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Active bool `json:"active"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Active {
		t.Error("expected active=false on a fresh state")
	}
}

func TestHistoryEmpty(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfgPath})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/history", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("history = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Runs []runDTO `json:"runs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Runs) != 0 {
		t.Errorf("expected 0 runs, got %d", len(body.Runs))
	}
}

// TestHistoryPaginationEnvelope checks the paginated response carries the
// page metadata and that limit is clamped to the server maximum.
func TestHistoryPaginationEnvelope(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfgPath})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/history?limit=9999&offset=0", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("history = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Runs   []runDTO `json:"runs"`
		Total  int      `json:"total"`
		Limit  int      `json:"limit"`
		Offset int      `json:"offset"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Total != 0 {
		t.Errorf("total = %d, want 0 on a fresh state", body.Total)
	}
	if body.Limit != historyMaxLimit {
		t.Errorf("limit = %d, want clamped to %d", body.Limit, historyMaxLimit)
	}
}

// TestHistoryInvalidStatus rejects an unknown status filter with 400 rather
// than silently returning an empty page.
func TestHistoryInvalidStatus(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: cfgPath})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/history?status=bogus", ""))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("history?status=bogus = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

// TestHistoryDTOExcludesConfig guards the secret-safety invariant: the raw
// serialized config (which can hold connection secrets) must never appear in
// the history JSON.
func TestHistoryDTOExcludesConfig(t *testing.T) {
	data, err := json.Marshal(newRunDTO(sampleRunWithSecret()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "super-secret-password") {
		t.Fatalf("history DTO leaked the config blob: %s", data)
	}
}
