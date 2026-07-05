package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigsListFiltersToDMTConfigs(t *testing.T) {
	dir := t.TempDir()
	// A real dmt config (has source: + target:).
	cfg := filepath.Join(dir, "prod.yaml")
	os.WriteFile(cfg, []byte("source:\n  type: sqlite\ntarget:\n  type: sqlite\n"), 0o644)
	// Unrelated YAML — must be excluded.
	os.WriteFile(filepath.Join(dir, "docker-compose.yaml"), []byte("services:\n  db: {}\n"), 0o644)
	// Non-YAML — must be excluded.
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("source: target:"), 0o644)

	s := newTestServer(t, Options{AuthToken: testToken, ConfigPath: filepath.Join(dir, "config.yaml")})
	h := s.buildHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/configs", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/configs = %d, want 200", rec.Code)
	}
	var body struct {
		Configs []configFileDTO `json:"configs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The config dir (from ConfigPath) is scanned; prod.yaml must appear,
	// the compose file and txt must not.
	var names []string
	for _, c := range body.Configs {
		names = append(names, c.Name)
		if !filepath.IsAbs(c.Path) {
			t.Errorf("config path %q is not absolute", c.Path)
		}
	}
	found := false
	for _, n := range names {
		if n == "prod.yaml" {
			found = true
		}
		if n == "docker-compose.yaml" || n == "notes.txt" {
			t.Errorf("non-dmt file %q leaked into the list", n)
		}
	}
	if !found {
		t.Errorf("prod.yaml not listed; got %v", names)
	}
}

func TestLooksLikeDMTConfig(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "g.yaml")
	os.WriteFile(good, []byte("source:\n  type: sqlite\ntarget:\n  type: sqlite\n"), 0o644)
	bad := filepath.Join(dir, "b.yaml")
	os.WriteFile(bad, []byte("services:\n  web: {}\n"), 0o644)
	if !looksLikeDMTConfig(good) {
		t.Error("a source/target config should be recognized")
	}
	if looksLikeDMTConfig(bad) {
		t.Error("an unrelated yaml should be rejected")
	}
	if looksLikeDMTConfig(filepath.Join(dir, "missing.yaml")) {
		t.Error("a missing file should not be recognized")
	}
}
