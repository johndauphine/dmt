package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSessionDefaultsValidation(t *testing.T) {
	d := newSessionDefaults()

	// Unknown key rejected.
	if err := d.set("bogus", "x"); err == nil {
		t.Error("unknown key should be rejected")
	}
	// Bad verbosity rejected; good one accepted.
	if err := d.set("verbosity", "nonsense"); err == nil {
		t.Error("invalid verbosity should be rejected")
	}
	if err := d.set("verbosity", "debug"); err != nil {
		t.Errorf("valid verbosity rejected: %v", err)
	}
	// Bad bool rejected.
	if err := d.set("no-audit", "maybe"); err == nil {
		t.Error("invalid bool should be rejected")
	}
	if err := d.set("no-audit", "true"); err != nil {
		t.Errorf("valid bool rejected: %v", err)
	}
	// metrics-addr must be host:port.
	if err := d.set("metrics-addr", "notaddr"); err == nil {
		t.Error("invalid metrics-addr should be rejected")
	}
	// A public metrics bind is rejected from a WebUI session (#602)...
	if err := d.set("metrics-addr", "0.0.0.0:9090"); err == nil {
		t.Error("non-loopback metrics-addr should be rejected from a session")
	}
	if err := d.set("metrics-addr", ":9090"); err == nil {
		t.Error("wildcard metrics-addr should be rejected from a session")
	}
	// ...but a loopback bind is fine.
	if err := d.set("metrics-addr", "127.0.0.1:9090"); err != nil {
		t.Errorf("loopback metrics-addr rejected: %v", err)
	}

	// Values with no validator (config/profile) accepted verbatim.
	if err := d.set("config", "/etc/dmt/config.yaml"); err != nil {
		t.Errorf("config set failed: %v", err)
	}
	if d.get("config") != "/etc/dmt/config.yaml" {
		t.Error("config value not stored")
	}
	// Empty value clears.
	if err := d.set("config", ""); err != nil {
		t.Errorf("clear failed: %v", err)
	}
	if d.get("config") != "" {
		t.Error("empty value should clear the key")
	}
}

func TestSessionEndpoints(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()

	// Set a value.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/session", `{"key":"state-file","value":"/tmp/state.yaml"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("session set = %d, want 200; %s", rec.Code, rec.Body.String())
	}
	if s.sessionDefaults.get("state-file") != "/tmp/state.yaml" {
		t.Error("value not persisted in store")
	}

	// Invalid value rejected.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/session", `{"key":"verbosity","value":"bad"}`))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid session set = %d, want 400", rec.Code)
	}

	// Get lists keys with the set value.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/session", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("session get = %d, want 200", rec.Code)
	}
	var body struct {
		Keys []sessionKeyInfo `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, k := range body.Keys {
		if k.Name == "state-file" && k.Value == "/tmp/state.yaml" {
			found = true
		}
	}
	if !found {
		t.Error("state-file value not reflected in GET /api/session")
	}

	// Clear it.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodDelete, "http://localhost/api/session/state-file", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("session clear = %d, want 200", rec.Code)
	}
	if s.sessionDefaults.get("state-file") != "" {
		t.Error("value not cleared")
	}
}

func TestSessionConfigResolutionPrecedence(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken})
	// Session default config should be used when the request omits one.
	if err := s.sessionDefaults.set("config", cfgPath); err != nil {
		t.Fatalf("set session config: %v", err)
	}
	cfg, path, _, err := s.resolveConfig(originReq{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath || cfg == nil {
		t.Errorf("session config not used: path=%q", path)
	}
	// Explicit request config overrides the session default.
	other := writeSQLiteConfig(t)
	_, path2, _, err := s.resolveConfig(originReq{Config: other})
	if err != nil {
		t.Fatalf("resolveConfig override: %v", err)
	}
	if path2 != other {
		t.Errorf("request config should override session: got %q", path2)
	}
}

// TestRequestConfigBeatsSessionProfile guards the precedence fix: an explicit
// request config must not be overridden by a broad session profile default.
func TestRequestConfigBeatsSessionProfile(t *testing.T) {
	cfgPath := writeSQLiteConfig(t)
	s := newTestServer(t, Options{AuthToken: testToken})
	// A session profile default is set, but the request names a config.
	if err := s.sessionDefaults.set("profile", "some-profile"); err != nil {
		t.Fatalf("set session profile: %v", err)
	}
	// Must resolve via the request config (and not attempt profile decryption).
	cfg, path, _, err := s.resolveConfig(originReq{Config: cfgPath})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath || cfg == nil {
		t.Errorf("explicit request config should win over session profile: path=%q", path)
	}
}
