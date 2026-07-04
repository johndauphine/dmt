package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/setup"
)

// TestSetupFlowStart drives the guided setup far enough to prove the
// server-side pump answers prompts and advances through the shared
// internal/setup state machine. It does not complete setup (that would write
// real secrets/config files), only that the first prompts render.
func TestSetupFlowStart(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()

	// Prompt before start → 409.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodGet, "http://localhost/api/setup/prompt", ""))
	if rec.Code != http.StatusConflict {
		t.Fatalf("prompt before start = %d, want 409", rec.Code)
	}

	// Start returns the first prompt (auto steps like CheckSecrets already
	// pumped through).
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/setup/start", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("setup start = %d, want 200; %s", rec.Code, rec.Body.String())
	}
	var p setupPromptDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode prompt: %v", err)
	}
	if p.Done {
		t.Fatal("setup should not be done at the first prompt")
	}
	if p.Text == "" {
		t.Error("expected a prompt text")
	}

	// Input advances the flow and returns a new prompt.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/setup/input", `{"input":"n"}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("setup input = %d, want 200; %s", rec.Code, rec.Body.String())
	}
	var p2 setupPromptDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &p2); err != nil {
		t.Fatalf("decode prompt2: %v", err)
	}
	// Either advanced to a different step or surfaced a validation error;
	// either way the flow is being driven, not stuck.
	if p2.Step == p.Step && p2.Error == "" {
		t.Errorf("setup did not advance from step %d", p.Step)
	}
}

func TestConnInput(t *testing.T) {
	if connInput(&setup.ConnTestResult{Connected: true}) != "" {
		t.Error("connected result should yield empty input")
	}
	if got := connInput(&setup.ConnTestResult{Connected: false, Error: "boom"}); got != "boom" {
		t.Errorf("failed result should yield error, got %q", got)
	}
}

func TestCacheClearEndpoint(t *testing.T) {
	s := newTestServer(t, Options{AuthToken: testToken})
	h := s.buildHandler()
	// ai_only avoids removing the whole cache file; safe and idempotent.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authedReq(http.MethodPost, "http://localhost/api/cache/clear", `{"ai_only":true}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("cache clear = %d, want 200; %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "status") {
		t.Error("expected a status body")
	}
}
