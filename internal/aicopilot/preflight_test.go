package aicopilot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

type fakeClient struct {
	response string
	err      error
	prompt   string
}

func (f *fakeClient) CallAI(_ context.Context, prompt string) (string, error) {
	f.prompt = prompt
	return f.response, f.err
}

func (f *fakeClient) ProviderName() string { return "fake" }
func (f *fakeClient) Model() string        { return "fake-model" }

func TestBuildPreflightPayloadRedactsConnectionValues(t *testing.T) {
	cfg := &config.Config{
		Source: dbconfig.SourceConfig{
			Type:     "postgres",
			Host:     "source.internal",
			Port:     5432,
			Database: "source_prod",
			Schema:   "public",
			User:     "source_user",
			Password: "source-secret",
			SSLMode:  "require",
		},
		Target: dbconfig.TargetConfig{
			Type:     "mssql",
			Host:     "target.internal",
			Port:     1433,
			Database: "target_prod",
			Schema:   "dbo",
			User:     "target_user",
			Password: "target-secret",
		},
		Migration: config.MigrationConfig{
			TargetMode:           "drop_recreate",
			Workers:              4,
			ChunkSize:            50000,
			MaxSourceConnections: 12,
			MaxTargetConnections: 12,
		},
	}
	payload := BuildPreflightPayload(cfg, HealthSummary{
		SourceConnected: true,
		TargetConnected: true,
		Healthy:         true,
	}, []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "privileges.create_table",
		Side:     driver.PreFlightSideTarget,
		Message:  "driver returned password=target-secret while checking dbo",
		Remedy:   "grant CREATE; token=abc123",
	}})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, secret := range []string{
		"source-secret",
		"target-secret",
		"source.internal",
		"target.internal",
		"source_user",
		"target_user",
		"source_prod",
		"target_prod",
		"abc123",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("payload leaked %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, "[REDACTED]") {
		t.Fatalf("payload should scrub secret-looking finding text: %s", text)
	}
	if len(payload.DeterministicBlockers) != 1 {
		t.Fatalf("DeterministicBlockers len = %d, want 1", len(payload.DeterministicBlockers))
	}
}

func TestGeneratePreflightReviewParsesStructuredResponse(t *testing.T) {
	payload := PreflightPayload{
		PromptVersion:         PreflightPromptVersion,
		Health:                HealthSummary{SourceConnected: true, TargetConnected: true, Healthy: true},
		DeterministicBlockers: []string{"target/backup.ack: backup confirmation required"},
	}
	client := &fakeClient{response: "```json\n" + `{
  "readiness": "blocked",
  "summary": "Backup acknowledgement is required before this run.",
  "findings": [
    {
      "severity": "warn",
      "category": "backup",
      "affected": "migration.confirm_backup",
      "rationale": "drop_recreate can remove existing target data.",
      "next_action": "Verify backup coverage and rerun with explicit confirmation."
    }
  ],
  "notes": ["Deterministic blockers remain authoritative."]
}` + "\n```"}

	review, err := GeneratePreflightReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GeneratePreflightReview() error = %v", err)
	}
	if review.Status != ReviewStatusOK || !review.Enabled {
		t.Fatalf("review status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Provider != "fake" || review.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", review.Provider, review.Model)
	}
	if review.Readiness != ReadinessBlocked {
		t.Fatalf("readiness = %q, want blocked", review.Readiness)
	}
	if len(review.DeterministicBlockers) != 1 {
		t.Fatalf("deterministic blockers len = %d, want 1", len(review.DeterministicBlockers))
	}
	if len(review.Findings) != 1 || review.Findings[0].Source != "ai_advisory" {
		t.Fatalf("review findings = %+v", review.Findings)
	}
	if !strings.Contains(client.prompt, `"deterministic_blockers"`) {
		t.Fatalf("prompt did not include payload facts: %s", client.prompt)
	}
}

func TestGeneratePreflightReviewRejectsInvalidJSON(t *testing.T) {
	client := &fakeClient{response: "not json"}
	_, err := GeneratePreflightReview(context.Background(), client, PreflightPayload{})
	if err == nil {
		t.Fatal("GeneratePreflightReview() error = nil, want parse error")
	}
}

func TestUnavailablePreflightReviewUsesDeterministicFallback(t *testing.T) {
	payload := PreflightPayload{
		Health: HealthSummary{SourceConnected: true, TargetConnected: true, Healthy: true},
		PreflightFindings: []PreflightFinding{{
			Severity: string(driver.SeverityWarn),
			Check:    "pool.headroom",
		}},
	}
	review := UnavailablePreflightReview("no AI provider configured", payload)
	if review.Status != ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Readiness != ReadinessAttention {
		t.Fatalf("readiness = %q, want attention", review.Readiness)
	}
}
