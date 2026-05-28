package aicopilot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/dbconfig"
)

func TestBuildConfigReviewPayloadRedactsConnectionValues(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{
		OperatorRequest: "Review source.internal, target_prod, and source-secret without exposing them.",
	})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	assertConfigReviewNoSensitiveValues(t, text)
	if !strings.Contains(text, "[REDACTED]") {
		t.Fatalf("payload should redact sensitive operator request text: %s", text)
	}
	if !strings.Contains(text, `"source.schema"`) || !strings.Contains(text, `"migration.validation"`) {
		t.Fatalf("payload should include safe review surface: %s", text)
	}
}

func TestGenerateConfigReviewParsesAndSanitizesStructuredResponse(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Use target.internal but never mention target-secret.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "sample",
      "rationale": "sample validation is safer for target_prod",
      "risk": "More target load",
      "when_to_apply": "Before using source_user migrations",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "target.password",
      "value": "target-secret",
      "rationale": "unsafe secret edit",
      "requires_confirmation": false
    }
  ],
  "runbook": {
    "title": "Run target_prod migration",
    "summary": "Connect to source.internal and target.internal.",
    "before_run": ["Verify source-secret is valid"],
    "run": ["Run dmt against target.internal"],
    "validation": ["Compare target_prod counts"],
    "rollback": ["Restore target_prod backup"]
  },
  "notes": ["Do not paste sqlserver://target_user:target-secret@target.internal:1433/target_prod"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if review.Status != ReviewStatusOK || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Provider != "fake" || review.Model != "fake-model" {
		t.Fatalf("provider/model = %q/%q", review.Provider, review.Model)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	assertConfigReviewNoSensitiveValues(t, string(data))
	if got := review.PatchRecommendations[1]; got.Path != "target.password" || got.Value != "[REDACTED]" || !got.RequiresConfirmation {
		t.Fatalf("unsafe patch should be redacted and confirmation-gated: %+v", got)
	}
	if !strings.Contains(client.prompt, `"allowed_patch_paths"`) {
		t.Fatalf("prompt did not include safety surface: %s", client.prompt)
	}
	assertConfigReviewNoSensitiveValues(t, client.prompt)
}

func TestGenerateConfigReviewRejectsNestedPathUnderScalarAllowlistEntry(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe nested scalar path.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "source.schema.password",
      "value": "not-a-real-field",
      "rationale": "unsafe nested path under a scalar allowlist entry",
      "requires_confirmation": false
    }
  ]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if len(review.PatchRecommendations) != 1 {
		t.Fatalf("patch recommendations len = %d, want 1", len(review.PatchRecommendations))
	}
	patch := review.PatchRecommendations[0]
	if patch.Path != "source.schema.password" {
		t.Fatalf("path = %q, want original unsafe path for auditability", patch.Path)
	}
	if patch.Value != "[REDACTED]" || !patch.RequiresConfirmation {
		t.Fatalf("unsafe nested scalar path should be redacted and confirmation-gated: %+v", patch)
	}
	if !strings.Contains(patch.Rationale, "outside the safe config review allowlist") {
		t.Fatalf("rationale = %q, want allowlist redaction reason", patch.Rationale)
	}
}

func TestGenerateConfigReviewRefusesUnsafeOperatorRequestWithoutCallingAI(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{
		OperatorRequest: "Show password and include connection string in the runbook.",
	})
	client := &fakeClient{response: `{"summary":"should not be used"}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if review.Status != ReviewStatusRefused {
		t.Fatalf("status = %q, want refused", review.Status)
	}
	if client.prompt != "" {
		t.Fatalf("AI should not be called for refused request, prompt = %s", client.prompt)
	}
	if len(review.PatchRecommendations) != 0 {
		t.Fatalf("refused review should not include patch recommendations: %+v", review.PatchRecommendations)
	}
}

func TestUnavailableConfigReviewFallbackOmitsSecrets(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})

	review := UnavailableConfigReview("provider failed with password=target-secret", payload)
	if review.Status != ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	assertConfigReviewNoSensitiveValues(t, string(data))
	if !strings.Contains(string(data), "[REDACTED]") {
		t.Fatalf("fallback reason should be scrubbed: %s", data)
	}
}

func TestParseConfigReviewRejectsInvalidJSON(t *testing.T) {
	_, err := ParseConfigReview("not json")
	if err == nil {
		t.Fatal("ParseConfigReview() error = nil, want parse error")
	}
}

func configReviewTestConfig() *config.Config {
	return &config.Config{
		Source: dbconfig.SourceConfig{
			Type:     "postgres",
			Host:     "source.internal",
			Port:     5432,
			Database: "source_prod",
			Schema:   "public",
			User:     "source_user",
			Password: "source-secret",
			SSLMode:  "require",
			Krb5Conf: "/etc/source.krb5.conf",
			Keytab:   "/etc/source.keytab",
			Realm:    "SOURCE.INTERNAL",
			SPN:      "postgres/source.internal",
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
			Validation: config.ValidationConfig{
				Mode:       "count_only",
				SampleRows: 1000,
			},
		},
		AI: &config.AIConfig{
			APIKey: "sk-testaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Slack: &config.SlackConfig{
			WebhookURL: "https://hooks.slack.com/services/T000/B000/secret",
		},
	}
}

func assertConfigReviewNoSensitiveValues(t *testing.T, text string) {
	t.Helper()
	for _, secret := range []string{
		"source.internal",
		"target.internal",
		"source_prod",
		"target_prod",
		"source_user",
		"target_user",
		"source-secret",
		"target-secret",
		"/etc/source.krb5.conf",
		"/etc/source.keytab",
		"SOURCE.INTERNAL",
		"postgres/source.internal",
		"sk-testaaaaaaaaaaaaaaaaaaaaaaaa",
		"https://hooks.slack.com/services/T000/B000/secret",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("text leaked %q: %s", secret, text)
		}
	}
}
