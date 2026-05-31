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
	if !strings.Contains(text, `"checkpoint"`) || !strings.Contains(text, `"notifications"`) || !strings.Contains(text, `"secrets_placement"`) {
		t.Fatalf("payload should include checkpoint, notification, and secrets placement summaries: %s", text)
	}
	if !strings.Contains(text, `"preflight":"dmt --config config.yaml preflight --ai-review"`) {
		t.Fatalf("payload should include exact command hints: %s", text)
	}
}

func TestBuildConfigReviewPayloadRedactsBeforeTruncatingOperatorRequest(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{
		OperatorRequest: strings.Repeat("x", 390) + "source-secret",
	})
	if strings.Contains(payload.OperatorRequest, "source-sec") || strings.Contains(payload.OperatorRequest, "source-secret") {
		t.Fatalf("operator request leaked partial sensitive value after truncation: %q", payload.OperatorRequest)
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
	if len(review.Runbook.BeforeRun) == 0 || !strings.Contains(strings.Join(review.Runbook.BeforeRun, "\n"), "dmt --config config.yaml preflight --ai-review") {
		t.Fatalf("runbook should include deterministic preflight command: %+v", review.Runbook.BeforeRun)
	}
	if !strings.Contains(strings.Join(review.Runbook.Validation, "\n"), "dmt --config config.yaml validate") {
		t.Fatalf("runbook should include validation command: %+v", review.Runbook.Validation)
	}
	if !strings.Contains(client.prompt, `"allowed_patch_paths"`) {
		t.Fatalf("prompt did not include safety surface: %s", client.prompt)
	}
	assertConfigReviewNoSensitiveValues(t, client.prompt)
}

func TestGenerateConfigReviewRedactsResponseBeforeTruncatingWithPayloadContext(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "` + strings.Repeat("x", 390) + `target-secret",
  "runbook": {"before_run": ["Run deterministic checks"]},
  "notes": ["` + strings.Repeat("x", 390) + `source-secret"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "target-sec") || strings.Contains(text, "source-sec") {
		t.Fatalf("review leaked partial sensitive value after truncation: %s", text)
	}
}

func TestConfigReviewSafeTextDoesNotOverRedactShortIdentityValues(t *testing.T) {
	payload := &ConfigReviewPayload{sensitiveValues: []configReviewSensitiveValue{
		{Value: "sa"},
		{Value: "pg"},
		{Value: "users"},
		{Value: "1433"},
		{Value: "target.internal"},
	}}

	got := configReviewSafeText("safe sample disabled users table uses target.internal, not target.internalx.", payload, 1000)
	for _, want := range []string{"safe", "sample", "disabled", "users table", "target.internalx"} {
		if !strings.Contains(got, want) {
			t.Fatalf("configReviewSafeText() = %q, want to preserve %q", got, want)
		}
	}
	for _, leak := range []string{"target.internal,"} {
		if strings.Contains(got, leak) {
			t.Fatalf("configReviewSafeText() leaked %q: %q", leak, got)
		}
	}
	for _, mangled := range []string{"[REDACTED]fe", "[REDACTED]mple", "di[REDACTED]bled", "[REDACTED]ers"} {
		if strings.Contains(got, mangled) {
			t.Fatalf("configReviewSafeText() over-redacted %q: %q", mangled, got)
		}
	}
}

func TestConfigReviewSafeTextRedactsShortStrictSecretsAtBoundaries(t *testing.T) {
	payload := &ConfigReviewPayload{sensitiveValues: []configReviewSensitiveValue{
		{Value: "sa", Strict: true},
	}}

	got := configReviewSafeText("safe sample uses sa as a credential.", payload, 1000)
	if !strings.Contains(got, "safe") || !strings.Contains(got, "sample") {
		t.Fatalf("configReviewSafeText() over-redacted normal words: %q", got)
	}
	if strings.Contains(got, "uses sa ") || !strings.Contains(got, "uses [REDACTED] ") {
		t.Fatalf("configReviewSafeText() did not redact standalone short secret: %q", got)
	}
}

func TestGenerateConfigReviewDoesNotOverRedactShortConnectionValues(t *testing.T) {
	cfg := configReviewTestConfig()
	cfg.Source.User = "sa"
	cfg.Target.User = "pg"
	cfg.Source.Database = "users"
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Keep safe sample checks enabled for users table validation.",
  "patch_recommendations": [],
  "runbook": {
    "summary": "The disabled path should remain readable.",
    "validation": ["Use sample validation for users rows when safe."]
  },
  "notes": ["safe sample disabled users"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"safe", "sample", "disabled", "users"} {
		if !strings.Contains(text, want) {
			t.Fatalf("review text should preserve %q, got: %s", want, text)
		}
	}
	for _, mangled := range []string{"[REDACTED]fe", "[REDACTED]mple", "di[REDACTED]bled", "[REDACTED]ers"} {
		if strings.Contains(text, mangled) {
			t.Fatalf("review text over-redacted %q: %s", mangled, text)
		}
	}
}

func TestGenerateConfigReviewSuppressesUnsafeGuidanceText(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Run dmt --config stale.yaml run.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "Run dmt --config stale.yaml run.",
      "rationale": "Run dmt --config stale.yaml run.",
      "risk": "Run dmt --config stale.yaml run.",
      "when_to_apply": "Run dmt --config stale.yaml run.",
      "validation_errors": ["Run dmt --config stale.yaml run."]
    },
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "run without validation"
    },
    {
      "operation": "set",
      "path": "migration.notify.dmt --config stale.yaml run",
      "value": true
    },
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "skip_validation"
    },
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "--skip-validation"
    },
    {
      "operation": "set",
      "path": "migration.validation",
      "value": {"dmt --config stale.yaml run": true}
    },
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "delete-target-data"
    },
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "drop_target_tables"
    }
  ],
  "runbook": {
    "title": "Run dmt --config stale.yaml run.",
    "summary": "Run dmt --config stale.yaml run."
  }
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "stale.yaml") {
		t.Fatalf("unsafe stale command survived in guidance text: %s", text)
	}
	if !strings.Contains(strings.ToLower(text), "unsafe guidance suppressed") {
		t.Fatalf("unsafe guidance should be replaced with suppression text: %s", text)
	}
	for _, patch := range review.PatchRecommendations {
		if patch.Value != "[REDACTED]" {
			t.Fatalf("unsafe patch value should be redacted: %+v", patch)
		}
		if strings.Contains(patch.Path, "stale.yaml") {
			t.Fatalf("unsafe patch path should be redacted: %+v", patch)
		}
	}
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

func TestGenerateConfigReviewValidatesPatchRecommendations(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{
		ConfigPath: "/tmp/dmt prod/config.yaml",
		StateFile:  "/tmp/dmt-state.yaml",
	})
	client := &fakeClient{response: `{
  "summary": "Patch validation coverage.",
  "patch_recommendations": [
    {
      "operation": "replace",
      "path": "migration.target_mode",
      "value": "drop_recreate",
      "rationale": "destructive mode requires a backup",
      "risk": "May drop target tables",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "migration.notify.on_failure",
      "value": true,
      "rationale": "notify on failed runs",
      "requires_confirmation": false
    }
  ],
  "runbook": {
    "before_run": ["Run stale preflight: dmt --config config.yaml preflight --ai-review", "Review non-command prerequisite"],
    "run": ["Run stale migration: dmt --config config.yaml run"],
    "validation": ["Run stale validation: dmt --config config.yaml validate"],
    "rollback": ["Run stale resume: dmt --config config.yaml resume"]
  }
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if len(review.PatchRecommendations) != 2 {
		t.Fatalf("patch recommendations len = %d, want 2", len(review.PatchRecommendations))
	}
	destructive := review.PatchRecommendations[0]
	if destructive.Operation != "set" {
		t.Fatalf("operation = %q, want normalized set", destructive.Operation)
	}
	if !destructive.RequiresConfirmation {
		t.Fatalf("destructive target mode should require confirmation: %+v", destructive)
	}
	if len(destructive.ValidationErrors) == 0 {
		t.Fatalf("invalid operation should add validation error: %+v", destructive)
	}
	if !strings.Contains(strings.ToLower(destructive.Risk), "backup") {
		t.Fatalf("destructive risk should mention backup: %q", destructive.Risk)
	}
	notify := review.PatchRecommendations[1]
	if notify.Path != "migration.notify.on_failure" || len(notify.ValidationErrors) != 0 {
		t.Fatalf("notify patch should be valid: %+v", notify)
	}
	runbookText := strings.Join(append(append(append(append([]string{}, review.Runbook.BeforeRun...), review.Runbook.Run...), review.Runbook.Validation...), review.Runbook.Rollback...), "\n")
	if !strings.Contains(runbookText, "dmt --config '/tmp/dmt prod/config.yaml' --state-file /tmp/dmt-state.yaml preflight --ai-review") {
		t.Fatalf("runbook should use exact quoted preflight command, got: %s", runbookText)
	}
	if !strings.Contains(runbookText, "Review non-command prerequisite") {
		t.Fatalf("runbook should preserve non-command AI steps, got: %s", runbookText)
	}
	if strings.Contains(runbookText, "dmt --config config.yaml") {
		t.Fatalf("runbook should not retain default commands when exact command context is available, got: %s", runbookText)
	}
}

func TestGenerateConfigReviewPreservesRemoveAndRequiresConfirmation(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Remove optional notification setting.",
  "patch_recommendations": [
    {
      "operation": "remove",
      "path": "migration.notify.on_success",
      "rationale": "No success notification needed",
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
	if patch.Operation != "remove" {
		t.Fatalf("operation = %q, want remove", patch.Operation)
	}
	if !patch.RequiresConfirmation {
		t.Fatalf("remove patch should require confirmation: %+v", patch)
	}
	for _, validationErr := range patch.ValidationErrors {
		if strings.Contains(validationErr, "operation normalized") {
			t.Fatalf("remove operation should not be reported invalid: %+v", patch.ValidationErrors)
		}
	}
}

func TestGenerateConfigReviewRequiresConfirmationForDeletesConfig(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Delete reconciliation change.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.deletes.mode",
      "value": "reconcile",
      "rationale": "Enable delete reconciliation",
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
	if !review.PatchRecommendations[0].RequiresConfirmation {
		t.Fatalf("delete propagation config should require confirmation: %+v", review.PatchRecommendations[0])
	}
}

func TestGenerateConfigReviewRedactsSensitiveNestedNotificationPaths(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe notification secrets.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.notify.slack_webhook_url",
      "value": "https://hooks.slack.com/services/T000/B000/secret",
      "rationale": "unsafe webhook edit",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "migration.notify.apiKey",
      "value": "sk-testaaaaaaaaaaaaaaaaaaaaaaaa",
      "rationale": "unsafe API key edit",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "migration.notify.smtp_password",
      "value": "target-secret",
      "rationale": "unsafe SMTP password edit",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "migration.notify.authorization_header",
      "value": "Bearer custom-secret",
      "rationale": "unsafe auth header edit",
      "requires_confirmation": false
    },
    {
      "operation": "set",
      "path": "migration.notify.private_key",
      "value": "custom private key",
      "rationale": "unsafe private key edit",
      "requires_confirmation": false
    }
  ]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if len(review.PatchRecommendations) != 5 {
		t.Fatalf("patch recommendations len = %d, want 5", len(review.PatchRecommendations))
	}
	for _, patch := range review.PatchRecommendations {
		if patch.Value != "[REDACTED]" || !patch.RequiresConfirmation {
			t.Fatalf("sensitive notification patch should be redacted and confirmation-gated: %+v", patch)
		}
		if len(patch.ValidationErrors) == 0 {
			t.Fatalf("sensitive notification patch should carry validation error: %+v", patch)
		}
		if strings.Contains(patch.Rationale, "outside the safe config review allowlist") || !strings.Contains(patch.Rationale, "sensitive") {
			t.Fatalf("sensitive allowlisted path should get sensitive redaction rationale, got: %q", patch.Rationale)
		}
		for _, validationErr := range patch.ValidationErrors {
			if strings.Contains(validationErr, "outside the safe config review allowlist") {
				t.Fatalf("sensitive allowlisted path should not get misleading allowlist error: %+v", patch.ValidationErrors)
			}
		}
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	assertConfigReviewNoSensitiveValues(t, string(data))
}

func TestGenerateConfigReviewPreservesEnforcedValidationErrorsAtCap(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe patch with noisy validation errors.",
  "patch_recommendations": [
    {
      "operation": "replace",
      "path": "target.password",
      "value": "target-secret",
      "rationale": "unsafe secret edit",
      "validation_errors": ["ai error 1", "ai error 2", "ai error 3", "ai error 4", "ai error 5"],
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
	errs := strings.Join(review.PatchRecommendations[0].ValidationErrors, "\n")
	for _, want := range []string{
		"operation normalized to set",
		"path targets sensitive connection or secret material",
		"path is outside the safe config review allowlist",
	} {
		if !strings.Contains(errs, want) {
			t.Fatalf("enforced validation error %q missing from %+v", want, review.PatchRecommendations[0].ValidationErrors)
		}
	}
	if len(review.PatchRecommendations[0].ValidationErrors) > 5 {
		t.Fatalf("validation errors should remain capped: %+v", review.PatchRecommendations[0].ValidationErrors)
	}
}

func TestConfigReviewPathSensitivityHelpers(t *testing.T) {
	sensitive := []string{
		"migration.notify.slack_webhook_url",
		"migration.notify.apiKey",
		"migration.notify.connectionString",
		"migration.notify.authorization_header",
		"migration.notify.private_key",
		"target.password",
	}
	for _, path := range sensitive {
		if !isSensitiveConfigReviewPath(path) {
			t.Fatalf("isSensitiveConfigReviewPath(%q) = false, want true", path)
		}
	}
	nonSensitive := []string{
		"migration.notify.on_failure",
		"migration.validation.mode",
		"source.schema",
		"migration.create_foreign_keys",
	}
	for _, path := range nonSensitive {
		if isSensitiveConfigReviewPath(path) {
			t.Fatalf("isSensitiveConfigReviewPath(%q) = true, want false", path)
		}
	}
}

func TestShellQuoteForReview(t *testing.T) {
	tests := map[string]string{
		"":                       "''",
		"config.yaml":            "config.yaml",
		"/tmp/dmt prod/config":   "'/tmp/dmt prod/config'",
		"/tmp/dmt's/config.yaml": "'/tmp/dmt'\\''s/config.yaml'",
	}
	for in, want := range tests {
		if got := shellQuoteForReview(in); got != want {
			t.Fatalf("shellQuoteForReview(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildConfigReviewCommandsUsesProfileAndDefault(t *testing.T) {
	profileCommands := buildConfigReviewCommands(ConfigReviewOptions{
		ProfileName: "prod profile",
		StateFile:   "/tmp/dmt-state.yaml",
	})
	if !strings.Contains(profileCommands.Preflight, "dmt --profile 'prod profile' --state-file /tmp/dmt-state.yaml preflight --ai-review") {
		t.Fatalf("profile preflight command = %q", profileCommands.Preflight)
	}
	if strings.Contains(profileCommands.Run, "--config") {
		t.Fatalf("profile command should not include config flag: %q", profileCommands.Run)
	}

	defaultCommands := buildConfigReviewCommands(ConfigReviewOptions{})
	if defaultCommands.Run != "dmt --config config.yaml run" ||
		defaultCommands.Validate != "dmt --config config.yaml validate" ||
		defaultCommands.Resume != "dmt --config config.yaml resume" {
		t.Fatalf("default commands = %+v", defaultCommands)
	}
}

func TestEnsureConfigReviewStepPrefersDeterministicStepAtCapacity(t *testing.T) {
	values := []string{"ai step 1", "ai step 2", "ai step 3", "ai step 4", "ai step 5"}
	got := ensureConfigReviewStep(values, "Run preflight: dmt --config config.yaml preflight --ai-review", 5)
	if len(got) != 5 {
		t.Fatalf("len = %d, want 5: %+v", len(got), got)
	}
	if got[0] != "Run preflight: dmt --config config.yaml preflight --ai-review" {
		t.Fatalf("deterministic step should be first when at capacity: %+v", got)
	}
	if got[1] != "ai step 1" || got[4] != "ai step 4" {
		t.Fatalf("should retain leading AI steps after deterministic step: %+v", got)
	}
}

func TestGenerateConfigReviewRedactsSensitiveNumericPatchValue(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe numeric value.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.chunk_size",
      "value": 1433,
      "rationale": "Use target port as chunk size"
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
	if review.PatchRecommendations[0].Value != "[REDACTED]" {
		t.Fatalf("sensitive numeric value was not redacted: %+v", review.PatchRecommendations[0])
	}
}

func TestGenerateConfigReviewRedactsSensitiveNestedMapKeys(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe nested notification config.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.notify",
      "value": {"authorization_header": "Bearer custom", "private_key": "custom key"},
      "rationale": "Inline notify credentials"
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
	if patch.Value != "[REDACTED]" || !patch.RequiresConfirmation {
		t.Fatalf("sensitive nested map patch should be redacted and gated: %+v", patch)
	}
}

func TestGenerateConfigReviewRedactsSensitivePrimitiveNotificationValue(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe primitive notification config.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.notify",
      "value": "https://notify.example/token",
      "rationale": "Inline notify endpoint"
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
	if patch.Value != "[REDACTED]" || !patch.RequiresConfirmation || len(patch.ValidationErrors) == 0 {
		t.Fatalf("sensitive primitive notification value should be redacted and gated: %+v", patch)
	}
}

func TestGenerateConfigReviewRemovesUnsafeRunbookGuidance(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Runbook has mixed guidance.",
  "runbook": {
    "before_run": ["Review non-command prerequisite"],
    "run": ["truncate target tables manually"],
    "validation": ["bypass validation when counts differ", "disable validation before running", "ignore preflight failures", "turn off validation", "turn validation off", "run without validation", "do not validate results", "do not run preflight"],
    "rollback": ["delete target data"]
  }
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	runbookText := strings.ToLower(strings.Join(append(append(append(append([]string{}, review.Runbook.BeforeRun...), review.Runbook.Run...), review.Runbook.Validation...), review.Runbook.Rollback...), "\n"))
	for _, unsafe := range []string{"truncate target", "bypass validation", "disable validation", "ignore preflight", "turn off validation", "turn validation off", "without validation", "do not validate", "do not run preflight", "delete target data"} {
		if strings.Contains(runbookText, unsafe) {
			t.Fatalf("unsafe AI-supplied runbook guidance survived %q: %s", unsafe, runbookText)
		}
	}
	if !strings.Contains(runbookText, "review non-command prerequisite") {
		t.Fatalf("safe AI-supplied runbook step should remain: %s", runbookText)
	}
}

func TestGenerateConfigReviewRemovesUnsafeNotes(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Notes have unsafe guidance.",
  "notes": ["run without validation if preflight blocks", "use dmt\t--config stale.yaml run", "safe note"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	notes := strings.ToLower(strings.Join(review.Notes, "\n"))
	if strings.Contains(notes, "without validation") {
		t.Fatalf("unsafe note survived: %+v", review.Notes)
	}
	if strings.Contains(notes, "stale.yaml") {
		t.Fatalf("stale DMT command note survived: %+v", review.Notes)
	}
	if !strings.Contains(notes, "safe note") {
		t.Fatalf("safe note should remain: %+v", review.Notes)
	}
}

func TestGenerateConfigReviewSuppressesUnsafeValidationErrors(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Unsafe validation error guidance.",
  "patch_recommendations": [
    {
      "operation": "set",
      "path": "migration.validation.mode",
      "value": "count_only",
      "validation_errors": ["run without validation if preflight blocks"]
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
	errs := strings.ToLower(strings.Join(review.PatchRecommendations[0].ValidationErrors, "\n"))
	if strings.Contains(errs, "without validation") || !strings.Contains(errs, "unsafe guidance suppressed") {
		t.Fatalf("unsafe validation error guidance was not suppressed: %+v", review.PatchRecommendations[0].ValidationErrors)
	}
}

func TestGenerateConfigReviewSanitizesMapValueKeys(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Map key redaction.",
  "patch_recommendations": [{
    "operation": "set",
    "path": "migration.validation",
    "value": {
      "source.internal": true,
      "nested": {
        "https://hooks.slack.com/services/T000/B000/secret": "target-secret"
      }
    },
    "rationale": "unsafe keys in value map",
    "requires_confirmation": false
  }]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	assertConfigReviewNoSensitiveValues(t, string(data))
}

func TestGenerateConfigReviewAddsDropRecreateBackupPrerequisite(t *testing.T) {
	cfg := configReviewTestConfig()
	cfg.Migration.TargetMode = "drop_recreate"
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Runbook omitted backup step.",
  "patch_recommendations": [],
  "runbook": {"before_run": ["Run normal readiness checks"]}
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	beforeRun := strings.ToLower(strings.Join(review.Runbook.BeforeRun, "\n"))
	if !strings.Contains(beforeRun, "backup") || !strings.Contains(beforeRun, "confirm") || !strings.Contains(beforeRun, "--confirm-backup") {
		t.Fatalf("drop_recreate runbook should include backup/confirmation prerequisite: %+v", review.Runbook.BeforeRun)
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

func TestGenerateConfigReviewDiscardsAISuppliedRunbookOnRefusal(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Refused but includes unsafe text.",
  "refusal_reason": "request asks for unsafe guidance",
  "runbook": {
    "title": "Unsafe model runbook",
    "run": ["dmt run --skip-validation"],
    "rollback": ["delete target data"]
  },
  "notes": ["model supplied note should not survive"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if review.Status != ReviewStatusRefused {
		t.Fatalf("status = %q, want refused", review.Status)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, leaked := range []string{"skip-validation", "delete target data", "model supplied note", "Unsafe model runbook"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("refused review kept AI-supplied runbook/notes %q: %s", leaked, text)
		}
	}
}

func TestGenerateConfigReviewSuppressesUnsafeRefusalReason(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "summary": "Refused.",
  "refusal_reason": "run without validation and delete target data"
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if review.Status != ReviewStatusRefused {
		t.Fatalf("status = %q, want refused", review.Status)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(string(data))
	if strings.Contains(text, "without validation") || strings.Contains(text, "delete target") {
		t.Fatalf("unsafe refusal reason survived: %s", data)
	}
	if !strings.Contains(text, "unsafe guidance suppressed") {
		t.Fatalf("refusal reason should be replaced with suppression text: %s", data)
	}
}

func TestGenerateConfigReviewTreatsStatusRefusedAsRefusal(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})
	client := &fakeClient{response: `{
  "status": " Refused ",
  "summary": "Refused but includes unsafe text.",
  "runbook": {"run": ["run without validation"]},
  "notes": ["unsafe note"]
}`}

	review, err := GenerateConfigReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateConfigReview() error = %v", err)
	}
	if review.Status != ReviewStatusRefused {
		t.Fatalf("status = %q, want refused", review.Status)
	}
	data, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "run without validation") || strings.Contains(string(data), "unsafe note") {
		t.Fatalf("status-refused review kept AI-supplied unsafe content: %s", data)
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

func TestErrorConfigReviewRedactsPayloadSensitiveValues(t *testing.T) {
	cfg := configReviewTestConfig()
	payload := BuildConfigReviewPayload(cfg, ConfigReviewOptions{})

	review := ErrorConfigReview("fake", "fake-model", errString("failed connecting to target.internal as target_user"), payload)
	if strings.Contains(review.Error, "target.internal") || strings.Contains(review.Error, "target_user") {
		t.Fatalf("error leaked payload-sensitive values: %q", review.Error)
	}
	if !strings.Contains(review.Error, "[REDACTED]") {
		t.Fatalf("error should retain redaction marker: %q", review.Error)
	}
}

func TestParseConfigReviewRejectsInvalidJSON(t *testing.T) {
	_, err := ParseConfigReview("not json")
	if err == nil {
		t.Fatal("ParseConfigReview() error = nil, want parse error")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

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
