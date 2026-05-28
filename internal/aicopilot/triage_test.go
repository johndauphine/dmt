package aicopilot

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestBuildMigrationFailureTriagePayloadRedactsErrorsAndSQL(t *testing.T) {
	payload := BuildMigrationFailureTriagePayload(nil, MigrationFailureFacts{
		Phase:   "copy",
		Table:   "dbo.Users",
		Error:   "insert failed password=supersecret near INSERT INTO dbo.Users(email) VALUES('person@example.com')",
		LastSQL: "SELECT email, password FROM dbo.Users WHERE token='abc123'",
		FailedTables: []FailedTableFact{{
			Table: "dbo.Users",
			Error: "sqlserver://user:secretpass@target:1433 failed on DELETE FROM dbo.Users",
		}},
	})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, leaked := range []string{"supersecret", "secretpass", "INSERT INTO", "SELECT email", "DELETE FROM"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("payload leaked %q: %s", leaked, text)
		}
	}
	if !strings.Contains(text, "[SQL_REDACTED]") || !strings.Contains(text, "[REDACTED]") {
		t.Fatalf("payload should contain redaction markers: %s", text)
	}
	if payload.Kind != TriageKindMigrationFailure {
		t.Fatalf("kind = %q, want migration failure", payload.Kind)
	}
	if len(payload.DeterministicFacts) == 0 {
		t.Fatal("deterministic facts should be populated")
	}
}

func TestGenerateTriageReviewSeparatesFactsFromHypotheses(t *testing.T) {
	payload := BuildValidationMismatchTriagePayload(nil, ValidationMismatchFacts{
		Mode:        "count_only",
		Table:       "public.orders",
		SourceCount: 100,
		TargetCount: 99,
		Difference:  1,
	})
	client := &fakeClient{response: `{
  "impact": "attention",
  "summary": "One row is missing from the target.",
  "findings": [{
    "severity": "warn",
    "category": "validation",
    "affected": "public.orders",
    "deterministic_facts": ["source_count=100 target_count=99"],
    "hypotheses": [{"confidence":"medium","rationale":"A late failed batch may explain the difference."}],
    "next_action": "Check checkpoint and retry only the affected table if supported."
  }]
}`}

	review, err := GenerateTriageReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateTriageReview() error = %v", err)
	}
	if review.Status != ReviewStatusOK || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if review.Kind != TriageKindValidationMismatch || review.Impact != TriageImpactAttention {
		t.Fatalf("kind/impact = %q/%q", review.Kind, review.Impact)
	}
	if len(review.DeterministicFacts) == 0 {
		t.Fatal("review should copy deterministic facts from payload")
	}
	if len(review.Findings) != 1 || len(review.Findings[0].Hypotheses) != 1 {
		t.Fatalf("findings = %+v", review.Findings)
	}
	if review.Findings[0].Source != TriageFindingSourceAIAdvisory {
		t.Fatalf("source = %q", review.Findings[0].Source)
	}
	if !strings.Contains(client.prompt, `"deterministic_facts"`) {
		t.Fatalf("prompt did not include deterministic facts: %s", client.prompt)
	}
}

func TestParseTriageReviewSuppressesUnsafeDestructiveRecommendation(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "hypotheses": [{"confidence":"high","rationale":"stale rows"}],
    "next_action": "Drop and recreate dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("unsafe destructive action was not suppressed: %q", action)
	}
	if !strings.Contains(strings.ToLower(action), "backup") || !strings.Contains(strings.ToLower(action), "confirmation") {
		t.Fatalf("suppressed action should require backup and confirmation: %q", action)
	}
}

func TestParseTriageReviewAllowsDestructiveRecommendationWithBackupAndConfirmation(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "blocked",
  "summary": "Operator confirmation is needed.",
  "findings": [{
    "severity": "error",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backup verification and operator confirmation, drop and recreate only dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	if !strings.Contains(strings.ToLower(review.Findings[0].NextAction), "drop and recreate") {
		t.Fatalf("expected guarded destructive action to remain: %q", review.Findings[0].NextAction)
	}
}

func TestParseTriageReviewRejectsInvalidJSON(t *testing.T) {
	if _, err := ParseTriageReview("not json"); err == nil {
		t.Fatal("ParseTriageReview() error = nil, want parse error")
	}
}

func TestTriageFallbacksUseDeterministicFacts(t *testing.T) {
	payload := BuildMigrationFailureTriagePayload(nil, MigrationFailureFacts{
		Phase: "copy",
		Table: "dbo.orders",
		Error: "network timeout password=secret",
	})

	unavailable := UnavailableTriageReview("no provider password=secret", payload)
	if unavailable.Status != ReviewStatusUnavailable || unavailable.Enabled {
		t.Fatalf("unavailable status/enabled = %q/%v", unavailable.Status, unavailable.Enabled)
	}
	if unavailable.Impact != TriageImpactBlocked {
		t.Fatalf("unavailable impact = %q, want blocked", unavailable.Impact)
	}
	if strings.Contains(unavailable.Summary, "secret") {
		t.Fatalf("unavailable summary leaked secret: %q", unavailable.Summary)
	}

	failed := ErrorTriageReview("fake", "fake-model", errors.New("api_key=secret failed"), payload)
	if failed.Status != ReviewStatusError || !failed.Enabled {
		t.Fatalf("error status/enabled = %q/%v", failed.Status, failed.Enabled)
	}
	if strings.Contains(failed.Error, "secret") {
		t.Fatalf("error fallback leaked secret: %q", failed.Error)
	}
	if len(failed.DeterministicFacts) == 0 {
		t.Fatal("error fallback should preserve deterministic facts")
	}
}
