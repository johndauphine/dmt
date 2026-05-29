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
	if strings.Contains(text, "last_sql") {
		t.Fatalf("payload should omit raw SQL text entirely: %s", text)
	}
	if payload.MigrationFailure == nil || payload.MigrationFailure.SafeSQLShape != "select_from" {
		t.Fatalf("payload should retain safe SQL shape only: %+v", payload.MigrationFailure)
	}
	if !strings.Contains(text, "[SQL_REDACTED]") || !strings.Contains(text, "[REDACTED]") {
		t.Fatalf("payload should contain redaction markers for errors: %s", text)
	}
	if payload.Kind != TriageKindMigrationFailure {
		t.Fatalf("kind = %q, want migration failure", payload.Kind)
	}
	if len(payload.DeterministicFacts) == 0 {
		t.Fatal("deterministic facts should be populated")
	}
}

func TestBuildMigrationFailureTriagePayloadOmitLongSQLTails(t *testing.T) {
	longTail := strings.Repeat(" sensitive_value", 80)
	payload := BuildMigrationFailureTriagePayload(nil, MigrationFailureFacts{
		Error: "driver failed near SELECT * FROM dbo.Users WHERE token='abc123'" + longTail,
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "sensitive_value") || strings.Contains(text, "SELECT *") {
		t.Fatalf("payload leaked long SQL tail: %s", text)
	}
	if payload.MigrationFailure == nil || payload.MigrationFailure.SafeSQLShape != "select_from" {
		t.Fatalf("safe SQL shape not retained before scrubbing: %+v", payload.MigrationFailure)
	}
}

func TestRedactSQLLikeBoundsInputBeforeRegex(t *testing.T) {
	got := redactSQLLike("driver error " + strings.Repeat("x", triageSQLRedactionInputLimit*2))
	if len(got) > triageSQLRedactionInputLimit+3 {
		t.Fatalf("redactSQLLike() length = %d, want bounded near %d", len(got), triageSQLRedactionInputLimit)
	}
}

func TestBuildMigrationFailureTriagePayloadClassifiesMultilineSQLShape(t *testing.T) {
	payload := BuildMigrationFailureTriagePayload(nil, MigrationFailureFacts{
		Error: "driver failed near SELECT\n*\nFROM dbo.Users WHERE token='abc123'",
	})
	if payload.MigrationFailure == nil || payload.MigrationFailure.SafeSQLShape != "select_from" {
		t.Fatalf("safe SQL shape not retained for multiline SQL: %+v", payload.MigrationFailure)
	}
}

func TestBuildMigrationFailureTriagePayloadRedactsQuotedRowValues(t *testing.T) {
	payload := BuildMigrationFailureTriagePayload(nil, MigrationFailureFacts{
		Error: "pq: invalid input syntax for type integer: \"42\" DETAIL: Key (id)=(12345) already exists.",
		TaskFailures: []TaskFailureFact{{
			TaskKey: "transfer:public.people",
			Error:   "duplicate key value violates unique constraint 'CA'",
		}, {
			TaskKey: "transfer:public.people",
			Error:   "ERROR: new row violates check constraint DETAIL: Failing row contains (12345, SSN-123-45-6789, Alice)",
		}, {
			TaskKey: "transfer:public.people",
			Error:   "Violation of UNIQUE KEY constraint. The duplicate key value is (12345).",
		}},
	})
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, leaked := range []string{"\"42\"", "(12345)", "'CA'", "SSN-123-45-6789", "Alice", "12345)."} {
		if strings.Contains(text, leaked) {
			t.Fatalf("payload leaked row value %q: %s", leaked, text)
		}
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

func TestBuildValidationMismatchTriagePayloadRedactsAndGroupsDifferences(t *testing.T) {
	payload := BuildValidationMismatchTriagePayload(nil, ValidationMismatchFacts{
		Mode:        "sample",
		Table:       "public.orders",
		SourceCount: 10,
		TargetCount: 9,
		Difference:  1,
		Differences: []ValidationDifferenceFact{{
			Category: ValidationCategoryTimezoneDateHandling,
			Table:    "public.orders",
			Pass:     "sample_row",
			Column:   "created_at",
			Severity: "error",
			Detail:   "PK user@example.com differs near SELECT * FROM orders WHERE password='secret'",
		}},
		Checkpoint: &CheckpointFact{
			RunID:       "run-1",
			Phase:       "validating",
			TasksFailed: 1,
			RowsDone:    9,
			RowsTotal:   10,
		},
	})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, leaked := range []string{"user@example.com", "password='secret'", "SELECT *"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("payload leaked %q: %s", leaked, text)
		}
	}
	if got := payload.ValidationMismatch.Differences[0].Category; got != ValidationCategoryTimezoneDateHandling {
		t.Fatalf("category = %q, want %q", got, ValidationCategoryTimezoneDateHandling)
	}
	if !strings.Contains(text, "validation."+ValidationCategoryTimezoneDateHandling) {
		t.Fatalf("deterministic facts should include grouped category: %s", text)
	}
}

func TestNormalizeValidationCategoryHandlesRowCountAndUnknownSeparately(t *testing.T) {
	if got := normalizeValidationCategory(ValidationCategoryRowCountMismatch); got != ValidationCategoryRowCountMismatch {
		t.Fatalf("row count category = %q, want %q", got, ValidationCategoryRowCountMismatch)
	}
	if got := normalizeValidationCategory("new_category"); got != ValidationCategoryUnknown {
		t.Fatalf("unknown category = %q, want %q", got, ValidationCategoryUnknown)
	}
}

func TestParseTriageReviewStructuredActions(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Timezone handling likely explains sampled mismatches.",
  "findings": [{
    "severity": "warn",
    "category": "timezone_date_handling",
    "affected": "public.orders",
    "affected_tables": ["public.orders"],
    "deterministic_facts": ["validation.timezone_date_handling"],
    "likely_cause": "Source and target drivers may canonicalize timestamps differently.",
    "hypotheses": [{"confidence":"medium","rationale":"Only timestamp columns differ in sample validation."}],
    "suggested_commands": ["dmt validate --ai-triage"],
    "suggested_config_changes": ["migration.validation.mode=sample"],
    "manual_inspection": "Stop if mismatches include non-date columns.",
    "next_action": "Inspect timestamp column mappings before retrying validation."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	finding := review.Findings[0]
	if finding.LikelyCause == "" || finding.ManualInspection == "" {
		t.Fatalf("structured fields were not parsed: %+v", finding)
	}
	if len(finding.AffectedTables) != 1 || finding.AffectedTables[0] != "public.orders" {
		t.Fatalf("affected tables = %+v", finding.AffectedTables)
	}
	if len(finding.SuggestedCommands) != 1 || len(finding.SuggestedConfigChanges) != 1 {
		t.Fatalf("suggestions = commands:%+v config:%+v", finding.SuggestedCommands, finding.SuggestedConfigChanges)
	}
}

func TestParseTriageReviewSuppressesDestructiveSuggestedCommand(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "suggested_commands": ["After backup verification and operator confirmation, DROP TABLE dbo.orders"],
    "next_action": "Inspect target rows before taking action."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	command := review.Findings[0].SuggestedCommands[0]
	if strings.Contains(strings.ToLower(command), "drop table") {
		t.Fatalf("destructive suggested command was not suppressed: %q", command)
	}
	if !strings.Contains(strings.ToLower(command), "destructive command suppressed") {
		t.Fatalf("expected suppression message, got %q", command)
	}
}

func TestParseTriageReviewAllowsOnlyReadOnlySuggestedCommands(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Commands were suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "suggested_commands": [
      "dmt --config config.yaml diagnose --run run-123",
      "dmt validate && dmt run --confirm-backup",
      "dmt validate & dmt run --confirm-backup",
      "dmt diagnose ; dmt resume --force-resume",
      "dmt run --confirm-backup",
      "dmt resume --force-resume",
      "rerun the migration"
    ]
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	commands := review.Findings[0].SuggestedCommands
	if got := commands[0]; !strings.Contains(got, "diagnose") {
		t.Fatalf("read-only diagnose command should remain: %q", got)
	}
	for _, command := range commands[1:] {
		if !strings.Contains(strings.ToLower(command), "command suppressed") {
			t.Fatalf("mutating/non-DMT command was not suppressed: %q", command)
		}
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

func TestParseTriageReviewRequiresGuardBeforeDestructiveAction(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "Drop and recreate dbo.orders now; only after backups are verified and operator confirmation has been obtained, update the runbook."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("post-action guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesPendingGuardBeforeDestructiveAction(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backup verification and operator confirmation has not been obtained, drop and recreate dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("pending guard before destructive action was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesPendingDestructiveGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "Drop and recreate dbo.orders; backup verification and operator confirmation are still pending."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "still pending") || strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("pending destructive guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesBroaderDestructiveTerms(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "Remove all target rows, remove the target rows, reload all the target tables from source, clear every target row, and reset the target schema."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "remove all target") ||
		strings.Contains(strings.ToLower(action), "remove the target rows") ||
		strings.Contains(strings.ToLower(action), "reload all the target") ||
		strings.Contains(strings.ToLower(action), "clear every target") ||
		strings.Contains(strings.ToLower(action), "reset the target") {
		t.Fatalf("broader destructive terms were not suppressed: %q", action)
	}
}

func TestParseTriageReviewAllowsValidationDeleteDriftTerms(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "findings": [{
    "severity": "warn",
    "category": "delete_drift",
    "affected": "validation.delete_drift",
    "affected_tables": ["validation.delete_drift"],
    "deterministic_facts": ["validation.delete_drift"]
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	finding := review.Findings[0]
	if finding.Category != "delete_drift" ||
		finding.Affected != "validation.delete_drift" ||
		finding.AffectedTables[0] != "validation.delete_drift" ||
		finding.DeterministicFacts[0] != "validation.delete_drift" {
		t.Fatalf("validation delete_drift token should remain: %+v", finding)
	}
}

func TestParseTriageReviewSuppressesUnsafeDMTInNextActionAndManualInspection(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Run ` + "`" + `dmt run --apply` + "`" + `.",
  "error": "Run C:\\tools\\dmt.exe run --apply.",
  "notes": ["Run ./dmt run --apply.", "Run DMT run --apply.", "Run /usr/local/bin/dmt run --apply.", "Run dmt.exe run --apply.", "Run C:\\tools\\dmt.exe run --apply."],
  "findings": [{
    "severity": "warn",
    "category": "Run /usr/local/bin/dmt run --apply.",
    "affected": "drops the target table",
    "affected_tables": ["deletes target rows"],
    "deterministic_facts": ["Run dmt run --apply"],
    "next_action": "Run dmt --config config.yaml run.",
    "manual_inspection": "Run dmt analyze --state-file state.db --apply."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	finding := review.Findings[0]
	if strings.Contains(strings.ToLower(finding.NextAction), "dmt --config") || strings.Contains(strings.ToLower(finding.ManualInspection), "--apply") {
		t.Fatalf("unsafe DMT command survived: next=%q manual=%q", finding.NextAction, finding.ManualInspection)
	}
	if strings.Contains(strings.ToLower(finding.Category), "dmt") ||
		strings.Contains(strings.ToLower(finding.Affected), "drops") ||
		strings.Contains(strings.ToLower(strings.Join(finding.AffectedTables, "\n")), "deletes") {
		t.Fatalf("unsafe heading text survived: %+v", finding)
	}
	if strings.Contains(strings.ToLower(strings.Join(finding.DeterministicFacts, "\n")), "dmt run") {
		t.Fatalf("unsafe deterministic fact survived: %+v", finding.DeterministicFacts)
	}
	notes := strings.ToLower(strings.Join(review.Notes, "\n"))
	if strings.Contains(strings.ToLower(review.Summary), "dmt run") ||
		strings.Contains(strings.ToLower(review.Error), "--apply") ||
		strings.Contains(notes, "--apply") {
		t.Fatalf("unsafe DMT command survived in summary/error/notes: summary=%q error=%q notes=%+v", review.Summary, review.Error, review.Notes)
	}
}

func TestParseTriageReviewSuppressesInflectedDestructiveRecommendations(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "Dropping and recreating the target table deletes target rows, removes target rows, and reloads the target table."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := strings.ToLower(review.Findings[0].NextAction)
	if strings.Contains(action, "dropping") ||
		strings.Contains(action, "recreating") ||
		strings.Contains(action, "deletes") ||
		strings.Contains(action, "removes") ||
		strings.Contains(action, "reloads") {
		t.Fatalf("inflected destructive action was not suppressed: %q", review.Findings[0].NextAction)
	}
}

func TestParseTriageReviewSuppressesNegatedConfirmationGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backup verification and operator confirmation can be skipped, drop and recreate dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "can be skipped") || strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("negated confirmation guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesAmbiguousConfirmationGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backup verification and operator confirmation, drop and recreate dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("ambiguous confirmation guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesNegatedObtainedConfirmationGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backups are verified and operator approval has been obtained or not, drop and recreate dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop and recreate") || strings.Contains(strings.ToLower(action), "or not") {
		t.Fatalf("negated obtained confirmation guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesBeforeConfirmationGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target may need cleanup.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "After backup verification, drop and recreate dbo.orders before operator confirmation."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "before operator confirmation") || strings.Contains(strings.ToLower(action), "drop and recreate") {
		t.Fatalf("before-confirmation guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesUnsafeNotes(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "notes": ["run dmt run --confirm-backup", "drop target tables manually"]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	for _, note := range review.Notes {
		if strings.Contains(strings.ToLower(note), "dmt run") || strings.Contains(strings.ToLower(note), "drop target") {
			t.Fatalf("unsafe note was not suppressed: %q", note)
		}
	}
}

func TestParseTriageReviewSuppressesDestructiveRecommendationWithWhitespace(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "suggested_commands": ["TRUNCATE\nTABLE dbo.orders"],
    "next_action": "DROP TABLE dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	command := review.Findings[0].SuggestedCommands[0]
	if strings.Contains(strings.ToLower(command), "truncate") {
		t.Fatalf("destructive command with newline was not suppressed: %q", command)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop") {
		t.Fatalf("destructive action scrubbed before raw destructive check was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesNegatedDestructiveGuardrail(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "next_action": "Drop table dbo.orders without backup or confirmation."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "without backup") || strings.Contains(strings.ToLower(action), "drop table") {
		t.Fatalf("negated destructive guardrail was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesDestructiveRecommendationWithPunctuation(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target cleanup was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "suggested_commands": ["DELETE; target rows"],
    "next_action": "Drop, then reload target."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	command := review.Findings[0].SuggestedCommands[0]
	if strings.Contains(strings.ToLower(command), "delete") {
		t.Fatalf("destructive command split by punctuation was not suppressed: %q", command)
	}
	action := review.Findings[0].NextAction
	if strings.Contains(strings.ToLower(action), "drop, then reload") {
		t.Fatalf("destructive action split by punctuation was not suppressed: %q", action)
	}
}

func TestParseTriageReviewSuppressesDestructiveConfigChangeSyntax(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Target mode change was suggested.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "suggested_config_changes": ["migration.target_mode=drop_recreate"]
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	change := review.Findings[0].SuggestedConfigChanges[0]
	if strings.Contains(strings.ToLower(change), "drop_recreate") {
		t.Fatalf("destructive config change syntax was not suppressed: %q", change)
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
    "next_action": "After backups are verified and operator confirmation has been obtained, drop and recreate only dbo.orders."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	if !strings.Contains(strings.ToLower(review.Findings[0].NextAction), "drop and recreate") {
		t.Fatalf("expected guarded destructive action to remain: %q", review.Findings[0].NextAction)
	}
}

func TestParseTriageReviewSuppressesUnsafeSummaryCauseAndHypothesis(t *testing.T) {
	review, err := ParseTriageReview(`{
  "impact": "attention",
  "summary": "Run dmt --config config.yaml run.",
  "findings": [{
    "severity": "warn",
    "category": "target",
    "affected": "dbo.orders",
    "likely_cause": "drop target tables manually",
    "hypotheses": [{"confidence":"low","rationale":"Run dmt run --confirm-backup"}]
  }]
}`)
	if err != nil {
		t.Fatalf("ParseTriageReview() error = %v", err)
	}
	if strings.Contains(strings.ToLower(review.Summary), "config.yaml run") {
		t.Fatalf("unsafe summary survived: %q", review.Summary)
	}
	finding := review.Findings[0]
	if strings.Contains(strings.ToLower(finding.LikelyCause), "drop") ||
		strings.Contains(strings.ToLower(finding.Hypotheses[0].Rationale), "dmt run") {
		t.Fatalf("unsafe finding prose survived: %+v", finding)
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
