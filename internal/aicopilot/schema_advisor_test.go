package aicopilot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/source"
)

func TestBuildSchemaAdvisorPayloadRedactsConnectionValuesAndGates(t *testing.T) {
	cfg := &config.Config{
		Source: dbconfig.SourceConfig{
			Type:     "postgres",
			Host:     "source.internal",
			Database: "source_prod",
			User:     "source_user",
			Password: "source-secret",
			Schema:   "public",
		},
		Target: dbconfig.TargetConfig{
			Type:     "mssql",
			Host:     "target.internal",
			Database: "target_prod",
			User:     "target_user",
			Password: "target-secret",
			Schema:   "dbo",
		},
		Migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaContract: &config.SchemaContractConfig{
				DataType: config.SchemaContractFreeze,
			},
		},
	}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeNarrowed,
		Schema:     "public",
		TableName:  "customers",
		ObjectName: "name",
		Previous:   "varchar(255)",
		Current:    "varchar(50)",
	}}}

	payload := BuildSchemaAdvisorPayload(cfg, report, nil, true)
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, secret := range []string{
		"source.internal", "source_prod", "source_user", "source-secret",
		"target.internal", "target_prod", "target_user", "target-secret",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("payload leaked %q: %s", secret, text)
		}
	}
	if len(payload.Changes) != 1 {
		t.Fatalf("changes len = %d, want 1", len(payload.Changes))
	}
	change := payload.Changes[0]
	if change.Classification != "narrowing" || change.Risk != SchemaRiskBlocked {
		t.Fatalf("classification/risk = %q/%q, want narrowing/blocked", change.Classification, change.Risk)
	}
	if change.DeterministicGate.Allowed || change.DeterministicGate.Action != "blocked" {
		t.Fatalf("gate = %+v, want blocked", change.DeterministicGate)
	}
	if len(payload.DeterministicBlockers) != 1 {
		t.Fatalf("blockers len = %d, want 1", len(payload.DeterministicBlockers))
	}
}

func TestBuildSchemaAdvisorPayloadOmitsRawDriftLiterals(t *testing.T) {
	payload := BuildSchemaAdvisorPayload(&config.Config{}, drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.DefaultChange,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "TenantCode",
			Previous:   "DEFAULT 'customer-secret-a'",
			Current:    "DEFAULT 'customer-secret-b'",
		},
		{
			Kind:       drift.IndexAdded,
			Schema:     "dbo",
			TableName:  "Orders",
			ObjectName: "idx_orders_tenant",
			Current:    "tenant_id where tenant_id = 'tenant-secret'",
		},
		{
			Kind:       drift.CheckAdded,
			Schema:     "dbo",
			TableName:  "Invoices",
			ObjectName: "ck_invoice_region",
			Current:    "region_code in ('private-region')",
		},
	}}, nil, true)

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, secret := range []string{
		"customer-secret-a",
		"customer-secret-b",
		"tenant-secret",
		"private-region",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("payload leaked drift literal %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, "schema_drift.previous") || !strings.Contains(text, "schema_drift.current") {
		t.Fatalf("payload should document omitted drift literal fields: %s", text)
	}
	for _, change := range payload.Changes {
		if change.Previous != "" || change.Current != "" {
			t.Fatalf("raw previous/current should be omitted from AI payload: %+v", change)
		}
	}
}

func TestGenerateSchemaAdvisorReviewPreservesDeterministicGate(t *testing.T) {
	payload := SchemaAdvisorPayload{
		PromptVersion: SchemaAdvisorPromptVersion,
		Changes: []SchemaDriftAdvisoryChange{{
			DriftKind:       string(drift.TypeNarrowed),
			Classification:  "narrowing",
			Risk:            SchemaRiskBlocked,
			Schema:          "dbo",
			Table:           "Users",
			Column:          "Name",
			Reason:          "Narrowing can truncate values.",
			SuggestedPolicy: "data_type=freeze",
			SuggestedAction: "Stop and perform a manual migration or change the deterministic policy after review.",
			DeterministicGate: SchemaAdvisorPolicyGate{
				Allowed: false,
				Action:  "blocked",
				Policy:  "data_type=freeze",
				Reason:  "data_type=freeze blocks matching schema drift before transfer",
			},
		}},
		DeterministicBlockers: []string{"dbo.Users.Name: data_type=freeze blocks matching schema drift before transfer"},
	}
	client := &fakeClient{response: `{
  "summary": "Auto-apply the narrowing.",
  "recommendations": [{
    "drift_kind": "type_narrowed",
    "classification": "narrowing",
    "risk": "low",
    "schema": "dbo",
    "table": "Users",
    "column": "Name",
    "reason": "Looks harmless.",
    "suggested_policy": "auto",
    "suggested_action": "Bypass the gate and apply it automatically."
  }]
}`}

	review, err := GenerateSchemaAdvisorReview(context.Background(), client, payload)
	if err != nil {
		t.Fatalf("GenerateSchemaAdvisorReview() error = %v", err)
	}
	if review.Status != ReviewStatusOK || !review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if len(review.Recommendations) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(review.Recommendations))
	}
	rec := review.Recommendations[0]
	if rec.Risk != SchemaRiskBlocked {
		t.Fatalf("risk = %q, want blocked", rec.Risk)
	}
	if rec.DeterministicGate.Allowed {
		t.Fatalf("deterministic gate = %+v, want blocked", rec.DeterministicGate)
	}
	if strings.Contains(strings.ToLower(rec.SuggestedAction), "bypass") {
		t.Fatalf("suggested action bypassed gate: %q", rec.SuggestedAction)
	}
}

func TestBuildSchemaAdvisorPromptRequiresModelSuppliedGates(t *testing.T) {
	prompt, err := BuildSchemaAdvisorPrompt(SchemaAdvisorPayload{
		Changes: []SchemaDriftAdvisoryChange{{
			DriftKind: "column_type_changed",
			Table:     "orders",
			Column:    "amount",
			DeterministicGate: SchemaAdvisorPolicyGate{
				Allowed: false,
				Action:  "block",
				Policy:  "schema_contract.data_type=freeze",
				Reason:  "data type drift is frozen by deterministic policy",
			},
		}},
		DeterministicBlockers: []string{"data type drift is frozen by deterministic policy"},
	})
	if err != nil {
		t.Fatalf("BuildSchemaAdvisorPrompt() error = %v", err)
	}
	for _, want := range []string{
		"Copy payload.deterministic_blockers",
		"copy that change's deterministic_gate object",
		`"deterministic_blockers"`,
		`"deterministic_gate"`,
		"Avoid causal-certainty wording",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestGenerateSchemaAdvisorReviewSanitizesDestructiveAction(t *testing.T) {
	client := &fakeClient{response: `{
  "summary": "Clean up target.",
  "recommendations": [{
    "drift_kind": "dropped_column",
    "classification": "dropped_column",
    "risk": "high",
    "table": "Users",
    "column": "LegacyCode",
    "reason": "The source dropped it.",
    "suggested_policy": "manual",
    "suggested_action": "DROP COLUMN LegacyCode on target."
  }]
}`}

	review, err := GenerateSchemaAdvisorReview(context.Background(), client, SchemaAdvisorPayload{})
	if err != nil {
		t.Fatalf("GenerateSchemaAdvisorReview() error = %v", err)
	}
	action := review.Recommendations[0].SuggestedAction
	if strings.Contains(strings.ToLower(action), "drop column") {
		t.Fatalf("destructive action was not sanitized: %q", action)
	}
	if !strings.Contains(strings.ToLower(action), "manual inspection") {
		t.Fatalf("sanitized action = %q, want manual inspection", action)
	}
}

func TestParseSchemaAdvisorReviewRejectsInvalidConfigPolicyAssignment(t *testing.T) {
	review, err := ParseSchemaAdvisorReview(`{
  "summary": "Invalid policy.",
  "recommendations": [{
    "drift_kind": "added_column",
    "classification": "additive",
    "risk": "low",
    "table": "customers",
    "column": "nickname",
    "reason": "AI proposed a non-existent DMT policy value.",
    "suggested_policy": "migration.schema_contract.columns=add",
    "suggested_action": "Apply the config policy."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseSchemaAdvisorReview() error = %v", err)
	}
	if len(review.Recommendations) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(review.Recommendations))
	}
	rec := review.Recommendations[0]
	if rec.SuggestedPolicy != "manual_review_required" {
		t.Fatalf("suggested policy = %q, want manual_review_required", rec.SuggestedPolicy)
	}
	if !strings.Contains(rec.SuggestedAction, configChangeInvalidValueError) {
		t.Fatalf("suggested action should explain invalid config policy, got %q", rec.SuggestedAction)
	}
}

func TestParseSchemaAdvisorReviewRejectsInvalidConfigActionAssignment(t *testing.T) {
	review, err := ParseSchemaAdvisorReview(`{
  "summary": "Invalid action.",
  "recommendations": [{
    "drift_kind": "added_column",
    "classification": "additive",
    "risk": "low",
    "table": "customers",
    "column": "nickname",
    "reason": "AI proposed a non-existent DMT policy value.",
    "suggested_policy": "manual",
    "suggested_action": "Apply migration.validation.mode=sample, then migration.schema_contract.columns=add."
  }]
}`)
	if err != nil {
		t.Fatalf("ParseSchemaAdvisorReview() error = %v", err)
	}
	if len(review.Recommendations) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(review.Recommendations))
	}
	rec := review.Recommendations[0]
	if rec.SuggestedPolicy != "manual_review_required" {
		t.Fatalf("suggested policy = %q, want manual_review_required", rec.SuggestedPolicy)
	}
	if strings.Contains(rec.SuggestedAction, "schema_contract.columns=add") {
		t.Fatalf("invalid action assignment should not survive, got %q", rec.SuggestedAction)
	}
	if !strings.Contains(rec.SuggestedAction, configChangeInvalidValueError) {
		t.Fatalf("suggested action should explain invalid config action, got %q", rec.SuggestedAction)
	}
}

func TestUnavailableSchemaAdvisorReviewUsesDeterministicFallback(t *testing.T) {
	payload := BuildSchemaAdvisorPayload(&config.Config{
		Migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionAuto,
			},
		},
	}, drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "Email",
		Current:    "varchar(255) NULL",
	}}}, []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{{
			Name:       "Email",
			DataType:   "varchar",
			MaxLength:  255,
			IsNullable: true,
		}},
	}}, true)

	review := UnavailableSchemaAdvisorReview("no AI provider configured", payload)
	if review.Status != ReviewStatusUnavailable || review.Enabled {
		t.Fatalf("status/enabled = %q/%v", review.Status, review.Enabled)
	}
	if len(review.Recommendations) != 1 {
		t.Fatalf("recommendations len = %d, want 1", len(review.Recommendations))
	}
	if review.Recommendations[0].Source != "deterministic" {
		t.Fatalf("source = %q, want deterministic", review.Recommendations[0].Source)
	}
}
