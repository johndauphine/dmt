package aicopilot

import (
	"regexp"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
)

var sqlLikePattern = regexp.MustCompile(`(?is)\b(select\b[\s\S]{0,120}\bfrom\b|insert\s+into\b|update\b[\s\S]{0,120}\bset\b|delete\s+from\b|merge\s+into\b|drop\s+table\b|truncate\s+table\b|alter\s+table\b|create\s+table\b)[\s\S]{0,400}`)

func BuildMigrationFailureTriagePayload(cfg *config.Config, facts MigrationFailureFacts) TriagePayload {
	facts = scrubMigrationFailureFacts(facts)
	payload := baseTriagePayload(cfg, TriageKindMigrationFailure)
	payload.Task = "Triage a failed DMT migration. Deterministic facts are authoritative; AI hypotheses are advisory only."
	payload.MigrationFailure = &facts
	payload.DeterministicFacts = append(payload.DeterministicFacts,
		triageFact("failure.phase", "migration", facts.Phase),
		triageFact("failure.table", facts.Table, facts.Error),
	)
	for _, failed := range facts.FailedTables {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("failure.failed_table", failed.Table, failed.Error))
	}
	payload.DeterministicFacts = compactTriageFacts(payload.DeterministicFacts)
	return payload
}

func BuildValidationMismatchTriagePayload(cfg *config.Config, facts ValidationMismatchFacts) TriagePayload {
	facts = scrubValidationMismatchFacts(facts)
	payload := baseTriagePayload(cfg, TriageKindValidationMismatch)
	payload.Task = "Triage a DMT validation mismatch. Deterministic counts and validation results are authoritative; AI hypotheses are advisory only."
	payload.ValidationMismatch = &facts
	payload.DeterministicFacts = append(payload.DeterministicFacts,
		triageFact("validation.mode", facts.Table, facts.Mode),
		triageFact("validation.counts", facts.Table, validationCountDetail(facts)),
	)
	if facts.Error != "" {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.error", facts.Table, facts.Error))
	}
	for _, pass := range facts.Passes {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.pass", pass.Name, pass.Result+": "+pass.Detail))
	}
	payload.DeterministicFacts = compactTriageFacts(payload.DeterministicFacts)
	return payload
}

func baseTriagePayload(cfg *config.Config, kind string) TriagePayload {
	return TriagePayload{
		PromptVersion: TriagePromptVersion,
		Kind:          normalizeTriageKind(kind),
		Config:        buildConfigSummary(cfg),
		Redaction: RedactionSummary{
			OmittedFields: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database",
				"target.host", "target.port", "target.user", "target.password", "target.database",
				"ai.api_key", "slack.webhook_url", "row_values", "raw_sql_text",
			},
			ScrubbedText: true,
		},
	}
}

func scrubMigrationFailureFacts(facts MigrationFailureFacts) MigrationFailureFacts {
	facts.RunID = scrubTriageText(facts.RunID, 120)
	facts.Phase = scrubTriageText(facts.Phase, 80)
	facts.Table = scrubTriageText(facts.Table, 160)
	facts.Error = scrubTriageText(facts.Error, 500)
	facts.LastSQL = redactSQLLike(facts.LastSQL)
	for i := range facts.FailedTables {
		facts.FailedTables[i].Table = scrubTriageText(facts.FailedTables[i].Table, 160)
		facts.FailedTables[i].Error = scrubTriageText(facts.FailedTables[i].Error, 400)
	}
	if len(facts.FailedTables) > 10 {
		facts.FailedTables = facts.FailedTables[:10]
	}
	return facts
}

func scrubValidationMismatchFacts(facts ValidationMismatchFacts) ValidationMismatchFacts {
	facts.Mode = scrubTriageText(facts.Mode, 80)
	facts.Table = scrubTriageText(facts.Table, 160)
	facts.Error = scrubTriageText(facts.Error, 500)
	for i := range facts.Passes {
		facts.Passes[i].Name = scrubTriageText(facts.Passes[i].Name, 80)
		facts.Passes[i].Result = scrubTriageText(facts.Passes[i].Result, 80)
		facts.Passes[i].Detail = scrubTriageText(facts.Passes[i].Detail, 400)
	}
	if len(facts.Passes) > 10 {
		facts.Passes = facts.Passes[:10]
	}
	return facts
}

func scrubTriageText(s string, max int) string {
	return limitText(logging.Scrub(strings.TrimSpace(redactSQLLike(s))), max)
}

func redactSQLLike(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	return sqlLikePattern.ReplaceAllString(logging.Scrub(s), "[SQL_REDACTED]")
}

func triageFact(category, affected, detail string) TriageFact {
	return TriageFact{
		Category: scrubTriageText(category, 80),
		Affected: scrubTriageText(affected, 160),
		Detail:   scrubTriageText(detail, 500),
	}
}

func compactTriageFacts(facts []TriageFact) []TriageFact {
	out := facts[:0]
	for _, fact := range facts {
		if fact.Category == "" && fact.Affected == "" && fact.Detail == "" {
			continue
		}
		out = append(out, fact)
	}
	if len(out) > 12 {
		return out[:12]
	}
	return out
}

func validationCountDetail(facts ValidationMismatchFacts) string {
	parts := []string{
		"source_count=" + formatInt64(facts.SourceCount),
		"target_count=" + formatInt64(facts.TargetCount),
		"difference=" + formatInt64(facts.Difference),
	}
	if facts.UsedEstimate {
		parts = append(parts, "used_estimate=true")
	}
	if facts.ExactTimedOut {
		parts = append(parts, "exact_timed_out=true")
	}
	if facts.TimedOut {
		parts = append(parts, "timed_out=true")
	}
	return strings.Join(parts, ", ")
}
