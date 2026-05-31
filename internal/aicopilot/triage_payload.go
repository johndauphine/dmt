package aicopilot

import (
	"regexp"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
)

var (
	sqlLikePattern           = regexp.MustCompile(`(?is)\b(select\b[\s\S]{0,120}\bfrom\b|insert\s+into\b|update\b[\s\S]{0,120}\bset\b|delete\s+from\b|merge\s+into\b|drop\s+table\b|truncate\s+table\b|alter\s+table\b|create\s+table\b)[\s\S]*`)
	emailPattern             = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	pkValuePattern           = regexp.MustCompile(`(?i)\bPK\s+(\[[^\]]*\]|\([^)]*\)|\{[^}]*\}|\S+)`)
	detailKeyPattern         = regexp.MustCompile(`(?i)\bDETAIL:\s*Key\s*\([^)]+\)=\([^)]+\)`)
	detailFailingRowPattern  = regexp.MustCompile(`(?i)\bDETAIL:\s*Failing row contains\s*\([^)]+\)`)
	duplicateKeyValuePattern = regexp.MustCompile(`(?i)\bduplicate key value is\s*\([^)]+\)`)
	quotedValuePattern       = regexp.MustCompile(`"[^"]*"|'[^']*'`)
)

const triageSQLRedactionInputLimit = 4096

func BuildMigrationFailureTriagePayload(cfg *config.Config, facts MigrationFailureFacts) TriagePayload {
	facts = scrubMigrationFailureFacts(facts)
	payload := baseTriagePayload(cfg, TriageKindMigrationFailure)
	payload.Task = "Triage a failed DMT migration. Deterministic facts are authoritative; AI hypotheses are advisory only."
	payload.MigrationFailure = &facts
	payload.DeterministicFacts = append(payload.DeterministicFacts,
		triageFact("failure.phase", "migration", facts.Phase),
		triageFact("failure.table", facts.Table, facts.Error),
	)
	if facts.RunStatus != nil {
		payload.DeterministicFacts = append(payload.DeterministicFacts,
			triageFact("run.status", facts.RunStatus.RunID, facts.RunStatus.Status+" phase="+facts.RunStatus.Phase),
		)
	}
	if facts.Checkpoint != nil {
		payload.DeterministicFacts = append(payload.DeterministicFacts,
			triageFact("checkpoint.state", facts.Checkpoint.RunID, checkpointDetail(*facts.Checkpoint)),
		)
	}
	if facts.SafeSQLShape != "" {
		payload.DeterministicFacts = append(payload.DeterministicFacts,
			triageFact("sql.shape", facts.Table, facts.SafeSQLShape),
		)
	}
	for _, failed := range facts.FailedTables {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("failure.failed_table", failed.Table, failed.Error))
	}
	for _, task := range facts.TaskFailures {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("failure.task", task.TaskKey, task.Error))
	}
	for _, diagnosis := range facts.DeterministicDiagnoses {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("diagnosis."+diagnosis.Category, facts.Table, diagnosis.Cause))
	}
	for _, drift := range facts.SchemaDrift {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("schema_drift."+drift.Action, drift.Table, drift.Reason))
	}
	payload.DeterministicFacts = compactTriageFacts(payload.DeterministicFacts)
	return payload
}

func BuildValidationMismatchTriagePayload(cfg *config.Config, facts ValidationMismatchFacts) TriagePayload {
	facts = scrubValidationMismatchFacts(facts)
	payload := baseTriagePayload(cfg, TriageKindValidationMismatch)
	payload.Task = "Triage a DMT validation mismatch. Deterministic counts and validation results are authoritative; AI hypotheses are advisory only."
	payload.ValidationMismatch = &facts
	if facts.Mode != "" {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.mode", facts.Table, facts.Mode))
	}
	if facts.HasRowCountFacts {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.counts", facts.Table, validationCountDetail(facts)))
	}
	if facts.Error != "" {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.error", facts.Table, facts.Error))
	}
	for _, pass := range facts.Passes {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation.pass", pass.Name, pass.Result+": "+pass.Detail))
	}
	for _, diff := range facts.Differences {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("validation."+diff.Category, diff.Table, diff.Detail))
	}
	if facts.Checkpoint != nil {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("checkpoint.state", facts.Checkpoint.RunID, checkpointDetail(*facts.Checkpoint)))
	}
	for _, drift := range facts.SchemaDrift {
		payload.DeterministicFacts = append(payload.DeterministicFacts, triageFact("schema_drift."+drift.Action, drift.Table, drift.Reason))
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
				"primary_key_values", "sample_values", "connection_strings",
			},
			ScrubbedText: true,
		},
	}
}

func scrubMigrationFailureFacts(facts MigrationFailureFacts) MigrationFailureFacts {
	sqlShape := firstNonEmpty(facts.SafeSQLShape, classifySQLShape(facts.LastSQL), classifySQLShape(facts.Error))
	facts.RunID = scrubTriageText(facts.RunID, 120)
	facts.Phase = scrubTriageText(facts.Phase, 80)
	facts.Table = scrubTriageText(facts.Table, 160)
	facts.Error = scrubTriageText(facts.Error, 500)
	facts.SafeSQLShape = scrubTriageText(sqlShape, 80)
	// The payload exposes only the classified SQL shape. Even redacted SQL text
	// can retain unsafe statement tails or object/value context.
	facts.LastSQL = ""
	if facts.RunStatus != nil {
		runStatus := scrubRunStatusFact(*facts.RunStatus)
		facts.RunStatus = &runStatus
	}
	for i := range facts.FailedTables {
		facts.FailedTables[i].Table = scrubTriageText(facts.FailedTables[i].Table, 160)
		facts.FailedTables[i].Error = scrubTriageText(facts.FailedTables[i].Error, 400)
	}
	if len(facts.FailedTables) > 10 {
		facts.FailedTables = facts.FailedTables[:10]
	}
	for i := range facts.TaskFailures {
		facts.TaskFailures[i] = scrubTaskFailureFact(facts.TaskFailures[i])
	}
	if len(facts.TaskFailures) > 10 {
		facts.TaskFailures = facts.TaskFailures[:10]
	}
	facts.DeterministicDiagnoses = scrubDeterministicDiagnosisFacts(facts.DeterministicDiagnoses)
	if facts.Checkpoint != nil {
		checkpoint := scrubCheckpointFact(*facts.Checkpoint)
		facts.Checkpoint = &checkpoint
	}
	facts.SchemaDrift = scrubSchemaDriftFacts(facts.SchemaDrift)
	facts.RecentCheckpoints = scrubCheckpointFacts(facts.RecentCheckpoints, 3)
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
	for i := range facts.Differences {
		facts.Differences[i] = scrubValidationDifferenceFact(facts.Differences[i])
	}
	if len(facts.Differences) > 20 {
		facts.Differences = facts.Differences[:20]
	}
	facts.SchemaDrift = scrubSchemaDriftFacts(facts.SchemaDrift)
	if facts.Checkpoint != nil {
		checkpoint := scrubCheckpointFact(*facts.Checkpoint)
		facts.Checkpoint = &checkpoint
	}
	facts.RecentCheckpoints = scrubCheckpointFacts(facts.RecentCheckpoints, 3)
	return facts
}

func scrubTriageText(s string, max int) string {
	return scrubTriageTextWithOptions(s, max, true)
}

func scrubTriageAdvisoryDisplayText(s string, max int) string {
	return scrubTriageTextWithOptions(s, max, false)
}

func scrubTriageTextWithOptions(s string, max int, redactQuotedValues bool) string {
	s = strings.TrimSpace(redactSQLLike(s))
	s = emailPattern.ReplaceAllString(s, "[REDACTED]")
	s = pkValuePattern.ReplaceAllString(s, "PK [REDACTED]")
	s = detailKeyPattern.ReplaceAllString(s, "DETAIL: Key [REDACTED]")
	s = detailFailingRowPattern.ReplaceAllString(s, "DETAIL: Failing row contains [REDACTED]")
	s = duplicateKeyValuePattern.ReplaceAllString(s, "duplicate key value is [REDACTED]")
	if redactQuotedValues {
		s = quotedValuePattern.ReplaceAllString(s, "\"[REDACTED]\"")
	}
	return limitText(logging.Scrub(s), max)
}

func redactSQLLike(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = limitText(s, triageSQLRedactionInputLimit)
	return sqlLikePattern.ReplaceAllString(logging.Scrub(s), "[SQL_REDACTED]")
}

func classifySQLShape(s string) string {
	lower := strings.ToLower(strings.Join(strings.Fields(s), " "))
	switch {
	case strings.Contains(lower, "select") && strings.Contains(lower, " from "):
		return "select_from"
	case strings.Contains(lower, "insert into"):
		return "insert_into"
	case strings.Contains(lower, "update") && strings.Contains(lower, " set "):
		return "update_set"
	case strings.Contains(lower, "delete from"):
		return "delete_from"
	case strings.Contains(lower, "merge into"):
		return "merge_into"
	case strings.Contains(lower, "drop table"):
		return "drop_table_destructive_ddl"
	case strings.Contains(lower, "truncate table"):
		return "truncate_table_destructive_ddl"
	case strings.Contains(lower, "alter table"):
		return "alter_table_ddl"
	case strings.Contains(lower, "create table"):
		return "create_table_ddl"
	default:
		return ""
	}
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

func scrubRunStatusFact(f RunStatusFact) RunStatusFact {
	return RunStatusFact{
		RunID:         scrubTriageText(f.RunID, 120),
		Status:        scrubTriageText(f.Status, 80),
		Phase:         scrubTriageText(f.Phase, 80),
		StartedAt:     scrubTriageText(f.StartedAt, 80),
		CompletedAt:   scrubTriageText(f.CompletedAt, 80),
		LastHeartbeat: scrubTriageText(f.LastHeartbeat, 80),
		Error:         scrubTriageText(f.Error, 500),
	}
}

func scrubTaskFailureFact(f TaskFailureFact) TaskFailureFact {
	return TaskFailureFact{
		TaskKey:    scrubTriageText(f.TaskKey, 180),
		TaskType:   scrubTriageText(f.TaskType, 80),
		Status:     scrubTriageText(f.Status, 80),
		Error:      scrubTriageText(f.Error, 500),
		RowsDone:   f.RowsDone,
		RowsTotal:  f.RowsTotal,
		RetryCount: f.RetryCount,
	}
}

func scrubCheckpointFact(f CheckpointFact) CheckpointFact {
	return CheckpointFact{
		RunID:          scrubTriageText(f.RunID, 120),
		Phase:          scrubTriageText(f.Phase, 80),
		LastHeartbeat:  scrubTriageText(f.LastHeartbeat, 80),
		TasksTotal:     f.TasksTotal,
		TasksPending:   f.TasksPending,
		TasksRunning:   f.TasksRunning,
		TasksSucceeded: f.TasksSucceeded,
		TasksFailed:    f.TasksFailed,
		RowsDone:       f.RowsDone,
		RowsTotal:      f.RowsTotal,
	}
}

func scrubCheckpointFacts(in []CheckpointFact, max int) []CheckpointFact {
	out := make([]CheckpointFact, 0, len(in))
	for _, f := range in {
		out = append(out, scrubCheckpointFact(f))
	}
	if max > 0 && len(out) > max {
		return out[:max]
	}
	return out
}

func scrubSchemaDriftFacts(in []SchemaDriftFact) []SchemaDriftFact {
	out := make([]SchemaDriftFact, 0, len(in))
	for _, f := range in {
		out = append(out, SchemaDriftFact{
			Entity: scrubTriageText(f.Entity, 80),
			Table:  scrubTriageText(f.Table, 160),
			Object: scrubTriageText(f.Object, 160),
			Drift:  scrubTriageText(f.Drift, 80),
			Action: scrubTriageText(f.Action, 80),
			Reason: scrubTriageText(f.Reason, 400),
		})
	}
	if len(out) > 10 {
		return out[:10]
	}
	return out
}

func scrubValidationDifferenceFact(f ValidationDifferenceFact) ValidationDifferenceFact {
	return ValidationDifferenceFact{
		Category: normalizeValidationCategory(f.Category),
		Table:    scrubTriageText(f.Table, 160),
		Pass:     scrubTriageText(f.Pass, 80),
		Column:   scrubTriageText(f.Column, 160),
		Severity: scrubTriageText(f.Severity, 80),
		Detail:   scrubTriageText(f.Detail, 500),
	}
}

func scrubDeterministicDiagnosisFacts(in []DeterministicDiagnosisFact) []DeterministicDiagnosisFact {
	out := make([]DeterministicDiagnosisFact, 0, len(in))
	for _, f := range in {
		diagnosis := DeterministicDiagnosisFact{
			Category:    scrubTriageText(f.Category, 80),
			Cause:       scrubTriageText(f.Cause, 500),
			Confidence:  normalizeConfidence(f.Confidence),
			PatternName: scrubTriageText(f.PatternName, 120),
		}
		for _, suggestion := range f.Suggestions {
			diagnosis.Suggestions = append(diagnosis.Suggestions, sanitizeTriageNextAction(suggestion))
		}
		if len(diagnosis.Suggestions) > 5 {
			diagnosis.Suggestions = diagnosis.Suggestions[:5]
		}
		out = append(out, diagnosis)
	}
	if len(out) > 5 {
		return out[:5]
	}
	return out
}

func normalizeValidationCategory(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case ValidationCategoryDeleteDrift:
		return ValidationCategoryDeleteDrift
	case ValidationCategoryTypeCoercion:
		return ValidationCategoryTypeCoercion
	case ValidationCategoryTimezoneDateHandling:
		return ValidationCategoryTimezoneDateHandling
	case ValidationCategoryWatermarkIssue:
		return ValidationCategoryWatermarkIssue
	case ValidationCategoryTargetTriggerDefaultBehavior:
		return ValidationCategoryTargetTriggerDefaultBehavior
	case ValidationCategoryRowCountMismatch:
		return ValidationCategoryRowCountMismatch
	case ValidationCategoryNullMismatch:
		return ValidationCategoryNullMismatch
	case ValidationCategorySampleMismatch:
		return ValidationCategorySampleMismatch
	case ValidationCategoryValidationRuntime:
		return ValidationCategoryValidationRuntime
	case ValidationCategoryUnknown:
		return ValidationCategoryUnknown
	default:
		return ValidationCategoryUnknown
	}
}

func checkpointDetail(f CheckpointFact) string {
	parts := []string{
		"phase=" + f.Phase,
		"tasks_failed=" + formatInt(f.TasksFailed),
		"tasks_running=" + formatInt(f.TasksRunning),
		"tasks_pending=" + formatInt(f.TasksPending),
		"rows_done=" + formatInt64(f.RowsDone),
		"rows_total=" + formatInt64(f.RowsTotal),
	}
	return strings.Join(parts, ", ")
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

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func formatInt(v int) string {
	return formatInt64(int64(v))
}
