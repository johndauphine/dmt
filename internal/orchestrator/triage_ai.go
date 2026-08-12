package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/v5/internal/aicopilot"
	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/driver/errordiag"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/orchestrator/validation"
)

// DiagnoseRunWithAI builds deterministic failure facts from checkpoint state
// and asks the configured AI provider for advisory triage. When AI is not
// configured or fails, the returned review still contains deterministic facts.
func (o *Orchestrator) DiagnoseRunWithAI(ctx context.Context, runID string) (*aicopilot.TriageReview, error) {
	return o.DiagnoseRun(ctx, runID, true)
}

// DiagnoseRun builds deterministic failure facts from checkpoint state. AI
// advisory triage is opt-in so diagnose can be used without sending data to a
// provider.
func (o *Orchestrator) DiagnoseRun(ctx context.Context, runID string, useAI bool) (*aicopilot.TriageReview, error) {
	run, err := o.triageRun(runID)
	if err != nil {
		return nil, err
	}
	payload := aicopilot.BuildMigrationFailureTriagePayload(o.config, o.migrationFailureFacts(run))
	if !useAI {
		return aicopilot.UnavailableTriageReview("AI triage not requested; pass --ai-triage to enable provider review", payload), nil
	}
	return o.reviewTriagePayloadWithAI(ctx, payload), nil
}

// ReviewValidationWithAI adds advisory AI triage to a deterministic validation
// failure. Successful validation returns nil because there is nothing to triage.
func (o *Orchestrator) ReviewValidationWithAI(ctx context.Context, result *ValidationRunResult, validationErr error) *aicopilot.TriageReview {
	if validationErr == nil && (result == nil || !result.Failed) {
		return nil
	}
	payload := aicopilot.BuildValidationMismatchTriagePayload(o.config, o.validationMismatchFacts(result, validationErr))
	return o.reviewTriagePayloadWithAI(ctx, payload)
}

func (o *Orchestrator) reviewTriagePayloadWithAI(ctx context.Context, payload aicopilot.TriagePayload) *aicopilot.TriageReview {
	client := o.aiReviewClient()
	if aicopilot.IsNilTextClient(client) {
		return aicopilot.UnavailableTriageReview("no AI provider configured in secrets", payload)
	}
	review, err := aicopilot.GenerateTriageReview(ctx, client, payload)
	if err != nil {
		logging.WarnEvent("AI triage failed",
			"provider", client.ProviderName(),
			"model", client.Model(),
			"error", logging.Scrub(err.Error()),
		)
		return aicopilot.ErrorTriageReview(client.ProviderName(), client.Model(), err, payload)
	}
	logging.InfoEvent("AI triage completed",
		"provider", review.Provider,
		"model", review.Model,
		"impact", review.Impact,
		"kind", review.Kind,
		"advisory_findings", len(review.Findings),
	)
	return review
}

func (o *Orchestrator) triageRun(runID string) (*checkpoint.Run, error) {
	if o == nil || o.state == nil {
		return nil, fmt.Errorf("checkpoint state is unavailable")
	}
	if strings.TrimSpace(runID) != "" {
		run, err := o.state.GetRunByID(strings.TrimSpace(runID))
		if err != nil {
			return nil, err
		}
		if run == nil {
			return nil, fmt.Errorf("run %s not found", runID)
		}
		if !isDiagnosableRun(*run) {
			return nil, fmt.Errorf("run %s is %s and has no failure to diagnose", runID, firstNonEmpty(run.Status, "not failed"))
		}
		return run, nil
	}
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run != nil {
		return run, nil
	}
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("no runs found")
	}
	for i := range runs {
		if isDiagnosableRun(runs[i]) {
			return &runs[i], nil
		}
	}
	return nil, fmt.Errorf("no failed or incomplete runs found")
}

func isDiagnosableRun(run checkpoint.Run) bool {
	if strings.TrimSpace(run.Error) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(run.Status)) {
	case "failed", "partial", "running", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func (o *Orchestrator) migrationFailureFacts(run *checkpoint.Run) aicopilot.MigrationFailureFacts {
	facts := aicopilot.MigrationFailureFacts{
		RunID:     run.ID,
		Phase:     run.Phase,
		Error:     run.Error,
		RunStatus: runStatusFact(run),
		Retryable: isRetryableError(fmt.Errorf("%s", run.Error)),
	}
	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		facts.ElapsedSeconds = run.CompletedAt.Sub(run.StartedAt).Seconds()
	} else if !run.StartedAt.IsZero() {
		facts.ElapsedSeconds = time.Since(run.StartedAt).Seconds()
	}

	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		logging.DebugEvent("AI triage task facts unavailable", "run_id", run.ID, "error", logging.Scrub(err.Error()))
	} else {
		facts.Checkpoint = checkpointFact(run, tasks)
		if strings.TrimSpace(facts.Phase) == "" && facts.Checkpoint != nil {
			facts.Phase = facts.Checkpoint.Phase
			if facts.RunStatus != nil {
				facts.RunStatus.Phase = facts.Phase
			}
		}
		for _, task := range tasks {
			if strings.HasPrefix(task.TaskKey, "transfer:") {
				facts.RowsCopied += task.RowsDone
			}
			if task.Status != "failed" && strings.TrimSpace(task.ErrorMessage) == "" {
				continue
			}
			table := transferTableName(task.TaskKey)
			if table != "" {
				facts.FailedTables = append(facts.FailedTables, aicopilot.FailedTableFact{
					Table: table,
					Error: task.ErrorMessage,
				})
				if facts.Table == "" {
					facts.Table = table
				}
			}
			facts.TaskFailures = append(facts.TaskFailures, aicopilot.TaskFailureFact{
				TaskKey:    task.TaskKey,
				TaskType:   task.TaskType,
				Status:     task.Status,
				Error:      task.ErrorMessage,
				RowsDone:   task.RowsDone,
				RowsTotal:  task.RowsTotal,
				RetryCount: task.RetryCount,
			})
			if facts.Error == "" {
				facts.Error = task.ErrorMessage
			}
		}
		if strings.TrimSpace(facts.Phase) == "" {
			facts.Phase = inferPhaseFromTasks(tasks)
			if facts.RunStatus != nil {
				facts.RunStatus.Phase = facts.Phase
			}
		}
	}
	facts.RecentCheckpoints = o.recentCheckpointFacts(3, run.ID)
	facts.DeterministicDiagnoses = o.deterministicDiagnoses(facts.Error)
	facts.SchemaDrift = schemaDriftFacts(o.schemaContractDecisionOutputForRun(run.ID))
	return facts
}

func (o *Orchestrator) validationMismatchFacts(result *ValidationRunResult, validationErr error) aicopilot.ValidationMismatchFacts {
	facts := aicopilot.ValidationMismatchFacts{}
	if result != nil {
		facts.Mode = result.Mode
		facts.Differences = append(facts.Differences, rowCountDifferences(o.config.Migration.TargetMode, result.Rows)...)
		facts.Passes = append(facts.Passes, validationPassFacts(result.Deep)...)
		facts.Differences = append(facts.Differences, deepValidationDifferences(result.Deep)...)
		if len(result.Rows) > 0 {
			for _, row := range result.Rows {
				if !row.Failed {
					continue
				}
				facts.Table = row.TableName
				facts.SourceCount = row.SourceCount
				facts.TargetCount = row.TargetCount
				facts.Difference = row.Difference
				facts.HasRowCountFacts = row.CountsKnown
				facts.UsedEstimate = row.UsedEstimate
				facts.ExactTimedOut = row.ExactTimedOut
				facts.TimedOut = row.TimedOut
				facts.Error = row.Error
				break
			}
		}
		if facts.Error == "" && len(facts.Differences) == 0 && len(facts.Passes) == 0 {
			facts.Error = result.Error
		}
	}
	if validationErr != nil && facts.Error == "" && len(facts.Differences) == 0 && len(facts.Passes) == 0 {
		facts.Error = validationErr.Error()
	}
	runID := ""
	if checkpoint := o.currentCheckpointFact(); checkpoint != nil {
		facts.Checkpoint = checkpoint
		runID = checkpoint.RunID
	}
	facts.RecentCheckpoints = o.recentCheckpointFacts(3, runID)
	if facts.Table == "" && len(facts.Differences) > 0 {
		facts.Table = facts.Differences[0].Table
	}
	facts.SchemaDrift = schemaDriftFacts(o.schemaContractDecisionOutputForRun(runID))
	return facts
}

func runStatusFact(run *checkpoint.Run) *aicopilot.RunStatusFact {
	if run == nil {
		return nil
	}
	fact := &aicopilot.RunStatusFact{
		RunID:         run.ID,
		Status:        run.Status,
		Phase:         run.Phase,
		StartedAt:     timeString(run.StartedAt),
		LastHeartbeat: timeString(run.LastHeartbeat),
		Error:         run.Error,
	}
	if run.CompletedAt != nil {
		fact.CompletedAt = timeString(*run.CompletedAt)
	}
	return fact
}

func checkpointFact(run *checkpoint.Run, tasks []checkpoint.TaskWithProgress) *aicopilot.CheckpointFact {
	if run == nil {
		return nil
	}
	fact := &aicopilot.CheckpointFact{
		RunID:         run.ID,
		Phase:         firstNonEmpty(run.Phase, inferPhaseFromTasks(tasks)),
		LastHeartbeat: timeString(run.LastHeartbeat),
		TasksTotal:    len(tasks),
	}
	for _, task := range tasks {
		fact.RowsDone += task.RowsDone
		fact.RowsTotal += task.RowsTotal
		switch task.Status {
		case "pending":
			fact.TasksPending++
		case "running":
			fact.TasksRunning++
		case "success":
			fact.TasksSucceeded++
		case "failed":
			fact.TasksFailed++
		}
	}
	return fact
}

func inferPhaseFromTasks(tasks []checkpoint.TaskWithProgress) string {
	for _, task := range tasks {
		if task.Status != "failed" && task.Status != "running" {
			continue
		}
		switch {
		case strings.HasPrefix(task.TaskKey, "transfer:") || task.TaskType == string(TaskTransfer):
			return "transferring"
		case task.TaskType == string(TaskValidate):
			return "validating"
		case task.TaskType == string(TaskCreateTables),
			task.TaskType == string(TaskCreatePKs),
			task.TaskType == string(TaskCreateIndexes),
			task.TaskType == string(TaskCreateFKs),
			task.TaskType == string(TaskCreateChecks):
			return "creating_schema"
		case task.TaskType == string(TaskResetSequences):
			return "finalizing"
		}
	}
	return ""
}

func (o *Orchestrator) currentCheckpointFact() *aicopilot.CheckpointFact {
	if o == nil || o.state == nil {
		return nil
	}
	run, err := o.state.GetLastIncompleteRun()
	if err != nil || run == nil {
		return nil
	}
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil
	}
	return checkpointFact(run, tasks)
}

func (o *Orchestrator) recentCheckpointFacts(limit int, excludeRunID string) []aicopilot.CheckpointFact {
	if o == nil || o.state == nil || limit <= 0 {
		return nil
	}
	runs, err := o.state.GetAllRuns()
	if err != nil {
		return nil
	}
	out := make([]aicopilot.CheckpointFact, 0, limit)
	for _, run := range runs {
		if run.ID == excludeRunID {
			continue
		}
		tasks, err := o.state.GetTasksWithProgress(run.ID)
		if err != nil {
			continue
		}
		fact := checkpointFact(&run, tasks)
		if fact == nil {
			continue
		}
		out = append(out, *fact)
		if len(out) == limit {
			break
		}
	}
	return out
}

func schemaDriftFacts(decisions []SchemaContractDecision) []aicopilot.SchemaDriftFact {
	out := make([]aicopilot.SchemaDriftFact, 0, len(decisions))
	for _, d := range decisions {
		out = append(out, aicopilot.SchemaDriftFact{
			Entity: d.Entity,
			Table:  d.Table,
			Object: d.Object,
			Drift:  d.Drift,
			Action: d.Action,
			Reason: d.Reason,
		})
	}
	return out
}

func (o *Orchestrator) deterministicDiagnoses(errText string) []aicopilot.DeterministicDiagnosisFact {
	if strings.TrimSpace(errText) == "" || o == nil || o.config == nil {
		return nil
	}
	drivers := []string{o.config.Target.Type, o.config.Source.Type}
	out := make([]aicopilot.DeterministicDiagnosisFact, 0, 2)
	seen := map[string]bool{}
	for _, driverName := range drivers {
		match, ok := errordiag.Lookup(driverName, errText)
		if !ok {
			continue
		}
		key := driverName + ":" + match.PatternName
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, aicopilot.DeterministicDiagnosisFact{
			Category:    match.Diagnosis.Category,
			Cause:       match.Diagnosis.Cause,
			Confidence:  match.Diagnosis.Confidence,
			PatternName: match.PatternName,
			Suggestions: match.Diagnosis.Suggestions,
		})
	}
	return out
}

func rowCountDifferences(targetMode string, rows []ValidationRowCountResult) []aicopilot.ValidationDifferenceFact {
	var out []aicopilot.ValidationDifferenceFact
	for _, row := range rows {
		if !row.Failed {
			continue
		}
		detail := fmt.Sprintf("source_count=%d target_count=%d difference=%d", row.SourceCount, row.TargetCount, row.Difference)
		if !row.CountsKnown {
			detail = "row count unavailable"
			switch {
			case row.Error != "":
				detail = row.Error
			case row.TimedOut:
				detail = "exact and estimated row counts timed out or were unavailable"
			case row.ExactTimedOut:
				detail = "exact row count timed out"
			}
		}
		if row.Error != "" {
			detail = row.Error
		}
		out = append(out, aicopilot.ValidationDifferenceFact{
			Category: categorizeRowCountDifference(targetMode, row),
			Table:    row.TableName,
			Pass:     "row_count",
			Severity: "error",
			Detail:   detail,
		})
	}
	return out
}

func validationPassFacts(result validation.Result) []aicopilot.ValidationPassFact {
	var out []aicopilot.ValidationPassFact
	for _, table := range result.Tables {
		for _, pass := range table.Passes {
			detail := deepValidationDetailForTriage(pass.Pass, pass.Detail)
			out = append(out, aicopilot.ValidationPassFact{
				Name:   pass.Pass,
				Result: pass.Status,
				Detail: table.TableName + ": " + detail,
			})
		}
	}
	return out
}

func deepValidationDifferences(result validation.Result) []aicopilot.ValidationDifferenceFact {
	var out []aicopilot.ValidationDifferenceFact
	for _, table := range result.Tables {
		for _, pass := range table.Passes {
			if pass.Status != "fail" {
				continue
			}
			if len(pass.Findings) == 0 {
				out = append(out, aicopilot.ValidationDifferenceFact{
					Category: categorizeDeepValidationDifference(pass.Pass, "", pass.Detail),
					Table:    table.TableName,
					Pass:     pass.Pass,
					Severity: "error",
					Detail:   deepValidationDetailForTriage(pass.Pass, pass.Detail),
				})
				continue
			}
			for _, finding := range pass.Findings {
				passName := firstNonEmpty(finding.Pass, pass.Pass)
				out = append(out, aicopilot.ValidationDifferenceFact{
					Category: categorizeDeepValidationDifference(pass.Pass, finding.Column, finding.Detail),
					Table:    firstNonEmpty(finding.Table, table.TableName),
					Pass:     passName,
					Column:   finding.Column,
					Severity: finding.Severity,
					Detail:   deepValidationDetailForTriage(passName, finding.Detail),
				})
			}
		}
	}
	return out
}

func deepValidationDetailForTriage(pass, detail string) string {
	if strings.EqualFold(strings.TrimSpace(pass), "sample_row") && strings.Contains(strings.ToLower(detail), "digest") {
		return "sample row validation mismatch; row digest values omitted"
	}
	return detail
}

func categorizeRowCountDifference(targetMode string, row ValidationRowCountResult) string {
	if row.TimedOut || row.ExactTimedOut || row.Error != "" {
		return aicopilot.ValidationCategoryValidationRuntime
	}
	if row.TargetCount > row.SourceCount {
		if strings.EqualFold(strings.TrimSpace(targetMode), "upsert") {
			return aicopilot.ValidationCategoryDeleteDrift
		}
		return aicopilot.ValidationCategoryTargetTriggerDefaultBehavior
	}
	if row.SourceCount > row.TargetCount {
		return aicopilot.ValidationCategoryWatermarkIssue
	}
	return aicopilot.ValidationCategoryRowCountMismatch
}

func categorizeDeepValidationDifference(pass, column, detail string) string {
	lower := strings.ToLower(pass + " " + column + " " + detail)
	if strings.Contains(lower, "date") || strings.Contains(lower, "time") || strings.Contains(lower, "timestamp") {
		return aicopilot.ValidationCategoryTimezoneDateHandling
	}
	if strings.Contains(lower, "null") || strings.EqualFold(strings.TrimSpace(pass), "null_parity") {
		return aicopilot.ValidationCategoryNullMismatch
	}
	if strings.Contains(lower, "trigger") || strings.Contains(lower, "default") {
		return aicopilot.ValidationCategoryTargetTriggerDefaultBehavior
	}
	if strings.Contains(lower, "missing") || strings.Contains(lower, "exists in source but not target") {
		return aicopilot.ValidationCategoryWatermarkIssue
	}
	if strings.Contains(lower, "digest") || strings.Contains(lower, "value") || pass == "sample_row" {
		return aicopilot.ValidationCategoryTypeCoercion
	}
	return aicopilot.ValidationCategorySampleMismatch
}

func transferTableName(taskKey string) string {
	if name := checkpoint.TransferTaskDisplayName(taskKey); name != "" {
		return name
	}
	table := strings.TrimPrefix(taskKey, "transfer:")
	if table == taskKey {
		return ""
	}
	if idx := strings.LastIndex(table, ":p"); idx > 0 {
		table = table[:idx]
	}
	return table
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
