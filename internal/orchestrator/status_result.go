package orchestrator

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
)

// GetLastRunResult builds a MigrationResult from the last run.
func (o *Orchestrator) GetLastRunResult() (*MigrationResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		// Try to get the most recent run from history
		runs, err := o.state.GetAllRuns()
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			return nil, fmt.Errorf("no runs found")
		}
		run = &runs[0] // Most recent
	}
	return o.buildResultFromRun(run)
}

// GetRunResult builds a MigrationResult for a specific run ID.
func (o *Orchestrator) GetRunResult(runID string) (*MigrationResult, error) {
	run, err := o.state.GetRunByID(runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("run %s not found", runID)
	}
	return o.buildResultFromRun(run)
}

func (o *Orchestrator) buildResultFromRun(run *checkpoint.Run) (*MigrationResult, error) {
	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	result := &MigrationResult{
		RunID:      run.ID,
		Status:     run.Status,
		StartedAt:  run.StartedAt,
		TableStats: make([]TableResult, 0),
	}

	if run.CompletedAt != nil && !run.CompletedAt.IsZero() {
		result.CompletedAt = *run.CompletedAt
		result.DurationSeconds = run.CompletedAt.Sub(run.StartedAt).Seconds()
	} else if run.Status == "running" {
		result.DurationSeconds = time.Since(run.StartedAt).Seconds()
	}

	if run.Error != "" {
		result.Error = run.Error
	}

	var totalRows int64
	tableMap := make(map[string]*TableResult)

	for _, t := range tasks {
		if !strings.HasPrefix(t.TaskKey, "transfer:") {
			continue
		}

		// Extract table name from task key (transfer:schema.table or transfer:schema.table:pN)
		tableName := strings.TrimPrefix(t.TaskKey, "transfer:")
		if idx := strings.LastIndex(tableName, ":p"); idx > 0 {
			tableName = tableName[:idx] // Remove partition suffix
		}

		if _, exists := tableMap[tableName]; !exists {
			tableMap[tableName] = &TableResult{
				Name:   tableName,
				Status: "pending",
			}
			result.TablesTotal++
		}

		tr := tableMap[tableName]
		tr.Rows += t.RowsDone
		totalRows += t.RowsDone

		// Update status (success only if all partitions succeed)
		switch t.Status {
		case "success":
			if tr.Status != "failed" {
				tr.Status = "success"
			}
		case "failed":
			tr.Status = "failed"
			tr.Error = t.ErrorMessage
		case "running":
			if tr.Status != "failed" {
				tr.Status = "running"
			}
		}
	}

	// Build table stats list, count successes/failures, and sort for deterministic output
	tableNames := make([]string, 0, len(tableMap))
	for name := range tableMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, name := range tableNames {
		tr := tableMap[name]
		result.TableStats = append(result.TableStats, *tr)
		switch tr.Status {
		case "success":
			result.TablesSuccess++
		case "failed":
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tr.Name)
		}
	}

	result.RowsTransferred = totalRows
	if result.DurationSeconds > 0 {
		result.RowsPerSecond = int64(float64(totalRows) / result.DurationSeconds)
	}

	if result.FailedTables == nil {
		result.FailedTables = []string{}
	}

	deleteRecords, err := o.state.GetDeleteReconciliationTables(run.ID)
	if err != nil {
		return nil, err
	}
	result.DeleteReconciliation = buildDeleteReconciliationSummary(deleteRecords)
	result.SchemaContractDecisions = o.schemaContractDecisionOutputForRun(run.ID)

	return result, nil
}

func buildDeleteReconciliationSummary(
	records []checkpoint.DeleteReconciliationTableRecord,
) *DeleteReconciliationSummary {
	if len(records) == 0 {
		return nil
	}

	summary := &DeleteReconciliationSummary{
		Tables: make([]DeleteReconciliationTableSummary, 0, len(records)),
	}
	for _, record := range records {
		summary.CandidateRows += record.CandidateRows
		summary.DeletedRows += record.DeletedRows
		summary.Tables = append(summary.Tables, DeleteReconciliationTableSummary{
			Table:         record.TableName,
			CandidateRows: record.CandidateRows,
			DeletedRows:   record.DeletedRows,
			Skipped:       record.Skipped,
			SkipReason:    record.SkipReason,
		})
	}
	return summary
}

// GetStatusResult builds a StatusResult for the current/last run.
func (o *Orchestrator) GetStatusResult() (*StatusResult, error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, fmt.Errorf("no active migration")
	}

	// Check if superseded
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return nil, err
	}
	if superseded {
		return nil, fmt.Errorf("no active migration")
	}

	total, pending, running, success, failed, err := o.state.GetRunStats(run.ID)
	if err != nil {
		return nil, err
	}

	tasks, err := o.state.GetTasksWithProgress(run.ID)
	if err != nil {
		return nil, err
	}

	var totalRows, totalRowsDone int64
	for _, t := range tasks {
		if strings.HasPrefix(t.TaskKey, "transfer:") {
			totalRows += t.RowsTotal
			totalRowsDone += t.RowsDone
		}
	}

	phase := run.Phase
	if phase == "" {
		phase = "initializing"
	}

	var progressPct float64
	if totalRows > 0 {
		progressPct = float64(totalRowsDone) / float64(totalRows) * 100
	}

	fallbacks := o.fallbackCountsForRun(run.ID)
	if len(fallbacks) == 0 {
		fallbacks = nil
	}

	return &StatusResult{
		RunID:                   run.ID,
		Status:                  run.Status,
		Phase:                   phase,
		StartedAt:               run.StartedAt,
		TablesTotal:             total,
		TablesComplete:          success,
		TablesRunning:           running,
		TablesPending:           pending,
		TablesFailed:            failed,
		RowsTransferred:         totalRowsDone,
		ProgressPercent:         progressPct,
		AIFallbacks:             fallbacks,
		SchemaContractDecisions: o.schemaContractDecisionOutputForRun(run.ID),
	}, nil
}
