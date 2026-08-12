package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/reconcile"
	"github.com/johndauphine/dmt/v5/internal/source"
)

type DeleteReconciliationRunResult struct {
	Preview       *DeleteReconciliationPreview
	TableResults  []DeleteReconciliationTableResult
	CandidateRows int64
	DeletedRows   int64
}

type DeleteReconciliationTableResult struct {
	Table         string
	CandidateRows int64
	DeletedRows   int64
	Skipped       bool
	SkipReason    string
}

func (o *Orchestrator) previewDeleteReconciliation(
	tables []source.Table,
	now time.Time,
) (*DeleteReconciliationPreview, error) {
	if !o.config.Migration.DeletesEnabled() {
		return nil, nil
	}

	preview := &DeleteReconciliationPreview{
		Enabled:        true,
		Interval:       o.config.Migration.DeleteReconcileInterval(),
		EligibleTables: countTablesWithPrimaryKey(tables),
	}
	preview.SkippedNoPKTables = len(tables) - preview.EligibleTables

	if preview.EligibleTables == 0 {
		preview.Reason = "no eligible primary-key tables"
		return preview, nil
	}

	interval, err := time.ParseDuration(preview.Interval)
	if err != nil {
		return nil, fmt.Errorf("parsing delete reconciliation interval: %w", err)
	}

	state, err := o.state.GetDeleteReconciliationState(
		o.config.Source.Schema,
		o.config.Target.Schema,
	)
	if err != nil {
		return nil, fmt.Errorf("loading delete reconciliation state: %w", err)
	}
	if state == nil {
		preview.Due = true
		preview.Reason = "no previous successful reconciliation"
		return preview, nil
	}

	lastSuccessAt := state.LastSuccessAt
	nextDueAt := lastSuccessAt.Add(interval)
	preview.LastSuccessAt = &lastSuccessAt
	preview.NextDueAt = &nextDueAt
	if now.Before(nextDueAt) {
		preview.Reason = "interval has not elapsed"
		return preview, nil
	}

	preview.Due = true
	preview.Reason = "interval elapsed"
	return preview, nil
}

func (o *Orchestrator) countDeleteReconciliationCandidates(
	ctx context.Context,
	tables []source.Table,
	preview *DeleteReconciliationPreview,
) error {
	if preview == nil || !preview.Due {
		return nil
	}

	sourceDialect := driver.GetDialect(o.sourcePool.DBType())
	if sourceDialect == nil {
		return fmt.Errorf("no dialect registered for source DB type %s", o.sourcePool.DBType())
	}
	targetDialect := driver.GetDialect(o.targetPool.DBType())
	if targetDialect == nil {
		return fmt.Errorf("no dialect registered for target DB type %s", o.targetPool.DBType())
	}

	var total int64
	batchSize := o.config.Migration.DeleteReconcileBatchSize()
	preview.Tables = preview.Tables[:0]
	for _, table := range tables {
		tablePreview := DeleteReconciliationTablePreview{Table: table.FullName()}
		if !table.HasPK() {
			tablePreview.Skipped = true
			tablePreview.SkipReason = "no primary key"
			preview.Tables = append(preview.Tables, tablePreview)
			continue
		}

		missing, err := reconcile.FindTargetOnlyKeys(
			ctx,
			o.sourcePool.DB(),
			o.targetPool.DB(),
			sourceDialect,
			targetDialect,
			reconcile.KeyDiffOptions{
				SourceSchema: table.Schema,
				TargetSchema: o.config.Target.Schema,
				Table:        table.Name,
				KeyColumns:   append([]string(nil), table.PrimaryKey...),
				BatchSize:    batchSize,
			},
			func(_ [][]any) error {
				return nil
			},
		)
		if err != nil {
			tablePreview.Error = err.Error()
			logging.Warn("Dry-run delete reconciliation candidate count failed for %s: %v",
				table.FullName(), err)
			preview.Tables = append(preview.Tables, tablePreview)
			continue
		}

		tablePreview.CandidateRows = missing
		total += missing
		preview.Tables = append(preview.Tables, tablePreview)
	}
	preview.CandidateRows = &total
	return nil
}

func (o *Orchestrator) reconcileDeletesIfDue(
	ctx context.Context,
	runID string,
	tables []source.Table,
) error {
	o.deleteReconciliationStrictValidation = false
	if !o.config.Migration.DeletesEnabled() {
		return nil
	}

	o.setPhase("delete_reconciliation")
	if err := o.state.UpdatePhase(runID, "delete_reconciliation"); err != nil {
		logging.Debug("failed to update delete reconciliation phase: %v", err)
	}
	if _, err := o.runDeleteReconciliation(ctx, runID, tables); err != nil {
		return err
	}

	strict, err := o.deleteReconciliationSucceededForRun(runID)
	if err != nil {
		return err
	}
	o.deleteReconciliationStrictValidation = strict
	return nil
}

func (o *Orchestrator) runDeleteReconciliation(
	ctx context.Context,
	runID string,
	tables []source.Table,
) (*DeleteReconciliationRunResult, error) {
	preview, err := o.previewDeleteReconciliation(tables, time.Now())
	if err != nil {
		return nil, err
	}
	result := &DeleteReconciliationRunResult{Preview: preview}
	if preview == nil {
		return result, nil
	}
	if !preview.Due {
		if preview.EligibleTables == 0 && preview.SkippedNoPKTables > 0 {
			if err := o.saveSkippedNoPKDeleteTables(runID, tables, result); err != nil {
				return result, err
			}
		}
		logging.Info("Delete reconciliation not due: %s", preview.Reason)
		return result, nil
	}

	sourceDialect := driver.GetDialect(o.sourcePool.DBType())
	if sourceDialect == nil {
		return result, fmt.Errorf("no dialect registered for source DB type %s", o.sourcePool.DBType())
	}
	targetDialect := driver.GetDialect(o.targetPool.DBType())
	if targetDialect == nil {
		return result, fmt.Errorf("no dialect registered for target DB type %s", o.targetPool.DBType())
	}

	logging.Info("Running delete reconciliation for %d eligible table(s)", preview.EligibleTables)
	batchSize := o.config.Migration.DeleteReconcileBatchSize()
	for _, table := range tables {
		if !table.HasPK() {
			tableResult := skippedNoPKDeleteReconciliationResult(table)
			result.TableResults = append(result.TableResults, tableResult)
			if err := o.saveDeleteReconciliationTable(runID, tableResult); err != nil {
				return result, err
			}
			logging.Warn("Delete reconciliation skipped %s: no primary key", table.FullName())
			continue
		}

		tableResult := DeleteReconciliationTableResult{Table: table.FullName()}
		keyColumns := append([]string(nil), table.PrimaryKey...)
		var targetOnlyKeys [][]any
		missing, err := reconcile.FindTargetOnlyKeys(
			ctx,
			o.sourcePool.DB(),
			o.targetPool.DB(),
			sourceDialect,
			targetDialect,
			reconcile.KeyDiffOptions{
				SourceSchema: table.Schema,
				TargetSchema: o.config.Target.Schema,
				Table:        table.Name,
				KeyColumns:   keyColumns,
				BatchSize:    batchSize,
			},
			func(keys [][]any) error {
				targetOnlyKeys = append(targetOnlyKeys, cloneKeyBatch(keys)...)
				return nil
			},
		)
		if err != nil {
			return result, fmt.Errorf("reconciling deletes for %s: %w", table.FullName(), err)
		}
		targetCount, err := o.targetPool.GetRowCountExact(
			ctx,
			o.config.Target.Schema,
			table.Name,
			o.config.Migration.StrictConsistency,
		)
		if err != nil {
			return result, fmt.Errorf("counting target keys for delete reconciliation guard on %s: %w", table.FullName(), err)
		}
		if shouldAbortDeleteReconciliation(missing, targetCount) {
			return result, fmt.Errorf(
				"delete reconciliation for %s would delete %d of %d target row(s); aborting because this likely indicates key fingerprint mismatch",
				table.FullName(), missing, targetCount,
			)
		}
		deleted, err := reconcile.DeleteKeys(
			ctx,
			o.targetPool.DB(),
			targetDialect,
			o.config.Target.Schema,
			table.Name,
			keyColumns,
			targetOnlyKeys,
			batchSize,
		)
		if err != nil {
			return result, fmt.Errorf("deleting reconciled keys for %s: %w", table.FullName(), err)
		}

		tableResult.CandidateRows = missing
		tableResult.DeletedRows = deleted
		result.CandidateRows += tableResult.CandidateRows
		result.DeletedRows += tableResult.DeletedRows
		result.TableResults = append(result.TableResults, tableResult)
		if err := o.saveDeleteReconciliationTable(runID, tableResult); err != nil {
			return result, err
		}
		logging.Info("Delete reconciliation %s: %d candidate(s), %d deleted",
			table.FullName(), tableResult.CandidateRows, tableResult.DeletedRows)
	}

	completedAt := time.Now()
	if err := o.state.RecordDeleteReconciliationSuccess(
		runID,
		o.config.Source.Schema,
		o.config.Target.Schema,
		completedAt,
	); err != nil {
		return result, fmt.Errorf("recording delete reconciliation success: %w", err)
	}
	logging.Info("Delete reconciliation complete: %d candidate(s), %d deleted",
		result.CandidateRows, result.DeletedRows)
	return result, nil
}

func shouldAbortDeleteReconciliation(candidateRows, targetRows int64) bool {
	if candidateRows <= 0 || targetRows <= 0 {
		return false
	}
	return float64(candidateRows)/float64(targetRows) >= 0.95
}

func (o *Orchestrator) saveSkippedNoPKDeleteTables(
	runID string,
	tables []source.Table,
	result *DeleteReconciliationRunResult,
) error {
	for _, table := range tables {
		if table.HasPK() {
			continue
		}
		tableResult := skippedNoPKDeleteReconciliationResult(table)
		result.TableResults = append(result.TableResults, tableResult)
		if err := o.saveDeleteReconciliationTable(runID, tableResult); err != nil {
			return err
		}
		logging.Warn("Delete reconciliation skipped %s: no primary key", table.FullName())
	}
	return nil
}

func skippedNoPKDeleteReconciliationResult(table source.Table) DeleteReconciliationTableResult {
	return DeleteReconciliationTableResult{
		Table:      table.FullName(),
		Skipped:    true,
		SkipReason: "no primary key",
	}
}

func (o *Orchestrator) deleteReconciliationSucceededForRun(runID string) (bool, error) {
	state, err := o.state.GetDeleteReconciliationState(
		o.config.Source.Schema,
		o.config.Target.Schema,
	)
	if err != nil {
		return false, fmt.Errorf("loading delete reconciliation state: %w", err)
	}
	if state == nil || state.LastRunID != runID {
		return false, nil
	}

	records, err := o.state.GetDeleteReconciliationTables(runID)
	if err != nil {
		return false, fmt.Errorf("loading delete reconciliation tables: %w", err)
	}
	for _, record := range records {
		if !record.Skipped {
			return true, nil
		}
	}
	return false, nil
}

func (o *Orchestrator) saveDeleteReconciliationTable(
	runID string,
	result DeleteReconciliationTableResult,
) error {
	err := o.state.SaveDeleteReconciliationTable(runID, checkpoint.DeleteReconciliationTableRecord{
		TableName:     result.Table,
		CandidateRows: result.CandidateRows,
		DeletedRows:   result.DeletedRows,
		Skipped:       result.Skipped,
		SkipReason:    result.SkipReason,
	})
	if err != nil {
		return fmt.Errorf("saving delete reconciliation result for %s: %w", result.Table, err)
	}
	return nil
}

func cloneKeyBatch(keys [][]any) [][]any {
	out := make([][]any, len(keys))
	for i, key := range keys {
		out[i] = append([]any(nil), key...)
	}
	return out
}

func countTablesWithPrimaryKey(tables []source.Table) int {
	count := 0
	for _, table := range tables {
		if table.HasPK() {
			count++
		}
	}
	return count
}
