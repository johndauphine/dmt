package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/reconcile"
	"github.com/johndauphine/dmt/internal/source"
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

func (o *Orchestrator) reconcileDeletesIfDue(
	ctx context.Context,
	runID string,
	tables []source.Table,
) error {
	if !o.config.Migration.DeletesEnabled() {
		return nil
	}

	o.setPhase("delete_reconciliation")
	if err := o.state.UpdatePhase(runID, "delete_reconciliation"); err != nil {
		logging.Debug("failed to update delete reconciliation phase: %v", err)
	}
	_, err := o.runDeleteReconciliation(ctx, runID, tables)
	return err
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
		tableResult := DeleteReconciliationTableResult{Table: table.FullName()}
		if !table.HasPK() {
			tableResult.Skipped = true
			tableResult.SkipReason = "no primary key"
			result.TableResults = append(result.TableResults, tableResult)
			logging.Warn("Delete reconciliation skipped %s: no primary key", table.FullName())
			continue
		}

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
