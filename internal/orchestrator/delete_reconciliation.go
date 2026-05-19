package orchestrator

import (
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/source"
)

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

func countTablesWithPrimaryKey(tables []source.Table) int {
	count := 0
	for _, table := range tables {
		if table.HasPK() {
			count++
		}
	}
	return count
}
