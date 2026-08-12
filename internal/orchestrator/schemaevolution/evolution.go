package schemaevolution

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/drift"
	"github.com/johndauphine/dmt/v5/internal/exitcodes"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/source"
)

// SchemaEvolutionError is returned when configured schema evolution cannot
// safely apply a detected schema drift change.
type SchemaEvolutionError struct {
	Message string
}

func (e *SchemaEvolutionError) Error() string { return e.Message }

func (e *SchemaEvolutionError) ExitCode() int { return exitcodes.TransferError }

type addedColumnEvolution struct {
	Table          source.Table
	Column         source.Column
	SourceNullable bool
}

type nullabilityEvolution struct {
	Table  source.Table
	Column source.Column
}

type typeEvolution struct {
	Table  source.Table
	Column source.Column
	Change drift.Change
}

func (e *Engine) ShouldApplyEvolution(report drift.Report) bool {
	if !report.HasChanges() ||
		!e.cfg.Migration.SchemaEvolutionEnabled() ||
		e.cfg.Migration.TargetMode != "upsert" {
		return false
	}

	addedColumnPolicy := e.cfg.Migration.AddedColumnSchemaEvolutionPolicy()
	nullabilityPolicy := e.cfg.Migration.NullabilityChangeSchemaEvolutionPolicy()
	typePolicy := e.cfg.Migration.TypeChangeSchemaEvolutionPolicy()
	return (schemaEvolutionPolicyRequiresTargetStep(addedColumnPolicy) && len(addedColumnChanges(report)) > 0) ||
		(schemaEvolutionPolicyRequiresTargetStep(nullabilityPolicy) && len(nullabilityChanges(report)) > 0) ||
		(schemaEvolutionPolicyRequiresTargetStep(typePolicy) && len(typeChanges(report)) > 0)
}

func enforceSchemaContractDecisions(decisions []SchemaContractDecision) error {
	var violations []string
	for _, decision := range decisions {
		if decision.Action == SchemaContractActionFrozen || decision.Action == SchemaContractActionBlocked {
			violations = append(violations, formatSchemaContractViolation(decision))
		}
	}
	if len(violations) == 0 {
		return nil
	}
	return &SchemaEvolutionError{Message: "schema contract violation: " + strings.Join(violations, "; ") +
		"; choose report to observe only, evolve for deterministic safe changes, or discard_row/discard_value where supported"}
}

func (e *Engine) ApplyEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	if !report.HasChanges() || !e.cfg.Migration.SchemaEvolutionEnabled() {
		return nil
	}
	if e.cfg.Migration.TargetMode != "upsert" {
		logging.Debug("schema evolution configured but target_mode=%s recreates tables; skipping target ALTERs",
			e.cfg.Migration.TargetMode)
		return nil
	}

	addedActions, addedLogOnly, err := planAddedColumnEvolution(
		report,
		tables,
		e.cfg.Migration.AddedColumnSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	nullabilityActions, nullabilityLogOnly, err := planNullabilityEvolution(
		report,
		tables,
		e.cfg.Migration.NullabilityChangeSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	typeActions, typeLogOnly, err := planTypeEvolution(
		report,
		tables,
		e.cfg.Migration.TypeChangeSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	label := e.schemaChangeLogLabel()
	if len(addedLogOnly) > 0 {
		policy := e.cfg.Migration.AddedColumnSchemaEvolutionPolicy()
		if e.cfg.Migration.SchemaContractEnabled() &&
			e.cfg.Migration.SchemaContractColumnsMode() == config.SchemaContractDiscardRow {
			logging.Warn("%s: %d added column(s) detected; columns=discard_row skips affected table(s) and leaves target columns unchanged",
				label, len(addedLogOnly))
		} else if policy == config.SchemaEvolutionDiscardValue {
			logging.Warn("%s: %d added column(s) detected; policy added_column=discard_value leaves target unchanged and omits values from transfer",
				label, len(addedLogOnly))
		} else {
			logging.Warn("%s: %d added column(s) detected; policy added_column=log leaves target unchanged",
				label, len(addedLogOnly))
		}
	}
	if len(nullabilityLogOnly) > 0 {
		logging.Warn("%s: %d nullability change(s) detected; policy nullability_change=log leaves target unchanged",
			label, len(nullabilityLogOnly))
	}
	if len(typeLogOnly) > 0 {
		logging.Warn("%s: %d type change(s) detected; policy type_change=log leaves target unchanged",
			label, len(typeLogOnly))
	}
	if len(addedActions) == 0 && len(nullabilityActions) == 0 && len(typeActions) == 0 {
		return nil
	}

	if len(addedActions) > 0 {
		logging.Info("%s: adding %d nullable column(s) to target", label, len(addedActions))
	}
	for _, action := range addedActions {
		exists, err := e.targetPool.TableExists(ctx, e.cfg.Target.Schema, action.Table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", action.Table.Name, err)}
		}
		if !exists {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema evolution cannot add column %s.%s: target table does not exist",
				action.Table.Name, action.Column.Name,
			)}
		}
		if !action.SourceNullable {
			logging.Warn("%s: source column %s.%s is NOT NULL; adding target column as NULL to preserve existing rows",
				label, action.Table.Name, action.Column.Name)
		}
		if err := e.targetPool.AddColumn(ctx, &action.Table, &action.Column, e.cfg.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"adding target column %s.%s: %v",
				action.Table.Name, action.Column.Name, err,
			)}
		}
	}

	if len(nullabilityActions) > 0 {
		logging.Info("%s: relaxing %d target column(s) from NOT NULL to NULL", label, len(nullabilityActions))
	}
	for _, action := range nullabilityActions {
		exists, err := e.targetPool.TableExists(ctx, e.cfg.Target.Schema, action.Table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", action.Table.Name, err)}
		}
		if !exists {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema evolution cannot relax nullability for %s.%s: target table does not exist",
				action.Table.Name, action.Column.Name,
			)}
		}
		if err := e.targetPool.DropColumnNotNull(ctx, &action.Table, &action.Column, e.cfg.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"relaxing target column nullability %s.%s: %v",
				action.Table.Name, action.Column.Name, err,
			)}
		}
	}

	if len(typeActions) > 0 {
		logging.Info("%s: widening %d target column type(s)", label, len(typeActions))
	}
	for _, action := range typeActions {
		exists, err := e.targetPool.TableExists(ctx, e.cfg.Target.Schema, action.Table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", action.Table.Name, err)}
		}
		if !exists {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema evolution cannot alter type for %s.%s: target table does not exist",
				action.Table.Name, action.Column.Name,
			)}
		}
		if err := e.targetPool.AlterColumnType(ctx, &action.Table, &action.Column, e.cfg.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"altering target column type %s.%s (%s -> %s): %v",
				action.Table.Name, action.Column.Name, action.Change.Previous, action.Change.Current, err,
			)}
		}
	}

	e.auditEvent(e.schemaChangeAuditName("schema_evolution_applied"), map[string]any{
		"added_columns":           len(addedActions),
		"nullability_relaxations": len(nullabilityActions),
		"type_widenings":          len(typeActions),
	})
	return nil
}

func (e *Engine) ApplyContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	if !report.HasChanges() ||
		!e.cfg.Migration.SchemaContractEnabled() ||
		e.cfg.Migration.SchemaContractTablesMode() != config.SchemaContractEvolve ||
		e.cfg.Migration.TargetMode != "upsert" {
		return nil
	}

	addedTables := tableAddedChanges(report)
	if len(addedTables) == 0 {
		return nil
	}

	label := e.schemaChangeLogLabel()
	logging.Info("%s: creating %d added target table(s) before upsert transfer", label, len(addedTables))
	created := 0
	for _, change := range addedTables {
		table, err := findSourceTable(tables, change)
		if err != nil {
			return &SchemaEvolutionError{Message: err.Error()}
		}
		if !table.HasPK() {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema contract cannot evolve added table %s: upsert mode requires a primary key",
				table.FullName(),
			)}
		}

		exists, err := e.targetPool.TableExists(ctx, e.cfg.Target.Schema, table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", table.Name, err)}
		}
		if exists {
			logging.Debug("%s: target table %s already exists; skipping create", label, table.Name)
			continue
		}

		if err := e.targetPool.CreateTableWithOptions(ctx, &table, e.cfg.Target.Schema, pool.TableOptions{}); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("creating target table %s: %v", table.FullName(), err)}
		}
		if err := e.targetPool.CreatePrimaryKey(ctx, &table, e.cfg.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("creating primary key for target table %s: %v", table.FullName(), err)}
		}
		created++
	}

	if created > 0 {
		e.auditEvent("schema_contract_tables_applied", map[string]any{
			"added_tables": created,
		})
	}
	return nil
}

func (e *Engine) FinalizeContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) {
	if !report.HasChanges() ||
		!e.cfg.Migration.SchemaContractEnabled() ||
		e.cfg.Migration.SchemaContractTablesMode() != config.SchemaContractEvolve ||
		e.cfg.Migration.TargetMode != "upsert" {
		return
	}

	addedTables := findAddedSourceTables(report, tables)
	if len(addedTables) == 0 {
		return
	}

	label := e.schemaChangeLogLabel()
	logging.Info("%s: finalizing %d evolved target table(s) after transfer", label, len(addedTables))

	resets := 0
	if sr, ok := e.targetPool.(driver.SequenceResetter); ok {
		for _, table := range addedTables {
			t := table
			if err := sr.ResetSequence(ctx, e.cfg.Target.Schema, &t); err != nil {
				logging.Warn("%s: resetting sequence for evolved table %s: %v", label, t.Name, err)
				continue
			}
			resets++
		}
	} else {
		logging.Debug("%s: skipping sequence reset: target engine %s has no sequence support", label, e.targetPool.DBType())
	}

	indexes := 0
	if e.cfg.Migration.CreateIndexesEnabled() {
		for _, table := range addedTables {
			for _, index := range table.Indexes {
				t := table
				idx := index
				if err := e.targetPool.CreateIndex(ctx, &t, &idx, e.cfg.Target.Schema); err != nil {
					logging.Warn("%s: creating index %s on evolved table %s: %v", label, idx.Name, t.Name, err)
					continue
				}
				indexes++
			}
		}
	}

	// Capability check (#460): same degradation contract as the
	// target-mode finalization phases. Warn only when constraint work
	// was actually requested AND exists on the evolved tables — an
	// added table with no FKs/CHECKs skips nothing (codex review).
	cw, hasConstraintWriter := e.targetPool.(driver.ConstraintWriter)
	if !hasConstraintWriter {
		skipped := 0
		if e.cfg.Migration.CreateForeignKeysEnabled() {
			for _, table := range addedTables {
				skipped += len(table.ForeignKeys)
			}
		}
		if e.cfg.Migration.CreateCheckConstraints {
			for _, table := range addedTables {
				skipped += len(table.CheckConstraints)
			}
		}
		if skipped > 0 {
			logging.Warn("%s: skipping %d FK/CHECK constraint(s) on evolved tables: target engine %s does not support post-transfer constraint creation", label, skipped, e.targetPool.DBType())
			// Audited via the engine's sink, NOT observability.RecordFallback —
			// that surface set is reserved for AI fallbacks and would print
			// this as one in status output (codex review).
			e.auditEvent("constraint_capability_skip", map[string]any{
				"target_db_type": e.targetPool.DBType(),
				"skipped":        skipped,
				"context":        "schema_evolution",
			})
		}
	}

	foreignKeys := 0
	if e.cfg.Migration.CreateForeignKeysEnabled() && hasConstraintWriter {
		for _, table := range addedTables {
			for _, fk := range table.ForeignKeys {
				t := table
				foreignKey := fk
				if err := cw.CreateForeignKey(ctx, &t, &foreignKey, e.cfg.Target.Schema); err != nil {
					logging.Warn("%s: creating foreign key %s on evolved table %s: %v", label, foreignKey.Name, t.Name, err)
					continue
				}
				foreignKeys++
			}
		}
	}

	checks := 0
	if e.cfg.Migration.CreateCheckConstraints && hasConstraintWriter {
		for _, table := range addedTables {
			for _, check := range table.CheckConstraints {
				t := table
				chk := check
				if err := cw.CreateCheckConstraint(ctx, &t, &chk, e.cfg.Target.Schema); err != nil {
					logging.Warn("%s: creating check constraint %s on evolved table %s: %v", label, chk.Name, t.Name, err)
					continue
				}
				checks++
			}
		}
	}

	e.auditEvent("schema_contract_tables_finalized", map[string]any{
		"tables":          len(addedTables),
		"sequence_resets": resets,
		"indexes":         indexes,
		"foreign_keys":    foreignKeys,
		"checks":          checks,
	})
}

func schemaEvolutionPolicyRequiresTargetStep(policy config.SchemaEvolutionPolicy) bool {
	switch policy {
	case config.SchemaEvolutionAuto, config.SchemaEvolutionFail:
		return true
	default:
		return false
	}
}

func (e *Engine) schemaChangeAuditName(base string) string {
	if e.cfg.Migration.SchemaContractEnabled() {
		return strings.Replace(base, "schema_evolution", "schema_contract", 1)
	}
	return base
}

func (e *Engine) schemaChangeLogLabel() string {
	if e.cfg.Migration.SchemaContractEnabled() {
		return "schema contract"
	}
	return "schema evolution"
}

func (e *Engine) EffectiveTables(report drift.Report, tables []source.Table) ([]source.Table, error) {
	if !report.HasChanges() ||
		!e.cfg.Migration.SchemaEvolutionEnabled() {
		return tables, nil
	}

	pruned := tables
	if e.cfg.Migration.SchemaContractEnabled() &&
		e.cfg.Migration.SchemaContractTablesMode() == config.SchemaContractDiscardRow {
		var discardedTables int
		pruned, discardedTables = pruneDiscardedAddedTables(report, pruned)
		if discardedTables > 0 {
			logging.Warn("%s: skipping %d added table(s) from transfer, validation, and schema snapshots because tables=discard_row",
				e.schemaChangeLogLabel(), discardedTables)
			e.auditEvent(e.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"added_tables": discardedTables,
			})
		}
	}

	if e.cfg.Migration.SchemaContractEnabled() &&
		e.cfg.Migration.SchemaContractColumnsMode() == config.SchemaContractDiscardRow {
		var skippedTables, skippedColumns int
		pruned, skippedTables, skippedColumns = pruneTablesWithAddedColumns(report, pruned)
		if skippedTables > 0 {
			logging.Warn("%s: skipping %d table(s) with %d added column(s) from transfer, validation, and schema snapshots because columns=discard_row",
				e.schemaChangeLogLabel(), skippedTables, skippedColumns)
			e.auditEvent(e.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"added_columns":  skippedColumns,
				"skipped_tables": skippedTables,
			})
		}
	}

	if e.cfg.Migration.SchemaContractEnabled() &&
		e.cfg.Migration.SchemaContractDataTypeMode() == config.SchemaContractDiscardRow {
		var skippedTables, skippedColumns int
		pruned, skippedTables, skippedColumns = pruneTablesWithDataTypeChanges(report, pruned)
		if skippedTables > 0 {
			logging.Warn("%s: skipping %d table(s) with %d data type/nullability-changed column(s) from transfer, validation, and schema snapshots because data_type=discard_row",
				e.schemaChangeLogLabel(), skippedTables, skippedColumns)
			e.auditEvent(e.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"data_type_columns": skippedColumns,
				"skipped_tables":    skippedTables,
			})
		}
	}

	if e.cfg.Migration.SchemaContractEnabled() &&
		e.cfg.Migration.SchemaContractDataTypeMode() == config.SchemaContractDiscardValue {
		var discarded int
		var err error
		pruned, discarded, err = pruneDiscardedDataTypeColumns(report, pruned, e.cfg.Migration.DateUpdatedColumns)
		if err != nil {
			return nil, &SchemaEvolutionError{Message: err.Error()}
		}
		if discarded > 0 {
			logging.Warn("%s: discarding %d data type/nullability-changed column(s) from transfer and validation because data_type=discard_value; previous schema snapshot metadata will be retained",
				e.schemaChangeLogLabel(), discarded)
			e.auditEvent(e.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"data_type_columns": discarded,
			})
		}
	}

	if e.cfg.Migration.AddedColumnSchemaEvolutionPolicy() != config.SchemaEvolutionDiscardValue {
		return pruned, nil
	}

	pruned, discarded, err := pruneDiscardedAddedColumns(report, pruned, e.cfg.Migration.DateUpdatedColumns)
	if err != nil {
		return nil, &SchemaEvolutionError{Message: err.Error()}
	}
	if discarded == 0 {
		return pruned, nil
	}

	logging.Warn("%s: discarding %d added column(s) from target DDL, transfer, validation, and snapshots because added_column=discard_value",
		e.schemaChangeLogLabel(), discarded)
	e.auditEvent(e.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
		"added_columns": discarded,
	})
	return pruned, nil
}
