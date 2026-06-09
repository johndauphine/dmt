package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
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

func (o *Orchestrator) shouldApplySchemaEvolution(report drift.Report) bool {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaEvolutionEnabled() ||
		o.config.Migration.TargetMode != "upsert" {
		return false
	}

	addedColumnPolicy := o.config.Migration.AddedColumnSchemaEvolutionPolicy()
	nullabilityPolicy := o.config.Migration.NullabilityChangeSchemaEvolutionPolicy()
	typePolicy := o.config.Migration.TypeChangeSchemaEvolutionPolicy()
	return (schemaEvolutionPolicyRequiresTargetStep(addedColumnPolicy) && len(addedColumnChanges(report)) > 0) ||
		(schemaEvolutionPolicyRequiresTargetStep(nullabilityPolicy) && len(nullabilityChanges(report)) > 0) ||
		(schemaEvolutionPolicyRequiresTargetStep(typePolicy) && len(typeChanges(report)) > 0)
}

func enforceSchemaContractDecisions(decisions []schemaContractDecision) error {
	var violations []string
	for _, decision := range decisions {
		if decision.Action == schemaContractActionFrozen || decision.Action == schemaContractActionBlocked {
			violations = append(violations, formatSchemaContractViolation(decision))
		}
	}
	if len(violations) == 0 {
		return nil
	}
	return &SchemaEvolutionError{Message: "schema contract violation: " + strings.Join(violations, "; ") +
		"; choose report to observe only, evolve for deterministic safe changes, or discard_row/discard_value where supported"}
}

func (o *Orchestrator) applySchemaEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	if !report.HasChanges() || !o.config.Migration.SchemaEvolutionEnabled() {
		return nil
	}
	if o.config.Migration.TargetMode != "upsert" {
		logging.Debug("schema evolution configured but target_mode=%s recreates tables; skipping target ALTERs",
			o.config.Migration.TargetMode)
		return nil
	}

	addedActions, addedLogOnly, err := planAddedColumnEvolution(
		report,
		tables,
		o.config.Migration.AddedColumnSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	nullabilityActions, nullabilityLogOnly, err := planNullabilityEvolution(
		report,
		tables,
		o.config.Migration.NullabilityChangeSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	typeActions, typeLogOnly, err := planTypeEvolution(
		report,
		tables,
		o.config.Migration.TypeChangeSchemaEvolutionPolicy(),
	)
	if err != nil {
		return &SchemaEvolutionError{Message: err.Error()}
	}
	label := o.schemaChangeLogLabel()
	if len(addedLogOnly) > 0 {
		policy := o.config.Migration.AddedColumnSchemaEvolutionPolicy()
		if o.config.Migration.SchemaContractEnabled() &&
			o.config.Migration.SchemaContractColumnsMode() == config.SchemaContractDiscardRow {
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
		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, action.Table.Name)
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
		if err := o.targetPool.AddColumn(ctx, &action.Table, &action.Column, o.config.Target.Schema); err != nil {
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
		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, action.Table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", action.Table.Name, err)}
		}
		if !exists {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema evolution cannot relax nullability for %s.%s: target table does not exist",
				action.Table.Name, action.Column.Name,
			)}
		}
		if err := o.targetPool.DropColumnNotNull(ctx, &action.Table, &action.Column, o.config.Target.Schema); err != nil {
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
		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, action.Table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", action.Table.Name, err)}
		}
		if !exists {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"schema evolution cannot alter type for %s.%s: target table does not exist",
				action.Table.Name, action.Column.Name,
			)}
		}
		if err := o.targetPool.AlterColumnType(ctx, &action.Table, &action.Column, o.config.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"altering target column type %s.%s (%s -> %s): %v",
				action.Table.Name, action.Column.Name, action.Change.Previous, action.Change.Current, err,
			)}
		}
	}

	o.auditEvent(o.schemaChangeAuditName("schema_evolution_applied"), map[string]any{
		"added_columns":           len(addedActions),
		"nullability_relaxations": len(nullabilityActions),
		"type_widenings":          len(typeActions),
	})
	return nil
}

func (o *Orchestrator) applySchemaContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaContractEnabled() ||
		o.config.Migration.SchemaContractTablesMode() != config.SchemaContractEvolve ||
		o.config.Migration.TargetMode != "upsert" {
		return nil
	}

	addedTables := tableAddedChanges(report)
	if len(addedTables) == 0 {
		return nil
	}

	label := o.schemaChangeLogLabel()
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

		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, table.Name)
		if err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("checking target table %s: %v", table.Name, err)}
		}
		if exists {
			logging.Debug("%s: target table %s already exists; skipping create", label, table.Name)
			continue
		}

		if err := o.targetPool.CreateTableWithOptions(ctx, &table, o.config.Target.Schema, pool.TableOptions{}); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("creating target table %s: %v", table.FullName(), err)}
		}
		if err := o.targetPool.CreatePrimaryKey(ctx, &table, o.config.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf("creating primary key for target table %s: %v", table.FullName(), err)}
		}
		created++
	}

	if created > 0 {
		o.auditEvent("schema_contract_tables_applied", map[string]any{
			"added_tables": created,
		})
	}
	return nil
}

func (o *Orchestrator) finalizeSchemaContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaContractEnabled() ||
		o.config.Migration.SchemaContractTablesMode() != config.SchemaContractEvolve ||
		o.config.Migration.TargetMode != "upsert" {
		return
	}

	addedTables := findAddedSourceTables(report, tables)
	if len(addedTables) == 0 {
		return
	}

	label := o.schemaChangeLogLabel()
	logging.Info("%s: finalizing %d evolved target table(s) after transfer", label, len(addedTables))

	resets := 0
	for _, table := range addedTables {
		t := table
		if err := o.targetPool.ResetSequence(ctx, o.config.Target.Schema, &t); err != nil {
			logging.Warn("%s: resetting sequence for evolved table %s: %v", label, t.Name, err)
			continue
		}
		resets++
	}

	indexes := 0
	if o.config.Migration.CreateIndexesEnabled() {
		for _, table := range addedTables {
			for _, index := range table.Indexes {
				t := table
				idx := index
				if err := o.targetPool.CreateIndex(ctx, &t, &idx, o.config.Target.Schema); err != nil {
					logging.Warn("%s: creating index %s on evolved table %s: %v", label, idx.Name, t.Name, err)
					continue
				}
				indexes++
			}
		}
	}

	foreignKeys := 0
	if o.config.Migration.CreateForeignKeysEnabled() {
		for _, table := range addedTables {
			for _, fk := range table.ForeignKeys {
				t := table
				foreignKey := fk
				if err := o.targetPool.CreateForeignKey(ctx, &t, &foreignKey, o.config.Target.Schema); err != nil {
					logging.Warn("%s: creating foreign key %s on evolved table %s: %v", label, foreignKey.Name, t.Name, err)
					continue
				}
				foreignKeys++
			}
		}
	}

	checks := 0
	if o.config.Migration.CreateCheckConstraints {
		for _, table := range addedTables {
			for _, check := range table.CheckConstraints {
				t := table
				chk := check
				if err := o.targetPool.CreateCheckConstraint(ctx, &t, &chk, o.config.Target.Schema); err != nil {
					logging.Warn("%s: creating check constraint %s on evolved table %s: %v", label, chk.Name, t.Name, err)
					continue
				}
				checks++
			}
		}
	}

	o.auditEvent("schema_contract_tables_finalized", map[string]any{
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

func (o *Orchestrator) schemaChangeAuditName(base string) string {
	if o.config.Migration.SchemaContractEnabled() {
		return strings.Replace(base, "schema_evolution", "schema_contract", 1)
	}
	return base
}

func (o *Orchestrator) schemaChangeLogLabel() string {
	if o.config.Migration.SchemaContractEnabled() {
		return "schema contract"
	}
	return "schema evolution"
}

func (o *Orchestrator) effectiveTablesForSchemaEvolution(report drift.Report, tables []source.Table) ([]source.Table, error) {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaEvolutionEnabled() {
		return tables, nil
	}

	pruned := tables
	if o.config.Migration.SchemaContractEnabled() &&
		o.config.Migration.SchemaContractTablesMode() == config.SchemaContractDiscardRow {
		var discardedTables int
		pruned, discardedTables = pruneDiscardedAddedTables(report, pruned)
		if discardedTables > 0 {
			logging.Warn("%s: skipping %d added table(s) from transfer, validation, and schema snapshots because tables=discard_row",
				o.schemaChangeLogLabel(), discardedTables)
			o.auditEvent(o.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"added_tables": discardedTables,
			})
		}
	}

	if o.config.Migration.SchemaContractEnabled() &&
		o.config.Migration.SchemaContractColumnsMode() == config.SchemaContractDiscardRow {
		var skippedTables, skippedColumns int
		pruned, skippedTables, skippedColumns = pruneTablesWithAddedColumns(report, pruned)
		if skippedTables > 0 {
			logging.Warn("%s: skipping %d table(s) with %d added column(s) from transfer, validation, and schema snapshots because columns=discard_row",
				o.schemaChangeLogLabel(), skippedTables, skippedColumns)
			o.auditEvent(o.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"added_columns":  skippedColumns,
				"skipped_tables": skippedTables,
			})
		}
	}

	if o.config.Migration.SchemaContractEnabled() &&
		o.config.Migration.SchemaContractDataTypeMode() == config.SchemaContractDiscardRow {
		var skippedTables, skippedColumns int
		pruned, skippedTables, skippedColumns = pruneTablesWithDataTypeChanges(report, pruned)
		if skippedTables > 0 {
			logging.Warn("%s: skipping %d table(s) with %d data type/nullability-changed column(s) from transfer, validation, and schema snapshots because data_type=discard_row",
				o.schemaChangeLogLabel(), skippedTables, skippedColumns)
			o.auditEvent(o.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"data_type_columns": skippedColumns,
				"skipped_tables":    skippedTables,
			})
		}
	}

	if o.config.Migration.SchemaContractEnabled() &&
		o.config.Migration.SchemaContractDataTypeMode() == config.SchemaContractDiscardValue {
		var discarded int
		var err error
		pruned, discarded, err = pruneDiscardedDataTypeColumns(report, pruned, o.config.Migration.DateUpdatedColumns)
		if err != nil {
			return nil, &SchemaEvolutionError{Message: err.Error()}
		}
		if discarded > 0 {
			logging.Warn("%s: discarding %d data type/nullability-changed column(s) from transfer and validation because data_type=discard_value; previous schema snapshot metadata will be retained",
				o.schemaChangeLogLabel(), discarded)
			o.auditEvent(o.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
				"data_type_columns": discarded,
			})
		}
	}

	if o.config.Migration.AddedColumnSchemaEvolutionPolicy() != config.SchemaEvolutionDiscardValue {
		return pruned, nil
	}

	pruned, discarded, err := pruneDiscardedAddedColumns(report, pruned, o.config.Migration.DateUpdatedColumns)
	if err != nil {
		return nil, &SchemaEvolutionError{Message: err.Error()}
	}
	if discarded == 0 {
		return pruned, nil
	}

	logging.Warn("%s: discarding %d added column(s) from target DDL, transfer, validation, and snapshots because added_column=discard_value",
		o.schemaChangeLogLabel(), discarded)
	o.auditEvent(o.schemaChangeAuditName("schema_evolution_discarded"), map[string]any{
		"added_columns": discarded,
	})
	return pruned, nil
}
