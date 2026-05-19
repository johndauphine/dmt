package orchestrator

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
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

func (o *Orchestrator) shouldApplySchemaEvolution(report drift.Report) bool {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaEvolutionEnabled() ||
		o.config.Migration.TargetMode != "upsert" {
		return false
	}

	addedColumnPolicy := o.config.Migration.AddedColumnSchemaEvolutionPolicy()
	nullabilityPolicy := o.config.Migration.NullabilityChangeSchemaEvolutionPolicy()
	return (addedColumnPolicy != config.SchemaEvolutionLog && len(addedColumnChanges(report)) > 0) ||
		(nullabilityPolicy != config.SchemaEvolutionLog && len(nullabilityChanges(report)) > 0)
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
	if len(addedLogOnly) > 0 {
		logging.Warn("schema evolution: %d added column(s) detected; policy added_column=log leaves target unchanged",
			len(addedLogOnly))
	}
	if len(nullabilityLogOnly) > 0 {
		logging.Warn("schema evolution: %d nullability change(s) detected; policy nullability_change=log leaves target unchanged",
			len(nullabilityLogOnly))
	}
	if len(addedActions) == 0 && len(nullabilityActions) == 0 {
		return nil
	}

	if len(addedActions) > 0 {
		logging.Info("schema evolution: adding %d nullable column(s) to target", len(addedActions))
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
			logging.Warn("schema evolution: source column %s.%s is NOT NULL; adding target column as NULL to preserve existing rows",
				action.Table.Name, action.Column.Name)
		}
		if err := o.targetPool.AddColumn(ctx, &action.Table, &action.Column, o.config.Target.Schema); err != nil {
			return &SchemaEvolutionError{Message: fmt.Sprintf(
				"adding target column %s.%s: %v",
				action.Table.Name, action.Column.Name, err,
			)}
		}
	}

	if len(nullabilityActions) > 0 {
		logging.Info("schema evolution: relaxing %d target column(s) from NOT NULL to NULL", len(nullabilityActions))
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

	o.auditEvent("schema_evolution_applied", map[string]any{
		"added_columns":           len(addedActions),
		"nullability_relaxations": len(nullabilityActions),
	})
	return nil
}

func planAddedColumnEvolution(
	report drift.Report,
	tables []source.Table,
	policy config.SchemaEvolutionPolicy,
) ([]addedColumnEvolution, []drift.Change, error) {
	added := addedColumnChanges(report)
	if len(added) == 0 {
		return nil, nil, nil
	}

	switch policy {
	case config.SchemaEvolutionLog:
		return nil, added, nil
	case config.SchemaEvolutionFail:
		return nil, nil, fmt.Errorf("schema evolution policy added_column=fail; %d added column(s) detected",
			len(added))
	case config.SchemaEvolutionAuto:
		// Continue below.
	default:
		return nil, nil, fmt.Errorf("unknown schema evolution added_column policy %q", policy)
	}

	actions := make([]addedColumnEvolution, 0, len(added))
	for _, change := range added {
		table, column, err := findSourceColumn(tables, change)
		if err != nil {
			return nil, nil, err
		}
		if column.IsIdentity {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-add identity column %s.%s",
				table.Name, column.Name)
		}
		if tablePrimaryKeyContains(table, column.Name) {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-add primary-key column %s.%s",
				table.Name, column.Name)
		}

		targetColumn := column
		targetColumn.IsNullable = true
		actions = append(actions, addedColumnEvolution{
			Table:          table,
			Column:         targetColumn,
			SourceNullable: column.IsNullable,
		})
	}
	return actions, nil, nil
}

func planNullabilityEvolution(
	report drift.Report,
	tables []source.Table,
	policy config.SchemaEvolutionPolicy,
) ([]nullabilityEvolution, []drift.Change, error) {
	changes := nullabilityChanges(report)
	if len(changes) == 0 {
		return nil, nil, nil
	}

	switch policy {
	case config.SchemaEvolutionLog:
		return nil, changes, nil
	case config.SchemaEvolutionFail:
		return nil, nil, fmt.Errorf("schema evolution policy nullability_change=fail; %d nullability change(s) detected",
			len(changes))
	case config.SchemaEvolutionAuto:
		// Continue below.
	default:
		return nil, nil, fmt.Errorf("unknown schema evolution nullability_change policy %q", policy)
	}

	actions := make([]nullabilityEvolution, 0, len(changes))
	for _, change := range changes {
		table, column, err := findSourceColumn(tables, change)
		if err != nil {
			return nil, nil, err
		}
		if change.Previous != "NOT NULL" || change.Current != "NULL" || !column.IsNullable {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-tighten nullability for %s.%s (%s -> %s); set nullability_change=log to report only",
				table.Name, column.Name, change.Previous, change.Current,
			)
		}
		if column.IsIdentity {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-relax identity column %s.%s",
				table.Name, column.Name)
		}
		if tablePrimaryKeyContains(table, column.Name) {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-relax primary-key column %s.%s",
				table.Name, column.Name)
		}
		actions = append(actions, nullabilityEvolution{Table: table, Column: column})
	}
	return actions, nil, nil
}

func addedColumnChanges(report drift.Report) []drift.Change {
	var changes []drift.Change
	for _, change := range report.Changes {
		if change.Kind == drift.AddedColumn {
			changes = append(changes, change)
		}
	}
	return changes
}

func nullabilityChanges(report drift.Report) []drift.Change {
	var changes []drift.Change
	for _, change := range report.Changes {
		if change.Kind == drift.NullabilityChange {
			changes = append(changes, change)
		}
	}
	return changes
}

func findSourceColumn(tables []source.Table, change drift.Change) (source.Table, source.Column, error) {
	for _, table := range tables {
		if table.Schema != change.Schema || table.Name != change.TableName {
			continue
		}
		for _, column := range table.Columns {
			if column.Name == change.ObjectName {
				return table, column, nil
			}
		}
		return source.Table{}, source.Column{}, fmt.Errorf("schema evolution could not find source column %s.%s",
			table.Name, change.ObjectName)
	}
	return source.Table{}, source.Column{}, fmt.Errorf("schema evolution could not find source table %s.%s",
		change.Schema, change.TableName)
}

func tablePrimaryKeyContains(table source.Table, columnName string) bool {
	for _, pk := range table.PrimaryKey {
		if strings.EqualFold(pk, columnName) {
			return true
		}
	}
	return false
}
