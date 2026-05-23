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
	if len(addedLogOnly) > 0 {
		policy := o.config.Migration.AddedColumnSchemaEvolutionPolicy()
		if policy == config.SchemaEvolutionDiscardValue {
			logging.Warn("schema evolution: %d added column(s) detected; policy added_column=discard_value leaves target unchanged and omits values from transfer",
				len(addedLogOnly))
		} else {
			logging.Warn("schema evolution: %d added column(s) detected; policy added_column=log leaves target unchanged",
				len(addedLogOnly))
		}
	}
	if len(nullabilityLogOnly) > 0 {
		logging.Warn("schema evolution: %d nullability change(s) detected; policy nullability_change=log leaves target unchanged",
			len(nullabilityLogOnly))
	}
	if len(typeLogOnly) > 0 {
		logging.Warn("schema evolution: %d type change(s) detected; policy type_change=log leaves target unchanged",
			len(typeLogOnly))
	}
	if len(addedActions) == 0 && len(nullabilityActions) == 0 && len(typeActions) == 0 {
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

	if len(typeActions) > 0 {
		logging.Info("schema evolution: widening %d target column type(s)", len(typeActions))
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

	o.auditEvent("schema_evolution_applied", map[string]any{
		"added_columns":           len(addedActions),
		"nullability_relaxations": len(nullabilityActions),
		"type_widenings":          len(typeActions),
	})
	return nil
}

func schemaEvolutionPolicyRequiresTargetStep(policy config.SchemaEvolutionPolicy) bool {
	switch policy {
	case config.SchemaEvolutionAuto, config.SchemaEvolutionFail:
		return true
	default:
		return false
	}
}

func (o *Orchestrator) effectiveTablesForSchemaEvolution(report drift.Report, tables []source.Table) ([]source.Table, error) {
	if !report.HasChanges() ||
		!o.config.Migration.SchemaEvolutionEnabled() ||
		o.config.Migration.AddedColumnSchemaEvolutionPolicy() != config.SchemaEvolutionDiscardValue {
		return tables, nil
	}

	pruned, discarded, err := pruneDiscardedAddedColumns(report, tables)
	if err != nil {
		return nil, &SchemaEvolutionError{Message: err.Error()}
	}
	if discarded == 0 {
		return pruned, nil
	}

	logging.Warn("schema evolution: discarding %d added column(s) from target DDL, transfer, validation, and snapshots because added_column=discard_value",
		discarded)
	o.auditEvent("schema_evolution_discarded", map[string]any{
		"added_columns": discarded,
	})
	return pruned, nil
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
	case config.SchemaEvolutionLog, config.SchemaEvolutionDiscard, config.SchemaEvolutionDiscardValue:
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

func pruneDiscardedAddedColumns(report drift.Report, tables []source.Table) ([]source.Table, int, error) {
	discardByTable := discardedAddedColumnsByTable(report)
	if len(discardByTable) == 0 {
		return tables, 0, nil
	}

	pruned := make([]source.Table, 0, len(tables))
	discarded := 0
	for _, table := range tables {
		discardCols, ok := discardByTable[schemaEvolutionTableKey(table.Schema, table.Name)]
		if !ok {
			pruned = append(pruned, table)
			continue
		}

		for _, pk := range table.PrimaryKey {
			if stringSetContains(discardCols, pk) {
				return nil, 0, fmt.Errorf(
					"schema evolution cannot discard added primary-key column %s.%s",
					table.FullName(), pk,
				)
			}
		}

		next := table
		next.Columns = filterColumnsWithoutDiscarded(table.Columns, discardCols)
		removed := len(table.Columns) - len(next.Columns)
		if removed == 0 {
			pruned = append(pruned, next)
			continue
		}

		next.PopulatePKColumns()
		next.Indexes = filterIndexesWithoutDiscarded(table.Indexes, discardCols)
		next.ForeignKeys = filterForeignKeysWithoutDiscarded(table, table.ForeignKeys, discardByTable)
		next.CheckConstraints = filterChecksWithoutDiscarded(table.CheckConstraints, discardCols)
		pruned = append(pruned, next)
		discarded += removed
	}

	return pruned, discarded, nil
}

func discardedAddedColumnsByTable(report drift.Report) map[string]map[string]struct{} {
	byTable := make(map[string]map[string]struct{})
	for _, change := range addedColumnChanges(report) {
		key := schemaEvolutionTableKey(change.Schema, change.TableName)
		if byTable[key] == nil {
			byTable[key] = make(map[string]struct{})
		}
		byTable[key][change.ObjectName] = struct{}{}
	}
	return byTable
}

func filterColumnsWithoutDiscarded(columns []source.Column, discardCols map[string]struct{}) []source.Column {
	out := make([]source.Column, 0, len(columns))
	for _, column := range columns {
		if stringSetContains(discardCols, column.Name) {
			continue
		}
		out = append(out, column)
	}
	return out
}

func filterIndexesWithoutDiscarded(indexes []source.Index, discardCols map[string]struct{}) []source.Index {
	out := make([]source.Index, 0, len(indexes))
	for _, index := range indexes {
		if anyNameInSet(index.Columns, discardCols) || anyNameInSet(index.IncludeCols, discardCols) {
			continue
		}
		out = append(out, index)
	}
	return out
}

func filterForeignKeysWithoutDiscarded(
	table source.Table,
	foreignKeys []source.ForeignKey,
	discardByTable map[string]map[string]struct{},
) []source.ForeignKey {
	out := make([]source.ForeignKey, 0, len(foreignKeys))
	localDiscard := discardByTable[schemaEvolutionTableKey(table.Schema, table.Name)]
	for _, fk := range foreignKeys {
		if anyNameInSet(fk.Columns, localDiscard) {
			continue
		}

		refSchema := fk.RefSchema
		if refSchema == "" {
			refSchema = table.Schema
		}
		refDiscard := discardByTable[schemaEvolutionTableKey(refSchema, fk.RefTable)]
		if anyNameInSet(fk.RefColumns, refDiscard) {
			continue
		}

		out = append(out, fk)
	}
	return out
}

func filterChecksWithoutDiscarded(checks []source.CheckConstraint, discardCols map[string]struct{}) []source.CheckConstraint {
	out := make([]source.CheckConstraint, 0, len(checks))
	for _, check := range checks {
		if checkReferencesDiscardedColumn(check.Definition, discardCols) {
			continue
		}
		out = append(out, check)
	}
	return out
}

func checkReferencesDiscardedColumn(definition string, discardCols map[string]struct{}) bool {
	for column := range discardCols {
		if definitionContainsIdentifier(definition, column) {
			return true
		}
	}
	return false
}

func definitionContainsIdentifier(definition, name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(definition); {
		switch definition[i] {
		case '[', '"', '`':
			identifier, next := readDelimitedIdentifier(definition, i)
			if next > i {
				if strings.EqualFold(identifier, name) {
					return true
				}
				i = next
				continue
			}
		}

		if !isIdentifierByte(definition[i]) {
			i++
			continue
		}

		start := i
		for i < len(definition) && isIdentifierByte(definition[i]) {
			i++
		}
		if strings.EqualFold(definition[start:i], name) {
			return true
		}
	}
	return false
}

func readDelimitedIdentifier(definition string, start int) (string, int) {
	if start >= len(definition) {
		return "", start
	}

	open := definition[start]
	close := open
	if open == '[' {
		close = ']'
	}
	if open != '[' && open != '"' && open != '`' {
		return "", start
	}

	for i := start + 1; i < len(definition); i++ {
		if definition[i] == close {
			return definition[start+1 : i], i + 1
		}
	}
	return "", start
}

func isIdentifierByte(b byte) bool {
	return b == '_' ||
		b == '$' ||
		b == '#' ||
		(b >= '0' && b <= '9') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z')
}

func anyNameInSet(names []string, set map[string]struct{}) bool {
	if len(set) == 0 {
		return false
	}
	for _, name := range names {
		if stringSetContains(set, name) {
			return true
		}
	}
	return false
}

func stringSetContains(set map[string]struct{}, name string) bool {
	if len(set) == 0 {
		return false
	}
	if _, ok := set[name]; ok {
		return true
	}
	for existing := range set {
		if strings.EqualFold(existing, name) {
			return true
		}
	}
	return false
}

func schemaEvolutionTableKey(schema, table string) string {
	return schema + "\x00" + table
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
		if hasColumnTypeDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-relax nullability for %s.%s while type drift is also present",
				table.Name, column.Name,
			)
		}
		if hasColumnDefaultDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-relax nullability for %s.%s while default drift is also present",
				table.Name, column.Name,
			)
		}
		if hasPrimaryKeyDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-relax nullability for %s.%s while primary-key drift is also present",
				table.Name, column.Name,
			)
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

func planTypeEvolution(
	report drift.Report,
	tables []source.Table,
	policy config.SchemaEvolutionPolicy,
) ([]typeEvolution, []drift.Change, error) {
	changes := typeChanges(report)
	if len(changes) == 0 {
		return nil, nil, nil
	}

	switch policy {
	case config.SchemaEvolutionLog:
		return nil, changes, nil
	case config.SchemaEvolutionFail:
		return nil, nil, fmt.Errorf("schema evolution policy type_change=fail; %d type change(s) detected",
			len(changes))
	case config.SchemaEvolutionAuto:
		// Continue below.
	default:
		return nil, nil, fmt.Errorf("unknown schema evolution type_change policy %q", policy)
	}

	actions := make([]typeEvolution, 0, len(changes))
	for _, change := range changes {
		table, column, err := findSourceColumn(tables, change)
		if err != nil {
			return nil, nil, err
		}
		if change.Kind != drift.TypeWidened {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-apply %s for %s.%s (%s -> %s); set type_change=log to report only",
				change.Kind, table.Name, column.Name, change.Previous, change.Current,
			)
		}
		if hasColumnNullabilityDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-widen type for %s.%s while nullability drift is also present",
				table.Name, column.Name,
			)
		}
		if hasColumnDefaultDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-widen type for %s.%s while default drift is also present",
				table.Name, column.Name,
			)
		}
		if hasPrimaryKeyDrift(report, change) {
			return nil, nil, fmt.Errorf(
				"schema evolution cannot auto-widen type for %s.%s while primary-key drift is also present",
				table.Name, column.Name,
			)
		}
		if column.IsIdentity {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-widen identity column %s.%s",
				table.Name, column.Name)
		}
		if tablePrimaryKeyContains(table, column.Name) {
			return nil, nil, fmt.Errorf("schema evolution cannot auto-widen primary-key column %s.%s",
				table.Name, column.Name)
		}
		actions = append(actions, typeEvolution{Table: table, Column: column, Change: change})
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

func typeChanges(report drift.Report) []drift.Change {
	var changes []drift.Change
	for _, change := range report.Changes {
		switch change.Kind {
		case drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
			changes = append(changes, change)
		}
	}
	return changes
}

func hasColumnTypeDrift(report drift.Report, candidate drift.Change) bool {
	for _, change := range report.Changes {
		if change.Schema != candidate.Schema ||
			change.TableName != candidate.TableName ||
			change.ObjectName != candidate.ObjectName {
			continue
		}
		switch change.Kind {
		case drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
			return true
		}
	}
	return false
}

func hasColumnNullabilityDrift(report drift.Report, candidate drift.Change) bool {
	for _, change := range report.Changes {
		if change.Kind == drift.NullabilityChange &&
			change.Schema == candidate.Schema &&
			change.TableName == candidate.TableName &&
			change.ObjectName == candidate.ObjectName {
			return true
		}
	}
	return false
}

func hasColumnDefaultDrift(report drift.Report, candidate drift.Change) bool {
	for _, change := range report.Changes {
		if change.Kind == drift.DefaultChange &&
			change.Schema == candidate.Schema &&
			change.TableName == candidate.TableName &&
			change.ObjectName == candidate.ObjectName {
			return true
		}
	}
	return false
}

func hasPrimaryKeyDrift(report drift.Report, candidate drift.Change) bool {
	for _, change := range report.Changes {
		if change.Kind == drift.PKChange &&
			change.Schema == candidate.Schema &&
			change.TableName == candidate.TableName {
			return true
		}
	}
	return false
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
