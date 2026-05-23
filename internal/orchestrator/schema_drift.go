package orchestrator

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// SchemaDriftError is returned when migration.fail_on_schema_drift is enabled
// and the current source schema differs from the last successful snapshot.
type SchemaDriftError struct {
	Report drift.Report
}

func (e *SchemaDriftError) Error() string {
	return "schema drift detected and migration.fail_on_schema_drift is true"
}

func (e *SchemaDriftError) ExitCode() int { return exitcodes.TransferError }

func (o *Orchestrator) reportSchemaDrift(tables []source.Table, allowSchemaEvolution bool) (drift.Report, error) {
	records, err := o.state.GetLatestSchemaSnapshots(o.schemaSnapshotNamespace())
	if err != nil {
		return drift.Report{}, fmt.Errorf("loading schema snapshots: %w", err)
	}
	if len(records) == 0 {
		logging.Debug("No previous schema snapshot found; baseline will be captured after a successful run")
		return drift.Report{}, nil
	}

	previous := make([]drift.TableSnapshot, 0, len(records))
	for _, record := range records {
		snapshot, err := drift.UnmarshalTableSnapshot(record.SchemaJSON)
		if err != nil {
			return drift.Report{}, fmt.Errorf("decoding schema snapshot for %s.%s: %w",
				record.SourceSchema, record.TableName, err)
		}
		if o.tableInCurrentFilterScope(snapshot.Name) {
			previous = append(previous, snapshot)
		}
	}

	report := drift.Compare(previous, drift.BuildTableSnapshots(tables))
	if !report.HasChanges() {
		logging.Debug("No schema drift detected")
		return drift.Report{}, nil
	}

	logging.Warn("%s", report.FormatWithFooter(o.schemaDriftReportFooter(report, allowSchemaEvolution)))
	o.auditEvent("schema_drift_detected", map[string]any{
		"tables_affected": report.TablesAffected(),
		"changes":         len(report.Changes),
	})
	if o.config.Migration.FailOnSchemaDrift {
		return report, &SchemaDriftError{Report: report}
	}
	if err := o.enforceSchemaContractPolicy(report); err != nil {
		return report, err
	}
	return report, nil
}

func (o *Orchestrator) schemaDriftReportFooter(report drift.Report, allowSchemaEvolution bool) string {
	if !o.config.Migration.SchemaEvolutionEnabled() {
		return "No automatic schema alignment will be applied (read-only mode)."
	}
	if o.config.Migration.FailOnSchemaDrift {
		return "migration.fail_on_schema_drift is true; transfer will abort before schema evolution."
	}
	if o.config.Migration.SchemaContractEnabled() {
		return o.schemaContractReportFooter(report, allowSchemaEvolution)
	}
	if !allowSchemaEvolution {
		if o.config.Migration.AddedColumnSchemaEvolutionPolicy() == config.SchemaEvolutionDiscardValue {
			if part := addedColumnDiscardValueFooterPart(len(addedColumnChanges(report))); part != "" {
				return fmt.Sprintf("Schema evolution %s. No target ALTERs will be applied in read-only mode.", part)
			}
		}
		return "No automatic schema alignment will be applied (read-only mode)."
	}
	if o.config.Migration.TargetMode != "upsert" {
		addedPolicy := o.config.Migration.AddedColumnSchemaEvolutionPolicy()
		if addedPolicy == config.SchemaEvolutionDiscardValue && len(addedColumnChanges(report)) > 0 {
			return fmt.Sprintf(
				"Schema evolution %s. target_mode=%s will not apply target ALTERs for other changes.",
				addedColumnDiscardValueFooterPart(len(addedColumnChanges(report))),
				o.config.Migration.TargetMode,
			)
		}
		return fmt.Sprintf("Schema evolution is configured, but target_mode=%s will not apply target ALTERs.",
			o.config.Migration.TargetMode)
	}

	var parts []string
	if part := schemaEvolutionFooterPart(
		"added_column",
		len(addedColumnChanges(report)),
		"added column(s)",
		o.config.Migration.AddedColumnSchemaEvolutionPolicy(),
	); part != "" {
		parts = append(parts, part)
	}
	if part := schemaEvolutionFooterPart(
		"nullability_change",
		len(nullabilityChanges(report)),
		"nullability change(s)",
		o.config.Migration.NullabilityChangeSchemaEvolutionPolicy(),
	); part != "" {
		parts = append(parts, part)
	}
	if part := typeChangeFooterPart(report, o.config.Migration.TypeChangeSchemaEvolutionPolicy()); part != "" {
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return "Schema evolution is enabled, but this report contains no currently supported auto-apply changes."
	}
	return "Schema evolution " + strings.Join(parts, "; ") + "."
}

func (o *Orchestrator) schemaContractReportFooter(report drift.Report, allowSchemaEvolution bool) string {
	var parts []string
	if part := schemaContractTablesFooterPart(
		len(tableAddedChanges(report)),
		len(tableDroppedChanges(report)),
		o.config.Migration.SchemaContractTablesMode(),
		allowSchemaEvolution,
	); part != "" {
		parts = append(parts, part)
	}
	if part := schemaContractColumnsFooterPart(
		len(addedColumnChanges(report)),
		len(droppedColumnChanges(report)),
		o.config.Migration.SchemaContractColumnsMode(),
		allowSchemaEvolution,
	); part != "" {
		parts = append(parts, part)
	}
	if part := schemaContractDataTypeFooterPart(
		report,
		o.config.Migration.SchemaContractDataTypeMode(),
		allowSchemaEvolution,
	); part != "" {
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return "Schema contract is enabled, but this report contains no currently supported contract actions."
	}
	footer := "Schema contract " + strings.Join(parts, "; ") + "."
	if !allowSchemaEvolution {
		footer += " No target ALTERs will be applied in read-only mode."
	}
	return footer
}

func schemaContractFooterPart(entity string, count int, noun string, mode config.SchemaContractMode, allowSchemaEvolution bool) string {
	if count == 0 {
		return ""
	}

	switch mode {
	case config.SchemaContractEvolve, "":
		if !allowSchemaEvolution {
			return fmt.Sprintf("%s=evolve; %d %s will be reported only in read-only mode", entity, count, noun)
		}
		return fmt.Sprintf("%s=evolve; %d %s will follow target_mode behavior", entity, count, noun)
	case config.SchemaContractFreeze:
		return fmt.Sprintf("%s=freeze; %d %s will abort before transfer", entity, count, noun)
	case config.SchemaContractDiscardValue:
		return fmt.Sprintf("%s=discard_value; %d %s will be omitted from target DDL, transfer, validation, and schema snapshots",
			entity, count, noun)
	case config.SchemaContractReport:
		return fmt.Sprintf("%s=report; %d %s will be reported only", entity, count, noun)
	default:
		return fmt.Sprintf("%s policy is invalid", entity)
	}
}

func schemaContractTablesFooterPart(added, dropped int, mode config.SchemaContractMode, allowSchemaEvolution bool) string {
	if added == 0 && dropped == 0 {
		return ""
	}

	total := added + dropped
	var parts []string
	switch mode {
	case config.SchemaContractEvolve, "":
		if allowSchemaEvolution {
			if added > 0 {
				parts = append(parts, fmt.Sprintf("%d added table(s) may be created before transfer", added))
			}
		} else if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added table(s) will be reported only in read-only mode", added))
		}
		if dropped > 0 {
			parts = append(parts, fmt.Sprintf("%d dropped table(s) will be reported; target tables are retained", dropped))
		}
		return "tables=evolve; " + strings.Join(parts, "; ")
	case config.SchemaContractFreeze:
		return fmt.Sprintf("tables=freeze; %d table change(s) will abort before transfer", total)
	case config.SchemaContractDiscardRow:
		if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added table(s) will be skipped for this run", added))
		}
		if dropped > 0 {
			parts = append(parts, fmt.Sprintf("%d dropped table(s) will be reported; target tables are retained", dropped))
		}
		return "tables=discard_row; " + strings.Join(parts, "; ")
	case config.SchemaContractReport:
		return fmt.Sprintf("tables=report; %d table change(s) will be reported only", total)
	default:
		return "tables policy is invalid"
	}
}

func schemaContractColumnsFooterPart(added, dropped int, mode config.SchemaContractMode, allowSchemaEvolution bool) string {
	if added == 0 && dropped == 0 {
		return ""
	}

	var parts []string
	switch mode {
	case config.SchemaContractEvolve, "":
		if allowSchemaEvolution {
			if added > 0 {
				parts = append(parts, fmt.Sprintf("%d added column(s) may be added before transfer", added))
			}
		} else if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added column(s) will be reported only in read-only mode", added))
		}
	case config.SchemaContractFreeze:
		if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added column(s) will abort before transfer", added))
		}
		if dropped > 0 {
			parts = append(parts, fmt.Sprintf("%d dropped source column(s) will abort before transfer", dropped))
		}
	case config.SchemaContractDiscardRow:
		if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added column(s) will skip affected table(s) for this run", added))
		}
	case config.SchemaContractDiscardValue:
		if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added column(s) will be omitted from target DDL, transfer, validation, and schema snapshots", added))
		}
	case config.SchemaContractReport:
		if added > 0 {
			parts = append(parts, fmt.Sprintf("%d added column(s) will be reported only", added))
		}
	default:
		return "columns policy is invalid"
	}

	if dropped > 0 && mode != config.SchemaContractFreeze {
		parts = append(parts, fmt.Sprintf("%d dropped source column(s) will be reported; target columns are retained and omitted from writes/validation", dropped))
	}
	if len(parts) == 0 {
		return ""
	}
	if mode == "" {
		mode = config.SchemaContractEvolve
	}
	return "columns=" + string(mode) + "; " + strings.Join(parts, "; ")
}

func schemaContractDataTypeFooterPart(report drift.Report, mode config.SchemaContractMode, allowSchemaEvolution bool) string {
	changes := dataTypeContractChanges(report)
	if len(changes) == 0 {
		return ""
	}

	if mode == "" {
		mode = config.SchemaContractEvolve
	}
	switch mode {
	case config.SchemaContractEvolve:
		if !allowSchemaEvolution {
			return fmt.Sprintf("data_type=evolve; %d data type/nullability change(s) will be reported only in read-only mode", len(changes))
		}
		return dataTypeEvolveFooterPart(report)
	case config.SchemaContractFreeze:
		return fmt.Sprintf("data_type=freeze; %d data type/nullability change(s) will abort before transfer", len(changes))
	case config.SchemaContractDiscardRow:
		return fmt.Sprintf("data_type=discard_row; %d data type/nullability-changed column(s) found; affected table(s) will be skipped for this run", dataTypeChangedColumnCount(report))
	case config.SchemaContractDiscardValue:
		return fmt.Sprintf("data_type=discard_value; %d data type/nullability-changed column(s) will be omitted from transfer and validation; previous schema snapshot metadata will be retained", dataTypeChangedColumnCount(report))
	case config.SchemaContractReport:
		return fmt.Sprintf("data_type=report; %d data type/nullability change(s) will be reported only", len(changes))
	default:
		return "data_type policy is invalid"
	}
}

func dataTypeEvolveFooterPart(report drift.Report) string {
	safeNullability := 0
	unsafeNullability := 0
	for _, change := range nullabilityChanges(report) {
		if change.Previous == "NOT NULL" && change.Current == "NULL" {
			safeNullability++
		} else {
			unsafeNullability++
		}
	}

	widened := len(typeWidenedChanges(report))
	unsafeType := len(typeNarrowedOrLossyChanges(report))
	var parts []string
	if safeNullability > 0 {
		parts = append(parts, fmt.Sprintf("%d nullability relaxation(s) may be applied before transfer", safeNullability))
	}
	if widened > 0 {
		parts = append(parts, fmt.Sprintf("%d widened type change(s) may be applied before transfer", widened))
	}
	if unsafeNullability+unsafeType > 0 {
		parts = append(parts, fmt.Sprintf("%d unsafe data type/nullability change(s) will abort before transfer", unsafeNullability+unsafeType))
	}
	if len(parts) == 0 {
		return "data_type=evolve; data type/nullability changes will be reported only"
	}
	return "data_type=evolve; " + strings.Join(parts, "; ")
}

func schemaEvolutionFooterPart(kind string, count int, noun string, policy config.SchemaEvolutionPolicy) string {
	if count == 0 {
		return ""
	}

	switch policy {
	case config.SchemaEvolutionAuto:
		return fmt.Sprintf("%s=auto; %d %s may be applied before transfer", kind, count, noun)
	case config.SchemaEvolutionLog:
		return fmt.Sprintf("%s=log; %d %s will be reported only", kind, count, noun)
	case config.SchemaEvolutionFail:
		return fmt.Sprintf("%s=fail; %d %s will abort before transfer", kind, count, noun)
	case config.SchemaEvolutionDiscard, config.SchemaEvolutionDiscardValue:
		return fmt.Sprintf("%s=discard_value; %d %s will be omitted from target DDL, transfer, validation, and schema snapshots",
			kind, count, noun)
	default:
		return fmt.Sprintf("%s policy is invalid", kind)
	}
}

func typeChangeFooterPart(report drift.Report, policy config.SchemaEvolutionPolicy) string {
	changes := typeChanges(report)
	if len(changes) == 0 {
		return ""
	}

	if policy != config.SchemaEvolutionAuto {
		return schemaEvolutionFooterPart("type_change", len(changes), "type change(s)", policy)
	}

	widened := 0
	unsafe := 0
	for _, change := range changes {
		if change.Kind == drift.TypeWidened {
			widened++
		} else {
			unsafe++
		}
	}
	if unsafe == 0 {
		return fmt.Sprintf("type_change=auto; %d widened type change(s) may be applied before transfer", widened)
	}
	if widened == 0 {
		return fmt.Sprintf("type_change=auto; %d narrowed/lossy type change(s) will abort before transfer", unsafe)
	}
	return fmt.Sprintf("type_change=auto; %d widened type change(s) may be applied before transfer; %d narrowed/lossy type change(s) will abort before transfer",
		widened, unsafe)
}

func addedColumnDiscardValueFooterPart(count int) string {
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("added_column=discard_value; %d added column(s) will be omitted from target DDL, transfer, validation, and schema snapshots", count)
}

func (o *Orchestrator) captureSchemaSnapshotsForReport(runID string, report drift.Report, tables []source.Table) {
	o.captureSchemaSnapshotSet(runID, o.schemaSnapshotPlan(report, tables))
}

func (o *Orchestrator) captureSchemaSnapshotSet(runID string, snapshots []drift.TableSnapshot) {
	for _, snapshot := range snapshots {
		schemaJSON, err := drift.MarshalTableSnapshot(snapshot)
		if err != nil {
			logging.Warn("failed to encode schema snapshot for %s.%s: %v", snapshot.Schema, snapshot.Name, err)
			continue
		}
		if err := o.state.SaveSchemaSnapshot(
			runID,
			o.schemaSnapshotNamespace(),
			snapshot.Name,
			schemaJSON,
		); err != nil {
			logging.Warn("failed to save schema snapshot for %s.%s: %v", snapshot.Schema, snapshot.Name, err)
		}
	}
	logging.Debug("Captured %d source schema snapshot(s)", len(snapshots))
}

func (o *Orchestrator) schemaSnapshotPlan(report drift.Report, tables []source.Table) []drift.TableSnapshot {
	snapshots := drift.BuildTableSnapshots(tables)
	if !o.config.Migration.SchemaContractEnabled() ||
		o.config.Migration.SchemaContractDataTypeMode() != config.SchemaContractDiscardValue ||
		len(report.Previous) == 0 {
		return snapshots
	}

	previousByTable := tableSnapshotsByKey(report.Previous)
	discardByTable := dataTypeChangesByTable(report)
	for i := range snapshots {
		key := schemaEvolutionTableKey(snapshots[i].Schema, snapshots[i].Name)
		discardCols := discardByTable[key]
		previous, ok := previousByTable[key]
		if !ok {
			continue
		}
		restoreDiscardedDataTypeSnapshotMetadata(&snapshots[i], previous, discardCols, discardByTable)
	}
	return snapshots
}

func tableSnapshotsByKey(snapshots []drift.TableSnapshot) map[string]drift.TableSnapshot {
	out := make(map[string]drift.TableSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		out[schemaEvolutionTableKey(snapshot.Schema, snapshot.Name)] = snapshot
	}
	return out
}

func restoreDiscardedDataTypeSnapshotMetadata(
	current *drift.TableSnapshot,
	previous drift.TableSnapshot,
	discardCols map[string]struct{},
	discardByTable map[string]map[string]struct{},
) {
	current.Columns = mergePreviousDiscardedColumns(current.Columns, previous.Columns, discardCols)
	current.Indexes = mergePreviousDiscardedIndexes(current.Indexes, previous.Indexes, discardCols)
	current.ForeignKeys = mergePreviousDiscardedForeignKeys(
		current.Schema,
		current.ForeignKeys,
		previous.ForeignKeys,
		discardCols,
		discardByTable,
	)
	current.CheckConstraints = mergePreviousDiscardedChecks(current.CheckConstraints, previous.CheckConstraints, discardCols)
}

func mergePreviousDiscardedColumns(
	current []drift.ColumnSnapshot,
	previous []drift.ColumnSnapshot,
	discardCols map[string]struct{},
) []drift.ColumnSnapshot {
	byName := make(map[string]drift.ColumnSnapshot, len(current)+len(previous))
	for _, column := range current {
		byName[column.Name] = column
	}
	for _, column := range previous {
		if stringSetContains(discardCols, column.Name) {
			byName[column.Name] = column
		}
	}

	merged := make([]drift.ColumnSnapshot, 0, len(byName))
	for _, column := range byName {
		merged = append(merged, column)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		left, right := merged[i], merged[j]
		if left.OrdinalPosition != right.OrdinalPosition {
			return left.OrdinalPosition < right.OrdinalPosition
		}
		return left.Name < right.Name
	})
	return merged
}

func mergePreviousDiscardedIndexes(
	current []drift.IndexSnapshot,
	previous []drift.IndexSnapshot,
	discardCols map[string]struct{},
) []drift.IndexSnapshot {
	byName := make(map[string]drift.IndexSnapshot, len(current)+len(previous))
	for _, index := range current {
		byName[index.Name] = index
	}
	for _, index := range previous {
		if anyNameInSet(index.Columns, discardCols) || anyNameInSet(index.IncludeColumns, discardCols) {
			byName[index.Name] = index
		}
	}

	merged := make([]drift.IndexSnapshot, 0, len(byName))
	for _, index := range byName {
		merged = append(merged, index)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].Name < merged[j].Name
	})
	return merged
}

func mergePreviousDiscardedForeignKeys(
	tableSchema string,
	current []drift.ForeignKeySnapshot,
	previous []drift.ForeignKeySnapshot,
	discardCols map[string]struct{},
	discardByTable map[string]map[string]struct{},
) []drift.ForeignKeySnapshot {
	byName := make(map[string]drift.ForeignKeySnapshot, len(current)+len(previous))
	for _, fk := range current {
		byName[fk.Name] = fk
	}
	for _, fk := range previous {
		if anyNameInSet(fk.Columns, discardCols) || foreignKeyReferencesDiscardedColumns(tableSchema, fk, discardByTable) {
			byName[fk.Name] = fk
		}
	}

	merged := make([]drift.ForeignKeySnapshot, 0, len(byName))
	for _, fk := range byName {
		merged = append(merged, fk)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].Name < merged[j].Name
	})
	return merged
}

func foreignKeyReferencesDiscardedColumns(
	tableSchema string,
	fk drift.ForeignKeySnapshot,
	discardByTable map[string]map[string]struct{},
) bool {
	refSchema := fk.RefSchema
	if refSchema == "" {
		refSchema = tableSchema
	}
	refDiscard := discardByTable[schemaEvolutionTableKey(refSchema, fk.RefTable)]
	return anyNameInSet(fk.RefColumns, refDiscard)
}

func mergePreviousDiscardedChecks(
	current []drift.CheckConstraintSnapshot,
	previous []drift.CheckConstraintSnapshot,
	discardCols map[string]struct{},
) []drift.CheckConstraintSnapshot {
	byName := make(map[string]drift.CheckConstraintSnapshot, len(current)+len(previous))
	for _, check := range current {
		byName[check.Name] = check
	}
	for _, check := range previous {
		if checkReferencesDiscardedColumn(check.Definition, discardCols) {
			byName[check.Name] = check
		}
	}

	merged := make([]drift.CheckConstraintSnapshot, 0, len(byName))
	for _, check := range byName {
		merged = append(merged, check)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].Name < merged[j].Name
	})
	return merged
}

func (o *Orchestrator) schemaSnapshotNamespace() string {
	return strings.Join([]string{
		o.config.Source.Type,
		o.config.Source.Database,
		o.config.Source.Schema,
	}, "|")
}

func (o *Orchestrator) tableInCurrentFilterScope(tableName string) bool {
	include := o.config.Migration.IncludeTables
	exclude := o.config.Migration.ExcludeTables
	lowerName := strings.ToLower(tableName)

	if len(include) > 0 {
		matched := false
		for _, pattern := range include {
			if match, _ := filepath.Match(strings.ToLower(pattern), lowerName); match {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	for _, pattern := range exclude {
		if match, _ := filepath.Match(strings.ToLower(pattern), lowerName); match {
			return false
		}
	}
	return true
}
