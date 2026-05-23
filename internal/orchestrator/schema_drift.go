package orchestrator

import (
	"fmt"
	"path/filepath"
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
	if part := schemaContractFooterPart(
		"columns",
		len(addedColumnChanges(report)),
		"added column(s)",
		o.config.Migration.SchemaContractColumnsMode(),
		allowSchemaEvolution,
	); part != "" {
		parts = append(parts, part)
	}
	if part := schemaContractFooterPart(
		"data_type",
		len(nullabilityChanges(report))+len(typeChanges(report)),
		"data type/nullability change(s)",
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

func (o *Orchestrator) captureSchemaSnapshots(runID string, tables []source.Table) {
	snapshots := drift.BuildTableSnapshots(tables)
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
