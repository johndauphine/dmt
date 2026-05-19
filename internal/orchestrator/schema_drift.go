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
	return report, nil
}

func (o *Orchestrator) schemaDriftReportFooter(report drift.Report, allowSchemaEvolution bool) string {
	if !allowSchemaEvolution || !o.config.Migration.SchemaEvolutionEnabled() {
		return "No automatic schema alignment will be applied (read-only mode)."
	}
	if o.config.Migration.FailOnSchemaDrift {
		return "migration.fail_on_schema_drift is true; transfer will abort before schema evolution."
	}
	if o.config.Migration.TargetMode != "upsert" {
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
	if len(parts) == 0 {
		return "Schema evolution is enabled, but this report contains no currently supported auto-apply changes."
	}
	return "Schema evolution " + strings.Join(parts, "; ") + "."
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
	default:
		return fmt.Sprintf("%s policy is invalid", kind)
	}
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
