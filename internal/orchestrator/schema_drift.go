package orchestrator

import (
	"fmt"
	"path/filepath"
	"strings"

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

func (o *Orchestrator) reportSchemaDrift(tables []source.Table) error {
	records, err := o.state.GetLatestSchemaSnapshots(o.schemaSnapshotNamespace())
	if err != nil {
		return fmt.Errorf("loading schema snapshots: %w", err)
	}
	if len(records) == 0 {
		logging.Debug("No previous schema snapshot found; baseline will be captured after a successful run")
		return nil
	}

	previous := make([]drift.TableSnapshot, 0, len(records))
	for _, record := range records {
		snapshot, err := drift.UnmarshalTableSnapshot(record.SchemaJSON)
		if err != nil {
			return fmt.Errorf("decoding schema snapshot for %s.%s: %w",
				record.SourceSchema, record.TableName, err)
		}
		if o.tableInCurrentFilterScope(snapshot.Name) {
			previous = append(previous, snapshot)
		}
	}

	report := drift.Compare(previous, drift.BuildTableSnapshots(tables))
	if !report.HasChanges() {
		logging.Debug("No schema drift detected")
		return nil
	}

	logging.Warn("%s", report.Format())
	o.auditEvent("schema_drift_detected", map[string]any{
		"tables_affected": report.TablesAffected(),
		"changes":         len(report.Changes),
	})
	if o.config.Migration.FailOnSchemaDrift {
		return &SchemaDriftError{Report: report}
	}
	return nil
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
