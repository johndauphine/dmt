package orchestrator

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

func validateForceResumeConfigCompatibility(run *checkpoint.Run, current *config.Config) ([]string, error) {
	if run == nil || current == nil {
		return nil, nil
	}

	var warnings []string
	var forbidden []string

	addForbidden := func(field string, oldValue, newValue any) {
		if !reflect.DeepEqual(oldValue, newValue) {
			forbidden = append(forbidden, fmt.Sprintf("%s changed from %v to %v", field, oldValue, newValue))
		}
	}
	addWarning := func(field string, oldValue, newValue any) {
		if !reflect.DeepEqual(oldValue, newValue) {
			warnings = append(warnings, fmt.Sprintf("%s changed from %v to %v", field, oldValue, newValue))
		}
	}

	// These schema fields are always available on both SQLite and file state,
	// even when the backend cannot persist the full config snapshot.
	addForbidden("source.schema", run.SourceSchema, current.Source.Schema)
	addForbidden("target.schema", run.TargetSchema, current.Target.Schema)

	if run.Config == "" {
		if len(forbidden) > 0 {
			return warnings, forceResumeCompatibilityError(run.ID, forbidden)
		}
		return append(warnings, "stored config snapshot unavailable; only source/target schema drift could be checked"), nil
	}

	var original config.Config
	if err := json.Unmarshal([]byte(run.Config), &original); err != nil {
		return warnings, fmt.Errorf("cannot validate --force-resume config compatibility for run %s: stored config snapshot is invalid: %w", run.ID, err)
	}

	addForbidden("source.type", original.Source.Type, current.Source.Type)
	addForbidden("source.host", original.Source.Host, current.Source.Host)
	addForbidden("source.port", original.Source.Port, current.Source.Port)
	addForbidden("source.database", original.Source.Database, current.Source.Database)
	addForbidden("source.schema", original.Source.Schema, current.Source.Schema)
	addForbidden("source.user", original.Source.User, current.Source.User)

	addForbidden("target.type", original.Target.Type, current.Target.Type)
	addForbidden("target.host", original.Target.Host, current.Target.Host)
	addForbidden("target.port", original.Target.Port, current.Target.Port)
	addForbidden("target.database", original.Target.Database, current.Target.Database)
	addForbidden("target.schema", original.Target.Schema, current.Target.Schema)
	addForbidden("target.user", original.Target.User, current.Target.User)

	addForbidden("migration.target_mode", original.Migration.TargetMode, current.Migration.TargetMode)

	addWarning("migration.include_tables", original.Migration.IncludeTables, current.Migration.IncludeTables)
	addWarning("migration.exclude_tables", original.Migration.ExcludeTables, current.Migration.ExcludeTables)
	addWarning("migration.chunk_size", original.Migration.ChunkSize, current.Migration.ChunkSize)
	addWarning("migration.max_partitions", original.Migration.MaxPartitions, current.Migration.MaxPartitions)
	addWarning("migration.large_table_threshold", original.Migration.LargeTableThreshold, current.Migration.LargeTableThreshold)
	addWarning("migration.parallel_readers", original.Migration.ParallelReaders, current.Migration.ParallelReaders)
	addWarning("source.chunk_size", original.Source.ChunkSize, current.Source.ChunkSize)
	addWarning("target.chunk_size", original.Target.ChunkSize, current.Target.ChunkSize)
	addWarning("migration.date_updated_columns", original.Migration.DateUpdatedColumns, current.Migration.DateUpdatedColumns)

	if len(forbidden) > 0 {
		return warnings, forceResumeCompatibilityError(run.ID, forbidden)
	}
	return warnings, nil
}

func forceResumeCompatibilityError(runID string, changes []string) error {
	return fmt.Errorf("--force-resume refused for run %s because incompatible config fields changed: %v. Start a new run or restore the original config", runID, changes)
}
