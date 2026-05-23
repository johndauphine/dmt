package orchestrator

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
)

func TestValidateForceResumeConfigCompatibilityRejectsIdentityDrift(t *testing.T) {
	original := testResumeCompatConfig()
	current := cloneResumeCompatConfig(t, original)
	current.Target.Database = "other_target"

	_, err := validateForceResumeConfigCompatibility(testResumeCompatRun(t, original), current)
	if err == nil {
		t.Fatal("expected incompatible target database drift to be rejected")
	}
	if !strings.Contains(err.Error(), "target.database changed from target_db to other_target") {
		t.Fatalf("error %q does not name target database drift", err)
	}
}

func TestValidateForceResumeConfigCompatibilityRejectsTargetModeDrift(t *testing.T) {
	original := testResumeCompatConfig()
	current := cloneResumeCompatConfig(t, original)
	current.Migration.TargetMode = "upsert"

	_, err := validateForceResumeConfigCompatibility(testResumeCompatRun(t, original), current)
	if err == nil {
		t.Fatal("expected target_mode drift to be rejected")
	}
	if !strings.Contains(err.Error(), "migration.target_mode changed from drop_recreate to upsert") {
		t.Fatalf("error %q does not name target_mode drift", err)
	}
}

func TestValidateForceResumeConfigCompatibilityWarnsForRiskyButAllowedDrift(t *testing.T) {
	original := testResumeCompatConfig()
	current := cloneResumeCompatConfig(t, original)
	current.Migration.IncludeTables = []string{"orders"}
	current.Migration.ExcludeTables = []string{"archive_*"}
	current.Migration.ChunkSize = 25000
	current.Migration.MaxPartitions = 8
	current.Migration.ParallelReaders = 4

	warnings, err := validateForceResumeConfigCompatibility(testResumeCompatRun(t, original), current)
	if err != nil {
		t.Fatalf("expected risky drift to warn, not fail: %v", err)
	}
	for _, want := range []string{
		"migration.include_tables changed",
		"migration.exclude_tables changed",
		"migration.chunk_size changed from 50000 to 25000",
		"migration.max_partitions changed from 4 to 8",
		"migration.parallel_readers changed from 2 to 4",
	} {
		if !containsWarning(warnings, want) {
			t.Fatalf("warnings %v missing %q", warnings, want)
		}
	}
}

func TestValidateForceResumeConfigCompatibilityFallsBackWhenSnapshotUnavailable(t *testing.T) {
	current := testResumeCompatConfig()
	run := &checkpoint.Run{
		ID:           "run-1",
		SourceSchema: current.Source.Schema,
		TargetSchema: current.Target.Schema,
	}

	warnings, err := validateForceResumeConfigCompatibility(run, current)
	if err != nil {
		t.Fatalf("expected missing snapshot to warn, not fail: %v", err)
	}
	if !containsWarning(warnings, "stored config snapshot unavailable") {
		t.Fatalf("warnings %v missing unavailable snapshot warning", warnings)
	}
}

func TestValidateForceResumeConfigCompatibilityRejectsSchemaDriftWithoutSnapshot(t *testing.T) {
	current := testResumeCompatConfig()
	run := &checkpoint.Run{
		ID:           "run-1",
		SourceSchema: "dbo",
		TargetSchema: "public",
	}
	current.Target.Schema = "other_schema"

	_, err := validateForceResumeConfigCompatibility(run, current)
	if err == nil {
		t.Fatal("expected schema drift to be rejected without snapshot")
	}
	if !strings.Contains(err.Error(), "target.schema changed from public to other_schema") {
		t.Fatalf("error %q does not name target schema drift", err)
	}
}

func testResumeCompatConfig() *config.Config {
	return &config.Config{
		Source: config.SourceConfig{
			Type:     "mssql",
			Host:     "source.local",
			Port:     1433,
			Database: "source_db",
			Schema:   "dbo",
			User:     "reader",
			Password: "source-secret",
		},
		Target: config.TargetConfig{
			Type:     "postgres",
			Host:     "target.local",
			Port:     5432,
			Database: "target_db",
			Schema:   "public",
			User:     "writer",
			Password: "target-secret",
		},
		Migration: config.MigrationConfig{
			TargetMode:          "drop_recreate",
			ChunkSize:           50000,
			MaxPartitions:       4,
			ParallelReaders:     2,
			LargeTableThreshold: 5000000,
		},
	}
}

func testResumeCompatRun(t *testing.T, cfg *config.Config) *checkpoint.Run {
	t.Helper()
	data, err := json.Marshal(cfg.Sanitized())
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return &checkpoint.Run{
		ID:           "run-1",
		SourceSchema: cfg.Source.Schema,
		TargetSchema: cfg.Target.Schema,
		Config:       string(data),
	}
}

func cloneResumeCompatConfig(t *testing.T, cfg *config.Config) *config.Config {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	var cloned config.Config
	if err := json.Unmarshal(data, &cloned); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return &cloned
}

func containsWarning(warnings []string, want string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, want) {
			return true
		}
	}
	return false
}
