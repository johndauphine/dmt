package orchestrator

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/source"
)

// Moved from schema_evolution_test.go during the #456 extraction:
// validateResumeMissingTargetTable is resume-path code, not schema
// evolution, so its test stays with the orchestrator package.
func TestValidateResumeMissingTargetTableRequiresPrimaryKeyForUpsert(t *testing.T) {
	err := validateResumeMissingTargetTable(
		source.Table{Schema: "dbo", Name: "events"},
		config.MigrationConfig{TargetMode: "upsert"},
		drift.Report{},
	)
	if err == nil {
		t.Fatal("validateResumeMissingTargetTable() error = nil, want primary-key error")
	}
	if !strings.Contains(err.Error(), "source table has no primary key") {
		t.Fatalf("error = %q, want primary-key message", err)
	}

	if err := validateResumeMissingTargetTable(
		source.Table{Schema: "dbo", Name: "events"},
		config.MigrationConfig{TargetMode: "drop_recreate"},
		drift.Report{},
	); err != nil {
		t.Fatalf("drop_recreate missing target table validation error: %v", err)
	}
	if err := validateResumeMissingTargetTable(
		source.Table{Schema: "dbo", Name: "events", PrimaryKey: []string{"id"}},
		config.MigrationConfig{TargetMode: "upsert"},
		drift.Report{},
	); err != nil {
		t.Fatalf("upsert table with primary key validation error: %v", err)
	}
}

func TestValidateResumeMissingTargetTableHonorsSchemaContractReport(t *testing.T) {
	table := source.Table{Schema: "dbo", Name: "events", PrimaryKey: []string{"id"}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:      drift.TableAdded,
		Schema:    "dbo",
		TableName: "events",
	}}}

	err := validateResumeMissingTargetTable(table, config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractReport},
	}, report)
	if err == nil {
		t.Fatal("validateResumeMissingTargetTable() error = nil, want tables=report error")
	}
	if !strings.Contains(err.Error(), "tables=report") {
		t.Fatalf("error = %q, want tables=report message", err)
	}

	if err := validateResumeMissingTargetTable(table, config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
	}, report); err != nil {
		t.Fatalf("tables=evolve validation error: %v", err)
	}
}
