package orchestrator

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/source"
)

func TestPlanAddedColumnEvolutionAutoAddsNullableColumn(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
	}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false},
		},
	}}

	actions, logOnly, err := planAddedColumnEvolution(report, tables, config.SchemaEvolutionAuto)
	if err != nil {
		t.Fatalf("planAddedColumnEvolution returned error: %v", err)
	}
	if len(logOnly) != 0 {
		t.Fatalf("logOnly = %+v, want empty", logOnly)
	}
	if len(actions) != 1 {
		t.Fatalf("actions = %+v, want one action", actions)
	}
	action := actions[0]
	if action.Table.Name != "Users" || action.Column.Name != "email" {
		t.Fatalf("action = %+v, want Users.email", action)
	}
	if !action.Column.IsNullable {
		t.Fatal("target column should be forced nullable")
	}
	if action.SourceNullable {
		t.Fatal("SourceNullable = true, want false")
	}
}

func TestPlanAddedColumnEvolutionLogOnly(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
	}}}

	actions, logOnly, err := planAddedColumnEvolution(report, nil, config.SchemaEvolutionLog)
	if err != nil {
		t.Fatalf("planAddedColumnEvolution returned error: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("actions = %+v, want empty", actions)
	}
	if len(logOnly) != 1 {
		t.Fatalf("logOnly = %+v, want one change", logOnly)
	}
}

func TestPlanAddedColumnEvolutionFailPolicy(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
	}}}

	_, _, err := planAddedColumnEvolution(report, nil, config.SchemaEvolutionFail)
	if err == nil {
		t.Fatal("planAddedColumnEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "added_column=fail") {
		t.Fatalf("error = %q, want added_column=fail", err.Error())
	}
}

func TestPlanAddedColumnEvolutionRejectsUnsafeColumns(t *testing.T) {
	tests := []struct {
		name    string
		table   source.Table
		wantErr string
	}{
		{
			name: "identity",
			table: source.Table{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "external_id", DataType: "int", IsIdentity: true, IsNullable: false},
				},
			},
			wantErr: "identity column",
		},
		{
			name: "primary key",
			table: source.Table{
				Schema:     "dbo",
				Name:       "Users",
				PrimaryKey: []string{"external_id"},
				Columns: []source.Column{
					{Name: "external_id", DataType: "int", IsNullable: false},
				},
			},
			wantErr: "primary-key column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := drift.Report{Changes: []drift.Change{{
				Kind:       drift.AddedColumn,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "external_id",
			}}}

			_, _, err := planAddedColumnEvolution(report, []source.Table{tt.table}, config.SchemaEvolutionAuto)
			if err == nil {
				t.Fatal("planAddedColumnEvolution returned nil error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestShouldApplySchemaEvolutionOnlyForOptInUpsert(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}

	tests := []struct {
		name      string
		migration config.MigrationConfig
		want      bool
	}{
		{name: "disabled", migration: config.MigrationConfig{TargetMode: "upsert"}, want: false},
		{name: "drop recreate", migration: config.MigrationConfig{
			TargetMode:      "drop_recreate",
			SchemaEvolution: &config.SchemaEvolutionConfig{},
		}, want: false},
		{name: "upsert enabled", migration: config.MigrationConfig{
			TargetMode:      "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{},
		}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Orchestrator{config: &config.Config{Migration: tt.migration}}
			if got := o.shouldApplySchemaEvolution(report); got != tt.want {
				t.Fatalf("shouldApplySchemaEvolution() = %v, want %v", got, tt.want)
			}
		})
	}
}
