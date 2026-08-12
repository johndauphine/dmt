package schemaevolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/audit"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/drift"
	"github.com/johndauphine/dmt/v5/internal/source"
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

func TestPlanAddedColumnEvolutionDiscardValue(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
	}}}

	actions, logOnly, err := planAddedColumnEvolution(report, nil, config.SchemaEvolutionDiscardValue)
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

func TestPruneDiscardedAddedColumnsRemovesColumnFromEffectiveSchema(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
	}}}
	table := source.Table{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "name", DataType: "varchar", MaxLength: 100, IsNullable: true},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true},
		},
		Indexes: []source.Index{
			{Name: "idx_users_name", Columns: []string{"name"}},
			{Name: "idx_users_email", Columns: []string{"email"}},
		},
		CheckConstraints: []source.CheckConstraint{
			{Name: "chk_users_email", Definition: "email <> ''"},
		},
	}
	table.PopulatePKColumns()

	pruned, discarded, err := pruneDiscardedAddedColumns(report, []source.Table{table}, nil)
	if err != nil {
		t.Fatalf("pruneDiscardedAddedColumns returned error: %v", err)
	}
	if discarded != 1 {
		t.Fatalf("discarded = %d, want 1", discarded)
	}
	if len(pruned) != 1 {
		t.Fatalf("pruned table count = %d, want 1", len(pruned))
	}
	got := pruned[0]
	if names := got.GetColumnNames(); strings.Join(names, ",") != "id,name" {
		t.Fatalf("columns = %v, want [id name]", names)
	}
	if len(got.PKColumns) != 1 || got.PKColumns[0].Name != "id" {
		t.Fatalf("PKColumns = %+v, want id metadata", got.PKColumns)
	}
	if len(got.Indexes) != 1 || got.Indexes[0].Name != "idx_users_name" {
		t.Fatalf("Indexes = %+v, want only idx_users_name", got.Indexes)
	}
	if len(got.CheckConstraints) != 0 {
		t.Fatalf("CheckConstraints = %+v, want discarded-column checks removed", got.CheckConstraints)
	}
	if len(table.Columns) != 3 {
		t.Fatalf("original table mutated: columns = %+v", table.Columns)
	}
}

func TestPruneDiscardedAddedColumnsRejectsPrimaryKeyColumn(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "external_id",
	}}}
	table := source.Table{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id", "external_id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "external_id", DataType: "int", IsNullable: false},
		},
	}
	table.PopulatePKColumns()

	_, _, err := pruneDiscardedAddedColumns(report, []source.Table{table}, nil)
	if err == nil {
		t.Fatal("pruneDiscardedAddedColumns returned nil error")
	}
	if !strings.Contains(err.Error(), "primary-key column") {
		t.Fatalf("error = %q, want primary-key column", err.Error())
	}
	if !strings.Contains(err.Error(), "dbo.Users.external_id") {
		t.Fatalf("error = %q, want fully qualified column name", err.Error())
	}
}

func TestSchemaDriftReportFooterReadOnlyDoesNotReportDiscardForOtherPolicies(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode:      "upsert",
		SchemaEvolution: &config.SchemaEvolutionConfig{},
	}}}

	got := e.schemaDriftReportFooter(report, false)
	if !strings.Contains(got, "read-only mode") {
		t.Fatalf("schemaDriftReportFooter() = %q, want read-only mode", got)
	}
	if strings.Contains(got, "discard_value") {
		t.Fatalf("schemaDriftReportFooter() = %q, should not report discard_value for default auto policy", got)
	}
}

func TestDefinitionContainsIdentifierMatchesWholeIdentifiers(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		column     string
		want       bool
	}{
		{name: "plain identifier", definition: "email <> ''", column: "email", want: true},
		{name: "case insensitive", definition: "Email <> ''", column: "email", want: true},
		{name: "bracket delimited", definition: "[Order Date] IS NOT NULL", column: "Order Date", want: true},
		{name: "double quote delimited", definition: "\"Order Date\" IS NOT NULL", column: "Order Date", want: true},
		{name: "substring is not match", definition: "customer_id > 0", column: "id", want: false},
		{name: "suffix is not match", definition: "id2 > 0", column: "id", want: false},
		{name: "prefix is not match", definition: "old_id > 0", column: "id", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := definitionContainsIdentifier(tt.definition, tt.column); got != tt.want {
				t.Fatalf("definitionContainsIdentifier(%q, %q) = %v, want %v",
					tt.definition, tt.column, got, tt.want)
			}
		})
	}
}

func TestPlanNullabilityEvolutionAutoRelaxesNotNull(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NOT NULL",
		Current:    "NULL",
	}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true},
		},
	}}

	actions, logOnly, err := planNullabilityEvolution(report, tables, config.SchemaEvolutionAuto)
	if err != nil {
		t.Fatalf("planNullabilityEvolution returned error: %v", err)
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
		t.Fatal("source column should be nullable for a relaxation")
	}
}

func TestPlanNullabilityEvolutionLogOnly(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NOT NULL",
		Current:    "NULL",
	}}}

	actions, logOnly, err := planNullabilityEvolution(report, nil, config.SchemaEvolutionLog)
	if err != nil {
		t.Fatalf("planNullabilityEvolution returned error: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("actions = %+v, want empty", actions)
	}
	if len(logOnly) != 1 {
		t.Fatalf("logOnly = %+v, want one change", logOnly)
	}
}

func TestPlanNullabilityEvolutionFailPolicy(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NOT NULL",
		Current:    "NULL",
	}}}

	_, _, err := planNullabilityEvolution(report, nil, config.SchemaEvolutionFail)
	if err == nil {
		t.Fatal("planNullabilityEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "nullability_change=fail") {
		t.Fatalf("error = %q, want nullability_change=fail", err.Error())
	}
}

func TestPlanNullabilityEvolutionRejectsTightening(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NULL",
		Current:    "NOT NULL",
	}}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: false},
		},
	}}

	_, _, err := planNullabilityEvolution(report, tables, config.SchemaEvolutionAuto)
	if err == nil {
		t.Fatal("planNullabilityEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "cannot auto-tighten") {
		t.Fatalf("error = %q, want auto-tighten rejection", err.Error())
	}
}

func TestPlanNullabilityEvolutionRejectsSameColumnTypeDrift(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.TypeNarrowed,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Previous:   "varchar(255)",
			Current:    "varchar(50)",
		},
		{
			Kind:       drift.NullabilityChange,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Previous:   "NOT NULL",
			Current:    "NULL",
		},
	}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "email", DataType: "varchar", MaxLength: 50, IsNullable: true},
		},
	}}

	_, _, err := planNullabilityEvolution(report, tables, config.SchemaEvolutionAuto)
	if err == nil {
		t.Fatal("planNullabilityEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "type drift") {
		t.Fatalf("error = %q, want type drift rejection", err.Error())
	}
}

func TestPlanNullabilityEvolutionRejectsSameColumnDefaultDrift(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.DefaultChange,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Previous:   "'old'",
			Current:    "'new'",
		},
		{
			Kind:       drift.NullabilityChange,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Previous:   "NOT NULL",
			Current:    "NULL",
		},
	}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true, DefaultValue: "'new'"},
		},
	}}

	_, _, err := planNullabilityEvolution(report, tables, config.SchemaEvolutionAuto)
	if err == nil {
		t.Fatal("planNullabilityEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "default drift") {
		t.Fatalf("error = %q, want default drift rejection", err.Error())
	}
}

func TestPlanNullabilityEvolutionRejectsPrimaryKeyDrift(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:      drift.PKChange,
			Schema:    "dbo",
			TableName: "Users",
			Previous:  "email",
			Current:   "id",
		},
		{
			Kind:       drift.NullabilityChange,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Previous:   "NOT NULL",
			Current:    "NULL",
		},
	}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true},
		},
	}}

	_, _, err := planNullabilityEvolution(report, tables, config.SchemaEvolutionAuto)
	if err == nil {
		t.Fatal("planNullabilityEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "primary-key drift") {
		t.Fatalf("error = %q, want primary-key drift rejection", err.Error())
	}
}

func TestPlanTypeEvolutionAutoWidensType(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeWidened,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "display_name",
		Previous:   "varchar(100)",
		Current:    "varchar(255)",
	}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "display_name", DataType: "varchar", MaxLength: 255, IsNullable: true},
		},
	}}

	actions, logOnly, err := planTypeEvolution(report, tables, config.SchemaEvolutionAuto)
	if err != nil {
		t.Fatalf("planTypeEvolution returned error: %v", err)
	}
	if len(logOnly) != 0 {
		t.Fatalf("logOnly = %+v, want empty", logOnly)
	}
	if len(actions) != 1 {
		t.Fatalf("actions = %+v, want one action", actions)
	}
	action := actions[0]
	if action.Table.Name != "Users" || action.Column.Name != "display_name" {
		t.Fatalf("action = %+v, want Users.display_name", action)
	}
	if action.Change.Kind != drift.TypeWidened {
		t.Fatalf("change kind = %s, want %s", action.Change.Kind, drift.TypeWidened)
	}
}

func TestPlanTypeEvolutionLogOnly(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeWidened,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "display_name",
		Previous:   "varchar(100)",
		Current:    "varchar(255)",
	}}}

	actions, logOnly, err := planTypeEvolution(report, nil, config.SchemaEvolutionLog)
	if err != nil {
		t.Fatalf("planTypeEvolution returned error: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("actions = %+v, want empty", actions)
	}
	if len(logOnly) != 1 {
		t.Fatalf("logOnly = %+v, want one change", logOnly)
	}
}

func TestPlanTypeEvolutionFailPolicy(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeWidened,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "display_name",
		Previous:   "varchar(100)",
		Current:    "varchar(255)",
	}}}

	_, _, err := planTypeEvolution(report, nil, config.SchemaEvolutionFail)
	if err == nil {
		t.Fatal("planTypeEvolution returned nil error")
	}
	if !strings.Contains(err.Error(), "type_change=fail") {
		t.Fatalf("error = %q, want type_change=fail", err.Error())
	}
}

func TestPlanTypeEvolutionRejectsUnsafeChanges(t *testing.T) {
	tests := []struct {
		name    string
		change  drift.Change
		table   source.Table
		wantErr string
	}{
		{
			name: "narrowed",
			change: drift.Change{
				Kind:       drift.TypeNarrowed,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "display_name",
				Previous:   "varchar(255)",
				Current:    "varchar(100)",
			},
			table: source.Table{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "display_name", DataType: "varchar", MaxLength: 100, IsNullable: true},
				},
			},
			wantErr: "cannot auto-apply type_narrowed",
		},
		{
			name: "lossy",
			change: drift.Change{
				Kind:       drift.TypeChangedLossy,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "score",
				Previous:   "integer",
				Current:    "varchar(50)",
			},
			table: source.Table{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "score", DataType: "varchar", MaxLength: 50, IsNullable: true},
				},
			},
			wantErr: "cannot auto-apply type_changed_lossy",
		},
		{
			name: "primary key",
			change: drift.Change{
				Kind:       drift.TypeWidened,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "id",
				Previous:   "int",
				Current:    "bigint",
			},
			table: source.Table{
				Schema:     "dbo",
				Name:       "Users",
				PrimaryKey: []string{"id"},
				Columns: []source.Column{
					{Name: "id", DataType: "bigint", IsNullable: false},
				},
			},
			wantErr: "primary-key column",
		},
		{
			name: "identity",
			change: drift.Change{
				Kind:       drift.TypeWidened,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "external_id",
				Previous:   "int",
				Current:    "bigint",
			},
			table: source.Table{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "external_id", DataType: "bigint", IsIdentity: true, IsNullable: false},
				},
			},
			wantErr: "identity column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := drift.Report{Changes: []drift.Change{tt.change}}
			_, _, err := planTypeEvolution(report, []source.Table{tt.table}, config.SchemaEvolutionAuto)
			if err == nil {
				t.Fatal("planTypeEvolution returned nil error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestPlanTypeEvolutionRejectsCoupledColumnDrift(t *testing.T) {
	tests := []struct {
		name    string
		extra   drift.Change
		wantErr string
	}{
		{
			name: "nullability",
			extra: drift.Change{
				Kind:       drift.NullabilityChange,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "display_name",
				Previous:   "NOT NULL",
				Current:    "NULL",
			},
			wantErr: "nullability drift",
		},
		{
			name: "default",
			extra: drift.Change{
				Kind:       drift.DefaultChange,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "display_name",
				Previous:   "'old'",
				Current:    "'new'",
			},
			wantErr: "default drift",
		},
		{
			name: "primary key drift",
			extra: drift.Change{
				Kind:      drift.PKChange,
				Schema:    "dbo",
				TableName: "Users",
				Previous:  "display_name",
				Current:   "id",
			},
			wantErr: "primary-key drift",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := drift.Report{Changes: []drift.Change{
				{
					Kind:       drift.TypeWidened,
					Schema:     "dbo",
					TableName:  "Users",
					ObjectName: "display_name",
					Previous:   "varchar(100)",
					Current:    "varchar(255)",
				},
				tt.extra,
			}}
			tables := []source.Table{{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "display_name", DataType: "varchar", MaxLength: 255, IsNullable: true},
				},
			}}

			_, _, err := planTypeEvolution(report, tables, config.SchemaEvolutionAuto)
			if err == nil {
				t.Fatal("planTypeEvolution returned nil error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestShouldApplySchemaEvolutionOnlyForOptInUpsert(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}
	nullabilityReport := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NOT NULL",
		Current:    "NULL",
	}}}
	typeReport := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeWidened,
		TableName:  "Users",
		ObjectName: "display_name",
		Previous:   "varchar(100)",
		Current:    "varchar(255)",
	}}}

	tests := []struct {
		name      string
		migration config.MigrationConfig
		want      bool
		report    drift.Report
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
		{name: "log policy reports only", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionLog,
			},
		}, want: false},
		{name: "fail policy runs gate", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionFail,
			},
		}, want: true},
		{name: "discard policy prunes without target step", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionDiscardValue,
			},
		}, want: false},
		{name: "schema contract report is report only", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractReport},
		}, want: false},
		{name: "schema contract freeze runs gate", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractFreeze},
		}, want: true},
		{name: "schema contract discard value prunes without target step", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
		}, want: false},
		{name: "schema contract discard row prunes without target step", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardRow},
		}, want: false},
		{name: "nullability enabled", migration: config.MigrationConfig{
			TargetMode:      "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{},
		}, want: true, report: nullabilityReport},
		{name: "nullability log policy reports only", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				NullabilityChange: config.SchemaEvolutionLog,
			},
		}, want: false, report: nullabilityReport},
		{name: "type change omitted reports only", migration: config.MigrationConfig{
			TargetMode:      "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{},
		}, want: false, report: typeReport},
		{name: "type change auto runs gate", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				TypeChange: config.SchemaEvolutionAuto,
			},
		}, want: true, report: typeReport},
		{name: "type change fail runs gate", migration: config.MigrationConfig{
			TargetMode: "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{
				TypeChange: config.SchemaEvolutionFail,
			},
		}, want: true, report: typeReport},
		{name: "schema contract data type report is report only", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractReport},
		}, want: false, report: typeReport},
		{name: "schema contract data type discard row prunes without target step", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardRow},
		}, want: false, report: typeReport},
		{name: "schema contract data type discard value prunes without target step", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
		}, want: false, report: typeReport},
		{name: "schema contract data type evolve runs gate", migration: config.MigrationConfig{
			TargetMode:     "upsert",
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
		}, want: true, report: typeReport},
		{name: "unsupported drift only", migration: config.MigrationConfig{
			TargetMode:      "upsert",
			SchemaEvolution: &config.SchemaEvolutionConfig{},
		}, want: false, report: drift.Report{Changes: []drift.Change{{Kind: drift.DroppedColumn, TableName: "Users"}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := report
			if tt.report.HasChanges() {
				report = tt.report
			}
			e := &Engine{cfg: &config.Config{Migration: tt.migration}}
			if got := e.ShouldApplyEvolution(report); got != tt.want {
				t.Fatalf("ShouldApplyEvolution() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSchemaDriftReportFooterDescribesEffectiveSchemaEvolutionOutcome(t *testing.T) {
	addedColumnReport := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}
	nullabilityReport := drift.Report{Changes: []drift.Change{{Kind: drift.NullabilityChange, TableName: "Users"}}}
	safeNullabilityReport := drift.Report{Changes: []drift.Change{{
		Kind:      drift.NullabilityChange,
		TableName: "Users",
		Previous:  "NOT NULL",
		Current:   "NULL",
	}}}
	typeWidenedReport := drift.Report{Changes: []drift.Change{{Kind: drift.TypeWidened, TableName: "Users", ObjectName: "name"}}}
	typeNarrowedReport := drift.Report{Changes: []drift.Change{{Kind: drift.TypeNarrowed, TableName: "Users", ObjectName: "name"}}}
	tableDroppedReport := drift.Report{Changes: []drift.Change{{Kind: drift.TableDropped, TableName: "Legacy"}}}
	unsupportedReport := drift.Report{Changes: []drift.Change{{Kind: drift.DroppedColumn, TableName: "Users"}}}

	tests := []struct {
		name      string
		report    drift.Report
		allow     bool
		migration config.MigrationConfig
		want      string
	}{
		{
			name:      "resume remains read only",
			report:    addedColumnReport,
			allow:     false,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{}},
			want:      "read-only mode",
		},
		{
			name:   "resume discard policy describes effective pruning",
			report: addedColumnReport,
			allow:  false,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionDiscardValue,
			}},
			want: "omitted from target DDL, transfer, validation, and schema snapshots",
		},
		{
			name:      "auto added columns may apply",
			report:    addedColumnReport,
			allow:     true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{}},
			want:      "added_column=auto",
		},
		{
			name:      "auto nullability changes may apply",
			report:    nullabilityReport,
			allow:     true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{}},
			want:      "nullability_change=auto",
		},
		{
			name:      "type changes default to report only",
			report:    typeWidenedReport,
			allow:     true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{}},
			want:      "type_change=log",
		},
		{
			name:   "auto widened type changes may apply",
			report: typeWidenedReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				TypeChange: config.SchemaEvolutionAuto,
			}},
			want: "widened type change",
		},
		{
			name:   "auto narrowed type changes abort",
			report: typeNarrowedReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				TypeChange: config.SchemaEvolutionAuto,
			}},
			want: "will abort",
		},
		{
			name:   "log policy reports only",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionLog,
			}},
			want: "reported only",
		},
		{
			name:   "fail policy names abort",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionFail,
			}},
			want: "will abort",
		},
		{
			name:   "discard policy names omitted transfer",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionDiscardValue,
			}},
			want: "discard_value",
		},
		{
			name:   "discard policy works outside upsert",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{TargetMode: "drop_recreate", SchemaEvolution: &config.SchemaEvolutionConfig{
				AddedColumn: config.SchemaEvolutionDiscardValue,
			}},
			want: "omitted from target DDL",
		},
		{
			name:      "unsupported drift only",
			report:    unsupportedReport,
			allow:     true,
			migration: config.MigrationConfig{TargetMode: "upsert", SchemaEvolution: &config.SchemaEvolutionConfig{}},
			want:      "no currently supported",
		},
		{
			name:   "fail on schema drift wins",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:        "upsert",
				SchemaEvolution:   &config.SchemaEvolutionConfig{},
				FailOnSchemaDrift: true,
			},
			want: "fail_on_schema_drift",
		},
		{
			name:   "schema contract report mode names report",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractReport},
			},
			want: "columns=report",
		},
		{
			name:   "schema contract resume discard still describes pruning",
			report: addedColumnReport,
			allow:  false,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
			},
			want: "omitted from target DDL",
		},
		{
			name:   "schema contract resume freeze still describes abort",
			report: addedColumnReport,
			allow:  false,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractFreeze},
			},
			want: "columns=freeze",
		},
		{
			name:   "schema contract column discard row names skipped table",
			report: addedColumnReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardRow},
			},
			want: "columns=discard_row",
		},
		{
			name:   "schema contract dropped source column names retained target",
			report: drift.Report{Changes: []drift.Change{{Kind: drift.DroppedColumn, TableName: "Users", ObjectName: "legacy_code"}}},
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractReport},
			},
			want: "target columns are retained",
		},
		{
			name:   "schema contract data type report mode names report",
			report: typeWidenedReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractReport},
			},
			want: "data_type=report",
		},
		{
			name:   "schema contract data type discard row names skipped table",
			report: typeWidenedReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardRow},
			},
			want: "data_type=discard_row",
		},
		{
			name:   "schema contract data type discard value names omitted column",
			report: typeWidenedReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
			},
			want: "data_type=discard_value",
		},
		{
			name:   "schema contract data type evolve unsafe names abort",
			report: typeNarrowedReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
			},
			want: "will abort",
		},
		{
			name:   "schema contract data type evolve safe nullability names relaxation",
			report: safeNullabilityReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
			},
			want: "nullability relaxation",
		},
		{
			name:   "schema contract freeze mode names abort",
			report: drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, TableName: "Orders"}}},
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractFreeze},
			},
			want: "tables=freeze",
		},
		{
			name:   "schema contract table discard row names skip",
			report: drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, TableName: "Orders"}}},
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractDiscardRow},
			},
			want: "tables=discard_row",
		},
		{
			name:   "schema contract table dropped is report only",
			report: tableDroppedReport,
			allow:  true,
			migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
			want: "target tables are retained",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Engine{cfg: &config.Config{Migration: tt.migration}}
			got := e.schemaDriftReportFooter(tt.report, tt.allow)
			if !strings.Contains(got, tt.want) {
				t.Fatalf("schemaDriftReportFooter() = %q, want substring %q", got, tt.want)
			}
		})
	}
}

func TestSchemaContractTablesDiscardRowSkipsAddedTables(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractDiscardRow},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"},
		{Kind: drift.TableDropped, Schema: "dbo", TableName: "legacy"},
	}}
	tables := []source.Table{
		{Schema: "dbo", Name: "users"},
		{Schema: "dbo", Name: "orders"},
	}

	got, err := e.EffectiveTables(report, tables)
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("effective tables = %#v, want only users", got)
	}
}

func TestSchemaContractColumnsDiscardRowSkipsTablesWithAddedColumns(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardRow},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "orders", ObjectName: "new_status"},
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "orders", ObjectName: "new_note"},
		{Kind: drift.DroppedColumn, Schema: "dbo", TableName: "users", ObjectName: "legacy_code"},
	}}
	tables := []source.Table{
		{Schema: "dbo", Name: "users"},
		{Schema: "dbo", Name: "orders"},
	}

	got, err := e.EffectiveTables(report, tables)
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("effective tables = %#v, want only users", got)
	}
}

func TestFilterSchemaDriftReportForTablesRemovesSkippedTableChanges(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "orders", ObjectName: "new_status"},
		{Kind: drift.TypeWidened, Schema: "dbo", TableName: "orders", ObjectName: "description"},
		{Kind: drift.TypeWidened, Schema: "dbo", TableName: "users", ObjectName: "display_name"},
	}}
	tables := []source.Table{{Schema: "dbo", Name: "users"}}

	got := FilterDriftReportForTables(report, tables)
	if len(got.Changes) != 1 {
		t.Fatalf("filtered changes = %#v, want one change", got.Changes)
	}
	if got.Changes[0].TableName != "users" {
		t.Fatalf("filtered change table = %q, want users", got.Changes[0].TableName)
	}
}

func TestSchemaContractColumnsDiscardValueRejectsAddedIdentityColumns(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "orders", ObjectName: "line_id"},
	}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "orders",
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
			{Name: "line_id", DataType: "int", IsIdentity: true},
		},
	}}

	_, err := e.EffectiveTables(report, tables)
	if err == nil {
		t.Fatal("EffectiveTables() error = nil, want identity discard error")
	}
	if !strings.Contains(err.Error(), "identity column") {
		t.Fatalf("error = %q, want identity column message", err)
	}
}

func TestSchemaContractDataTypeDiscardRowSkipsTablesWithDataTypeChanges(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardRow},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TypeWidened, Schema: "dbo", TableName: "orders", ObjectName: "description"},
		{Kind: drift.NullabilityChange, Schema: "dbo", TableName: "orders", ObjectName: "note"},
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "users", ObjectName: "email"},
	}}
	tables := []source.Table{
		{Schema: "dbo", Name: "users"},
		{Schema: "dbo", Name: "orders"},
	}

	got, err := e.EffectiveTables(report, tables)
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("effective tables = %#v, want only users", got)
	}
}

func TestSchemaContractDataTypeDiscardRowSnapshotPlanOmitsSkippedTables(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardRow},
		}},
	}
	previous := source.Table{
		Schema: "dbo",
		Name:   "orders",
		Columns: []source.Column{
			{Name: "id", DataType: "int", OrdinalPos: 1},
			{Name: "description", DataType: "varchar", MaxLength: 255, OrdinalPos: 2},
		},
	}
	current := previous
	current.Columns = []source.Column{
		{Name: "id", DataType: "int", OrdinalPos: 1},
		{Name: "description", DataType: "varchar", MaxLength: 100, OrdinalPos: 2},
	}
	users := source.Table{
		Schema: "dbo",
		Name:   "users",
		Columns: []source.Column{
			{Name: "id", DataType: "int", OrdinalPos: 1},
		},
	}
	report := drift.Compare(
		[]drift.TableSnapshot{drift.BuildTableSnapshot(previous), drift.BuildTableSnapshot(users)},
		[]drift.TableSnapshot{drift.BuildTableSnapshot(current), drift.BuildTableSnapshot(users)},
	)

	effective, err := e.EffectiveTables(report, []source.Table{current, users})
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	snapshots := e.schemaSnapshotPlan(report, effective)
	if len(snapshots) != 1 || snapshots[0].Name != "users" {
		t.Fatalf("snapshots = %#v, want only non-skipped users table", snapshots)
	}
}

func TestSchemaContractDataTypeDiscardValuePrunesAffectedColumns(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TypeNarrowed, Schema: "dbo", TableName: "orders", ObjectName: "description"},
		{Kind: drift.TypeWidened, Schema: "dbo", TableName: "orders", ObjectName: "note"},
	}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
			{Name: "description", DataType: "varchar", MaxLength: 100},
			{Name: "note", DataType: "varchar", MaxLength: 500},
			{Name: "amount", DataType: "decimal"},
		},
		Indexes: []source.Index{
			{Name: "ix_orders_description", Columns: []string{"description"}},
			{Name: "ix_orders_note", Columns: []string{"note"}},
			{Name: "ix_orders_amount", Columns: []string{"amount"}},
		},
		CheckConstraints: []source.CheckConstraint{
			{Name: "chk_orders_description", Definition: "description <> ''"},
			{Name: "chk_orders_note", Definition: "note <> ''"},
			{Name: "chk_orders_amount", Definition: "amount >= 0"},
		},
	}}

	got, err := e.EffectiveTables(report, tables)
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(effective tables) = %d, want 1", len(got))
	}
	if len(got[0].Columns) != 2 {
		t.Fatalf("columns = %#v, want description and note pruned", got[0].Columns)
	}
	for _, column := range got[0].Columns {
		if column.Name == "description" || column.Name == "note" {
			t.Fatalf("discarded data type column was not pruned: %#v", got[0].Columns)
		}
	}
	if len(got[0].Indexes) != 1 || got[0].Indexes[0].Name != "ix_orders_amount" {
		t.Fatalf("indexes = %#v, want only ix_orders_amount", got[0].Indexes)
	}
	if len(got[0].CheckConstraints) != 1 || got[0].CheckConstraints[0].Name != "chk_orders_amount" {
		t.Fatalf("checks = %#v, want only chk_orders_amount", got[0].CheckConstraints)
	}
}

func TestSchemaContractDataTypeDiscardValueAllowsOnlyRequiredColumnsToRemain(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
		}},
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TypeChangedLossy, Schema: "dbo", TableName: "orders", ObjectName: "description"},
		{Kind: drift.NullabilityChange, Schema: "dbo", TableName: "orders", ObjectName: "note"},
	}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
			{Name: "description", DataType: "varchar", MaxLength: 100},
			{Name: "note", DataType: "varchar", MaxLength: 500},
		},
	}}

	got, err := e.EffectiveTables(report, tables)
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	if len(got) != 1 || len(got[0].Columns) != 1 || got[0].Columns[0].Name != "id" {
		t.Fatalf("columns = %#v, want only required id column", got)
	}
	if len(got[0].PrimaryKey) != 1 || got[0].PrimaryKey[0] != "id" {
		t.Fatalf("primary key = %#v, want id retained", got[0].PrimaryKey)
	}
}

func TestSchemaContractDataTypeDiscardValueSnapshotPlanRetainsPreviousDiscardedMetadata(t *testing.T) {
	e := &Engine{
		cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
		}},
	}
	previous := source.Table{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", OrdinalPos: 1},
			{Name: "description", DataType: "varchar", MaxLength: 255, OrdinalPos: 2},
			{Name: "amount", DataType: "decimal", OrdinalPos: 3},
		},
		Indexes: []source.Index{
			{Name: "ix_orders_description", Columns: []string{"description"}},
			{Name: "ix_orders_amount", Columns: []string{"amount"}},
		},
		CheckConstraints: []source.CheckConstraint{
			{Name: "chk_orders_description", Definition: "description <> ''"},
			{Name: "chk_orders_amount", Definition: "amount >= 0"},
		},
	}
	current := previous
	current.Columns = []source.Column{
		{Name: "id", DataType: "int", OrdinalPos: 1},
		{Name: "description", DataType: "varchar", MaxLength: 100, OrdinalPos: 2},
		{Name: "amount", DataType: "decimal", OrdinalPos: 3},
	}
	items := source.Table{
		Schema: "dbo",
		Name:   "order_items",
		Columns: []source.Column{
			{Name: "id", DataType: "int", OrdinalPos: 1},
			{Name: "order_description", DataType: "varchar", MaxLength: 255, OrdinalPos: 2},
		},
		ForeignKeys: []source.ForeignKey{{
			Name:       "fk_items_orders_description",
			Columns:    []string{"order_description"},
			RefTable:   "orders",
			RefColumns: []string{"description"},
		}},
	}

	report := drift.Compare(
		[]drift.TableSnapshot{drift.BuildTableSnapshot(previous), drift.BuildTableSnapshot(items)},
		[]drift.TableSnapshot{drift.BuildTableSnapshot(current), drift.BuildTableSnapshot(items)},
	)
	effective, err := e.EffectiveTables(report, []source.Table{current, items})
	if err != nil {
		t.Fatalf("EffectiveTables() error: %v", err)
	}
	var effectiveItems *source.Table
	for i := range effective {
		if effective[i].Name == "order_items" {
			effectiveItems = &effective[i]
			break
		}
	}
	if effectiveItems == nil {
		t.Fatalf("effective tables = %#v, want order_items retained", effective)
	}
	if len(effectiveItems.ForeignKeys) != 0 {
		t.Fatalf("effective order_items foreign keys = %#v, want FK referencing discarded column pruned", effectiveItems.ForeignKeys)
	}
	snapshots := e.schemaSnapshotPlan(report, effective)
	if len(snapshots) != 2 {
		t.Fatalf("snapshot count = %d, want 2", len(snapshots))
	}

	ordersSnapshot := snapshots[1]
	itemsSnapshot := snapshots[0]
	if ordersSnapshot.Name != "orders" {
		ordersSnapshot, itemsSnapshot = itemsSnapshot, ordersSnapshot
	}
	var description *drift.ColumnSnapshot
	for i := range ordersSnapshot.Columns {
		if ordersSnapshot.Columns[i].Name == "description" {
			description = &ordersSnapshot.Columns[i]
			break
		}
	}
	if description == nil {
		t.Fatalf("snapshot columns = %#v, want previous description metadata retained", ordersSnapshot.Columns)
	}
	if description.MaxLength != 255 {
		t.Fatalf("description max length = %d, want previous 255", description.MaxLength)
	}
	if len(ordersSnapshot.Indexes) != 2 {
		t.Fatalf("snapshot indexes = %#v, want previous discarded-column index retained", ordersSnapshot.Indexes)
	}
	if len(ordersSnapshot.CheckConstraints) != 2 {
		t.Fatalf("snapshot checks = %#v, want previous discarded-column check retained", ordersSnapshot.CheckConstraints)
	}
	if len(itemsSnapshot.ForeignKeys) != 1 {
		t.Fatalf("item snapshot foreign keys = %#v, want referenced discarded-column FK retained", itemsSnapshot.ForeignKeys)
	}

	nextReport := drift.Compare(snapshots, []drift.TableSnapshot{drift.BuildTableSnapshot(current), drift.BuildTableSnapshot(items)})
	if len(addedColumnChanges(nextReport)) != 0 {
		t.Fatalf("next report added columns = %#v, want discarded data type column to remain a type change", addedColumnChanges(nextReport))
	}
	if got := len(typeNarrowedOrLossyChanges(nextReport)); got != 1 {
		t.Fatalf("next report narrowed/lossy changes = %d, want 1", got)
	}
	for _, change := range nextReport.Changes {
		if change.Kind == drift.FKAdded {
			t.Fatalf("next report includes FKAdded = %#v, want referenced FK retained in snapshot", change)
		}
	}
}

func TestSchemaContractDataTypeDiscardValueRejectsPrimaryKeyAndIdentityColumns(t *testing.T) {
	tests := []struct {
		name    string
		table   source.Table
		wantErr string
	}{
		{
			name: "primary key",
			table: source.Table{
				Schema:     "dbo",
				Name:       "orders",
				PrimaryKey: []string{"id"},
				Columns: []source.Column{
					{Name: "id", DataType: "int"},
				},
			},
			wantErr: "primary-key column",
		},
		{
			name: "identity",
			table: source.Table{
				Schema: "dbo",
				Name:   "orders",
				Columns: []source.Column{
					{Name: "id", DataType: "int", IsIdentity: true},
				},
			},
			wantErr: "identity column",
		},
		{
			name: "resolved date tracking column",
			table: source.Table{
				Schema:     "dbo",
				Name:       "orders",
				DateColumn: "updated_at",
				Columns: []source.Column{
					{Name: "updated_at", DataType: "datetime2"},
				},
			},
			wantErr: "date tracking column",
		},
		{
			name: "configured date tracking candidate",
			table: source.Table{
				Schema: "dbo",
				Name:   "orders",
				Columns: []source.Column{
					{Name: "updated_at", DataType: "datetime2"},
				},
			},
			wantErr: "date tracking column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Engine{
				cfg: &config.Config{Migration: config.MigrationConfig{
					DateUpdatedColumns: []string{"updated_at"},
					SchemaContract:     &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
				}},
			}
			columnName := "id"
			if tt.wantErr == "date tracking column" {
				columnName = "updated_at"
			}
			report := drift.Report{Changes: []drift.Change{
				{Kind: drift.TypeChangedLossy, Schema: "dbo", TableName: "orders", ObjectName: columnName},
			}}

			_, err := e.EffectiveTables(report, []source.Table{tt.table})
			if err == nil {
				t.Fatal("EffectiveTables() error = nil, want discard guardrail error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaContractTableEvolveCreatesAddedTablesBeforeUpsert(t *testing.T) {
	targetPool := &fakeTargetPool{existing: map[string]bool{"users": true}}
	e := &Engine{
		cfg: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false},
			{Name: "name", DataType: "varchar", MaxLength: 255, IsNullable: true},
		},
	}}

	if err := e.ApplyContractTableEvolution(context.Background(), report, tables); err != nil {
		t.Fatalf("ApplyContractTableEvolution() error: %v", err)
	}
	if got, want := targetPool.createdTables(), []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created tables = %v, want %v", got, want)
	}
	if got, want := targetPool.primaryKeyTables(), []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("primary key tables = %v, want %v", got, want)
	}
}

func TestSchemaContractTableEvolveSkipsExistingTargetTables(t *testing.T) {
	targetPool := &fakeTargetPool{existing: map[string]bool{"orders": true}}
	e := &Engine{
		cfg: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{Schema: "dbo", Name: "orders", PrimaryKey: []string{"id"}}}

	if err := e.ApplyContractTableEvolution(context.Background(), report, tables); err != nil {
		t.Fatalf("ApplyContractTableEvolution() error: %v", err)
	}
	if got := targetPool.createdTables(); len(got) != 0 {
		t.Fatalf("created tables = %v, want none", got)
	}
	if got := targetPool.primaryKeyTables(); len(got) != 0 {
		t.Fatalf("primary key tables = %v, want none", got)
	}
}

func TestSchemaContractTableEvolveRequiresPrimaryKeyForUpsert(t *testing.T) {
	targetPool := &fakeTargetPool{}
	e := &Engine{
		cfg: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{Schema: "dbo", Name: "orders"}}

	err := e.ApplyContractTableEvolution(context.Background(), report, tables)
	if err == nil {
		t.Fatal("ApplyContractTableEvolution() error = nil, want primary-key error")
	}
	if !strings.Contains(err.Error(), "requires a primary key") {
		t.Fatalf("error = %q, want primary-key message", err)
	}
}

func TestFinalizeSchemaContractTableEvolutionCreatesPostTransferDDL(t *testing.T) {
	targetPool := &fakeTargetPool{}
	e := &Engine{
		cfg: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:             "upsert",
				SchemaContract:         &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
				CreateCheckConstraints: true,
			},
		},
		targetPool: targetPool,
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Indexes: []source.Index{
			{Name: "ix_orders_name", Columns: []string{"name"}},
		},
		ForeignKeys: []source.ForeignKey{
			{Name: "fk_orders_customer", Columns: []string{"customer_id"}, RefTable: "customers", RefColumns: []string{"id"}},
		},
		CheckConstraints: []source.CheckConstraint{
			{Name: "chk_orders_amount", Definition: "amount >= 0"},
		},
	}}

	e.FinalizeContractTableEvolution(context.Background(), report, tables)

	if got, want := targetPool.resetTables(), []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reset tables = %v, want %v", got, want)
	}
	if got, want := targetPool.createdIndexes(), []string{"orders.ix_orders_name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created indexes = %v, want %v", got, want)
	}
	if got, want := targetPool.createdForeignKeys(), []string{"orders.fk_orders_customer"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created foreign keys = %v, want %v", got, want)
	}
	if got, want := targetPool.createdChecks(), []string{"orders.chk_orders_amount"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created checks = %v, want %v", got, want)
	}
}

func enforceSchemaContractDecisionsForTest(e *Engine, report drift.Report) error {
	return enforceSchemaContractDecisions(e.schemaContractDecisions(report, nil, true))
}

func TestSchemaContractFreezeFailsBeforeTargetPreparation(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{
			Tables:   config.SchemaContractFreeze,
			Columns:  config.SchemaContractFreeze,
			DataType: config.SchemaContractFreeze,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, TableName: "Orders"},
		{Kind: drift.AddedColumn, TableName: "Users", ObjectName: "email"},
		{Kind: drift.TypeWidened, TableName: "Users", ObjectName: "name"},
	}}

	err := enforceSchemaContractDecisionsForTest(e, report)
	if err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want freeze violation")
	}
	for _, want := range []string{
		"tables=freeze blocked table_added on Orders",
		"columns=freeze blocked added_column on Users.email",
		"data_type=freeze blocked type_widened on Users.name",
		"choose report to observe only",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want substring %q", err, want)
		}
	}
}

func TestSchemaContractTableFreezeFormatsRealDriftTableTargetOnce(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractFreeze},
	}}}
	report := drift.Compare(nil, []drift.TableSnapshot{drift.BuildTableSnapshot(source.Table{
		Schema: "dbo",
		Name:   "Orders",
	})})

	err := enforceSchemaContractDecisionsForTest(e, report)
	if err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want freeze violation")
	}
	if !strings.Contains(err.Error(), "tables=freeze blocked table_added on dbo.Orders") {
		t.Fatalf("error = %q, want one schema-qualified table target", err)
	}
	if strings.Contains(err.Error(), "dbo.Orders.Orders") {
		t.Fatalf("error = %q, duplicated table object name", err)
	}
}

func TestSchemaContractDecisionsIncludeStructuredContext(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{
			Tables:   config.SchemaContractDiscardRow,
			Columns:  config.SchemaContractDiscardValue,
			DataType: config.SchemaContractFreeze,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, Schema: "dbo", TableName: "Orders"},
		{Kind: drift.AddedColumn, Schema: "dbo", TableName: "Users", ObjectName: "email", Current: "varchar(255) NULL"},
		{Kind: drift.TypeNarrowed, Schema: "dbo", TableName: "Users", ObjectName: "name", Previous: "varchar(255)", Current: "varchar(50)"},
	}}

	got := e.schemaContractDecisions(report, nil, true)
	if len(got) != 3 {
		t.Fatalf("decision count = %d, want 3: %#v", len(got), got)
	}
	want := []SchemaContractDecision{
		{
			Entity: SchemaContractEntityTables,
			Mode:   string(config.SchemaContractDiscardRow),
			Drift:  string(drift.TableAdded),
			Schema: "dbo",
			Table:  "Orders",
			Action: SchemaContractActionDiscardedRow,
		},
		{
			Entity:  SchemaContractEntityColumns,
			Mode:    string(config.SchemaContractDiscardValue),
			Drift:   string(drift.AddedColumn),
			Schema:  "dbo",
			Table:   "Users",
			Object:  "email",
			Current: "varchar(255) NULL",
			Action:  SchemaContractActionDiscardedValue,
		},
		{
			Entity:   SchemaContractEntityDataType,
			Mode:     string(config.SchemaContractFreeze),
			Drift:    string(drift.TypeNarrowed),
			Schema:   "dbo",
			Table:    "Users",
			Object:   "name",
			Previous: "varchar(255)",
			Current:  "varchar(50)",
			Action:   SchemaContractActionFrozen,
		},
	}
	for i := range want {
		if got[i].Entity != want[i].Entity ||
			got[i].Mode != want[i].Mode ||
			got[i].Drift != want[i].Drift ||
			got[i].Schema != want[i].Schema ||
			got[i].Table != want[i].Table ||
			got[i].Object != want[i].Object ||
			got[i].Previous != want[i].Previous ||
			got[i].Current != want[i].Current ||
			got[i].Action != want[i].Action {
			t.Fatalf("decision[%d] = %#v, want matching %#v", i, got[i], want[i])
		}
		if got[i].Reason == "" {
			t.Fatalf("decision[%d] missing reason: %#v", i, got[i])
		}
	}
}

func TestSchemaContractDecisionsReportDroppedObjectsUnderDiscardModes(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{
			Tables:  config.SchemaContractDiscardRow,
			Columns: config.SchemaContractDiscardValue,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableDropped, Schema: "dbo", TableName: "Legacy"},
		{Kind: drift.DroppedColumn, Schema: "dbo", TableName: "Users", ObjectName: "legacy_code"},
	}}

	got := e.schemaContractDecisions(report, nil, true)
	if len(got) != 2 {
		t.Fatalf("decision count = %d, want 2: %#v", len(got), got)
	}
	for _, decision := range got {
		if decision.Action != SchemaContractActionReported {
			t.Fatalf("decision = %#v, want reported action for dropped object under discard modes", decision)
		}
		if decision.Reason == "" {
			t.Fatalf("decision missing reason: %#v", decision)
		}
	}
}

func TestSchemaContractDataTypeEvolveUnsafeDecisionIsBlocked(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeChangedLossy,
		TableName:  "Users",
		ObjectName: "name",
		Previous:   "varchar(255)",
		Current:    "int",
	}, {
		Kind:       drift.NullabilityChange,
		TableName:  "Users",
		ObjectName: "status",
		Previous:   "NULL",
		Current:    "NOT NULL",
	}}}

	decisions := e.schemaContractDecisions(report, nil, true)
	if len(decisions) != 2 {
		t.Fatalf("decision count = %d, want 2: %#v", len(decisions), decisions)
	}
	for _, decision := range decisions {
		if decision.Mode != string(config.SchemaContractEvolve) ||
			decision.Action != SchemaContractActionBlocked {
			t.Fatalf("decision = %#v, want mode evolve action blocked", decision)
		}
	}
	err := enforceSchemaContractDecisionsForTest(e, report)
	if err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want blocked unsafe evolve violation")
	}
	if !strings.Contains(err.Error(), "data_type=evolve blocked") {
		t.Fatalf("error = %q, want data_type=evolve blocked", err)
	}
}

func TestSchemaContractDataTypeEvolveSafeNullabilityRelaxationDecisionEvolves(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		TableName:  "Users",
		ObjectName: "nickname",
		Previous:   "NOT NULL",
		Current:    "NULL",
	}}}

	decisions := e.schemaContractDecisions(report, nil, true)
	if len(decisions) != 1 {
		t.Fatalf("decision count = %d, want 1: %#v", len(decisions), decisions)
	}
	if decisions[0].Action != SchemaContractActionEvolved {
		t.Fatalf("decision = %#v, want evolved action for safe nullability relaxation", decisions[0])
	}
}

func TestSchemaContractDataTypeDiscardValueRequiredColumnDecisionIsBlocked(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractDiscardValue},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeNarrowed,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "id",
		Previous:   "bigint",
		Current:    "int",
	}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
		},
	}}

	decisions := e.schemaContractDecisions(report, tables, true)
	if len(decisions) != 1 {
		t.Fatalf("decision count = %d, want 1: %#v", len(decisions), decisions)
	}
	if decisions[0].Action != SchemaContractActionBlocked {
		t.Fatalf("decision = %#v, want blocked action for required column discard", decisions[0])
	}
	if err := enforceSchemaContractDecisions(decisions); err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want blocked required-column discard")
	}
}

func TestSchemaContractColumnDiscardValueDateTrackingDecisionIsBlocked(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		DateUpdatedColumns: []string{"updated_at"},
		SchemaContract:     &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "updated_at",
		Current:    "datetime2 NULL",
	}}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
			{Name: "updated_at", DataType: "datetime2"},
		},
	}}

	decisions := e.schemaContractDecisions(report, tables, true)
	if len(decisions) != 1 {
		t.Fatalf("decision count = %d, want 1: %#v", len(decisions), decisions)
	}
	if decisions[0].Action != SchemaContractActionBlocked {
		t.Fatalf("decision = %#v, want blocked action for date tracking column discard", decisions[0])
	}
	if _, _, err := pruneDiscardedAddedColumns(report, tables, e.cfg.Migration.DateUpdatedColumns); err == nil {
		t.Fatal("pruneDiscardedAddedColumns() error = nil, want date tracking discard guardrail")
	}
}

func TestSchemaContractDataTypeEvolveCombinedDriftDecisionIsBlocked(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.TypeWidened,
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "varchar(50)",
			Current:    "varchar(255)",
		},
		{
			Kind:       drift.NullabilityChange,
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "NOT NULL",
			Current:    "NULL",
		},
	}}

	decisions := e.schemaContractDecisions(report, nil, true)
	if len(decisions) != 2 {
		t.Fatalf("decision count = %d, want 2: %#v", len(decisions), decisions)
	}
	for _, decision := range decisions {
		if decision.Action != SchemaContractActionBlocked {
			t.Fatalf("decision = %#v, want blocked action for combined data_type drift", decision)
		}
	}
}

func TestSchemaContractEvolveDecisionMirrorsPlannerGuardrails(t *testing.T) {
	tests := []struct {
		name    string
		config  config.SchemaContractConfig
		report  drift.Report
		tables  []source.Table
		wantErr string
	}{
		{
			name:   "added upsert table requires primary key",
			config: config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			report: drift.Report{Changes: []drift.Change{{
				Kind:      drift.TableAdded,
				Schema:    "dbo",
				TableName: "Orders",
			}}},
			tables:  []source.Table{{Schema: "dbo", Name: "Orders"}},
			wantErr: "upsert mode requires a primary key",
		},
		{
			name:   "added identity column",
			config: config.SchemaContractConfig{Columns: config.SchemaContractEvolve},
			report: drift.Report{Changes: []drift.Change{{
				Kind:       drift.AddedColumn,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "line_id",
			}}},
			tables: []source.Table{{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "id", DataType: "int"},
					{Name: "line_id", DataType: "int", IsIdentity: true},
				},
			}},
			wantErr: "identity column",
		},
		{
			name:   "data type primary key column",
			config: config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
			report: drift.Report{Changes: []drift.Change{{
				Kind:       drift.TypeWidened,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "id",
				Previous:   "int",
				Current:    "bigint",
			}}},
			tables: []source.Table{{
				Schema:     "dbo",
				Name:       "Users",
				PrimaryKey: []string{"id"},
				Columns: []source.Column{
					{Name: "id", DataType: "bigint"},
				},
			}},
			wantErr: "primary-key column",
		},
		{
			name:   "data type identity column",
			config: config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
			report: drift.Report{Changes: []drift.Change{{
				Kind:       drift.NullabilityChange,
				Schema:     "dbo",
				TableName:  "Users",
				ObjectName: "line_id",
				Previous:   "NOT NULL",
				Current:    "NULL",
			}}},
			tables: []source.Table{{
				Schema: "dbo",
				Name:   "Users",
				Columns: []source.Column{
					{Name: "line_id", DataType: "int", IsNullable: true, IsIdentity: true},
				},
			}},
			wantErr: "identity column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &tt.config,
			}}}
			decisions := e.schemaContractDecisions(tt.report, tt.tables, true)
			if len(decisions) == 0 {
				t.Fatal("schemaContractDecisions() returned no decisions")
			}
			if decisions[0].Action != SchemaContractActionBlocked {
				t.Fatalf("decision = %#v, want blocked action", decisions[0])
			}
			err := enforceSchemaContractDecisions(decisions)
			if err == nil {
				t.Fatal("enforceSchemaContractDecisions() error = nil, want blocked planner guardrail")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaContractDiscardRowDecisionPrecedenceReportsSkippedTableDrift(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode: "upsert",
		SchemaContract: &config.SchemaContractConfig{
			Columns:  config.SchemaContractDiscardRow,
			DataType: config.SchemaContractEvolve,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.AddedColumn,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Current:    "varchar(255) NULL",
		},
		{
			Kind:       drift.TypeNarrowed,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "varchar(255)",
			Current:    "varchar(50)",
		},
	}}

	decisions := e.schemaContractDecisions(report, nil, true)
	if len(decisions) != 2 {
		t.Fatalf("decision count = %d, want 2: %#v", len(decisions), decisions)
	}
	for _, decision := range decisions {
		if decision.Action == SchemaContractActionBlocked {
			t.Fatalf("decision = %#v, want skipped-table data_type drift to avoid blocked action", decision)
		}
	}
	if decisions[1].Action != SchemaContractActionReported ||
		!strings.Contains(decisions[1].Reason, "columns=discard_row skips table dbo.Users") {
		t.Fatalf("data_type decision = %#v, want report-only skipped-table reason", decisions[1])
	}
	if err := enforceSchemaContractDecisions(decisions); err != nil {
		t.Fatalf("enforceSchemaContractDecisions() error = %v, want nil for skipped table drift", err)
	}

	footer := e.schemaDriftReportFooterWithDecisions(report, true, decisions)
	if !strings.Contains(footer, "columns=discard_row; 1 change(s) will skip affected table(s) for this run") {
		t.Fatalf("footer = %q, want columns discard_row skip wording", footer)
	}
	if strings.Contains(footer, "will abort before transfer") {
		t.Fatalf("footer = %q, should not abort for skipped-table drift", footer)
	}
}

func TestSchemaContractEvolveDecisionsDoNotBlockDropRecreateTargetMode(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode: "drop_recreate",
		SchemaContract: &config.SchemaContractConfig{
			Columns:  config.SchemaContractEvolve,
			DataType: config.SchemaContractEvolve,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.AddedColumn,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "line_id",
			Current:    "int NOT NULL",
		},
		{
			Kind:       drift.TypeNarrowed,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "varchar(255)",
			Current:    "varchar(50)",
		},
	}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"line_id"},
		Columns: []source.Column{
			{Name: "line_id", DataType: "int", IsIdentity: true},
			{Name: "name", DataType: "varchar", MaxLength: 50},
		},
	}}

	decisions := e.schemaContractDecisions(report, tables, true)
	if len(decisions) != 2 {
		t.Fatalf("decision count = %d, want 2: %#v", len(decisions), decisions)
	}
	for _, decision := range decisions {
		if decision.Action != SchemaContractActionEvolved {
			t.Fatalf("decision = %#v, want evolved action for drop_recreate target rebuild", decision)
		}
	}
	if err := enforceSchemaContractDecisions(decisions); err != nil {
		t.Fatalf("enforceSchemaContractDecisions() error = %v, want nil for drop_recreate evolve", err)
	}
	footer := e.schemaDriftReportFooterWithDecisions(report, true, decisions)
	if strings.Contains(footer, "will abort before transfer") {
		t.Fatalf("footer = %q, should not abort for drop_recreate evolve decisions", footer)
	}
	if !strings.Contains(footer, "will follow target_mode behavior") {
		t.Fatalf("footer = %q, want target_mode behavior wording", footer)
	}
}

func TestSchemaContractDataTypeEvolveFooterBlocksCombinedDrift(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.TypeWidened,
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "varchar(50)",
			Current:    "varchar(255)",
		},
		{
			Kind:       drift.NullabilityChange,
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "NOT NULL",
			Current:    "NULL",
		},
	}}

	footer := e.schemaDriftReportFooter(report, true)
	if !strings.Contains(footer, "2 unsafe data type/nullability change(s) will abort before transfer") {
		t.Fatalf("footer = %q, want combined drift to be unsafe", footer)
	}
	if strings.Contains(footer, "may be applied before transfer") {
		t.Fatalf("footer = %q, should not claim combined drift may be applied", footer)
	}
}

func TestSchemaContractDecisionFooterBlocksRequiredDataTypeColumn(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode:     "upsert",
		SchemaContract: &config.SchemaContractConfig{DataType: config.SchemaContractEvolve},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.TypeWidened,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "id",
		Previous:   "int",
		Current:    "bigint",
	}}}
	tables := []source.Table{{
		Schema:     "dbo",
		Name:       "Users",
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "bigint"},
		},
	}}

	decisions := e.schemaContractDecisions(report, tables, true)
	footer := e.schemaDriftReportFooterWithDecisions(report, true, decisions)
	if !strings.Contains(footer, "data_type=evolve; 1 blocked change(s) will abort before transfer") {
		t.Fatalf("footer = %q, want blocked data_type=evolve wording", footer)
	}
	if strings.Contains(footer, "may be applied before transfer") {
		t.Fatalf("footer = %q, should not claim required-column drift may be applied", footer)
	}
}

func TestSchemaContractDecisionFooterSuppressesActionsWhenRunWillAbort(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		TargetMode: "upsert",
		SchemaContract: &config.SchemaContractConfig{
			Columns:  config.SchemaContractEvolve,
			DataType: config.SchemaContractEvolve,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{
			Kind:       drift.AddedColumn,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "email",
			Current:    "varchar(255) NULL",
		},
		{
			Kind:       drift.TypeNarrowed,
			Schema:     "dbo",
			TableName:  "Users",
			ObjectName: "name",
			Previous:   "varchar(255)",
			Current:    "varchar(50)",
		},
	}}

	decisions := e.schemaContractDecisions(report, nil, true)
	footer := e.schemaDriftReportFooterWithDecisions(report, true, decisions)
	if !strings.Contains(footer, "data_type=evolve; 1 blocked change(s) will abort before transfer") {
		t.Fatalf("footer = %q, want blocked data_type wording", footer)
	}
	if !strings.Contains(footer, "1 other change(s) will not be applied because transfer will abort") {
		t.Fatalf("footer = %q, want remaining change abort wording", footer)
	}
	for _, forbidden := range []string{
		"may be applied before transfer",
		"will be omitted",
		"will skip affected table",
	} {
		if strings.Contains(footer, forbidden) {
			t.Fatalf("footer = %q, should not contain %q when transfer will abort", footer, forbidden)
		}
	}
}

func TestSchemaContractDecisionFooterBlocksDiscardValueDateTrackingColumn(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		DateUpdatedColumns: []string{"updated_at"},
		SchemaContract:     &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
	}}}
	report := drift.Report{Changes: []drift.Change{{
		Kind:       drift.AddedColumn,
		Schema:     "dbo",
		TableName:  "Users",
		ObjectName: "updated_at",
		Current:    "datetime2 NULL",
	}}}
	tables := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "id", DataType: "int"},
			{Name: "updated_at", DataType: "datetime2"},
		},
	}}

	decisions := e.schemaContractDecisions(report, tables, true)
	footer := e.schemaDriftReportFooterWithDecisions(report, true, decisions)
	if !strings.Contains(footer, "columns=discard_value; 1 blocked change(s) will abort before transfer") {
		t.Fatalf("footer = %q, want blocked columns=discard_value wording", footer)
	}
	if strings.Contains(footer, "will be omitted") {
		t.Fatalf("footer = %q, should not claim blocked date-tracking column will be omitted", footer)
	}
}

func TestSchemaContractEvolveDecisionsAreReportedInReadOnlyMode(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{
			Tables:   config.SchemaContractEvolve,
			Columns:  config.SchemaContractEvolve,
			DataType: config.SchemaContractEvolve,
		},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, TableName: "Orders"},
		{Kind: drift.AddedColumn, TableName: "Users", ObjectName: "email"},
		{Kind: drift.TypeChangedLossy, TableName: "Users", ObjectName: "name"},
	}}

	decisions := e.schemaContractDecisions(report, nil, false)
	if len(decisions) != 3 {
		t.Fatalf("decision count = %d, want 3: %#v", len(decisions), decisions)
	}
	for _, decision := range decisions {
		if decision.Action != SchemaContractActionReported {
			t.Fatalf("decision = %#v, want reported action in read-only mode", decision)
		}
	}
	if err := enforceSchemaContractDecisions(decisions); err != nil {
		t.Fatalf("enforceSchemaContractDecisions() error = %v, want nil for read-only reported decisions", err)
	}
}

func TestSchemaContractNonFreezeModesDoNotRaiseFreezeViolations(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, TableName: "Orders"},
		{Kind: drift.AddedColumn, TableName: "Users", ObjectName: "email"},
		{Kind: drift.TypeWidened, TableName: "Users", ObjectName: "name"},
	}}
	modes := []config.SchemaContractMode{
		config.SchemaContractEvolve,
		config.SchemaContractDiscardRow,
		config.SchemaContractReport,
	}
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
				SchemaContract: &config.SchemaContractConfig{
					Tables:   mode,
					Columns:  mode,
					DataType: mode,
				},
			}}}
			if err := enforceSchemaContractDecisionsForTest(e, report); err != nil {
				t.Fatalf("enforceSchemaContractDecisions() error = %v, want nil", err)
			}
		})
	}

	t.Run("discard_value", func(t *testing.T) {
		e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{
				Columns:  config.SchemaContractDiscardValue,
				DataType: config.SchemaContractDiscardValue,
			},
		}}}
		if err := enforceSchemaContractDecisionsForTest(e, drift.Report{Changes: report.Changes[1:]}); err != nil {
			t.Fatalf("enforceSchemaContractDecisions() error = %v, want nil", err)
		}
	})
}

func TestAuditSchemaContractDecisionsWritesStructuredPayload(t *testing.T) {
	auditDir := t.TempDir()
	logger, err := audit.New(audit.Options{Dir: auditDir, RunID: "schema-contract", TamperEvident: true})
	if err != nil {
		t.Fatalf("audit.New() error: %v", err)
	}
	e := &Engine{audit: func(typeName string, fields map[string]any) {
		if err := logger.RecordEvent(audit.Event{Type: typeName, Fields: fields}); err != nil {
			t.Fatalf("audit record: %v", err)
		}
	}}
	decisions := []SchemaContractDecision{{
		Entity:  SchemaContractEntityColumns,
		Mode:    string(config.SchemaContractDiscardValue),
		Drift:   string(drift.AddedColumn),
		Schema:  "dbo",
		Table:   "Users",
		Object:  "email",
		Current: "varchar(255) NULL",
		Action:  SchemaContractActionDiscardedValue,
		Reason:  "columns=discard_value omits newly added source columns from the effective plan",
	}, {
		Entity:   SchemaContractEntityDataType,
		Mode:     string(config.SchemaContractEvolve),
		Drift:    string(drift.TypeWidened),
		Schema:   "dbo",
		Table:    "Users",
		Object:   "name",
		Previous: "varchar(50)",
		Current:  "varchar(255)",
		Action:   SchemaContractActionEvolved,
		Reason:   "data_type=evolve applies deterministic safe type/nullability changes",
	}, {
		Entity: SchemaContractEntityTables,
		Mode:   string(config.SchemaContractDiscardRow),
		Drift:  string(drift.TableAdded),
		Schema: "dbo",
		Table:  "Events",
		Action: SchemaContractActionDiscardedRow,
		Reason: "tables=discard_row skips newly added source tables for this run",
	}, {
		Entity: SchemaContractEntityTables,
		Mode:   string(config.SchemaContractFreeze),
		Drift:  string(drift.TableDropped),
		Schema: "dbo",
		Table:  "Legacy",
		Action: SchemaContractActionFrozen,
		Reason: "tables=freeze blocks table_dropped before transfer",
	}}

	e.auditSchemaContractDecisions(decisions)
	if err := logger.Close(); err != nil {
		t.Fatalf("audit Close() error: %v", err)
	}
	body, err := os.ReadFile(logger.Path())
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(body))), &event); err != nil {
		t.Fatalf("decode audit event: %v", err)
	}
	if event["type"] != "schema_contract_decisions" {
		t.Fatalf("event type = %v, want schema_contract_decisions", event["type"])
	}
	if event["count"] != float64(len(decisions)) {
		t.Fatalf("count = %v, want %d", event["count"], len(decisions))
	}
	rawDecisions, ok := event["decisions"].([]any)
	if !ok || len(rawDecisions) != len(decisions) {
		t.Fatalf("decisions = %#v, want %d decisions", event["decisions"], len(decisions))
	}

	for i, raw := range rawDecisions {
		decision, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("decision[%d] shape = %#v, want object", i, raw)
		}
		want := decisions[i]
		for key, value := range map[string]string{
			"entity":     want.Entity,
			"mode":       want.Mode,
			"drift_kind": want.Drift,
			"schema":     want.Schema,
			"table":      want.Table,
			"object":     want.Object,
			"previous":   want.Previous,
			"current":    want.Current,
			"action":     want.Action,
			"reason":     want.Reason,
		} {
			if value == "" {
				if _, exists := decision[key]; exists {
					t.Fatalf("decision[%d][%s] = %v, want omitted empty field", i, key, decision[key])
				}
				continue
			}
			if decision[key] != value {
				t.Fatalf("decision[%d][%s] = %v, want %q", i, key, decision[key], value)
			}
		}
	}

	readBack, err := readSchemaContractDecisionsFromAudit(auditDir, "schema-contract")
	if err != nil {
		t.Fatalf("readSchemaContractDecisionsFromAudit() error: %v", err)
	}
	if !reflect.DeepEqual(readBack, []SchemaContractDecision(decisions)) {
		t.Fatalf("readBack = %#v, want %#v", readBack, decisions)
	}

	hashOnDisk, ok := event["hash"].(string)
	if !ok || hashOnDisk == "" {
		t.Fatalf("event hash = %v, want tamper-evident hash", event["hash"])
	}
	prevHash, ok := event["prev_hash"].(string)
	if !ok || prevHash == "" {
		t.Fatalf("event prev_hash = %v, want tamper-evident prev_hash", event["prev_hash"])
	}
	delete(event, "hash")
	canonical, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("canonical marshal audit event: %v", err)
	}
	sum := sha256.Sum256(append([]byte(prevHash), canonical...))
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != hashOnDisk {
		t.Fatalf("tamper-evident hash = %s, want %s for canonical body %s", hashOnDisk, got, canonical)
	}
}

func TestSchemaContractTablesFreezeFailsOnDroppedTable(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractFreeze},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableDropped, TableName: "Legacy"},
	}}

	err := enforceSchemaContractDecisionsForTest(e, report)
	if err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want dropped-table freeze violation")
	}
	if !strings.Contains(err.Error(), "tables=freeze") {
		t.Fatalf("error = %q, want tables=freeze violation", err)
	}
}

func TestSchemaContractColumnsFreezeFailsOnDroppedColumn(t *testing.T) {
	e := &Engine{cfg: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractFreeze},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.DroppedColumn, TableName: "Users", ObjectName: "legacy_code"},
	}}

	err := enforceSchemaContractDecisionsForTest(e, report)
	if err == nil {
		t.Fatal("enforceSchemaContractDecisions() error = nil, want dropped-column freeze violation")
	}
	if !strings.Contains(err.Error(), "columns=freeze") {
		t.Fatalf("error = %q, want columns=freeze violation", err)
	}

	footer := e.schemaDriftReportFooter(report, true)
	if !strings.Contains(footer, "will abort before transfer") {
		t.Fatalf("footer = %q, want abort wording", footer)
	}
	if strings.Contains(footer, "target columns are retained") {
		t.Fatalf("footer = %q, should not describe report-only retention under freeze", footer)
	}
}
