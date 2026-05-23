package orchestrator

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/audit"
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

	pruned, discarded, err := pruneDiscardedAddedColumns(report, []source.Table{table})
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

	_, _, err := pruneDiscardedAddedColumns(report, []source.Table{table})
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
	o := &Orchestrator{config: &config.Config{Migration: config.MigrationConfig{
		TargetMode:      "upsert",
		SchemaEvolution: &config.SchemaEvolutionConfig{},
	}}}

	got := o.schemaDriftReportFooter(report, false)
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
			o := &Orchestrator{config: &config.Config{Migration: tt.migration}}
			if got := o.shouldApplySchemaEvolution(report); got != tt.want {
				t.Fatalf("shouldApplySchemaEvolution() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSchemaDriftReportFooterDescribesEffectiveSchemaEvolutionOutcome(t *testing.T) {
	addedColumnReport := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}
	nullabilityReport := drift.Report{Changes: []drift.Change{{Kind: drift.NullabilityChange, TableName: "Users"}}}
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
			o := &Orchestrator{config: &config.Config{Migration: tt.migration}}
			got := o.schemaDriftReportFooter(tt.report, tt.allow)
			if !strings.Contains(got, tt.want) {
				t.Fatalf("schemaDriftReportFooter() = %q, want substring %q", got, tt.want)
			}
		})
	}
}

func TestSchemaContractTablesDiscardRowSkipsAddedTables(t *testing.T) {
	o := &Orchestrator{
		config: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractDiscardRow},
		}},
		auditor: audit.Disabled(),
	}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"},
		{Kind: drift.TableDropped, Schema: "dbo", TableName: "legacy"},
	}}
	tables := []source.Table{
		{Schema: "dbo", Name: "users"},
		{Schema: "dbo", Name: "orders"},
	}

	got, err := o.effectiveTablesForSchemaEvolution(report, tables)
	if err != nil {
		t.Fatalf("effectiveTablesForSchemaEvolution() error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "users" {
		t.Fatalf("effective tables = %#v, want only users", got)
	}
}

func TestSchemaContractColumnsDiscardRowSkipsTablesWithAddedColumns(t *testing.T) {
	o := &Orchestrator{
		config: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardRow},
		}},
		auditor: audit.Disabled(),
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

	got, err := o.effectiveTablesForSchemaEvolution(report, tables)
	if err != nil {
		t.Fatalf("effectiveTablesForSchemaEvolution() error: %v", err)
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

	got := filterSchemaDriftReportForTables(report, tables)
	if len(got.Changes) != 1 {
		t.Fatalf("filtered changes = %#v, want one change", got.Changes)
	}
	if got.Changes[0].TableName != "users" {
		t.Fatalf("filtered change table = %q, want users", got.Changes[0].TableName)
	}
}

func TestSchemaContractColumnsDiscardValueRejectsAddedIdentityColumns(t *testing.T) {
	o := &Orchestrator{
		config: &config.Config{Migration: config.MigrationConfig{
			SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractDiscardValue},
		}},
		auditor: audit.Disabled(),
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

	_, err := o.effectiveTablesForSchemaEvolution(report, tables)
	if err == nil {
		t.Fatal("effectiveTablesForSchemaEvolution() error = nil, want identity discard error")
	}
	if !strings.Contains(err.Error(), "identity column") {
		t.Fatalf("error = %q, want identity column message", err)
	}
}

func TestSchemaContractTableEvolveCreatesAddedTablesBeforeUpsert(t *testing.T) {
	targetPool := &targetModeTestPool{existing: map[string]bool{"users": true}}
	o := &Orchestrator{
		config: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
		auditor:    audit.Disabled(),
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

	if err := o.applySchemaContractTableEvolution(context.Background(), report, tables); err != nil {
		t.Fatalf("applySchemaContractTableEvolution() error: %v", err)
	}
	if got, want := targetPool.createdTables(), []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created tables = %v, want %v", got, want)
	}
	if got, want := targetPool.primaryKeyTables(), []string{"orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("primary key tables = %v, want %v", got, want)
	}
}

func TestSchemaContractTableEvolveSkipsExistingTargetTables(t *testing.T) {
	targetPool := &targetModeTestPool{existing: map[string]bool{"orders": true}}
	o := &Orchestrator{
		config: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
		auditor:    audit.Disabled(),
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{Schema: "dbo", Name: "orders", PrimaryKey: []string{"id"}}}

	if err := o.applySchemaContractTableEvolution(context.Background(), report, tables); err != nil {
		t.Fatalf("applySchemaContractTableEvolution() error: %v", err)
	}
	if got := targetPool.createdTables(); len(got) != 0 {
		t.Fatalf("created tables = %v, want none", got)
	}
	if got := targetPool.primaryKeyTables(); len(got) != 0 {
		t.Fatalf("primary key tables = %v, want none", got)
	}
}

func TestSchemaContractTableEvolveRequiresPrimaryKeyForUpsert(t *testing.T) {
	targetPool := &targetModeTestPool{}
	o := &Orchestrator{
		config: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:     "upsert",
				SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
			},
		},
		targetPool: targetPool,
		auditor:    audit.Disabled(),
	}
	report := drift.Report{Changes: []drift.Change{{Kind: drift.TableAdded, Schema: "dbo", TableName: "orders"}}}
	tables := []source.Table{{Schema: "dbo", Name: "orders"}}

	err := o.applySchemaContractTableEvolution(context.Background(), report, tables)
	if err == nil {
		t.Fatal("applySchemaContractTableEvolution() error = nil, want primary-key error")
	}
	if !strings.Contains(err.Error(), "requires a primary key") {
		t.Fatalf("error = %q, want primary-key message", err)
	}
}

func TestFinalizeSchemaContractTableEvolutionCreatesPostTransferDDL(t *testing.T) {
	targetPool := &targetModeTestPool{}
	o := &Orchestrator{
		config: &config.Config{
			Target: config.TargetConfig{Schema: "public"},
			Migration: config.MigrationConfig{
				TargetMode:             "upsert",
				SchemaContract:         &config.SchemaContractConfig{Tables: config.SchemaContractEvolve},
				CreateCheckConstraints: true,
			},
		},
		targetPool: targetPool,
		auditor:    audit.Disabled(),
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

	o.finalizeSchemaContractTableEvolution(context.Background(), report, tables)

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

func TestSchemaContractFreezeFailsBeforeTargetPreparation(t *testing.T) {
	o := &Orchestrator{config: &config.Config{Migration: config.MigrationConfig{
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

	err := o.enforceSchemaContractPolicy(report)
	if err == nil {
		t.Fatal("enforceSchemaContractPolicy() error = nil, want freeze violation")
	}
	for _, want := range []string{"tables=freeze", "columns=freeze", "data_type=freeze"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want substring %q", err, want)
		}
	}
}

func TestSchemaContractTablesFreezeFailsOnDroppedTable(t *testing.T) {
	o := &Orchestrator{config: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{Tables: config.SchemaContractFreeze},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.TableDropped, TableName: "Legacy"},
	}}

	err := o.enforceSchemaContractPolicy(report)
	if err == nil {
		t.Fatal("enforceSchemaContractPolicy() error = nil, want dropped-table freeze violation")
	}
	if !strings.Contains(err.Error(), "tables=freeze") {
		t.Fatalf("error = %q, want tables=freeze violation", err)
	}
}

func TestSchemaContractColumnsFreezeFailsOnDroppedColumn(t *testing.T) {
	o := &Orchestrator{config: &config.Config{Migration: config.MigrationConfig{
		SchemaContract: &config.SchemaContractConfig{Columns: config.SchemaContractFreeze},
	}}}
	report := drift.Report{Changes: []drift.Change{
		{Kind: drift.DroppedColumn, TableName: "Users", ObjectName: "legacy_code"},
	}}

	err := o.enforceSchemaContractPolicy(report)
	if err == nil {
		t.Fatal("enforceSchemaContractPolicy() error = nil, want dropped-column freeze violation")
	}
	if !strings.Contains(err.Error(), "columns=freeze") {
		t.Fatalf("error = %q, want columns=freeze violation", err)
	}

	footer := o.schemaDriftReportFooter(report, true)
	if !strings.Contains(footer, "will abort before transfer") {
		t.Fatalf("footer = %q, want abort wording", footer)
	}
	if strings.Contains(footer, "target columns are retained") {
		t.Fatalf("footer = %q, should not describe report-only retention under freeze", footer)
	}
}

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
