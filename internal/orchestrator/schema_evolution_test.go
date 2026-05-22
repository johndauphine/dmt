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

func TestShouldApplySchemaEvolutionOnlyForOptInUpsert(t *testing.T) {
	report := drift.Report{Changes: []drift.Change{{Kind: drift.AddedColumn, TableName: "Users"}}}
	nullabilityReport := drift.Report{Changes: []drift.Change{{
		Kind:       drift.NullabilityChange,
		TableName:  "Users",
		ObjectName: "email",
		Previous:   "NOT NULL",
		Current:    "NULL",
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
