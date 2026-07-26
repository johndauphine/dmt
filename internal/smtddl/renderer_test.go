package smtddl

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/schema"
)

func TestPlanCreateSelectsVerbatimPublicPlanStatements(t *testing.T) {
	req := Request{
		SourceDialect: "postgres",
		TargetDialect: "postgres",
		TargetSchema:  "app",
		Table: Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int8", IsNullable: false},
				{Name: "name", DataType: "varchar", MaxLength: 80, IsNullable: false},
			},
			PrimaryKey: []string{"id"},
		},
	}

	plan, err := PlanCreate(req)
	if err != nil {
		t.Fatalf("PlanCreate: %v", err)
	}
	if len(plan.Statements) != 2 {
		t.Fatalf("plan statements = %#v, want schema then table", plan.Statements)
	}
	if plan.Statements[0].Kind != schema.StatementCreateSchema || plan.Statements[1].Kind != schema.StatementCreateTable {
		t.Fatalf("plan statement kinds = %#v", plan.Statements)
	}

	schemaSQL, err := RenderCreateSchema(req)
	if err != nil {
		t.Fatalf("RenderCreateSchema: %v", err)
	}
	if schemaSQL != plan.Statements[0].SQL {
		t.Fatalf("schema SQL changed at DMT boundary:\n got: %q\nwant: %q", schemaSQL, plan.Statements[0].SQL)
	}
	tableSQL, err := RenderCreateTable(req)
	if err != nil {
		t.Fatalf("RenderCreateTable: %v", err)
	}
	if tableSQL != plan.Statements[1].SQL {
		t.Fatalf("table SQL changed at DMT boundary:\n got: %q\nwant: %q", tableSQL, plan.Statements[1].SQL)
	}
	if strings.HasSuffix(tableSQL, ";") {
		t.Fatalf("DMT added a SQL terminator to SMT plan output: %q", tableSQL)
	}
}

func TestPlanCreateRawSourceTypeReturnsSMTUnsupportedPolicy(t *testing.T) {
	_, err := PlanCreate(Request{
		SourceDialect: "postgres",
		TargetDialect: "mssql",
		Table: Table{
			Name:    "events",
			Columns: []Column{{Name: "client_ip", DataType: "inet"}},
		},
	})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("PlanCreate error = %v, want SMT UnsupportedFeatureError", err)
	}
	if unsupported.Dialect != "mssql" {
		t.Errorf("unsupported dialect = %q, want mssql", unsupported.Dialect)
	}
}

func TestRenderSideObjectsUsesVerbatimPublicResults(t *testing.T) {
	req := Request{
		SourceDialect: "postgres",
		TargetDialect: "postgres",
		TargetSchema:  "public",
		Table: Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int8", IsNullable: false},
				{Name: "code", DataType: "varchar", MaxLength: 80, IsNullable: false},
			},
		},
	}
	tests := []struct {
		name string
		call func() (string, error)
		want string
	}{
		{
			name: "index",
			call: func() (string, error) {
				return RenderIndex(req, Index{Name: "ix_events_code", Columns: []string{"code"}, IsUnique: true})
			},
			want: `CREATE UNIQUE INDEX "ix_events_code" ON "public"."events" ("code")`,
		},
		{
			name: "primary key",
			call: func() (string, error) { return RenderPrimaryKey(req, PrimaryKey{Columns: []string{"id"}}) },
			want: `ALTER TABLE "public"."events" ADD CONSTRAINT "pk_events" PRIMARY KEY ("id")`,
		},
		{
			name: "check",
			call: func() (string, error) {
				return RenderCheckConstraint(req, CheckConstraint{Name: "ck_events_code", Expression: "code <> ''"})
			},
			want: `ALTER TABLE "public"."events" ADD CONSTRAINT "ck_events_code" CHECK ("code" <> '')`,
		},
		{
			name: "foreign key",
			call: func() (string, error) {
				return RenderForeignKey(req, ForeignKey{Name: "fk_events_parent", Columns: []string{"id"}, RefTable: "parent_events", RefColumns: []string{"id"}, OnDelete: schema.ReferentialActionCascade})
			},
			want: `ALTER TABLE "public"."events" ADD CONSTRAINT "fk_events_parent" FOREIGN KEY ("id") REFERENCES "public"."parent_events" ("id") ON DELETE CASCADE`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("SMT output changed at DMT boundary:\n got: %s\nwant: %s", got, tt.want)
			}
			if strings.HasSuffix(got, ";") {
				t.Fatalf("DMT rewrote SMT side-object SQL: %q", got)
			}
		})
	}
}

func TestRenderIndexClusteredReturnsSMTTypedPolicy(t *testing.T) {
	_, err := RenderIndex(Request{TargetDialect: "mssql", Table: Table{Name: "events"}}, Index{Name: "ix_events", Columns: []string{"id"}, IsClustered: true})
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("RenderIndex error = %T %v, want SMT UnsupportedFeatureError", err, err)
	}
	if unsupported.Dialect != "mssql" || unsupported.Feature != "clustered indexes" {
		t.Fatalf("unsupported policy = %#v, want mssql clustered-index policy", unsupported)
	}
}

func TestRenderSideObjectsAllowUnrelatedRawSourceTypeContext(t *testing.T) {
	req := Request{
		SourceDialect: "postgres",
		TargetDialect: "postgres",
		TargetSchema:  "public",
		Table: Table{
			Name: "events",
			Columns: []Column{
				{Name: "client_ip", DataType: "inet"}, // canonical.Raw in the DMT source model
				{Name: "status", DataType: "integer", IsNullable: false},
			},
		},
	}

	indexSQL, err := RenderIndex(req, Index{
		Name:    "ix_events_status",
		Columns: []string{"status"},
		Filter:  "status IS NOT NULL",
	})
	if err != nil {
		t.Fatalf("RenderIndex with unrelated raw type: %v", err)
	}
	if !strings.Contains(indexSQL, `WHERE status IS NOT NULL`) {
		t.Fatalf("filtered index SQL = %q, want SMT-rendered filter", indexSQL)
	}

	checkSQL, err := RenderCheckConstraint(req, CheckConstraint{Name: "ck_events_status", Expression: "status IS NOT NULL"})
	if err != nil {
		t.Fatalf("RenderCheckConstraint with unrelated raw type: %v", err)
	}
	if !strings.Contains(checkSQL, `CHECK ("status" IS NOT NULL)`) {
		t.Fatalf("check SQL = %q, want SMT-rendered predicate", checkSQL)
	}
}

func TestRenderEvolutionReturnsVerbatimPublicBatches(t *testing.T) {
	req := Request{
		SourceDialect: "postgres",
		TargetDialect: "postgres",
		TargetSchema:  "public",
		Table: Table{
			Name: "events",
			Columns: []Column{
				{Name: "id", DataType: "int8", IsNullable: false},
				{Name: "note", DataType: "varchar", MaxLength: 80, IsNullable: true},
			},
		},
	}
	column := Column{Name: "note", DataType: "varchar", MaxLength: 160, IsNullable: true}
	renderer, err := schema.NewRenderer(schema.Options{
		SourceDialect:     req.SourceDialect,
		TargetDialect:     req.TargetDialect,
		Schema:            req.TargetSchema,
		UnknownTypePolicy: schema.UnknownTypeFail,
	})
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	publicTable := schema.TableRef{
		Name: req.Table.Name,
		Columns: []schema.Column{
			{Name: "id", DataType: "int8", IsNullable: false},
			{Name: "note", DataType: "varchar", MaxLength: 80, IsNullable: true},
		},
	}
	publicColumn := schema.Column{Name: "note", DataType: "varchar", MaxLength: 160, IsNullable: true}

	tests := []struct {
		name string
		got  func() (schema.Batch, error)
		want func() (schema.Batch, error)
	}{
		{
			name: "add column",
			got:  func() (schema.Batch, error) { return RenderAddColumn(req, column) },
			want: func() (schema.Batch, error) { return renderer.AddColumn(publicTable, publicColumn) },
		},
		{
			name: "alter nullability",
			got:  func() (schema.Batch, error) { return RenderAlterColumnNullability(req, column) },
			want: func() (schema.Batch, error) { return renderer.AlterColumnNullability(publicTable, publicColumn) },
		},
		{
			name: "alter type",
			got:  func() (schema.Batch, error) { return RenderAlterColumnType(req, column) },
			want: func() (schema.Batch, error) { return renderer.AlterColumnType(publicTable, publicColumn) },
		},
		{
			name: "drop table cascade",
			got:  func() (schema.Batch, error) { return RenderDropTable(req, true) },
			want: func() (schema.Batch, error) {
				return renderer.DropTable(publicTable, schema.DropOptions{Cascade: true})
			},
		},
		{
			name: "truncate table cascade",
			got:  func() (schema.Batch, error) { return RenderTruncateTable(req, true) },
			want: func() (schema.Batch, error) {
				return renderer.TruncateTable(publicTable, schema.TruncateOptions{Cascade: true})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.got()
			if err != nil {
				t.Fatalf("DMT wrapper: %v", err)
			}
			want, err := tt.want()
			if err != nil {
				t.Fatalf("SMT public renderer: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("DMT changed SMT batch:\n got: %+v\nwant: %+v", got, want)
			}
			for _, statement := range got.Statements {
				if strings.HasSuffix(statement.SQL, ";") {
					t.Fatalf("DMT added a SQL terminator: %q", statement.SQL)
				}
			}
		})
	}
}

func TestRenderEvolutionProjectsMySQLFullDataTypeAndDefault(t *testing.T) {
	req := Request{
		SourceDialect: "mysql",
		TargetDialect: "mysql",
		TargetSchema:  "crm",
		Table:         Table{Name: "events"},
	}
	enumColumn := Column{
		Name:         "status",
		DataType:     "enum",
		FullDataType: "enum('new','owner''s')",
		IsNullable:   true,
	}
	add, err := RenderAddColumn(req, enumColumn)
	if err != nil {
		t.Fatalf("RenderAddColumn enum: %v", err)
	}
	if got := add.Statements[0].SQL; got != "ALTER TABLE `crm`.`events` ADD COLUMN `status` ENUM('new','owner''s')" {
		t.Fatalf("MySQL enum add SQL = %q", got)
	}

	unsigned := Column{
		Name:              "flags",
		DataType:          "int",
		FullDataType:      "int(10) unsigned",
		IsNullable:        true,
		DefaultExpression: "0",
		HasDefault:        true,
	}
	alter, err := RenderAlterColumnType(req, unsigned)
	if err != nil {
		t.Fatalf("RenderAlterColumnType unsigned: %v", err)
	}
	if got := alter.Statements[0].SQL; got != "ALTER TABLE `crm`.`events` MODIFY COLUMN `flags` INT UNSIGNED DEFAULT 0" {
		t.Fatalf("MySQL alter SQL = %q", got)
	}
}

func TestRenderEvolutionProjectsMySQLTemporalPrecision(t *testing.T) {
	req := Request{
		SourceDialect: "mysql",
		TargetDialect: "mysql",
		TargetSchema:  "crm",
		Table:         Table{Name: "events"},
	}
	for _, tc := range []struct {
		name     string
		dataType string
		fullType string
		wantType string
	}{
		{name: "fsp0", dataType: "time", fullType: "time", wantType: "TIME"},
		{name: "fsp3", dataType: "datetime", fullType: "datetime(3)", wantType: "DATETIME(3)"},
		{name: "fsp6", dataType: "timestamp", fullType: "timestamp(6)", wantType: "TIMESTAMP(6)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := RenderAddColumn(req, Column{
				Name:         tc.name,
				DataType:     tc.dataType,
				FullDataType: tc.fullType,
				IsNullable:   true,
			})
			if err != nil {
				t.Fatalf("RenderAddColumn: %v", err)
			}
			want := "ALTER TABLE `crm`.`events` ADD COLUMN `" + tc.name + "` " + tc.wantType
			if got := batch.Statements[0].SQL; got != want {
				t.Fatalf("MySQL temporal add SQL = %q, want %q", got, want)
			}
		})
	}
}
