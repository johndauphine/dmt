package smtddl

import (
	"errors"
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
