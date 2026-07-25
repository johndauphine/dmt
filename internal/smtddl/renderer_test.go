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

func TestUnsupportedStandalonePrimaryKeyUsesSMTPolicy(t *testing.T) {
	err := UnsupportedStandalonePrimaryKey("postgres")
	var unsupported *schema.UnsupportedFeatureError
	if !errors.As(err, &unsupported) {
		t.Fatalf("error = %v, want SMT UnsupportedFeatureError", err)
	}
	if !strings.Contains(unsupported.Feature, "standalone primary-key") {
		t.Errorf("feature = %q, want standalone-primary-key guidance", unsupported.Feature)
	}
}
