package dialect

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/data-transfer-tool/internal/driver"
	// Import driver packages to register dialects
	_ "github.com/johndauphine/data-transfer-tool/internal/driver/mssql"
	_ "github.com/johndauphine/data-transfer-tool/internal/driver/postgres"
)

func TestGetDialect(t *testing.T) {
	tests := []struct {
		dbType   string
		wantType string
	}{
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"mssql", "mssql"},
		{"sqlserver", "mssql"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			d := GetDialect(tt.dbType)
			if d == nil {
				t.Fatalf("GetDialect(%q) returned nil", tt.dbType)
			}
			if d.DBType() != tt.wantType {
				t.Errorf("GetDialect(%q).DBType() = %q, want %q", tt.dbType, d.DBType(), tt.wantType)
			}
		})
	}
}

func TestGetDialect_UnknownType(t *testing.T) {
	// Unknown or empty database types should return nil
	tests := []string{"", "unknown", "mysql", "oracle"}

	for _, dbType := range tests {
		t.Run(dbType, func(t *testing.T) {
			d := GetDialect(dbType)
			if d != nil {
				t.Errorf("GetDialect(%q) = %v, want nil for unregistered type", dbType, d)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	tests := []struct {
		name     string
		dialect  Dialect
		input    string
		expected string
	}{
		{"postgres simple", pgDialect, "users", `"users"`},
		{"postgres with quote", pgDialect, `user"name`, `"user""name"`},
		{"mssql simple", mssqlDialect, "users", "[users]"},
		{"mssql with bracket", mssqlDialect, "user]name", "[user]]name]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dialect.QuoteIdentifier(tt.input)
			if got != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestQualifyTable(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	tests := []struct {
		name     string
		dialect  Dialect
		schema   string
		table    string
		expected string
	}{
		{"postgres", pgDialect, "public", "users", `"public"."users"`},
		{"mssql", mssqlDialect, "dbo", "users", "[dbo].[users]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dialect.QualifyTable(tt.schema, tt.table)
			if got != tt.expected {
				t.Errorf("QualifyTable(%q, %q) = %q, want %q", tt.schema, tt.table, got, tt.expected)
			}
		})
	}
}

func TestColumnList(t *testing.T) {
	cols := []string{"id", "name", "email"}

	pgDialect := driver.GetDialect("postgres")
	pgResult := pgDialect.ColumnList(cols)
	if pgResult != `"id", "name", "email"` {
		t.Errorf("PostgreSQL ColumnList = %q, want %q", pgResult, `"id", "name", "email"`)
	}

	mssqlDialect := driver.GetDialect("mssql")
	mssqlResult := mssqlDialect.ColumnList(cols)
	if mssqlResult != "[id], [name], [email]" {
		t.Errorf("MSSQL ColumnList = %q, want %q", mssqlResult, "[id], [name], [email]")
	}
}

func TestTableHint(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	if pgDialect.TableHint(false) != "" {
		t.Error("PostgreSQL TableHint should be empty")
	}

	mssqlDialect := driver.GetDialect("mssql")
	if mssqlDialect.TableHint(false) != "WITH (NOLOCK)" {
		t.Error("MSSQL TableHint should be WITH (NOLOCK)")
	}
	if mssqlDialect.TableHint(true) != "" {
		t.Error("MSSQL TableHint with strict consistency should be empty")
	}
}

func TestColumnListForSelect_CrossEngine(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	tests := []struct {
		name         string
		dialect      Dialect
		cols         []string
		colTypes     []string
		targetDBType string
		wantContains []string
	}{
		{
			name:         "postgres to mssql with geography",
			dialect:      pgDialect,
			cols:         []string{"id", "name", "location"},
			colTypes:     []string{"int", "text", "geography"},
			targetDBType: "mssql",
			wantContains: []string{"ST_AsText", `"location"`},
		},
		{
			name:         "mssql to postgres with geography",
			dialect:      mssqlDialect,
			cols:         []string{"id", "name", "location"},
			colTypes:     []string{"int", "nvarchar", "geography"},
			targetDBType: "postgres",
			wantContains: []string{".STAsText()", "[location]"},
		},
		{
			name:         "same engine - no conversion",
			dialect:      pgDialect,
			cols:         []string{"id", "location"},
			colTypes:     []string{"int", "geography"},
			targetDBType: "postgres",
			wantContains: []string{`"location"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dialect.ColumnListForSelect(tt.cols, tt.colTypes, tt.targetDBType)
			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("ColumnListForSelect() = %q, want to contain %q", got, want)
				}
			}
		})
	}
}

func TestBuildKeysetQuery(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	// PostgreSQL keyset query
	pgQuery := pgDialect.BuildKeysetQuery("id, name", "id", "public", "users", "", true, nil)
	if !strings.Contains(pgQuery, "$1") || !strings.Contains(pgQuery, "LIMIT") {
		t.Errorf("PostgreSQL keyset query missing expected syntax: %s", pgQuery)
	}

	// MSSQL keyset query
	mssqlQuery := mssqlDialect.BuildKeysetQuery("[id], [name]", "id", "dbo", "users", "WITH (NOLOCK)", true, nil)
	if !strings.Contains(mssqlQuery, "@lastPK") || !strings.Contains(mssqlQuery, "TOP") {
		t.Errorf("MSSQL keyset query missing expected syntax: %s", mssqlQuery)
	}

	// With date filter
	dateFilter := &DateFilter{Column: "updated_at", Timestamp: time.Now()}
	pgQueryWithDate := pgDialect.BuildKeysetQuery("id", "id", "public", "users", "", false, dateFilter)
	if !strings.Contains(pgQueryWithDate, "updated_at") || !strings.Contains(pgQueryWithDate, "IS NULL") {
		t.Errorf("PostgreSQL keyset query missing date filter: %s", pgQueryWithDate)
	}
}

func TestBuildKeysetArgs(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	// PostgreSQL args
	pgArgs := pgDialect.BuildKeysetArgs(100, 200, 1000, true, nil)
	if len(pgArgs) != 3 {
		t.Errorf("PostgreSQL args count = %d, want 3", len(pgArgs))
	}

	// MSSQL uses named parameters
	mssqlArgs := mssqlDialect.BuildKeysetArgs(100, 200, 1000, true, nil)
	if len(mssqlArgs) != 3 {
		t.Errorf("MSSQL args count = %d, want 3", len(mssqlArgs))
	}
}

func TestBuildRowNumberQuery(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	pgQuery := pgDialect.BuildRowNumberQuery("id, name", "id", "public", "users", "")
	if !strings.Contains(pgQuery, "ROW_NUMBER()") || !strings.Contains(pgQuery, "__rn") {
		t.Errorf("PostgreSQL ROW_NUMBER query missing expected syntax: %s", pgQuery)
	}

	mssqlQuery := mssqlDialect.BuildRowNumberQuery("[id], [name]", "[id]", "dbo", "users", "WITH (NOLOCK)")
	if !strings.Contains(mssqlQuery, "ROW_NUMBER()") || !strings.Contains(mssqlQuery, "@rowNum") {
		t.Errorf("MSSQL ROW_NUMBER query missing expected syntax: %s", mssqlQuery)
	}
}

func TestBuildDSN(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	pgDSN := pgDialect.BuildDSN("localhost", 5432, "testdb", "user", "pass", map[string]any{
		"sslmode": "disable",
	})
	if !strings.Contains(pgDSN, "postgres://") || !strings.Contains(pgDSN, "sslmode=disable") {
		t.Errorf("PostgreSQL DSN unexpected format: %s", pgDSN)
	}

	mssqlDialect := driver.GetDialect("mssql")
	mssqlDSN := mssqlDialect.BuildDSN("localhost", 1433, "testdb", "user", "pass", map[string]any{
		"encrypt":                false,
		"trustServerCertificate": true,
		"packetSize":             32767,
	})
	if !strings.Contains(mssqlDSN, "sqlserver://") || !strings.Contains(mssqlDSN, "encrypt=false") {
		t.Errorf("MSSQL DSN unexpected format: %s", mssqlDSN)
	}
}

func TestPartitionBoundariesQuery(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	mssqlDialect := driver.GetDialect("mssql")

	pgQuery := pgDialect.PartitionBoundariesQuery("id", "public", "users", 4)
	if !strings.Contains(pgQuery, "NTILE(4)") || !strings.Contains(pgQuery, "partition_id") {
		t.Errorf("PostgreSQL partition query unexpected: %s", pgQuery)
	}

	mssqlQuery := mssqlDialect.PartitionBoundariesQuery("id", "dbo", "users", 4)
	if !strings.Contains(mssqlQuery, "NTILE(4)") || !strings.Contains(mssqlQuery, "[id]") {
		t.Errorf("MSSQL partition query unexpected: %s", mssqlQuery)
	}
}

func TestValidDateTypes(t *testing.T) {
	pgDialect := driver.GetDialect("postgres")
	pgTypes := pgDialect.ValidDateTypes()
	if !pgTypes["timestamp"] || !pgTypes["timestamptz"] {
		t.Error("PostgreSQL missing expected date types")
	}

	mssqlDialect := driver.GetDialect("mssql")
	mssqlTypes := mssqlDialect.ValidDateTypes()
	if !mssqlTypes["datetime"] || !mssqlTypes["datetime2"] {
		t.Error("MSSQL missing expected date types")
	}
}
