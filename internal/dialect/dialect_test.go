package dialect

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	// Import driver packages to register dialects
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
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
		{"mysql", "mysql"},
		{"mariadb", "mysql"},
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
	tests := []string{"", "unknown", "oracle", "sqlite"}

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
	if !strings.Contains(pgQueryWithDate, "updated_at") || !strings.Contains(pgQueryWithDate, ">=") {
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
	mysqlDialect := driver.GetDialect("mysql")

	pgQuery := pgDialect.BuildRowNumberQuery("id, name", "id", "public", "users", "", nil)
	if !strings.Contains(pgQuery, "ROW_NUMBER()") || !strings.Contains(pgQuery, "__rn") {
		t.Errorf("PostgreSQL ROW_NUMBER query missing expected syntax: %s", pgQuery)
	}

	mssqlQuery := mssqlDialect.BuildRowNumberQuery("[id], [name]", "[id]", "dbo", "users", "WITH (NOLOCK)", nil)
	if !strings.Contains(mssqlQuery, "ROW_NUMBER()") || !strings.Contains(mssqlQuery, "@rowNum") {
		t.Errorf("MSSQL ROW_NUMBER query missing expected syntax: %s", mssqlQuery)
	}

	mysqlQuery := mysqlDialect.BuildRowNumberQuery("`id`, `name`", "`id`", "", "users", "", nil)
	if !strings.Contains(mysqlQuery, "ROW_NUMBER()") || !strings.Contains(mysqlQuery, "__rn") {
		t.Errorf("MySQL ROW_NUMBER query missing expected syntax: %s", mysqlQuery)
	}
}

func TestBuildRowNumberQueryWithDateFilter(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	dateFilter := &DateFilter{Column: "updated_at", Timestamp: testTime}

	tests := []struct {
		name         string
		dbType       string
		cols         string
		orderBy      string
		schema       string
		table        string
		tableHint    string
		dateFilter   *DateFilter
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:       "postgres with date filter",
			dbType:     "postgres",
			cols:       `"id", "name"`,
			orderBy:    `"id"`,
			schema:     "public",
			table:      "users",
			tableHint:  "",
			dateFilter: dateFilter,
			wantContains: []string{
				"ROW_NUMBER()",
				`"updated_at" >= $3`,
				"WHERE",
			},
			wantAbsent: []string{"IS NULL"},
		},
		{
			name:       "postgres without date filter",
			dbType:     "postgres",
			cols:       `"id", "name"`,
			orderBy:    `"id"`,
			schema:     "public",
			table:      "users",
			tableHint:  "",
			dateFilter: nil,
			wantContains: []string{
				"ROW_NUMBER()",
				"__rn > $1",
			},
			wantAbsent: []string{"updated_at", "$3"},
		},
		{
			name:       "mssql with date filter",
			dbType:     "mssql",
			cols:       "[id], [name]",
			orderBy:    "[id]",
			schema:     "dbo",
			table:      "users",
			tableHint:  "WITH (NOLOCK)",
			dateFilter: dateFilter,
			wantContains: []string{
				"ROW_NUMBER()",
				"[updated_at] >= @lastSyncDate",
				"WHERE",
			},
			wantAbsent: []string{"IS NULL"},
		},
		{
			name:       "mssql without date filter",
			dbType:     "mssql",
			cols:       "[id], [name]",
			orderBy:    "[id]",
			schema:     "dbo",
			table:      "users",
			tableHint:  "WITH (NOLOCK)",
			dateFilter: nil,
			wantContains: []string{
				"ROW_NUMBER()",
				"@rowNum",
			},
			wantAbsent: []string{"updated_at", "@lastSyncDate"},
		},
		{
			name:       "mysql with date filter",
			dbType:     "mysql",
			cols:       "`id`, `name`",
			orderBy:    "`id`",
			schema:     "",
			table:      "users",
			tableHint:  "",
			dateFilter: dateFilter,
			wantContains: []string{
				"ROW_NUMBER()",
				"`updated_at` >= ?",
				"WHERE",
			},
			wantAbsent: []string{"IS NULL"},
		},
		{
			name:       "mysql without date filter",
			dbType:     "mysql",
			cols:       "`id`, `name`",
			orderBy:    "`id`",
			schema:     "",
			table:      "users",
			tableHint:  "",
			dateFilter: nil,
			wantContains: []string{
				"ROW_NUMBER()",
				"__rn",
			},
			wantAbsent: []string{"updated_at"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect := driver.GetDialect(tt.dbType)
			if dialect == nil {
				t.Fatalf("dialect for %q not registered", tt.dbType)
			}

			query := dialect.BuildRowNumberQuery(tt.cols, tt.orderBy, tt.schema, tt.table, tt.tableHint, tt.dateFilter)

			for _, want := range tt.wantContains {
				if !strings.Contains(query, want) {
					t.Errorf("Query missing expected substring %q.\nGot: %s", want, query)
				}
			}

			for _, absent := range tt.wantAbsent {
				if strings.Contains(query, absent) {
					t.Errorf("Query should not contain %q.\nGot: %s", absent, query)
				}
			}
		})
	}
}

func TestBuildRowNumberArgs(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	dateFilter := &DateFilter{Column: "updated_at", Timestamp: testTime}

	tests := []struct {
		name       string
		dbType     string
		rowNum     int64
		limit      int
		dateFilter *DateFilter
		wantCount  int
	}{
		{
			name:       "postgres without date filter",
			dbType:     "postgres",
			rowNum:     100,
			limit:      1000,
			dateFilter: nil,
			wantCount:  2, // rowNum, rowNum+limit
		},
		{
			name:       "postgres with date filter",
			dbType:     "postgres",
			rowNum:     100,
			limit:      1000,
			dateFilter: dateFilter,
			wantCount:  3, // rowNum, rowNum+limit, timestamp
		},
		{
			name:       "mssql without date filter",
			dbType:     "mssql",
			rowNum:     100,
			limit:      1000,
			dateFilter: nil,
			wantCount:  2, // named params: rowNum, rowNumEnd
		},
		{
			name:       "mssql with date filter",
			dbType:     "mssql",
			rowNum:     100,
			limit:      1000,
			dateFilter: dateFilter,
			wantCount:  3, // named params: rowNum, rowNumEnd, lastSyncDate
		},
		{
			name:       "mysql without date filter",
			dbType:     "mysql",
			rowNum:     100,
			limit:      1000,
			dateFilter: nil,
			wantCount:  2, // rowNum, rowNum+limit
		},
		{
			name:       "mysql with date filter",
			dbType:     "mysql",
			rowNum:     100,
			limit:      1000,
			dateFilter: dateFilter,
			wantCount:  3, // timestamp, rowNum, rowNum+limit (MySQL has date first)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialect := driver.GetDialect(tt.dbType)
			if dialect == nil {
				t.Fatalf("dialect for %q not registered", tt.dbType)
			}

			args := dialect.BuildRowNumberArgs(tt.rowNum, tt.limit, tt.dateFilter)

			if len(args) != tt.wantCount {
				t.Errorf("BuildRowNumberArgs() returned %d args, want %d", len(args), tt.wantCount)
			}
		})
	}
}

func TestBuildRowNumberArgsOrder(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	dateFilter := &DateFilter{Column: "updated_at", Timestamp: testTime}

	// Test MySQL specifically - date filter comes FIRST
	mysqlDialect := driver.GetDialect("mysql")
	mysqlArgs := mysqlDialect.BuildRowNumberArgs(100, 1000, dateFilter)

	if len(mysqlArgs) != 3 {
		t.Fatalf("MySQL args count = %d, want 3", len(mysqlArgs))
	}

	// First arg should be the timestamp (MySQL puts date filter first)
	if ts, ok := mysqlArgs[0].(time.Time); !ok || !ts.Equal(testTime) {
		t.Errorf("MySQL first arg should be timestamp, got %T: %v", mysqlArgs[0], mysqlArgs[0])
	}

	// Second arg should be rowNum (100)
	if mysqlArgs[1] != int64(100) {
		t.Errorf("MySQL second arg should be 100, got %v", mysqlArgs[1])
	}

	// Third arg should be rowNum + limit (1100)
	if mysqlArgs[2] != int64(1100) {
		t.Errorf("MySQL third arg should be 1100, got %v", mysqlArgs[2])
	}

	// Test PostgreSQL - date filter comes LAST
	pgDialect := driver.GetDialect("postgres")
	pgArgs := pgDialect.BuildRowNumberArgs(100, 1000, dateFilter)

	if len(pgArgs) != 3 {
		t.Fatalf("PostgreSQL args count = %d, want 3", len(pgArgs))
	}

	// First arg should be rowNum
	if pgArgs[0] != int64(100) {
		t.Errorf("PostgreSQL first arg should be 100, got %v", pgArgs[0])
	}

	// Second arg should be rowNum + limit
	if pgArgs[1] != int64(1100) {
		t.Errorf("PostgreSQL second arg should be 1100, got %v", pgArgs[1])
	}

	// Third arg should be timestamp
	if ts, ok := pgArgs[2].(time.Time); !ok || !ts.Equal(testTime) {
		t.Errorf("PostgreSQL third arg should be timestamp, got %T: %v", pgArgs[2], pgArgs[2])
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
