package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

// BCP index-risk preflight tests ported from the hand-written mssql
// driver with its removal (#509 cleanup). A sqlite-backed fake of the
// sys.* catalog keeps them hermetic — the modernc.org/sqlite driver is
// already a generic backend import.

func TestMssqlPFParallelBCPIndexRiskWarnsForTargetNonclusteredIndexes(t *testing.T) {
	db := openMssqlCatalogSQLite(t)
	seedMssqlCatalogIndexes(t, db)

	findings := mssqlPFParallelBCPIndexRisk(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideTarget,
		Schema: "dbo",
	})
	if len(findings) != 1 {
		t.Fatalf("mssqlPFParallelBCPIndexRisk produced %d findings, want 1", len(findings))
	}

	got := findings[0]
	if got.Severity != driver.SeverityWarn ||
		got.Check != "bulk.parallel_bcp_indexes" ||
		got.Side != driver.PreFlightSideTarget {
		t.Fatalf("finding metadata = %+v", got)
	}
	for _, want := range []string{
		"2 enabled nonclustered indexes across 2 tables",
		"parallel BCP without TABLOCK",
	} {
		if !strings.Contains(got.Message, want) {
			t.Fatalf("finding message %q missing %q", got.Message, want)
		}
	}
	if !strings.Contains(got.Remedy, "migration.write_ahead_writers: 1") {
		t.Fatalf("finding remedy = %q, want write_ahead_writers mitigation", got.Remedy)
	}
}

func TestMSSQLSnapshotVersionSupport(t *testing.T) {
	tests := []struct {
		version string
		edition int
		want    bool
	}{
		{version: "13.0.3999.0", edition: 2},
		{version: "13.0.4001.0", edition: 2, want: true},
		{version: "12.0.1000.0", edition: 3, want: true},
		{version: "16.0.1000.0", edition: 2, want: true},
		{version: "invalid", edition: 2},
	}
	for _, tc := range tests {
		if got := mssqlSnapshotVersionSupported(tc.version, tc.edition); got != tc.want {
			t.Errorf("version=%q edition=%d supported=%t, want %t", tc.version, tc.edition, got, tc.want)
		}
	}
}

func TestMSSQLSnapshotPreflightGate(t *testing.T) {
	for _, req := range []driver.PreFlightRequest{
		{Side: driver.PreFlightSideTarget, StrictConsistency: true, StrictConsistencyScope: "migration"},
		{Side: driver.PreFlightSideSource, StrictConsistencyScope: "migration"},
		{Side: driver.PreFlightSideSource, StrictConsistency: true, StrictConsistencyScope: "table"},
	} {
		if got := mssqlPFDatabaseSnapshot(context.Background(), nil, req); len(got) != 0 {
			t.Fatalf("gated request %+v produced findings %+v", req, got)
		}
	}
}

func TestMSSQLSnapshotCapabilityFindings(t *testing.T) {
	req := driver.PreFlightRequest{Side: driver.PreFlightSideSource}
	tests := []struct {
		name               string
		edition, createAny int
		createDatabase     int
		alterAnyDatabase   int
		dbCreator          int
		version            string
		wantChecks         []string
	}{
		{name: "supported", edition: 2, version: "13.0.4001.0", createAny: 1},
		{name: "master create database supported", edition: 2, version: "16.0.1000.0", createDatabase: 1},
		{name: "alter any database supported", edition: 2, version: "16.0.1000.0", alterAnyDatabase: 1},
		{name: "dbcreator supported", edition: 2, version: "16.0.1000.0", dbCreator: 1},
		{name: "azure", edition: 5, version: "16.0.1000.0", createAny: 1, wantChecks: []string{"compat.database_snapshot"}},
		{name: "pre SP1 and missing permission", edition: 2, version: "13.0.3999.0", wantChecks: []string{"compat.database_snapshot", "privileges.database_snapshot"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			findings := mssqlPFSnapshotCapabilityFindings(req, tc.edition, tc.version, tc.createAny, tc.createDatabase, tc.alterAnyDatabase, tc.dbCreator)
			var checks []string
			for _, finding := range findings {
				checks = append(checks, finding.Check)
			}
			if fmt.Sprint(checks) != fmt.Sprint(tc.wantChecks) {
				t.Fatalf("checks = %v, want %v", checks, tc.wantChecks)
			}
		})
	}
}

func TestMssqlPFParallelBCPIndexRiskSkipsSourceSide(t *testing.T) {
	db := openMssqlCatalogSQLite(t)
	seedMssqlCatalogIndexes(t, db)

	findings := mssqlPFParallelBCPIndexRisk(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideSource,
		Schema: "dbo",
	})
	if len(findings) != 0 {
		t.Fatalf("source-side check produced findings: %+v", findings)
	}
}

func TestMssqlPFParallelBCPIndexRiskNoWarnWithoutNonclusteredIndexes(t *testing.T) {
	db := openMssqlCatalogSQLite(t)
	execMssqlCatalogSQL(t, db, `INSERT INTO sys.schemas (schema_id, name) VALUES (1, 'dbo')`)
	execMssqlCatalogSQL(t, db, `INSERT INTO sys.tables (object_id, schema_id, name, is_ms_shipped) VALUES (10, 1, 'Users', 0)`)
	execMssqlCatalogSQL(t, db, `
		INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical)
		VALUES (10, 'PK_Users', 'CLUSTERED', 1, 0, 0)`)

	findings := mssqlPFParallelBCPIndexRisk(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideTarget,
		Schema: "dbo",
	})
	if len(findings) != 0 {
		t.Fatalf("clustered-only catalog produced findings: %+v", findings)
	}
}

func openMssqlCatalogSQLite(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening sqlite catalog db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("closing sqlite catalog db: %v", err)
		}
	})

	for _, stmt := range []string{
		`ATTACH DATABASE ':memory:' AS sys`,
		`CREATE TABLE sys.schemas (schema_id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE sys.tables (object_id INTEGER PRIMARY KEY, schema_id INTEGER NOT NULL, name TEXT NOT NULL, is_ms_shipped INTEGER NOT NULL)`,
		`CREATE TABLE sys.indexes (object_id INTEGER NOT NULL, name TEXT NOT NULL, type_desc TEXT NOT NULL, is_primary_key INTEGER NOT NULL, is_disabled INTEGER NOT NULL, is_hypothetical INTEGER NOT NULL)`,
	} {
		execMssqlCatalogSQL(t, db, stmt)
	}
	return db
}

func seedMssqlCatalogIndexes(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, stmt := range []string{
		`INSERT INTO sys.schemas (schema_id, name) VALUES (1, 'dbo')`,
		`INSERT INTO sys.tables (object_id, schema_id, name, is_ms_shipped) VALUES (10, 1, 'Users', 0)`,
		`INSERT INTO sys.tables (object_id, schema_id, name, is_ms_shipped) VALUES (20, 1, 'Posts', 0)`,
		`INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical) VALUES (10, 'PK_Users', 'CLUSTERED', 1, 0, 0)`,
		`INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical) VALUES (10, 'IX_Users_Email', 'NONCLUSTERED', 0, 0, 0)`,
		`INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical) VALUES (20, 'IX_Posts_CreatedAt', 'NONCLUSTERED COLUMNSTORE', 0, 0, 0)`,
		`INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical) VALUES (20, 'IX_Posts_Disabled', 'NONCLUSTERED', 0, 1, 0)`,
	} {
		execMssqlCatalogSQL(t, db, stmt)
	}
}

func execMssqlCatalogSQL(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()

	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("executing catalog SQL %q: %v", stmt, err)
	}
}

// DSN encrypt passthrough (ported): encrypt=false must render the
// go-mssqldb "disable" form, not "false".
func TestSqlserverDSNEncrypt(t *testing.T) {
	tests := []struct {
		name     string
		encrypt  bool
		expected string
	}{
		{"encrypt true", true, "&encrypt=true"},
		{"encrypt false uses disable", false, "&encrypt=disable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := sqlserverURLDSN("localhost", 1433, "testdb", "sa", "pass", map[string]any{
				"encrypt": tt.encrypt,
			})
			if !strings.Contains(dsn, tt.expected) {
				t.Errorf("DSN with encrypt=%v should contain %q, got %q", tt.encrypt, tt.expected, dsn)
			}
		})
	}
}

// The #253 NOLOCK contract on exact counts: relaxed reads keep the
// dirty-read hint, strict_consistency drops it. (The hand-written
// reader's buildExactRowCountQuery pinned the same strings.)
func TestMssqlExactRowCountHint(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDialect(cat)
	if h := d.TableHint(false); h != "WITH (NOLOCK)" {
		t.Errorf("relaxed hint = %q, want WITH (NOLOCK)", h)
	}
	if h := d.TableHint(true); h != "" {
		t.Errorf("strict hint = %q, want empty", h)
	}
}
