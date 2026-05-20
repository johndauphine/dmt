package mssql

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"

	_ "modernc.org/sqlite"
)

func TestCheckParallelBCPIndexRiskMSSQLWarnsForTargetNonclusteredIndexes(t *testing.T) {
	db := openMSSQLCatalogSQLite(t)
	seedMSSQLCatalogIndexes(t, db)

	findings := checkParallelBCPIndexRiskMSSQL(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideTarget,
		Schema: "dbo",
	})
	if len(findings) != 1 {
		t.Fatalf("checkParallelBCPIndexRiskMSSQL produced %d findings, want 1", len(findings))
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

func TestCheckParallelBCPIndexRiskMSSQLSkipsSourceSide(t *testing.T) {
	db := openMSSQLCatalogSQLite(t)
	seedMSSQLCatalogIndexes(t, db)

	findings := checkParallelBCPIndexRiskMSSQL(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideSource,
		Schema: "dbo",
	})
	if len(findings) != 0 {
		t.Fatalf("source-side check produced findings: %+v", findings)
	}
}

func TestCheckParallelBCPIndexRiskMSSQLNoWarnWithoutNonclusteredIndexes(t *testing.T) {
	db := openMSSQLCatalogSQLite(t)
	execMSSQLCatalogSQL(t, db, `INSERT INTO sys.schemas (schema_id, name) VALUES (1, 'dbo')`)
	execMSSQLCatalogSQL(t, db, `INSERT INTO sys.tables (object_id, schema_id, name, is_ms_shipped) VALUES (10, 1, 'Users', 0)`)
	execMSSQLCatalogSQL(t, db, `
		INSERT INTO sys.indexes (object_id, name, type_desc, is_primary_key, is_disabled, is_hypothetical)
		VALUES (10, 'PK_Users', 'CLUSTERED', 1, 0, 0)`)

	findings := checkParallelBCPIndexRiskMSSQL(context.Background(), db, driver.PreFlightRequest{
		Side:   driver.PreFlightSideTarget,
		Schema: "dbo",
	})
	if len(findings) != 0 {
		t.Fatalf("clustered-only catalog produced findings: %+v", findings)
	}
}

func openMSSQLCatalogSQLite(t *testing.T) *sql.DB {
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
		execMSSQLCatalogSQL(t, db, stmt)
	}
	return db
}

func seedMSSQLCatalogIndexes(t *testing.T, db *sql.DB) {
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
		execMSSQLCatalogSQL(t, db, stmt)
	}
}

func execMSSQLCatalogSQL(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()

	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("executing catalog SQL %q: %v", stmt, err)
	}
}
