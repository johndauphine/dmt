package generic

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestMSSQLDatabaseSnapshotPreflightOrphanIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("sqlserver", mssqlDSNBase+"master&encrypt=disable")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MSSQL_REQUIRED") == "1" {
			t.Fatalf("SQL Server required but unreachable: %v", err)
		}
		t.Skipf("SQL Server unreachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	suffix := strconv.FormatUint(uint64(uint32(time.Now().UnixNano())), 16)
	dbName := "dmt_pf_" + suffix
	snapshotName := "dmt_strict_pf_" + suffix
	if _, err := admin.Exec("CREATE DATABASE " + mssqlTestQuote(dbName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + mssqlTestQuote(snapshotName))
		_, _ = admin.Exec("ALTER DATABASE " + mssqlTestQuote(dbName) + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE " + mssqlTestQuote(dbName))
	})
	db, err := sql.Open("sqlserver", mssqlDSNBase+dbName+"&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	req := driver.PreFlightRequest{Side: driver.PreFlightSideSource, Database: dbName, StrictConsistency: true, StrictConsistencyScope: "migration"}
	if findings := mssqlPFDatabaseSnapshot(context.Background(), db, req); len(findings) != 0 {
		t.Fatalf("supported snapshot preflight findings = %+v", findings)
	}
	var logical, physical string
	if err := admin.QueryRow(`SELECT TOP 1 name, physical_name FROM sys.master_files WHERE database_id = DB_ID(@p1) AND type = 0 ORDER BY file_id`, dbName).Scan(&logical, &physical); err != nil {
		t.Fatal(err)
	}
	pos := strings.LastIndexAny(physical, `/\`)
	dir := ""
	if pos >= 0 {
		dir = physical[:pos+1]
	}
	create := fmt.Sprintf("CREATE DATABASE %s ON (NAME = %s, FILENAME = %s) AS SNAPSHOT OF %s", mssqlTestQuote(snapshotName), mssqlTestLiteral(logical), mssqlTestLiteral(dir+snapshotName+".ss"), mssqlTestQuote(dbName))
	if _, err := admin.Exec(create); err != nil {
		t.Fatal(err)
	}
	findings := mssqlPFDatabaseSnapshot(context.Background(), db, req)
	if len(findings) != 1 || findings[0].Check != "strict.snapshot_orphans" || findings[0].Severity != driver.SeverityWarn || !strings.Contains(findings[0].Message, snapshotName) {
		t.Fatalf("orphan findings = %+v", findings)
	}
}

func mssqlTestQuote(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func mssqlTestLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}
