package generic

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmt/internal/driver"
)

func TestMySQLLockTablesPreflightPrivilegeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/?multiStatements=true")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MYSQL_REQUIRED") == "1" {
			t.Fatalf("mysql required but not reachable: %v", err)
		}
		t.Skipf("mysql not reachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	dbName := "dmt_preflight_lock_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	user := "dmt_lp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	_, _ = admin.Exec("DROP USER IF EXISTS '" + user + "'@'%'")
	_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP USER IF EXISTS '" + user + "'@'%'"); err != nil {
			t.Errorf("drop temporary MySQL user %s: %v", user, err)
		}
		if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`"); err != nil {
			t.Errorf("drop temporary MySQL database %s: %v", dbName, err)
		}
	})
	if _, err := admin.Exec("CREATE DATABASE `" + dbName + "`; CREATE TABLE `" + dbName + "`.events (id INT PRIMARY KEY) ENGINE=InnoDB; CREATE USER '" + user + "'@'%' IDENTIFIED BY 'TestPass2024'; GRANT SELECT ON `" + dbName + "`.* TO '" + user + "'@'%'"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", user+":TestPass2024@tcp(localhost:3306)/"+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	req := driver.PreFlightRequest{
		Side:              driver.PreFlightSideSource,
		Schema:            dbName,
		StrictConsistency: true,
		ParallelReaders:   4,
		IncludeTables:     []string{"events"},
	}
	findings := mysqlPFCheckLockTablesMySQL(context.Background(), db, req)
	if len(findings) != 1 || findings[0].Check != "privileges.lock_tables" || findings[0].Severity != driver.SeverityWarn {
		t.Fatalf("missing-privilege findings = %+v", findings)
	}
	if _, err := admin.Exec("GRANT LOCK TABLES ON `" + dbName + "`.* TO '" + user + "'@'%'"); err != nil {
		t.Fatal(err)
	}
	// MySQL caches static privilege state on an established account session.
	// Reconnect just as an operator would after applying the preflight remedy.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("mysql", user+":TestPass2024@tcp(localhost:3306)/"+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if findings := mysqlPFCheckLockTablesMySQL(context.Background(), db, req); len(findings) != 0 {
		t.Fatalf("granted LOCK TABLES findings = %+v, want none", findings)
	}
}
