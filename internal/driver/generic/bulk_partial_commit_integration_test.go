package generic

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

// TestRunBatchesPartialCommitOffset pins #541: on a non-transactional target,
// runBatches autocommits each sub-batch, so a mid-chunk failure leaves a
// committed prefix. It must report that prefix via *driver.PartialWriteError so
// the transfer retry resumes after it instead of duplicating from row 0. The
// transactional path rolls the whole chunk back and reports a plain error.
// Requires the mysql-test container (localhost:3306, root/TestPass2024); skips
// via mysqlBootstrap when unreachable unless MYSQL_REQUIRED=1.
func TestRunBatchesPartialCommitOffset(t *testing.T) {
	ctx := context.Background()
	const dbName = "dmt_behav_partial541"
	mysqlBootstrap(t, dbName)

	raw, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/"+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	// val is NOT NULL, so the third row (NULL) fails; batch size 1 makes each
	// row its own autocommitted sub-batch.
	if _, err := raw.Exec(`CREATE TABLE partial (id INT PRIMARY KEY, val INT NOT NULL)`); err != nil {
		t.Fatal(err)
	}

	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDialect(cat)
	identity := func(r []any) []any { return r }
	rows := [][]any{{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), nil}, {int64(4), int64(40)}}
	opts := driver.WriteBatchOptions{
		Schema: dbName, Table: "partial",
		Columns: []string{"id", "val"}, Rows: rows, BatchSize: 1,
	}
	count := func() int {
		var n int
		if err := raw.QueryRow("SELECT COUNT(*) FROM partial").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("non-transactional reports committed offset and keeps committed rows", func(t *testing.T) {
		if _, err := raw.Exec("TRUNCATE partial"); err != nil {
			t.Fatal(err)
		}
		env := bulkEnv{db: raw, dialect: d, batchSize: 1, transactional: false, convert: identity, engine: "mysql"}
		err := batchedInsert(ctx, env, opts)
		var pw *driver.PartialWriteError
		if !errors.As(err, &pw) {
			t.Fatalf("want *driver.PartialWriteError, got %T: %v", err, err)
		}
		if pw.Committed != 2 {
			t.Errorf("Committed = %d, want 2", pw.Committed)
		}
		if n := count(); n != 2 {
			t.Errorf("committed rows = %d, want 2 (autocommit kept rows 1..2)", n)
		}
	})

	t.Run("transactional rolls back and reports no committed prefix", func(t *testing.T) {
		if _, err := raw.Exec("TRUNCATE partial"); err != nil {
			t.Fatal(err)
		}
		env := bulkEnv{db: raw, dialect: d, batchSize: 1, transactional: true, convert: identity, engine: "mysql"}
		err := batchedInsert(ctx, env, opts)
		if err == nil {
			t.Fatal("expected the NOT NULL violation error")
		}
		var pw *driver.PartialWriteError
		if errors.As(err, &pw) {
			t.Fatalf("transactional path must not report PartialWriteError, got: %v", err)
		}
		if n := count(); n != 0 {
			t.Errorf("rolled-back rows = %d, want 0", n)
		}
	})
}
