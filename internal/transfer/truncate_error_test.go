package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
)

// TestIsTableNotFoundError pins the cross-engine "table does not exist"
// classification used by the pre-transfer truncate (#619). Not-found is
// benign (quiet); anything else — permission, lock, syntax — must return
// false so it is surfaced instead of silently swallowed.
func TestIsTableNotFoundError(t *testing.T) {
	notFound := []string{
		// postgres (pq / pgx)
		`pq: relation "public.orders" does not exist`,
		`ERROR: relation "orders" does not exist (SQLSTATE 42P01)`,
		// mysql / mariadb
		`Error 1146: Table 'shop.orders' doesn't exist`,
		`Unknown table 'shop.orders'`,
		// sqlite
		`no such table: orders`,
		// mssql — pure not-found (no permission wording)
		`mssql: Invalid object name 'dbo.orders'.`,
		// case-insensitivity
		`RELATION "ORDERS" DOES NOT EXIST`,
	}
	for _, msg := range notFound {
		if !isTableNotFoundError(errors.New(msg)) {
			t.Errorf("isTableNotFoundError(%q) = false, want true", msg)
		}
	}

	realErrors := []string{
		`pq: permission denied for table orders`,
		`Error 1044: Access denied for user 'app'@'%' to database 'shop'`,
		`mssql: The TRUNCATE TABLE permission was denied on the object 'orders'`,
		// mssql's ambiguous message: it fires on permission-denied too, so
		// the permission mention must win over the not-found wording and
		// keep it surfaced (codex review on #619).
		`Cannot find the object "dbo.orders" because it does not exist or you do not have permissions.`,
		`Lock wait timeout exceeded; try restarting transaction`,
		`database is locked`,
		`context deadline exceeded`,
		`connection reset by peer`,
	}
	for _, msg := range realErrors {
		if isTableNotFoundError(errors.New(msg)) {
			t.Errorf("isTableNotFoundError(%q) = true, want false (must be surfaced, not swallowed)", msg)
		}
	}

	if isTableNotFoundError(nil) {
		t.Error("isTableNotFoundError(nil) = true, want false")
	}
}

// TestTruncateErrorDoesNotAbortTransfer drives the pre-transfer truncate
// path with a target whose TruncateTable fails with a non-not-found error
// (permission denied). The transfer must proceed (warn-and-continue,
// matching the sibling cleanup branches) rather than silently swallow it
// or newly fail-fast (#619). Both the not-found and real-error cases keep
// the pipeline running; the difference is the log level.
func TestTruncateErrorDoesNotAbortTransfer(t *testing.T) {
	const totalRows = 40

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"permission error surfaces but continues", errors.New("pq: permission denied for table items")},
		{"not-found is benign and continues", errors.New(`pq: relation "items" does not exist`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := seedKeysetRuntimeTunerDB(t, totalRows)
			srcPool := &keysetRuntimeSourcePool{db: db}
			tgtPool := &keysetRuntimeTargetPool{updated: true, truncateErr: tc.err}

			table := driver.Table{
				Name: "items",
				Columns: []driver.Column{
					{Name: "id", DataType: "integer"},
					{Name: "payload", DataType: "text"},
				},
				PrimaryKey:       []string{"id"},
				RowCount:         totalRows,
				EstimatedRowSize: 32,
			}
			table.PopulatePKColumns()

			cfg := &config.Config{
				Target: config.TargetConfig{Schema: ""},
				Migration: config.MigrationConfig{
					ChunkSize:         10,
					ParallelReaders:   2,
					WriteAheadWriters: 1,
					TargetMode:        "drop_recreate",
				},
			}

			stats, err := Execute(context.Background(), srcPool, tgtPool, cfg, Job{Table: table}, progress.New(), nil)
			if err != nil {
				t.Fatalf("Execute aborted on truncate error, want warn-and-continue: %v", err)
			}
			if stats.Rows != totalRows {
				t.Fatalf("transferred %d rows, want %d (truncate failure must not drop rows)", stats.Rows, totalRows)
			}
		})
	}
}
