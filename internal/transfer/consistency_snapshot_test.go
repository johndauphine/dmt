package transfer

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/driver"
	_ "github.com/johndauphine/dmt/v5/internal/driver/generic" // register generic dialects

	_ "modernc.org/sqlite"
)

// TestRowNumberPaginationSubstitutesRowsUnderMutationSQLite is the deterministic
// (no-container) analogue of the live-PG reproduction: it drives the real SQLite
// ROW_NUMBER pagination query across a mutation between pages and shows a row is
// silently substituted while the count is unchanged (#640). Runs in the normal
// suite so the contract stays pinned without the Postgres container.
func TestRowNumberPaginationSubstitutesRowsUnderMutationSQLite(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	for _, q := range []string{
		`CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`,
		`INSERT INTO events VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	dialect := driver.GetDialect("sqlite")
	if dialect == nil {
		t.Fatal("no sqlite dialect registered")
	}

	page := func(rowStart int64, limit int) []int {
		q := dialect.BuildRowNumberQuery(`"id", "payload"`, `"id"`, "", "events", "", nil)
		args := dialect.BuildRowNumberArgs(rowStart, limit, nil)
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("page [%d,+%d): %v\n%s", rowStart, limit, err, q)
		}
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			var payload string
			if err := rows.Scan(&id, &payload); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		return ids
	}

	if got := page(0, 2); !snapshotEqualInts(got, []int{1, 2}) {
		t.Fatalf("page 1 = %v, want [1 2]", got)
	}

	// Mutation between pages: delete a transferred key, insert a new high key.
	if _, err := db.ExecContext(ctx, `DELETE FROM events WHERE id=1`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events VALUES (5,'e')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	transferred := append([]int{1, 2}, page(2, 2)...)
	if snapshotContainsInt(transferred, 3) {
		t.Fatalf("expected key 3 to be skipped, got transferred=%v", transferred)
	}

	var srcCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&srcCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if srcCount != len(transferred) {
		t.Fatalf("counts differ (src=%d transferred=%d); reproduction relies on equal counts", srcCount, len(transferred))
	}
}
