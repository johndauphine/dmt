package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	_ "github.com/johndauphine/dmt/internal/driver/generic" // register generic dialects
	"github.com/johndauphine/dmt/internal/source"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestRowNumberPaginationSubstitutesRowsUnderConcurrentMutation reproduces the
// #640 silent divergence: without a point-in-time source snapshot, a delete and
// insert between two ROW_NUMBER pages shift the ordinal window so a row is
// skipped and another substituted — while the total row count is unchanged, so
// count validation cannot detect it.
//
// Keys begin [1,2,3,4]. Page 1 (offset 0, limit 2) returns [1,2]. Before page 2,
// key 1 is deleted and key 5 inserted → set becomes [2,3,4,5]. Page 2
// (offset 2, limit 2) over the NEW set returns [4,5], silently skipping 3.
//
// Requires the pg test container (localhost:5432, postgres/TestPass2024); skips
// when unreachable unless PG_REQUIRED=1.
func TestRowNumberPaginationSubstitutesRowsUnderConcurrentMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	const dsn = "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = "dmt640_snapshot"
	const table = "events"
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, schema, table),
		fmt.Sprintf(`INSERT INTO %s.%s VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')`, schema, table),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)) })

	dialect := driver.GetDialect("postgres")
	if dialect == nil {
		t.Fatal("no postgres dialect registered")
	}

	page := func(rowStart int64, limit int) []int {
		q := dialect.BuildRowNumberQuery(`"id", "payload"`, `"id"`, schema, table, "", nil)
		args := dialect.BuildRowNumberArgs(rowStart, limit, nil)
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("page query [%d,+%d): %v\n%s", rowStart, limit, err, q)
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

	// Page 1 over [1,2,3,4].
	if got := page(0, 2); !snapshotEqualInts(got, []int{1, 2}) {
		t.Fatalf("page 1 = %v, want [1 2]", got)
	}

	// Concurrent mutation between pages: delete a transferred key, insert a new
	// high key. Each page is an independent query on a pooled connection, so
	// page 2 sees the mutated table.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s.%s WHERE id=1`, schema, table)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.%s VALUES (5,'e')`, schema, table)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Page 2 over the mutated set [2,3,4,5] at offset 2 → [4,5]; key 3 skipped.
	page2 := page(2, 2)
	transferred := append([]int{1, 2}, page2...)
	if snapshotContainsInt(transferred, 3) {
		t.Fatalf("expected reproduction: key 3 should be skipped, got transferred=%v", transferred)
	}

	// The divergence is silent: source and the transferred set have the SAME
	// count (4), so count validation would pass despite different key sets.
	var srcCount int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.%s`, schema, table)).Scan(&srcCount); err != nil {
		t.Fatalf("count: %v", err)
	}
	if srcCount != len(transferred) {
		t.Fatalf("counts differ (src=%d transferred=%d); the reproduction relies on equal counts", srcCount, len(transferred))
	}
	t.Logf("reproduced #640: transferred keys %v vs source [2 3 4 5] — count %d matches but key 3 lost", transferred, srcCount)
}

// TestRowNumberPaginationStrictSnapshotPreventsSubstitution verifies the
// PostgreSQL side of #640's contract. The writer commits the same delete/insert
// mutation between pages, but both reads execute through one repeatable-read
// transaction and therefore return the original keys [1,2,3,4].
func TestRowNumberPaginationStrictSnapshotPreventsSubstitution(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	const dsn = "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = "dmt640_strict_snapshot"
	const table = "events"
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, schema, table),
		fmt.Sprintf(`INSERT INTO %s.%s VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')`, schema, table),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)) })

	dialect := driver.GetDialect("postgres")
	if dialect == nil {
		t.Fatal("no postgres dialect registered")
	}
	snapshotCtx, releaseSnapshot, err := beginStrictSourceSnapshot(ctx, &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}, source.Table{Schema: schema, Name: table})
	if err != nil {
		t.Fatalf("beginStrictSourceSnapshot: %v", err)
	}
	defer releaseSnapshot()
	queryer := sourceQueryerFor(snapshotCtx, db)
	page := func(rowStart int64, limit int) []int {
		q := dialect.BuildRowNumberQuery(`"id", "payload"`, `"id"`, schema, table, "", nil)
		args := dialect.BuildRowNumberArgs(rowStart, limit, nil)
		rows, err := queryer.QueryContext(snapshotCtx, q, args...)
		if err != nil {
			t.Fatalf("page query [%d,+%d): %v\n%s", rowStart, limit, err, q)
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
		if err := rows.Err(); err != nil {
			t.Fatalf("page rows: %v", err)
		}
		return ids
	}

	if got := page(0, 2); !snapshotEqualInts(got, []int{1, 2}) {
		t.Fatalf("page 1 = %v, want [1 2]", got)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s.%s WHERE id=1`, schema, table)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.%s VALUES (5,'e')`, schema, table)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	page2 := page(2, 2)
	if !snapshotEqualInts(page2, []int{3, 4}) {
		t.Fatalf("snapshot page 2 = %v, want original [3 4]", page2)
	}
}

type postgresStrictSnapshotSource struct {
	*keysetRuntimeSourcePool
}

func (p *postgresStrictSnapshotSource) DBType() string { return "postgres" }

func snapshotEqualInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func snapshotContainsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
