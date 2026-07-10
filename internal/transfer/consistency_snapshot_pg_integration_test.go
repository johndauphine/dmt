package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
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
	strictSource := &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	// #664 captures validation evidence through this same pinned transaction,
	// before target preparation or concurrent source changes can affect it.
	snapshotCount, err := strictSnapshotRowCount(snapshotCtx, strictSource, source.Table{Schema: schema, Name: table})
	if err != nil || snapshotCount != 4 {
		t.Fatalf("strictSnapshotRowCount = (%d, %v), want (4, nil)", snapshotCount, err)
	}
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
	if countAfterMutation, err := strictSnapshotRowCount(snapshotCtx, strictSource, source.Table{Schema: schema, Name: table}); err != nil || countAfterMutation != 4 {
		t.Fatalf("strictSnapshotRowCount after mutation = (%d, %v), want pinned 4", countAfterMutation, err)
	}

	page2 := page(2, 2)
	if !snapshotEqualInts(page2, []int{3, 4}) {
		t.Fatalf("snapshot page 2 = %v, want original [3 4]", page2)
	}
}

// TestPostgresExportedSnapshotJoinsParallelReaders proves #662's strict
// keyset primitive. The lead exports its snapshot, the source mutates, then
// four independent reader transactions import that already-established view.
// Their combined key set must still be the original table, and the database
// pool must release every joined transaction before the lead is released.
func TestPostgresExportedSnapshotJoinsParallelReaders(t *testing.T) {
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
	db.SetMaxOpenConns(5) // one lead transaction plus four imported readers
	t.Cleanup(func() { _ = db.Close() })

	const schema = "dmt662_exported_snapshot"
	const table = "events"
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, schema, table),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	for id := 1; id <= 16; id++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s.%s VALUES ($1, $2)`, schema, table), id, fmt.Sprintf("v-%d", id)); err != nil {
			t.Fatalf("seed row %d: %v", id, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)) })

	sourcePool := &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	snapshotCtx, releaseLead, err := beginStrictSourceSnapshot(ctx, sourcePool, source.Table{Schema: schema, Name: table})
	if err != nil {
		t.Fatalf("beginStrictSourceSnapshot: %v", err)
	}
	defer releaseLead()
	factory := sourceQueryerFactoryFor(snapshotCtx)
	if factory == nil {
		t.Fatal("PostgreSQL strict snapshot did not install a reader factory")
	}

	// The exporting query already fixed the lead transaction's snapshot. Every
	// reader below joins after this mutation and must nevertheless see [1,16].
	for _, q := range []string{
		fmt.Sprintf(`DELETE FROM %s.%s WHERE id = 1`, schema, table),
		fmt.Sprintf(`UPDATE %s.%s SET payload = 'changed' WHERE id = 5`, schema, table),
		fmt.Sprintf(`INSERT INTO %s.%s VALUES (17, 'new')`, schema, table),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("concurrent mutation %q: %v", q, err)
		}
	}

	type readerResult struct {
		worker int
		pid    int
		keys   map[int]string
		err    error
	}
	results := make(chan readerResult, 4)
	releaseReaders := make(chan struct{})
	var releaseOnce sync.Once
	allowRelease := func() { releaseOnce.Do(func() { close(releaseReaders) }) }
	var readers sync.WaitGroup
	defer func() {
		allowRelease()
		readers.Wait()
	}()

	for worker := 0; worker < 4; worker++ {
		readers.Add(1)
		go func(worker int) {
			defer readers.Done()
			queryer, release, err := factory(snapshotCtx, worker)
			if err != nil {
				results <- readerResult{worker: worker, err: err}
				return
			}
			defer release()

			result := readerResult{worker: worker, keys: make(map[int]string)}
			if err := queryer.QueryRowContext(snapshotCtx, "SELECT pg_backend_pid()").Scan(&result.pid); err != nil {
				result.err = err
				results <- result
				return
			}
			rows, err := queryer.QueryContext(snapshotCtx,
				fmt.Sprintf(`SELECT id, payload FROM %s.%s WHERE (id - 1) %% 4 = $1 ORDER BY id`, schema, table), worker)
			if err != nil {
				result.err = err
				results <- result
				return
			}
			for rows.Next() {
				var id int
				var payload string
				if err := rows.Scan(&id, &payload); err != nil {
					result.err = err
					break
				}
				result.keys[id] = payload
			}
			if err := rows.Err(); err != nil && result.err == nil {
				result.err = err
			}
			if err := rows.Close(); err != nil && result.err == nil {
				result.err = err
			}
			results <- result
			<-releaseReaders
		}(worker)
	}

	keys := make(map[int]string)
	pids := make(map[int]struct{})
	for range 4 {
		result := <-results
		if result.err != nil {
			t.Fatalf("reader %d: %v", result.worker, result.err)
		}
		pids[result.pid] = struct{}{}
		for id, payload := range result.keys {
			if prior, exists := keys[id]; exists {
				t.Fatalf("duplicate snapshot key %d (%q and %q)", id, prior, payload)
			}
			keys[id] = payload
		}
	}
	if len(pids) != 4 {
		t.Fatalf("reader backend PIDs = %v, want four independent connections", pids)
	}
	if len(keys) != 16 {
		t.Fatalf("snapshot key count = %d, want 16 keys: %v", len(keys), keys)
	}
	for id := 1; id <= 16; id++ {
		if got := keys[id]; got != fmt.Sprintf("v-%d", id) {
			t.Fatalf("snapshot row %d = %q, want original %q", id, got, fmt.Sprintf("v-%d", id))
		}
	}
	if got := db.Stats().InUse; got != 5 {
		t.Fatalf("open strict transactions = %d, want lead plus four readers", got)
	}

	allowRelease()
	readers.Wait()
	if got := db.Stats().InUse; got != 1 {
		t.Fatalf("open strict transactions after reader release = %d, want lead only", got)
	}
	releaseLead()
	if got := db.Stats().InUse; got != 0 {
		t.Fatalf("open strict transactions after lead release = %d, want none", got)
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
