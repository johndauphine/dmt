package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	_ "github.com/johndauphine/dmt/internal/driver/generic" // register generic dialects
	"github.com/johndauphine/dmt/internal/progress"
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

// TestPostgresStrictParallelReadersMatchNonStrictThroughput is the #662
// performance acceptance check. A view adds a small fixed delay per keyset
// page, making the cost of reader concurrency measurable without faking the
// PostgreSQL snapshot or query paths. Four imported-snapshot readers must
// stay within 15% of four ordinary readers on the same table.
func TestPostgresStrictParallelReadersMatchNonStrictThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("integration benchmark; -short set")
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
	db.SetMaxOpenConns(5) // strict lead plus four imported snapshot readers
	t.Cleanup(func() { _ = db.Close() })

	const (
		dataTable = "dmt662_strict_parallel_bench_data"
		viewTable = "dmt662_strict_parallel_bench"
	)
	for _, q := range []string{
		`DROP VIEW IF EXISTS public.dmt662_strict_parallel_bench`,
		`DROP TABLE IF EXISTS public.dmt662_strict_parallel_bench_data`,
		`CREATE TABLE public.dmt662_strict_parallel_bench_data (id bigint PRIMARY KEY, payload text NOT NULL)`,
		`INSERT INTO public.dmt662_strict_parallel_bench_data SELECT id, 'v-' || id FROM generate_series(1, 128) id`,
		// pg_sleep is evaluated once per page query. It keeps the comparison
		// stable while the real keyset predicates and snapshot joins run.
		`CREATE VIEW public.dmt662_strict_parallel_bench AS SELECT d.id, d.payload FROM public.dmt662_strict_parallel_bench_data d CROSS JOIN LATERAL pg_sleep(0.008) AS page_delay`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP VIEW IF EXISTS public.dmt662_strict_parallel_bench`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS public.dmt662_strict_parallel_bench_data`)
	})

	table := driver.Table{
		Name:   viewTable,
		Schema: "public",
		Columns: []driver.Column{
			{Name: "id", DataType: "int8", IsNullable: false},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         128,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()
	src := &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	cols := []string{"id", "payload"}
	types := []string{"int8", "text"}
	run := func(strict bool) time.Duration {
		cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
			ChunkSize: 2, ParallelReaders: 4, MaxSourceConnections: 5, WriteAheadWriters: 1,
			TargetMode: "drop_recreate", StrictConsistency: strict,
		}}
		ctx := context.Background()
		release := func() {}
		if strict {
			var err error
			ctx, release, err = beginStrictSourceSnapshot(ctx, src, source.Table{Schema: table.Schema, Name: table.Name})
			if err != nil {
				t.Fatalf("begin strict snapshot: %v", err)
			}
		}
		defer release()

		target := &keysetRuntimeTargetPool{updated: true}
		start := time.Now()
		stats, err := executeKeysetPagination(ctx, src, target, cfg, Job{Table: table}, cols, cols, types, []int{0, 0}, progress.New(), nil, 0, nil, table.Name, nil, nil)
		if err != nil || stats.Rows != table.RowCount {
			t.Fatalf("strict=%t run = (rows=%v, err=%v), want %d rows", strict, stats, err, table.RowCount)
		}
		ids, _ := target.snapshot()
		seen := make(map[int]struct{}, len(ids))
		for _, id := range ids {
			seen[id] = struct{}{}
		}
		if len(seen) != int(table.RowCount) {
			t.Fatalf("strict=%t transferred %d unique ids, want %d", strict, len(seen), table.RowCount)
		}
		return time.Since(start)
	}
	median := func(samples []time.Duration) time.Duration {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples[len(samples)/2]
	}

	// Median-of-three absorbs normal CI scheduling noise while preserving a
	// short live integration test. Each pair uses the identical table and
	// configured reader count; strict mode is the only variable.
	nonStrict := median([]time.Duration{run(false), run(false), run(false)})
	strict := median([]time.Duration{run(true), run(true), run(true)})
	t.Logf("live PG strict keyset benchmark: non-strict=%v, strict=%v", nonStrict, strict)
	if strict*100 > nonStrict*115 {
		t.Fatalf("strict parallel readers took %v vs non-strict %v; want within 15%%", strict, nonStrict)
	}
}

// TestPostgresMigrationSnapshotEpochAllowsPartitionedStrictJobs verifies #663
// end to end: independently dispatched partitions import the epoch opened
// before the source mutation, so their combined transfer is the old key set.
func TestPostgresMigrationSnapshotEpochAllowsPartitionedStrictJobs(t *testing.T) {
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
	db.SetMaxOpenConns(3) // epoch lead plus the two partition readers
	t.Cleanup(func() { _ = db.Close() })

	const schema = "dmt663_partitioned_epoch"
	const tableName = "events"
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY, payload text NOT NULL)`, schema, tableName),
		fmt.Sprintf(`INSERT INTO %s.%s VALUES (1,'v-1'),(2,'v-2'),(3,'v-3'),(4,'v-4'),(5,'v-5'),(6,'v-6'),(7,'v-7'),(8,'v-8')`, schema, tableName),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)) })

	src := &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	epoch, err := BeginStrictSnapshotEpoch(ctx, src)
	if err != nil {
		t.Fatalf("BeginStrictSnapshotEpoch: %v", err)
	}
	defer epoch.Close()

	// The epoch predates this replacement. A table-scoped strict transfer
	// started below would see [2..9], while both partition jobs must retain
	// [1..8] from the one migration-wide source view.
	for _, q := range []string{
		fmt.Sprintf(`DELETE FROM %s.%s WHERE id = 1`, schema, tableName),
		fmt.Sprintf(`INSERT INTO %s.%s VALUES (9, 'v-9')`, schema, tableName),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("mutation %q: %v", q, err)
		}
	}

	table := source.Table{
		Schema:     schema,
		Name:       tableName,
		PrimaryKey: []string{"id"},
		Columns: []source.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		RowCount: 8,
	}
	table.PopulatePKColumns()
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			StrictConsistency:      true,
			StrictConsistencyScope: "migration",
			TargetMode:             "drop_recreate",
			ChunkSize:              1,
			ParallelReaders:        1,
			MaxSourceConnections:   3,
			ReadAheadBuffers:       1,
			WriteAheadWriters:      1,
		},
	}
	target := &keysetRuntimeTargetPool{updated: true}
	jobs := []Job{
		{Table: table, Partition: &source.Partition{TableName: tableName, PartitionID: 1, MinPK: int64(1), MaxPK: int64(4), RowCount: 4, IsFirstPartition: true}, StrictSnapshotEpoch: epoch},
		{Table: table, Partition: &source.Partition{TableName: tableName, PartitionID: 2, MinPK: int64(5), MaxPK: int64(8), RowCount: 4}, StrictSnapshotEpoch: epoch},
	}
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job Job) {
			defer wg.Done()
			stats, err := Execute(ctx, src, target, cfg, job, progress.New(), nil)
			if err == nil && stats.Rows != 4 {
				err = fmt.Errorf("partition %d transferred %d rows, want 4", job.Partition.PartitionID, stats.Rows)
			}
			errCh <- err
		}(job)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	ids, _ := target.snapshot()
	if len(ids) != 8 {
		t.Fatalf("partitioned strict transfer copied %d rows (%v), want 8", len(ids), ids)
	}
	seen := make(map[int]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	for id := 1; id <= 8; id++ {
		if !seen[id] {
			t.Fatalf("partitioned strict transfer missed snapshot key %d; got %v", id, ids)
		}
	}
	if seen[9] {
		t.Fatalf("partitioned strict transfer included post-epoch key 9: %v", ids)
	}
}

// TestPostgresMigrationSnapshotEpochKeepsFKRelatedTablesAligned shows the
// reason migration scope exists. Per-table snapshots can straddle a live
// parent/child replacement, while readers joined to one epoch retain the
// original, FK-consistent relationship.
func TestPostgresMigrationSnapshotEpochKeepsFKRelatedTablesAligned(t *testing.T) {
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

	const schema = "dmt663_fk_epoch"
	ctx := context.Background()
	for _, q := range []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s.parents (id int PRIMARY KEY)`, schema),
		fmt.Sprintf(`CREATE TABLE %s.children (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES %s.parents(id))`, schema, schema),
		fmt.Sprintf(`INSERT INTO %s.parents VALUES (1)`, schema),
		fmt.Sprintf(`INSERT INTO %s.children VALUES (1, 1)`, schema),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)) })

	src := &postgresStrictSnapshotSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	epoch, err := BeginStrictSnapshotEpoch(ctx, src)
	if err != nil {
		t.Fatalf("BeginStrictSnapshotEpoch: %v", err)
	}
	defer epoch.Close()

	replaceRelationship := func(next int) {
		t.Helper()
		for _, q := range []string{
			fmt.Sprintf(`DELETE FROM %s.children`, schema),
			fmt.Sprintf(`DELETE FROM %s.parents`, schema),
			fmt.Sprintf(`INSERT INTO %s.parents VALUES (%d)`, schema, next),
			fmt.Sprintf(`INSERT INTO %s.children VALUES (%d, %d)`, schema, next, next),
		} {
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatalf("replace relationship %q: %v", q, err)
			}
		}
	}
	readOne := func(q sourceQueryer, query string) int {
		t.Helper()
		var value int
		if err := q.QueryRowContext(ctx, query).Scan(&value); err != nil {
			t.Fatalf("snapshot query %q: %v", query, err)
		}
		return value
	}

	// Both epoch readers start after the live source has switched to key 2,
	// but they import the pre-replacement view and retain key 1.
	replaceRelationship(2)
	factory := sourceQueryerFactoryForJob(ctx, Job{StrictSnapshotEpoch: epoch})
	parentReader, releaseParent, err := factory(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	parentID := readOne(parentReader, fmt.Sprintf(`SELECT id FROM %s.parents`, schema))
	releaseParent()
	childReader, releaseChild, err := factory(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	childParentID := readOne(childReader, fmt.Sprintf(`SELECT parent_id FROM %s.children`, schema))
	releaseChild()
	if parentID != 1 || childParentID != 1 {
		t.Fatalf("migration epoch relationship = parent %d / child parent_id %d, want 1 / 1", parentID, childParentID)
	}

	// Table scope intentionally permits the two snapshots to differ: parent
	// starts at key 2, then child starts after the next live replacement.
	parentCtx, releaseTableParent, err := beginStrictSourceSnapshot(ctx, src, source.Table{Schema: schema, Name: "parents"})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseTableParent()
	tableParentID := readOne(sourceQueryerFor(parentCtx, db), fmt.Sprintf(`SELECT id FROM %s.parents`, schema))
	replaceRelationship(3)
	childCtx, releaseTableChild, err := beginStrictSourceSnapshot(ctx, src, source.Table{Schema: schema, Name: "children"})
	if err != nil {
		t.Fatal(err)
	}
	defer releaseTableChild()
	tableChildParentID := readOne(sourceQueryerFor(childCtx, db), fmt.Sprintf(`SELECT parent_id FROM %s.children`, schema))
	if tableParentID == tableChildParentID {
		t.Fatalf("table-scoped snapshots unexpectedly matched at key %d; want reproduction of cross-table skew", tableParentID)
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
