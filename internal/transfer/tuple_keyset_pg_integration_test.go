package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/progress"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type pgTupleSourcePool struct{ keysetRuntimeSourcePool }

func (p *pgTupleSourcePool) DBType() string { return "postgres" }

// TestTupleKeysetPostgresUUIDNumeric drives the tuple keyset path against a
// live PostgreSQL for the #629 types that motivated the issue: a uuid PK
// (whose MIN()/MAX() aggregates don't exist — the tuple path never issues
// one) and a numeric PK, each paged chunk-by-chunk with a crash-resume
// through the typed watermark codec. Every row must arrive exactly once.
//
// Requires the pg test container (localhost:5432, postgres/TestPass2024).
// Skips when unreachable unless PG_REQUIRED=1.
func TestTupleKeysetPostgresUUIDNumeric(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	db, err := sql.Open("pgx", "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable")
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	defer db.Close()

	setup := []string{
		`DROP TABLE IF EXISTS dmt_tuple_uuid_test`,
		`CREATE TABLE dmt_tuple_uuid_test (id uuid PRIMARY KEY, val int NOT NULL)`,
		`INSERT INTO dmt_tuple_uuid_test SELECT gen_random_uuid(), g FROM generate_series(1, 500) g`,
		`DROP TABLE IF EXISTS dmt_tuple_num_test`,
		`CREATE TABLE dmt_tuple_num_test (id numeric(20,4) PRIMARY KEY, val int NOT NULL)`,
		`INSERT INTO dmt_tuple_num_test SELECT g + 0.1234, g FROM generate_series(1, 500) g`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS dmt_tuple_uuid_test`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS dmt_tuple_num_test`)
	})

	cases := []struct {
		table  string
		pkType string
	}{
		{"dmt_tuple_uuid_test", "uuid"},
		{"dmt_tuple_num_test", "numeric"},
	}
	for _, tc := range cases {
		t.Run(tc.pkType, func(t *testing.T) {
			table := driver.Table{
				Name:   tc.table,
				Schema: "public",
				Columns: []driver.Column{
					{Name: "id", DataType: tc.pkType, IsNullable: false},
					{Name: "val", DataType: "int4"},
				},
				PrimaryKey:       []string{"id"},
				RowCount:         500,
				EstimatedRowSize: 48,
			}
			table.PopulatePKColumns()
			if !driver.TupleKeysetRoutable(&table, "postgres") {
				t.Fatalf("precondition: pg %s PK must be tuple-routable", tc.pkType)
			}

			cfg := &config.Config{
				Target: config.TargetConfig{Schema: ""},
				Migration: config.MigrationConfig{
					ChunkSize:         37, // many watermark boundaries
					WriteAheadWriters: 1,
					TargetMode:        "drop_recreate",
				},
			}

			run := func(resumeTuple []any) []string {
				src := &pgTupleSourcePool{keysetRuntimeSourcePool{db: db}}
				tgt := &compositeTextTargetPool{}
				_, err := executeCompositeKeysetPagination(
					context.Background(), src, tgt, cfg, Job{Table: table},
					[]string{"id", "val"}, []string{"id", "val"},
					[]string{tc.pkType, "int4"}, []int{0, 0},
					progress.New(), resumeTuple, 0, tc.table, nil, nil,
				)
				if err != nil {
					t.Fatalf("executeCompositeKeysetPagination(%s): %v", tc.pkType, err)
				}
				return tgt.codes()
			}

			got := run(nil)
			if len(got) != 500 {
				t.Fatalf("full paging returned %d rows, want 500", len(got))
			}
			seen := make(map[string]bool, 500)
			for _, k := range got {
				if seen[k] {
					t.Fatalf("duplicate PK %q — keyset re-read a row", k)
				}
				seen[k] = true
			}

			// Crash-resume from row ~200's watermark through the typed codec:
			// remaining rows must be exactly the tail, no dups, no gaps.
			wm := decodeCompositeTuple(encodeCompositeTuple([]any{got[199]}))
			if len(wm) != 1 {
				t.Fatalf("watermark round-trip = %v", wm)
			}
			rest := run(wm)
			if len(rest) != 300 {
				t.Fatalf("resume returned %d rows, want 300 (rows after index 199)", len(rest))
			}
			if fmt.Sprint(rest[0]) == fmt.Sprint(got[199]) {
				t.Fatal("resume re-read the watermark row (strict > violated)")
			}
			for i, k := range rest {
				if got[200+i] != k {
					t.Fatalf("resume row %d = %q, want %q (order/gap mismatch)", i, k, got[200+i])
				}
			}
		})
	}
}

// TestParallelTupleKeysetPostgres executes the #667 range-split path against
// PostgreSQL for both common composite shapes: integer/integer and an
// int64-safe leading component followed by text. The second shape protects
// the tuple comparison inside each numeric range from regressing back to a
// numeric-only assumption.
func TestParallelTupleKeysetPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	db, err := sql.Open("pgx", "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable")
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	defer db.Close()

	setup := []string{
		`DROP TABLE IF EXISTS dmt_tuple_parallel_pg_int`,
		`CREATE TABLE dmt_tuple_parallel_pg_int (tenant_id bigint NOT NULL, seq bigint NOT NULL, val text NOT NULL, PRIMARY KEY (tenant_id, seq))`,
		`INSERT INTO dmt_tuple_parallel_pg_int SELECT tenant, seq, 'v-' || tenant || '-' || seq FROM generate_series(1, 48) tenant, generate_series(1, 9) seq`,
		`DROP TABLE IF EXISTS dmt_tuple_parallel_pg_text`,
		`CREATE TABLE dmt_tuple_parallel_pg_text (tenant_id bigint NOT NULL, name text COLLATE "C" NOT NULL, val int NOT NULL, PRIMARY KEY (tenant_id, name))`,
		`INSERT INTO dmt_tuple_parallel_pg_text SELECT tenant, 'name-' || lpad(seq::text, 2, '0'), seq FROM generate_series(1, 48) tenant, generate_series(1, 9) seq`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP TABLE IF EXISTS dmt_tuple_parallel_pg_int`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS dmt_tuple_parallel_pg_text`)
	})

	cases := []struct {
		table string
		name  string
		cols  []string
		types []string
	}{
		{table: "dmt_tuple_parallel_pg_int", name: "integer_second_component", cols: []string{"tenant_id", "seq", "val"}, types: []string{"int8", "int8", "text"}},
		{table: "dmt_tuple_parallel_pg_text", name: "text_second_component", cols: []string{"tenant_id", "name", "val"}, types: []string{"int8", "text", "int4"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := driver.Table{
				Name:   tc.table,
				Schema: "public",
				Columns: []driver.Column{
					{Name: "tenant_id", DataType: "int8", IsNullable: false},
					{Name: tc.cols[1], DataType: tc.types[1], IsNullable: false},
					{Name: "val", DataType: tc.types[2]},
				},
				PrimaryKey:       []string{"tenant_id", tc.cols[1]},
				RowCount:         48 * 9,
				EstimatedRowSize: 64,
			}
			table.PopulatePKColumns()
			if !driver.TupleKeysetRoutable(&table, "postgres") {
				t.Fatal("precondition: PostgreSQL composite table must be tuple-routable")
			}
			cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
				ChunkSize: 5, ParallelReaders: 4, WriteAheadWriters: 1, TargetMode: "drop_recreate",
			}}
			tgt := newCompositeAnyTargetPool()
			stats, used, err := executeParallelCompositeKeysetPagination(
				context.Background(), &pgTupleSourcePool{keysetRuntimeSourcePool{db: db}}, tgt, cfg, Job{Table: table},
				tc.cols, tc.cols, tc.types, []int{0, 0, 0}, progress.New(), nil, 0, table.Name, nil, nil,
			)
			if err != nil || !used {
				t.Fatalf("parallel PostgreSQL tuple path = (used=%v, err=%v), want success", used, err)
			}
			if stats.Rows != table.RowCount {
				t.Fatalf("rows = %d, want %d", stats.Rows, table.RowCount)
			}
			tgt.assertExact(t, int(table.RowCount))

			if tc.name == "integer_second_component" {
				src := &pgTupleSourcePool{keysetRuntimeSourcePool{db: db}}
				epoch, err := BeginStrictSnapshotEpoch(context.Background(), src)
				if err != nil {
					t.Fatalf("begin migration epoch: %v", err)
				}
				defer epoch.Close()
				if _, err := db.Exec(`INSERT INTO dmt_tuple_parallel_pg_int VALUES (99, 1, 'later')`); err != nil {
					t.Fatalf("post-epoch insert: %v", err)
				}
				strictCfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
					StrictConsistency: true, StrictConsistencyScope: "migration", ChunkSize: 5, ParallelReaders: 4,
					MaxSourceConnections: 5, WriteAheadWriters: 1, TargetMode: "drop_recreate",
				}}
				strictTarget := newCompositeAnyTargetPool()
				strictStats, strictUsed, err := executeParallelCompositeKeysetPagination(
					context.Background(), src, strictTarget, strictCfg, Job{Table: table, StrictSnapshotEpoch: epoch},
					tc.cols, tc.cols, tc.types, []int{0, 0, 0}, progress.New(), nil, 0, table.Name, nil, nil,
				)
				if err != nil || !strictUsed || strictStats.Rows != table.RowCount {
					t.Fatalf("strict migration PG tuple path = (used=%v, rows=%v, err=%v), want snapshot %d", strictUsed, strictStats, err, table.RowCount)
				}
				strictTarget.assertExact(t, int(table.RowCount))
			}
		})
	}
}

// TestParallelTupleKeysetPostgresReaderSpeedup is a controlled live-PG
// benchmark regression. The view injects a small source-page latency so the
// reader ceiling is observable independently of a local bulk-writer's speed:
// one tuple reader pays it for every page serially, while four range readers
// overlap those page waits. This proves the configured N-reader path gives a
// measurable throughput win on a composite-PK table, not just multiple empty
// goroutines (#667).
func TestParallelTupleKeysetPostgresReaderSpeedup(t *testing.T) {
	if testing.Short() {
		t.Skip("integration benchmark; -short set")
	}
	db, err := sql.Open("pgx", "postgres://postgres:TestPass2024@localhost:5432/postgres?sslmode=disable")
	if err == nil {
		err = db.Ping()
	}
	if err != nil {
		if os.Getenv("PG_REQUIRED") == "1" {
			t.Fatalf("postgres required but not reachable: %v", err)
		}
		t.Skipf("postgres not reachable: %v", err)
	}
	defer db.Close()

	setup := []string{
		`DROP VIEW IF EXISTS public.dmt_tuple_parallel_pg_bench`,
		`DROP TABLE IF EXISTS public.dmt_tuple_parallel_pg_bench_data`,
		`CREATE TABLE public.dmt_tuple_parallel_pg_bench_data (tenant_id bigint NOT NULL, seq bigint NOT NULL, val text NOT NULL, PRIMARY KEY (tenant_id, seq))`,
		`INSERT INTO public.dmt_tuple_parallel_pg_bench_data SELECT tenant, seq, 'v-' || tenant || '-' || seq FROM generate_series(1, 64) tenant, generate_series(1, 2) seq`,
		// pg_sleep is evaluated once per page query. Its small fixed cost makes
		// the reader-concurrency benefit deterministic enough for CI while all
		// tuple predicates and scans remain PostgreSQL's real implementation.
		`CREATE VIEW public.dmt_tuple_parallel_pg_bench AS SELECT d.tenant_id, d.seq, d.val FROM public.dmt_tuple_parallel_pg_bench_data d CROSS JOIN LATERAL pg_sleep(0.008) AS page_delay`,
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP VIEW IF EXISTS public.dmt_tuple_parallel_pg_bench`)
		_, _ = db.Exec(`DROP TABLE IF EXISTS public.dmt_tuple_parallel_pg_bench_data`)
	})

	table := driver.Table{
		Name:   "dmt_tuple_parallel_pg_bench",
		Schema: "public",
		Columns: []driver.Column{
			{Name: "tenant_id", DataType: "int8", IsNullable: false},
			{Name: "seq", DataType: "int8", IsNullable: false},
			{Name: "val", DataType: "text"},
		},
		PrimaryKey:       []string{"tenant_id", "seq"},
		RowCount:         128,
		EstimatedRowSize: 64,
	}
	table.PopulatePKColumns()
	runStrict := func(readers int) time.Duration {
		src := &pgTupleSourcePool{keysetRuntimeSourcePool{db: db}}
		epoch, err := BeginStrictSnapshotEpoch(context.Background(), src)
		if err != nil {
			t.Fatalf("begin strict speed epoch: %v", err)
		}
		defer epoch.Close()
		cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
			StrictConsistency: true, StrictConsistencyScope: "migration", MaxSourceConnections: 5,
			ChunkSize: 2, ParallelReaders: readers, WriteAheadWriters: 1, TargetMode: "drop_recreate",
		}}
		start := time.Now()
		stats, err := Execute(context.Background(), src, newCompositeAnyTargetPool(), cfg, Job{Table: table, StrictSnapshotEpoch: epoch}, progress.New(), nil)
		if err != nil || stats.Rows != table.RowCount {
			t.Fatalf("strict Execute with %d reader(s) = (rows=%v, err=%v), want %d rows", readers, stats, err, table.RowCount)
		}
		return time.Since(start)
	}

	single := runStrict(1)
	parallel := runStrict(4)
	t.Logf("live PG composite tuple benchmark: one reader=%v, four readers=%v", single, parallel)
	if parallel >= single*80/100 {
		t.Fatalf("four tuple readers took %v vs one reader %v; want at least 20%% speedup", parallel, single)
	}
}

// compositeAnyTargetPool captures the two composite key components without
// assuming their Go scan types. It is shared by live PG/MySQL tuple tests.
type compositeAnyTargetPool struct {
	keysetRuntimeTargetPool
	mu         sync.Mutex
	keys       map[string]struct{}
	duplicates int
}

func newCompositeAnyTargetPool() *compositeAnyTargetPool {
	return &compositeAnyTargetPool{keys: make(map[string]struct{})}
}

func (p *compositeAnyTargetPool) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, row := range opts.Rows {
		if len(row) < 2 {
			return fmt.Errorf("composite row has %d values, want at least 2", len(row))
		}
		key := tupleTestComponent(row[0]) + "\x00" + tupleTestComponent(row[1])
		if _, exists := p.keys[key]; exists {
			p.duplicates++
			continue
		}
		p.keys[key] = struct{}{}
	}
	return nil
}

func (p *compositeAnyTargetPool) assertExact(t *testing.T, want int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) != want || p.duplicates != 0 {
		t.Fatalf("unique/duplicate keys = %d/%d, want %d/0", len(p.keys), p.duplicates, want)
	}
}

func tupleTestComponent(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}
