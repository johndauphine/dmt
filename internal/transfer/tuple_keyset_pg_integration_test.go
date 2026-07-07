package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"

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
