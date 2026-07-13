package generic

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
)

// Live-server integration for the ClickHouse catalog (#507): a sqlite
// fixture (no extra infrastructure on the source side) migrates
// through the real writer/reader paths against a ClickHouse server.
//
// Requires a server on localhost:9000 (see Makefile clickhouse-test-up;
// CLICKHOUSE_PASSWORD=TestPass2024). Skipped under -short and when the
// server isn't reachable unless CLICKHOUSE_REQUIRED=1 (CI sets it so a
// dead service fails instead of silently skipping).
func clickhouseTarget(t *testing.T) *dbconfig.TargetConfig {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	return &dbconfig.TargetConfig{
		Type: "clickhouse", Host: "localhost", Port: 9000,
		Database: "dmt_it", User: "default", Password: "TestPass2024",
		ChunkSize: 1000,
	}
}

// bootstrapClickhouseDB creates the test database from the default
// database — the writer's pool pings its target database at open, so
// it must exist first (the same job the pair scripts' prepare-target
// step does for the other engines).
func bootstrapClickhouseDB(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	db, err := sql.Open("clickhouse", "clickhouse://localhost:9000/default?password=TestPass2024&username=default")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		if os.Getenv("CLICKHOUSE_REQUIRED") == "1" {
			t.Fatalf("clickhouse required but unreachable: %v", err)
		}
		t.Skipf("clickhouse not reachable: %v", err)
	}
	// Drop + recreate for isolation: a previous run (or the bench
	// test) may have left tables behind, and the end-to-end test
	// asserts exact schema contents (codex).
	if _, err := db.Exec("DROP DATABASE IF EXISTS dmt_it"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE DATABASE dmt_it"); err != nil {
		t.Fatal(err)
	}
}

func openClickhouseWriter(t *testing.T) driver.Writer {
	t.Helper()
	bootstrapClickhouseDB(t)
	cfg := clickhouseTarget(t)
	tm, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatal(err)
	}
	cat, err := LoadCatalog("clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(cat, cfg, 4, driver.WriterOptions{
		BatchSize: 1000, SourceType: "sqlite", TypeMapper: tm,
	})
	if err != nil {
		if os.Getenv("CLICKHOUSE_REQUIRED") == "1" {
			t.Fatalf("clickhouse required but unreachable: %v", err)
		}
		t.Skipf("clickhouse not reachable: %v", err)
	}
	t.Cleanup(w.Close)
	if err := w.CreateSchema(context.Background(), "dmt_it"); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	return w
}

func TestClickHouseEndToEnd(t *testing.T) {
	ctx := context.Background()
	w := openClickhouseWriter(t)

	// Source fixture: sqlite with the type variety that exercises the
	// mapper (identity PK, bounded varchar, decimal, datetime, NULLs).
	srcPath := filepath.Join(t.TempDir(), "src.db")
	srcDB, err := sql.Open("sqlite", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			region VARCHAR(10) NOT NULL,
			amount NUMERIC(10,2),
			note TEXT,
			created_at DATETIME NOT NULL
		);
		INSERT INTO orders (region, amount, note, created_at) VALUES
			('us', 12.50, 'first', '2024-06-15 10:30:00'),
			('eu', NULL, NULL, '2024-06-16 11:00:00'),
			('ap', 7.25, 'third', '2024-06-17 12:00:00');
	`); err != nil {
		t.Fatal(err)
	}
	srcDB.Close()

	srcCat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(srcCat, &dbconfig.SourceConfig{Type: "sqlite", Database: srcPath}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	tables, err := r.ExtractSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %d", len(tables))
	}
	tbl := &tables[0]

	// Fresh target table through the deterministic DDL path
	// (MergeTree ORDER BY from the PK, Nullable wrapping).
	if err := w.DropTable(ctx, "dmt_it", "orders"); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateTable(ctx, tbl, "dmt_it"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if exists, err := w.TableExists(ctx, "dmt_it", "orders"); err != nil || !exists {
		t.Fatalf("TableExists: %v %v", exists, err)
	}
	if hasPK, err := w.HasPrimaryKey(ctx, "dmt_it", "orders"); err != nil || !hasPK {
		t.Fatalf("HasPrimaryKey (ORDER BY key): %v %v", hasPK, err)
	}

	// Bulk write through the batched_insert strategy.
	rows := [][]any{
		{int64(1), "us", 12.50, "first", "2024-06-15 10:30:00"},
		{int64(2), "eu", nil, nil, "2024-06-16 11:00:00"},
		{int64(3), "ap", 7.25, "third", "2024-06-17 12:00:00"},
	}
	if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "dmt_it", Table: "orders",
		Columns: []string{"id", "region", "amount", "note", "created_at"},
		Rows:    rows,
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if n, err := w.GetRowCount(ctx, "dmt_it", "orders"); err != nil || n != 3 {
		t.Fatalf("GetRowCount = %d, %v; want 3", n, err)
	}

	// Read back through the catalog reader: schema introspection
	// against system.columns and a keyset-shaped scan.
	chCat, err := LoadCatalog("clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	chr, err := NewReader(chCat, &dbconfig.SourceConfig{
		Type: "clickhouse", Host: "localhost", Port: 9000,
		Database: "dmt_it", User: "default", Password: "TestPass2024",
		Schema: "dmt_it",
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer chr.Close()

	chTables, err := chr.ExtractSchema(ctx, "dmt_it")
	if err != nil {
		t.Fatalf("clickhouse ExtractSchema: %v", err)
	}
	if len(chTables) != 1 || chTables[0].Name != "orders" {
		t.Fatalf("clickhouse tables = %+v", chTables)
	}
	got := chTables[0]
	if len(got.PrimaryKey) != 1 || got.PrimaryKey[0] != "id" {
		t.Errorf("ORDER BY key read back = %v, want [id]", got.PrimaryKey)
	}
	byName := map[string]driver.Column{}
	for _, c := range got.Columns {
		byName[c.Name] = c
	}
	if c := byName["note"]; !c.IsNullable {
		t.Errorf("note must read back nullable, got %+v", c)
	}
	if c := byName["region"]; c.IsNullable {
		t.Errorf("region must read back non-nullable, got %+v", c)
	}

	parts, err := chr.GetPartitionBoundaries(ctx, &got, 4)
	if err != nil || len(parts) != 1 || parts[0].RowCount != 3 {
		t.Fatalf("partitions = %+v, %v", parts, err)
	}

	// Idempotent replay must refuse loudly, not emit sqlite syntax.
	err = w.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema: "dmt_it", Table: "orders",
		Columns:         []string{"id", "region", "amount", "note", "created_at"},
		Rows:            rows,
		IdempotentOnDup: true, PKColumns: []string{"id"},
	})
	if err == nil {
		t.Fatal("IdempotentOnDup must be refused on clickhouse")
	}

	// Date watermark through the catalog's normalized date_column query.
	dates := chr.(driver.IncrementalDateReader)
	col, typ, found := dates.GetDateColumnInfo(ctx, "dmt_it", "orders", []string{"missing", "created_at"})
	if !found || col != "created_at" || typ != "datetime64" {
		t.Errorf("GetDateColumnInfo = (%q,%q,%v)", col, typ, found)
	}
}

func TestClickHouseSchemaStats(t *testing.T) {
	bootstrapClickhouseDB(t)
	ctx := context.Background()
	db, err := sql.Open("clickhouse", "clickhouse://localhost:9000/dmt_it?password=TestPass2024&username=default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		if os.Getenv("CLICKHOUSE_REQUIRED") == "1" {
			t.Fatalf("clickhouse required but schema-stats connection failed: %v", err)
		}
		t.Skipf("clickhouse not reachable: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE schema_stats_events (
			id UInt64,
			on_date Date,
			on_date32 Date32,
			at_time DateTime('UTC'),
			at_precise DateTime64(3, 'UTC'),
			at_nullable Nullable(DateTime64(6, 'UTC')),
			note String
		) ENGINE = MergeTree ORDER BY id
	`); err != nil {
		t.Fatalf("create schema-stats table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schema_stats_events VALUES
			(1, '2025-01-01', '2025-01-01', '2025-01-01 10:30:00', '2025-01-01 10:30:00.123', NULL, 'first'),
			(2, '2025-01-02', '2025-01-02', '2025-01-02 11:30:00', '2025-01-02 11:30:00.456', '2025-01-02 11:30:00.456789', 'second')
	`); err != nil {
		t.Fatalf("insert schema-stats rows: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE VIEW schema_stats_view AS SELECT * FROM schema_stats_events;
	`); err != nil {
		t.Fatalf("create schema-stats view: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE MATERIALIZED VIEW schema_stats_materialized
		ENGINE = MergeTree ORDER BY id
		AS SELECT * FROM schema_stats_events
	`); err != nil {
		t.Fatalf("create schema-stats materialized view: %v", err)
	}

	cat, err := LoadCatalog("clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	statsReader, supported := NewDriver(cat).SchemaStatsReader()
	if !supported || statsReader == nil {
		t.Fatal("clickhouse catalog did not expose schema-statistics support")
	}
	stats, err := statsReader.TableStats(ctx, db, "dmt_it", nil)
	if err != nil {
		t.Fatalf("TableStats: %v", err)
	}
	if len(stats) != 1 || stats[0].Name != "schema_stats_events" {
		t.Fatalf("table stats = %+v, want base table only (views excluded)", stats)
	}
	if stats[0].RowCount != 2 || stats[0].AvgRowSizeBytes <= 0 {
		t.Fatalf("table stats = %+v, want two rows and positive average width", stats[0])
	}

	dates, err := statsReader.DateColumns(ctx, db, "dmt_it", []string{"schema_stats_events"})
	if err != nil {
		t.Fatalf("DateColumns: %v", err)
	}
	wantDates := []string{"on_date", "on_date32", "at_time", "at_precise", "at_nullable"}
	if got := dates["schema_stats_events"]; !reflect.DeepEqual(got, wantDates) {
		t.Fatalf("date columns = %v, want Date/Date32/parameterized/nullable families %v", got, wantDates)
	}

	// Exercise the public analyzer and persistence boundary too: the catalog
	// reader must feed deterministic tuning and arm a history record using the
	// real ClickHouse endpoint identity.
	history := &sqliteSchemaStatsHistory{}
	analyzer := driver.NewSmartConfigAnalyzer(db, "clickhouse")
	analyzer.SetTargetDBType("clickhouse")
	analyzer.SetMemoryEnvelope(8*1024, 4*1024, 2*1024)
	analyzer.SetHistoryProvider(history)
	analyzer.SetWorkloadIdentity(
		"localhost", 9000, "dmt_it", "dmt_it",
		"localhost", 9000, "dmt_it", "dmt_it",
	)
	suggestions, err := analyzer.Analyze(ctx, "dmt_it")
	if err != nil {
		t.Fatalf("ClickHouse Analyze: %v", err)
	}
	if suggestions.TotalTables != 1 || suggestions.TotalRows != 2 ||
		!reflect.DeepEqual(suggestions.DateColumns["schema_stats_events"], wantDates) ||
		suggestions.Workers <= 0 || suggestions.ChunkSizeRecommendation <= 0 {
		t.Fatalf("ClickHouse analyzer suggestions = %+v", suggestions)
	}
	rowID := analyzer.SaveTuningWithActualParams(driver.ActualParams{
		Workers:              suggestions.Workers,
		ChunkSize:            suggestions.ChunkSizeRecommendation,
		ReadAheadBuffers:     suggestions.ReadAheadBuffers,
		WriteAheadWriters:    suggestions.WriteAheadWriters,
		ParallelReaders:      suggestions.ParallelReaders,
		MaxPartitions:        suggestions.MaxPartitions,
		MaxSourceConnections: suggestions.MaxSourceConnections,
		MaxTargetConnections: suggestions.MaxTargetConnections,
	})
	if rowID != 77 || history.saved == nil {
		t.Fatalf("ClickHouse pending history save = row %d / %#v, want row 77", rowID, history.saved)
	}
	if history.saved.SourceDBType != "clickhouse" || history.saved.TargetDBType != "clickhouse" ||
		history.saved.SourceHost != "localhost" || history.saved.TargetHost != "localhost" ||
		history.saved.SourcePort != 9000 || history.saved.TargetPort != 9000 ||
		history.saved.SourceDatabase != "dmt_it" || history.saved.TargetDatabase != "dmt_it" {
		t.Fatalf("ClickHouse history identity = %+v", history.saved)
	}
}

// Throughput measurement for the bulk.strategy decision (#507 PR 3):
// batched_insert (multi-row INSERT) vs a future native columnar batch.
// Run explicitly: CLICKHOUSE_BENCH=1 go test -run TestClickHouseBulkThroughput -v
func TestClickHouseBulkThroughput(t *testing.T) {
	if os.Getenv("CLICKHOUSE_BENCH") != "1" {
		t.Skip("set CLICKHOUSE_BENCH=1 to run")
	}
	ctx := context.Background()
	w := openClickhouseWriter(t)

	_, _ = w.ExecRaw(ctx, "DROP TABLE IF EXISTS `dmt_it`.`bench`")
	if _, err := w.ExecRaw(ctx, "CREATE TABLE `dmt_it`.`bench` (id Int64, name String, val Float64, at DateTime64(3)) ENGINE = MergeTree ORDER BY (`id`)"); err != nil {
		t.Fatal(err)
	}

	const total, batch = 1_000_000, 50_000
	cols := []string{"id", "name", "val", "at"}
	start := time.Now()
	for off := 0; off < total; off += batch {
		rows := make([][]any, batch)
		for i := range rows {
			id := int64(off + i)
			rows[i] = []any{id, fmt.Sprintf("name-%d", id), float64(id) * 1.5, "2024-06-15 10:30:00"}
		}
		if err := w.WriteBatch(ctx, driver.WriteBatchOptions{
			Schema: "dmt_it", Table: "bench", Columns: cols, Rows: rows, BatchSize: batch,
		}); err != nil {
			t.Fatalf("WriteBatch at %d: %v", off, err)
		}
	}
	secs := time.Since(start).Seconds()
	n, err := w.GetRowCount(ctx, "dmt_it", "bench")
	if err != nil || n != total {
		t.Fatalf("count = %d, %v", n, err)
	}
	t.Logf("batched_insert: %d rows in %.1fs = %.0f rows/s", total, secs, float64(total)/secs)
}
