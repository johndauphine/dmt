package transfer

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/dbconfig"
	"github.com/johndauphine/dmt/v5/internal/driver"
	generic "github.com/johndauphine/dmt/v5/internal/driver/generic"
	"github.com/johndauphine/dmt/v5/internal/progress"
	"github.com/johndauphine/dmt/v5/internal/source"
)

type rowNumberResumeOrder struct {
	Tenant string
	Code   string
	Amount int64
	Note   string
}

func TestRowNumberResumeReplaysPartialTargetWithoutDuplicatesOrStaleProgress(t *testing.T) {
	ctx := context.Background()
	runID := "run-388"

	src := newSQLiteTestReader(t, "source")
	defer src.Close()
	tgt := newSQLiteTestWriter(t, "target")
	defer tgt.Close()

	table := rowNumberResumeTable()
	if table.SupportsKeysetPagination() {
		t.Fatal("test table unexpectedly supports keyset pagination; want ROW_NUMBER path")
	}

	orders := []rowNumberResumeOrder{
		{Tenant: "acct-a", Code: "ord-001", Amount: 10, Note: "one"},
		{Tenant: "acct-a", Code: "ord-002", Amount: 20, Note: "two"},
		{Tenant: "acct-b", Code: "ord-001", Amount: 30, Note: "three"},
		{Tenant: "acct-b", Code: "ord-002", Amount: 40, Note: "four"},
		{Tenant: "acct-c", Code: "ord-001", Amount: 50, Note: "five"},
		{Tenant: "acct-c", Code: "ord-002", Amount: 60, Note: "six"},
	}
	createRowNumberResumeTable(t, ctx, src.DB())
	createRowNumberResumeTable(t, ctx, tgt.DB())
	insertRowNumberResumeOrders(t, ctx, src.DB(), orders)

	// Simulate an interrupted ROW_NUMBER resume:
	//   - partition 1 checkpointed through row 2, but row 3 committed after it;
	//   - partition 2 committed row 4 before its first checkpoint flush.
	// A resume must replay in place idempotently, not duplicate row 3/4 and not
	// leave partition 1 stuck at its stale row_number checkpoint.
	insertRowNumberResumeOrders(t, ctx, tgt.DB(), orders[:4])

	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()
	saver := checkpoint.NewProgressSaver(state)
	if err := state.CreateRun(runID, "main", "main", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	taskPrefix := "transfer:main.customer_orders"
	p1TaskID, err := state.CreateTask(runID, "transfer", taskPrefix+":p1")
	if err != nil {
		t.Fatalf("CreateTask p1: %v", err)
	}
	p2TaskID, err := state.CreateTask(runID, "transfer", taskPrefix+":p2")
	if err != nil {
		t.Fatalf("CreateTask p2: %v", err)
	}
	p1ID := 1
	if err := state.SaveTransferProgress(p1TaskID, table.Name, &p1ID, int64(2), 2, 3, ""); err != nil {
		t.Fatalf("SaveTransferProgress p1: %v", err)
	}

	cfg := &config.Config{
		Source: config.SourceConfig{Type: "sqlite"},
		Target: config.TargetConfig{Type: "sqlite", ChunkSize: 2},
		Migration: config.MigrationConfig{
			ChunkSize:            2,
			TargetMode:           "drop_recreate",
			WriteAheadWriters:    1,
			ReadAheadBuffers:     1,
			CheckpointFrequency:  1,
			UpsertMergeChunkSize: 2,
			MaxSourceConnections: 1,
			MaxTargetConnections: 1,
			Workers:              1,
			LargeTableThreshold:  1,
			MaxPartitions:        2,
		},
	}

	jobs := []Job{
		rowNumberResumeJob(table, p1TaskID, saver, source.Partition{
			TableName:        table.Name,
			PartitionID:      1,
			StartRow:         0,
			EndRow:           3,
			RowCount:         3,
			IsFirstPartition: true,
		}),
		rowNumberResumeJob(table, p2TaskID, saver, source.Partition{
			TableName:   table.Name,
			PartitionID: 2,
			StartRow:    3,
			EndRow:      6,
			RowCount:    3,
		}),
	}

	for _, job := range jobs {
		if _, err := Execute(ctx, src, tgt, cfg, job, progress.New(), nil); err != nil {
			t.Fatalf("first resume partition %d: %v", job.Partition.PartitionID, err)
		}
	}
	assertRowNumberResumeTarget(t, ctx, tgt.DB(), orders)
	assertRowNumberResumeProgress(t, state, p1TaskID, "3", 3)
	assertRowNumberResumeProgress(t, state, p2TaskID, "6", 3)
	assertRowNumberResumeSummary(t, state, runID, taskPrefix, 6, 2)

	for _, job := range jobs {
		if _, err := Execute(ctx, src, tgt, cfg, job, progress.New(), nil); err != nil {
			t.Fatalf("second idempotent resume partition %d: %v", job.Partition.PartitionID, err)
		}
	}
	assertRowNumberResumeTarget(t, ctx, tgt.DB(), orders)
	assertRowNumberResumeProgress(t, state, p1TaskID, "3", 3)
	assertRowNumberResumeProgress(t, state, p2TaskID, "6", 3)
	assertRowNumberResumeSummary(t, state, runID, taskPrefix, 6, 2)
}

func TestRowNumberReadsPastEstimatedRowCount(t *testing.T) {
	ctx := context.Background()

	src := newSQLiteTestReader(t, "source")
	defer src.Close()
	tgt := newSQLiteTestWriter(t, "target")
	defer tgt.Close()

	table := rowNumberResumeTable()
	table.RowCount = 4 // Deliberately under the six real source rows.
	orders := []rowNumberResumeOrder{
		{Tenant: "acct-a", Code: "ord-001", Amount: 10, Note: "one"},
		{Tenant: "acct-a", Code: "ord-002", Amount: 20, Note: "two"},
		{Tenant: "acct-b", Code: "ord-001", Amount: 30, Note: "three"},
		{Tenant: "acct-b", Code: "ord-002", Amount: 40, Note: "four"},
		{Tenant: "acct-c", Code: "ord-001", Amount: 50, Note: "five"},
		{Tenant: "acct-c", Code: "ord-002", Amount: 60, Note: "six"},
	}
	createRowNumberResumeTable(t, ctx, src.DB())
	createRowNumberResumeTable(t, ctx, tgt.DB())
	insertRowNumberResumeOrders(t, ctx, src.DB(), orders)

	cfg := rowNumberResumeConfig()
	cfg.Migration.LargeTableThreshold = 1_000_000 // single ROW_NUMBER job

	stats, err := Execute(ctx, src, tgt, cfg, Job{Table: table}, progress.New(), nil)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if stats.Rows != int64(len(orders)) {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, len(orders))
	}
	assertRowNumberResumeTarget(t, ctx, tgt.DB(), orders)
}

func TestRowNumberReplayPossibleEnablesIdempotentWritesWithoutResume(t *testing.T) {
	ctx := context.Background()

	src := newSQLiteTestReader(t, "source")
	defer src.Close()
	tgt := newSQLiteTestWriter(t, "target")
	defer tgt.Close()

	table := rowNumberResumeTable()
	orders := []rowNumberResumeOrder{
		{Tenant: "acct-a", Code: "ord-001", Amount: 10, Note: "one"},
		{Tenant: "acct-a", Code: "ord-002", Amount: 20, Note: "two"},
		{Tenant: "acct-b", Code: "ord-001", Amount: 30, Note: "three"},
		{Tenant: "acct-b", Code: "ord-002", Amount: 40, Note: "four"},
		{Tenant: "acct-c", Code: "ord-001", Amount: 50, Note: "five"},
		{Tenant: "acct-c", Code: "ord-002", Amount: 60, Note: "six"},
	}
	createRowNumberResumeTable(t, ctx, src.DB())
	createRowNumberResumeTable(t, ctx, tgt.DB())
	insertRowNumberResumeOrders(t, ctx, src.DB(), orders)
	insertRowNumberResumeOrders(t, ctx, tgt.DB(), orders[:2])

	cfg := rowNumberResumeConfig()
	job := Job{
		Table: table,
		Partition: &source.Partition{
			TableName:        table.Name,
			PartitionID:      1,
			StartRow:         0,
			EndRow:           table.RowCount,
			RowCount:         table.RowCount,
			IsFirstPartition: true,
		},
		ReplayPossible: true,
	}

	if _, err := Execute(ctx, src, tgt, cfg, job, progress.New(), nil); err != nil {
		t.Fatalf("Execute() with replay possible error: %v", err)
	}
	assertRowNumberResumeTarget(t, ctx, tgt.DB(), orders)
}

func rowNumberResumeTable() source.Table {
	t := source.Table{
		Schema: "main",
		Name:   "customer_orders",
		Columns: []source.Column{
			// IsNullable true matches real sqlite introspection (pragma_table_info
			// reports notnull=0 for PK columns not declared NOT NULL) and keeps
			// this table on the ROW_NUMBER path this test pins — a NOT NULL
			// text composite would route to tuple keyset instead (#629).
			{Name: "tenant", DataType: "varchar", MaxLength: 32, IsNullable: true, OrdinalPos: 1},
			{Name: "order_code", DataType: "varchar", MaxLength: 32, IsNullable: true, OrdinalPos: 2},
			{Name: "amount", DataType: "integer", IsNullable: false, OrdinalPos: 3},
			{Name: "note", DataType: "varchar", MaxLength: 64, IsNullable: false, OrdinalPos: 4},
		},
		PrimaryKey:       []string{"tenant", "order_code"},
		RowCount:         6,
		EstimatedRowSize: 128,
	}
	t.PopulatePKColumns()
	return t
}

func rowNumberResumeConfig() *config.Config {
	return &config.Config{
		Source: config.SourceConfig{Type: "sqlite"},
		Target: config.TargetConfig{Type: "sqlite", ChunkSize: 2},
		Migration: config.MigrationConfig{
			ChunkSize:            2,
			TargetMode:           "drop_recreate",
			WriteAheadWriters:    1,
			ReadAheadBuffers:     1,
			CheckpointFrequency:  1,
			UpsertMergeChunkSize: 2,
			MaxSourceConnections: 1,
			MaxTargetConnections: 1,
			Workers:              1,
			LargeTableThreshold:  1,
			MaxPartitions:        2,
		},
	}
}

func rowNumberResumeJob(table source.Table, taskID int64, saver ProgressSaver, partition source.Partition) Job {
	return Job{
		Table:     table,
		Partition: &partition,
		TaskID:    taskID,
		Saver:     saver,
		IsResume:  true,
	}
}

func newSQLiteTestReader(t *testing.T, name string) driver.Reader {
	t.Helper()
	reader, err := generic.NewReader(mustCatalog(t),
		&dbconfig.SourceConfig{
			Type:      "sqlite",
			Database:  filepath.Join(t.TempDir(), name+".db"),
			ChunkSize: 2,
		},
		1,
	)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader
}

func newSQLiteTestWriter(t *testing.T, name string) driver.Writer {
	t.Helper()
	typeMapper, err := driver.GetTypeMapper(driver.UnmappedActionFail, "")
	if err != nil {
		t.Fatalf("GetTypeMapper: %v", err)
	}
	writer, err := generic.NewWriter(mustCatalog(t),
		&dbconfig.TargetConfig{
			Type:      "sqlite",
			Database:  filepath.Join(t.TempDir(), name+".db"),
			ChunkSize: 2,
		},
		1,
		driver.WriterOptions{
			BatchSize:  2,
			SourceType: "sqlite",
			TypeMapper: typeMapper,
		},
	)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return writer
}

type sqliteTestDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func createRowNumberResumeTable(t *testing.T, ctx context.Context, db sqliteTestDB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE customer_orders (
			tenant TEXT NOT NULL,
			order_code TEXT NOT NULL,
			amount INTEGER NOT NULL,
			note TEXT NOT NULL,
			PRIMARY KEY (tenant, order_code)
		)
	`)
	if err != nil {
		t.Fatalf("creating customer_orders: %v", err)
	}
}

func insertRowNumberResumeOrders(t *testing.T, ctx context.Context, db sqliteTestDB, orders []rowNumberResumeOrder) {
	t.Helper()
	for _, order := range orders {
		_, err := db.ExecContext(ctx,
			`INSERT INTO customer_orders (tenant, order_code, amount, note) VALUES (?, ?, ?, ?)`,
			order.Tenant, order.Code, order.Amount, order.Note,
		)
		if err != nil {
			t.Fatalf("inserting %#v: %v", order, err)
		}
	}
}

func assertRowNumberResumeTarget(t *testing.T, ctx context.Context, db sqliteTestDB, want []rowNumberResumeOrder) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT tenant, order_code, amount, note
		FROM customer_orders
		ORDER BY tenant, order_code
	`)
	if err != nil {
		t.Fatalf("querying target rows: %v", err)
	}
	defer rows.Close()

	var got []rowNumberResumeOrder
	for rows.Next() {
		var row rowNumberResumeOrder
		if err := rows.Scan(&row.Tenant, &row.Code, &row.Amount, &row.Note); err != nil {
			t.Fatalf("scanning target row: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating target rows: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("target rows = %#v, want %#v", got, want)
	}
}

func assertRowNumberResumeProgress(t *testing.T, state *checkpoint.State, taskID int64, wantLastPK string, wantRowsDone int64) {
	t.Helper()
	got, err := state.GetTransferProgress(taskID)
	if err != nil {
		t.Fatalf("GetTransferProgress(%d): %v", taskID, err)
	}
	if got == nil {
		t.Fatalf("GetTransferProgress(%d) = nil, want progress", taskID)
	}
	if got.LastPK != wantLastPK || got.RowsDone != wantRowsDone {
		t.Fatalf("progress task %d = (lastPK=%s rowsDone=%d), want (lastPK=%s rowsDone=%d)",
			taskID, got.LastPK, got.RowsDone, wantLastPK, wantRowsDone)
	}
}

func assertRowNumberResumeSummary(t *testing.T, state *checkpoint.State, runID, taskPrefix string, wantRowsDone int64, wantPartitions int) {
	t.Helper()
	summary, err := state.GetPartitionTransferProgressSummary(runID, taskPrefix)
	if err != nil {
		t.Fatalf("GetPartitionTransferProgressSummary: %v", err)
	}
	if summary.RowsDone != wantRowsDone || summary.PartitionsWithProgress != wantPartitions {
		t.Fatalf("partition summary = (rowsDone=%d partitions=%d), want (rowsDone=%d partitions=%d)",
			summary.RowsDone, summary.PartitionsWithProgress, wantRowsDone, wantPartitions)
	}
}

func mustCatalog(t *testing.T) *generic.Catalog {
	t.Helper()
	cat, err := generic.LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	return cat
}
