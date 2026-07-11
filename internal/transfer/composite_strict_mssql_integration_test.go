package transfer

import (
	"context"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
)

func TestMSSQLTableSharedLockParallelCompositeIntegration(t *testing.T) {
	_, db, writer, _, _ := openMSSQLStrictIntegration(t)
	if _, err := db.Exec(`CREATE TABLE dbo.tuple_events (tenant_id BIGINT NOT NULL, seq BIGINT NOT NULL, val INT NOT NULL, CONSTRAINT PK_tuple_events PRIMARY KEY (tenant_id, seq))`); err != nil {
		t.Fatal(err)
	}
	for tenant := 1; tenant <= 24; tenant++ {
		for seq := 1; seq <= 4; seq++ {
			if _, err := db.Exec(`INSERT INTO dbo.tuple_events VALUES (@p1, @p2, @p3)`, tenant, seq, tenant*10+seq); err != nil {
				t.Fatal(err)
			}
		}
	}
	table := driver.Table{
		Schema: "dbo", Name: "tuple_events",
		Columns: []driver.Column{
			{Name: "tenant_id", DataType: "bigint"},
			{Name: "seq", DataType: "bigint"},
			{Name: "val", DataType: "int"},
		},
		PrimaryKey: []string{"tenant_id", "seq"}, RowCount: 96,
	}
	table.PopulatePKColumns()
	src := &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}
	strictCtx, release, err := beginStrictSourceSnapshotForJob(context.Background(), src, Job{Table: table}, 4)
	if err != nil {
		t.Fatal(err)
	}
	writerDone := make(chan error, 1)
	go func() {
		_, writeErr := writer.Exec(`INSERT INTO dbo.tuple_events VALUES (99, 1, 991)`)
		writerDone <- writeErr
	}()
	select {
	case err := <-writerDone:
		release()
		t.Fatalf("writer completed before table-scoped strict view released: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	cfg := &config.Config{Target: config.TargetConfig{Schema: ""}, Migration: config.MigrationConfig{
		StrictConsistency: true, StrictConsistencyScope: "table", ChunkSize: 3, ParallelReaders: 4,
		MaxSourceConnections: 5, WriteAheadWriters: 1, TargetMode: "drop_recreate",
	}}
	target := newCompositeAnyTargetPool()
	stats, used, err := executeParallelCompositeKeysetPagination(
		strictCtx, src, target, cfg, Job{Table: table},
		[]string{"tenant_id", "seq", "val"}, []string{"tenant_id", "seq", "val"}, []string{"bigint", "bigint", "int"}, []int{0, 0, 0},
		progress.New(), nil, 0, table.Name, nil, nil,
	)
	release()
	if err != nil || !used || stats.Rows != table.RowCount {
		t.Fatalf("strict parallel SQL Server tuple path = (used=%v, rows=%v, err=%v), want snapshot %d", used, stats, err, table.RowCount)
	}
	target.assertExact(t, int(table.RowCount))
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("writer after shared-lock release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer remained blocked after shared-lock release")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dbo.tuple_events`).Scan(&count); err != nil || count != int(table.RowCount)+1 {
		t.Fatalf("live tuple row count = %d, err=%v, want %d", count, err, table.RowCount+1)
	}
	t.Logf("SQL Server strict composite copied %d frozen rows with four readers; post-lock writer committed", stats.Rows)
}
