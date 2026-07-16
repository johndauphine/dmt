package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
)

func TestProjectionExecutionContextUsesTransferWidthsAndPinPolicy(t *testing.T) {
	tables := []driver.Table{{
		Schema: "dbo", Name: "orders", RowCount: 1_000, EstimatedRowSize: 4_096,
	}}
	cfg := &config.Config{}
	cfg.Migration.Workers = 8
	cfg.Migration.WriteAheadWriters = 3
	cfg.Migration.ParallelReaders = 4
	cfg.Migration.ReadAheadBuffers = 8
	cfg.Migration.MaxSourceConnections = 19
	cfg.Migration.MaxTargetConnections = 17

	ctx := projectionExecutionContext(tables, cfg, map[string]bool{
		config.TunableWorkers:              true,
		config.TunableMaxSourceConnections: true,
	})
	if len(ctx.Tables) != 1 || ctx.Tables[0].EstimatedRowSize != 4_096 {
		t.Fatalf("projection context did not use extracted transfer inventory: %+v", ctx.Tables)
	}
	if !ctx.Workers.Pinned || ctx.Workers.Value != 8 {
		t.Fatalf("worker pin policy = %+v, want fixed 8", ctx.Workers)
	}
	if !ctx.MaxSourceConnections.Pinned || ctx.MaxSourceConnections.Value != 19 {
		t.Fatalf("source pool policy = %+v, want fixed 19", ctx.MaxSourceConnections)
	}
	if ctx.MaxTargetConnections.Pinned || ctx.MaxTargetConnections.Value != 0 {
		t.Fatalf("generated target pool leaked current numeric value into context: %+v", ctx.MaxTargetConnections)
	}
	if ctx.WriteAheadWriters.Pinned || ctx.WriteAheadWriters.Value != 0 ||
		ctx.ParallelReaders.Pinned || ctx.ParallelReaders.Value != 0 ||
		ctx.ReadAheadBuffers.Pinned || ctx.ReadAheadBuffers.Value != 0 {
		t.Fatalf("generated action knobs leaked numeric defaults: WAW=%+v PR=%+v RAB=%+v",
			ctx.WriteAheadWriters, ctx.ParallelReaders, ctx.ReadAheadBuffers)
	}
}
