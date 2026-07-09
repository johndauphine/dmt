package orchestrator

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/source"
)

func TestRowNumberResumeCleanupClearsStalePartitionProgress(t *testing.T) {
	ctx := context.Background()
	runID := "run-388"

	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()

	table := rowNumberCleanupTable()
	if table.SupportsKeysetPagination() {
		t.Fatal("test table unexpectedly supports keyset pagination; want ROW_NUMBER path")
	}

	cfg := &config.Config{
		Migration: config.MigrationConfig{
			ChunkSize:           2,
			MaxPartitions:       3,
			LargeTableThreshold: 1,
		},
	}
	buildResult, err := NewJobBuilder(nil, state, cfg).Build(ctx, runID, []source.Table{table})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := len(buildResult.Jobs); got != 3 {
		t.Fatalf("len(jobs) = %d, want 3 ROW_NUMBER partitions", got)
	}

	tableIdentity := checkpoint.TransferTaskIdentity{Schema: table.Schema, Table: table.Name}
	tableTaskID, taskKey, err := checkpoint.CreateTransferTask(state, runID, tableIdentity)
	if err != nil {
		t.Fatalf("CreateTask table: %v", err)
	}

	for _, job := range buildResult.Jobs {
		if job.Partition == nil {
			t.Fatalf("job %d has nil partition", job.TaskID)
		}
		partitionID := job.Partition.PartitionID
		lastRowNum := job.Partition.StartRow + 1
		if err := state.SaveTransferProgress(job.TaskID, table.Name, &partitionID, lastRowNum, 1, job.Partition.RowCount, ""); err != nil {
			t.Fatalf("SaveTransferProgress partition %d: %v", partitionID, err)
		}
	}

	o := &Orchestrator{state: state}
	expectedRows, hasProgress, err := o.expectedResumeRows(runID, taskKey, true, nil, 0)
	if err != nil {
		t.Fatalf("expectedResumeRows: %v", err)
	}
	if !hasProgress || expectedRows != 3 {
		t.Fatalf("expectedResumeRows = (rows=%d hasProgress=%t), want (rows=3 hasProgress=true)",
			expectedRows, hasProgress)
	}

	targetCount := int64(2)
	if targetCount >= expectedRows {
		t.Fatalf("test setup invalid: targetCount=%d expectedRows=%d", targetCount, expectedRows)
	}
	if err := o.clearResumeProgress(runID, taskKey, tableTaskID, table.Name); err != nil {
		t.Fatalf("clearResumeProgress: %v", err)
	}

	summary, err := checkpoint.GetTransferPartitionProgressSummary(state, runID, table.Schema, table.Name)
	if err != nil {
		t.Fatalf("GetPartitionTransferProgressSummary after cleanup: %v", err)
	}
	if summary.HasProgress() || summary.RowsDone != 0 {
		t.Fatalf("partition progress after cleanup = (rows=%d partitions=%d), want none",
			summary.RowsDone, summary.PartitionsWithProgress)
	}
	for _, job := range buildResult.Jobs {
		progress, err := state.GetTransferProgress(job.TaskID)
		if err != nil {
			t.Fatalf("GetTransferProgress partition %d: %v", job.Partition.PartitionID, err)
		}
		if progress != nil {
			t.Fatalf("partition %d progress survived cleanup: %#v", job.Partition.PartitionID, progress)
		}
	}
}

func rowNumberCleanupTable() source.Table {
	t := source.Table{
		Schema: "main",
		Name:   "customer_orders",
		Columns: []source.Column{
			{Name: "tenant", DataType: "varchar", MaxLength: 32, IsNullable: false, OrdinalPos: 1},
			{Name: "order_code", DataType: "varchar", MaxLength: 32, IsNullable: false, OrdinalPos: 2},
			{Name: "amount", DataType: "integer", IsNullable: false, OrdinalPos: 3},
		},
		PrimaryKey: []string{"tenant", "order_code"},
		RowCount:   6,
	}
	t.PopulatePKColumns()
	return t
}
