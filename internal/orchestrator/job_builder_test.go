package orchestrator

import (
	"context"
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/source"
)

func TestDateColumnCandidatesForTableUsesEffectiveColumns(t *testing.T) {
	table := source.Table{
		Name: "Users",
		Columns: []source.Column{
			{Name: "id"},
			{Name: "modified_at"},
		},
	}

	got := dateColumnCandidatesForTable(&table, []string{"updated_at", "Modified_At", "created_at"})
	want := []string{"Modified_At"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dateColumnCandidatesForTable() = %v, want %v", got, want)
	}
}

func TestDateColumnCandidatesForTableKeepsCandidatesWhenColumnsUnknown(t *testing.T) {
	table := source.Table{Name: "Users"}
	candidates := []string{"updated_at", "created_at"}

	got := dateColumnCandidatesForTable(&table, candidates)
	if !reflect.DeepEqual(got, candidates) {
		t.Fatalf("dateColumnCandidatesForTable() = %v, want %v", got, candidates)
	}
}

func TestCreateRowNumberPartitionJobsClearsProgressWhenBoundariesShift(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()

	const runID = "run-row-number-boundary"
	if err := state.CreateRun(runID, "public", "public", nil, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	table := source.Table{
		Schema:     "public",
		Name:       "votes",
		PrimaryKey: []string{"tenant", "vote_id"},
		RowCount:   120,
	}
	cfg := &config.Config{Migration: config.MigrationConfig{
		ChunkSize:     100,
		MaxPartitions: 2,
	}}
	builder := &JobBuilder{state: state, config: cfg}

	for partition := 1; partition <= 2; partition++ {
		pid := partition
		taskID, err := state.CreateTransferTask(runID, checkpoint.TransferTaskIdentity{
			Schema: table.Schema, Table: table.Name, PartitionID: &pid,
		})
		if err != nil {
			t.Fatalf("CreateTask p%d: %v", partition, err)
		}
		if err := state.SaveTransferProgress(taskID, table.Name, &pid, int64(partition*50), 50, 50, ""); err != nil {
			t.Fatalf("SaveTransferProgress p%d: %v", partition, err)
		}
	}

	result := &BuildResult{
		TableJobCounts: make(map[string]int),
		ProgressSaver:  checkpoint.NewProgressSaver(state),
	}
	if err := builder.createRowNumberPartitionJobs(runID, table, nil, result); err != nil {
		t.Fatalf("createRowNumberPartitionJobs() error: %v", err)
	}
	if len(result.Jobs) != 2 {
		t.Fatalf("jobs = %d, want 2", len(result.Jobs))
	}
	if result.TableTaskIDs[table.Name] <= 0 || result.TableTaskKeys[table.Name] == "" {
		t.Fatalf("aggregate table task was not durably planned before transfer: ids=%v keys=%v", result.TableTaskIDs, result.TableTaskKeys)
	}
	for _, job := range result.Jobs {
		progress, err := state.GetTransferProgress(job.TaskID)
		if err != nil {
			t.Fatalf("GetTransferProgress(%d): %v", job.TaskID, err)
		}
		if progress != nil {
			t.Fatalf("partition %d progress survived boundary shift: %+v", job.Partition.PartitionID, progress)
		}
	}
}

func TestStrictConsistencyBuildsOneUnpartitionedJobPerTable(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	const runID = "strict-single-job"
	if err := state.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	table := source.Table{
		Schema:     "dbo",
		Name:       "orders",
		PrimaryKey: []string{"id"},
		RowCount:   1_000_000,
	}
	builder := &JobBuilder{
		state: state,
		config: &config.Config{Migration: config.MigrationConfig{
			StrictConsistency:   true,
			ChunkSize:           1_000,
			MaxPartitions:       8,
			LargeTableThreshold: 100,
		}},
	}
	result := &BuildResult{
		TableJobCounts: make(map[string]int),
		ProgressSaver:  checkpoint.NewProgressSaver(state),
	}
	if err := builder.createJobsForTable(context.Background(), runID, table, nil, result); err != nil {
		t.Fatal(err)
	}
	if len(result.Jobs) != 1 || result.Jobs[0].Partition != nil {
		t.Fatalf("strict_consistency jobs = %+v, want exactly one unpartitioned job", result.Jobs)
	}
	if result.TableJobCounts[table.Name] != 1 {
		t.Fatalf("table job count = %d, want 1", result.TableJobCounts[table.Name])
	}
}
