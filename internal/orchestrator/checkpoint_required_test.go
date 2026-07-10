package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

type requiredWriteFaultState struct {
	checkpoint.StateBackend
	createErr   error
	createID    int64
	statusErr   error
	completeErr error
	markErr     error
	resumeErr   error
}

func (s *requiredWriteFaultState) CreateTransferTask(string, checkpoint.TransferTaskIdentity) (int64, error) {
	return s.createID, s.createErr
}

func (s *requiredWriteFaultState) CountTransferPartitionTasks(string, string, string) (int, error) {
	return 0, nil
}

func (s *requiredWriteFaultState) ClearTransferPartitionProgress(string, string, string) error {
	return nil
}

func (s *requiredWriteFaultState) GetTransferPartitionProgressSummary(string, string, string) (checkpoint.PartitionProgressSummary, error) {
	return checkpoint.PartitionProgressSummary{}, nil
}

func (s *requiredWriteFaultState) MarkTransferTaskComplete(string, checkpoint.TransferTaskIdentity) error {
	return s.markErr
}

func (s *requiredWriteFaultState) UpdateTaskStatus(int64, string, string) error {
	return s.statusErr
}

func (s *requiredWriteFaultState) CompleteRun(string, string, string) error {
	return s.completeErr
}

func (s *requiredWriteFaultState) MarkRunAsResumed(string) error {
	return s.resumeErr
}

func TestJobBuilderFailsClosedWhenTaskCreationFails(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state *requiredWriteFaultState
	}{
		{name: "backend error", state: &requiredWriteFaultState{createErr: errors.New("checkpoint disk full")}},
		{name: "zero task ID", state: &requiredWriteFaultState{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewJobBuilder(nil, tt.state, &config.Config{})
			result, err := builder.Build(context.Background(), "run-645", []source.Table{{Schema: "dbo", Name: "orders", RowCount: 10}})
			if result != nil || !checkpoint.IsRequiredWriteError(err) {
				t.Fatalf("Build = (%+v, %v), want required-write error and no job plan", result, err)
			}
			if code := exitcodes.FromError(err); code != exitcodes.StateError {
				t.Fatalf("exit code = %d, want StateError", code)
			}
		})
	}
}

func TestExecuteJobStopsBeforeTransferWhenRunningStatusFails(t *testing.T) {
	state := &requiredWriteFaultState{statusErr: errors.New("state file is read-only")}
	runner := &TransferRunner{state: state, progress: progress.New()}
	errCh := make(chan tableError, 1)
	job := transfer.Job{TaskID: 42, Table: source.Table{Schema: "dbo", Name: "orders"}}

	// sourcePool, targetPool, config, and buildResult are deliberately nil:
	// reaching transfer.Execute after the required status failure would panic.
	runner.executeJob(context.Background(), "run-645", job, nil, nil, errCh, nil, nil, nil)
	close(errCh)
	te := <-errCh
	if !checkpoint.IsRequiredWriteError(te.err) {
		t.Fatalf("executeJob error = %v, want required-write error", te.err)
	}
}

func TestTaskAndRunCompletionFailuresAreStateErrors(t *testing.T) {
	state := &requiredWriteFaultState{
		markErr:     errors.New("cannot persist table completion"),
		completeErr: errors.New("cannot persist run completion"),
	}
	runner := &TransferRunner{state: state}
	job := transfer.Job{Table: source.Table{Schema: "dbo", Name: "orders"}}
	if err := runner.markTransferTaskComplete("run-645", job, nil); !checkpoint.IsRequiredWriteError(err) {
		t.Fatalf("markTransferTaskComplete error = %v, want required-write error", err)
	}
	orchestrator := &Orchestrator{state: state}
	if err := orchestrator.completeRunRequired("run-645", "success", ""); !checkpoint.IsRequiredWriteError(err) {
		t.Fatalf("completeRunRequired error = %v, want required-write error", err)
	}
}

func TestResumeResetFailureIsStateError(t *testing.T) {
	state := &requiredWriteFaultState{resumeErr: errors.New("cannot reset running tasks")}
	orchestrator := &Orchestrator{state: state}
	if err := orchestrator.markRunAsResumedRequired("run-645"); !checkpoint.IsRequiredWriteError(err) {
		t.Fatalf("markRunAsResumedRequired error = %v, want required-write error", err)
	}
}

func TestResetResumeTableTasksClearsStaleAggregateSuccess(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.CreateRun("run-645", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	identity := checkpoint.TransferTaskIdentity{Schema: "dbo", Table: "orders"}
	taskID, err := state.CreateTransferTask("run-645", identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkTransferTaskComplete("run-645", identity); err != nil {
		t.Fatal(err)
	}
	completed, err := state.GetCompletedTables("run-645")
	if err != nil || !completed[identity.TaskKey()] {
		t.Fatalf("precondition completed = (%v, %v)", completed, err)
	}

	orchestrator := &Orchestrator{state: state}
	plan := &BuildResult{
		TableTaskIDs:  map[string]int64{"orders": taskID},
		TableTaskKeys: map[string]string{"orders": identity.TaskKey()},
	}
	if err := orchestrator.resetResumeTableTasksRequired(plan, []source.Table{{Schema: "dbo", Name: "orders"}}); err != nil {
		t.Fatalf("resetResumeTableTasksRequired: %v", err)
	}
	completed, err = state.GetCompletedTables("run-645")
	if err != nil {
		t.Fatal(err)
	}
	if completed[identity.TaskKey()] {
		t.Fatal("stale aggregate success survived resume selection reset")
	}
}

func TestResetResumeTableTaskWriteFailureIsStateError(t *testing.T) {
	state := &requiredWriteFaultState{statusErr: errors.New("state db locked")}
	orchestrator := &Orchestrator{state: state}
	plan := &BuildResult{
		TableTaskIDs:  map[string]int64{"orders": 42},
		TableTaskKeys: map[string]string{"orders": "transfer:dbo.orders"},
	}
	err := orchestrator.resetResumeTableTasksRequired(plan, []source.Table{{Schema: "dbo", Name: "orders"}})
	if !checkpoint.IsRequiredWriteError(err) {
		t.Fatalf("resetResumeTableTasksRequired error = %v, want required-write error", err)
	}
}

func TestCollectFailuresPromotesRequiredWritesToGlobalFailure(t *testing.T) {
	runner := &TransferRunner{progress: progress.New()}
	errCh := make(chan tableError, 1)
	errCh <- tableError{
		tableName: "orders",
		err:       checkpoint.RequiredWrite("saving final checkpoint", errors.New("disk full")),
	}
	close(errCh)

	failures, err := runner.collectFailures(context.Background(), errCh)
	if failures != nil || !checkpoint.IsRequiredWriteError(err) {
		t.Fatalf("collectFailures = (%+v, %v), want global required-write error", failures, err)
	}
}
