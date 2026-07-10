package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

// schedulingRunner builds a TransferRunner whose per-job execution is faked, so
// executeJobs' partition-dependency scheduling (#648) can be tested without a
// live database.
func schedulingRunner(workers int, exec jobExecutor) *TransferRunner {
	return &TransferRunner{
		config:   &config.Config{Migration: config.MigrationConfig{Workers: workers}},
		progress: progress.New(),
		execJob:  exec,
	}
}

func partitionJob(table string, partitionID int, first bool) transfer.Job {
	return transfer.Job{
		Table:     source.Table{Schema: "dbo", Name: table},
		Partition: &source.Partition{TableName: table, PartitionID: partitionID, IsFirstPartition: first},
	}
}

func nonPartitionedJob(table string) transfer.Job {
	return transfer.Job{Table: source.Table{Schema: "dbo", Name: table}}
}

func jobLabel(j transfer.Job) string {
	if j.Partition == nil {
		return j.Table.Name
	}
	return j.Table.Name + ".p" + itoa(j.Partition.PartitionID)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// TestSchedulerRunsDependentPartitionWhileUnrelatedTableRuns is the core #648
// fix: table B's later partitions must be free to run as soon as B's first
// partition finishes, even while an unrelated non-partitioned table A is still
// transferring. The old two-phase barrier put A and B:p1 in one global phase
// and B:p2+ in the next, so a slow A stalled every other table's partitions.
func TestSchedulerRunsDependentPartitionWhileUnrelatedTableRuns(t *testing.T) {
	releaseA := make(chan struct{})
	bP2Ran := make(chan struct{})

	exec := func(ctx context.Context, runID string, j transfer.Job, _ *BuildResult, _ map[string]*tableStats, _ chan<- tableError, _ *monitor.Controller, _ transfer.RuntimeTuner, _ *runtimeAdjustmentRecorder) error {
		switch jobLabel(j) {
		case "A":
			<-releaseA // A is the slow unrelated table
		case "B.p2", "B.p3":
			select {
			case <-bP2Ran:
			default:
				close(bP2Ran)
			}
		}
		return nil
	}

	r := schedulingRunner(4, exec)
	jobs := []transfer.Job{
		nonPartitionedJob("A"),
		partitionJob("B", 1, true),
		partitionJob("B", 2, false),
		partitionJob("B", 3, false),
	}

	done := make(chan []TableFailure, 1)
	go func() {
		failures, err := r.executeJobs(context.Background(), "run", jobs, &BuildResult{}, map[string]*tableStats{}, nil, nil, nil)
		if err != nil {
			t.Errorf("executeJobs: %v", err)
		}
		done <- failures
	}()

	// B's dependent partitions must run before A is ever released.
	select {
	case <-bP2Ran:
	case <-time.After(2 * time.Second):
		close(releaseA)
		t.Fatal("B's later partitions stalled behind unrelated table A (cross-table barrier)")
	}

	close(releaseA)
	select {
	case failures := <-done:
		if len(failures) != 0 {
			t.Fatalf("unexpected failures: %+v", failures)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("executeJobs did not finish after releasing A")
	}
}

// TestSchedulerSuppressesDependentsWhenFirstPartitionFails covers criterion 2:
// once B:p1 fails, B:p2+ must never start (no more partial data for a table
// already known to have failed), while an unrelated table still completes.
func TestSchedulerSuppressesDependentsWhenFirstPartitionFails(t *testing.T) {
	firstPartitionErr := errors.New("boom: B first partition failed")
	var bDependentsRan int32
	var aRan int32

	exec := func(ctx context.Context, runID string, j transfer.Job, _ *BuildResult, _ map[string]*tableStats, errCh chan<- tableError, _ *monitor.Controller, _ transfer.RuntimeTuner, _ *runtimeAdjustmentRecorder) error {
		switch jobLabel(j) {
		case "B.p1":
			errCh <- tableError{tableName: j.Table.Name, err: firstPartitionErr}
			return firstPartitionErr
		case "B.p2", "B.p3":
			atomic.AddInt32(&bDependentsRan, 1)
		case "A":
			atomic.AddInt32(&aRan, 1)
		}
		return nil
	}

	r := schedulingRunner(4, exec)
	jobs := []transfer.Job{
		partitionJob("B", 1, true),
		partitionJob("B", 2, false),
		partitionJob("B", 3, false),
		nonPartitionedJob("A"),
	}

	failures, err := r.executeJobs(context.Background(), "run", jobs, &BuildResult{}, map[string]*tableStats{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("executeJobs: %v", err)
	}

	if got := atomic.LoadInt32(&bDependentsRan); got != 0 {
		t.Fatalf("B dependent partitions ran %d times after first-partition failure, want 0", got)
	}
	if got := atomic.LoadInt32(&aRan); got != 1 {
		t.Fatalf("unrelated table A ran %d times, want 1", got)
	}
	if len(failures) != 1 || failures[0].TableName != "B" || !errors.Is(failures[0].Error, firstPartitionErr) {
		t.Fatalf("failures = %+v, want single B first-partition failure", failures)
	}
}

// TestSchedulerBoundsGlobalConcurrency covers criterion 3: no matter how many
// runnable jobs exist, at most Workers execute at once.
func TestSchedulerBoundsGlobalConcurrency(t *testing.T) {
	const workers = 2
	var cur, peak int64
	release := make(chan struct{})

	exec := func(ctx context.Context, runID string, j transfer.Job, _ *BuildResult, _ map[string]*tableStats, _ chan<- tableError, _ *monitor.Controller, _ transfer.RuntimeTuner, _ *runtimeAdjustmentRecorder) error {
		n := atomic.AddInt64(&cur, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
				break
			}
		}
		<-release
		atomic.AddInt64(&cur, -1)
		return nil
	}

	// Eight independent non-partitioned tables: all immediately runnable.
	var jobs []transfer.Job
	for _, name := range []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"} {
		jobs = append(jobs, nonPartitionedJob(name))
	}

	r := schedulingRunner(workers, exec)
	done := make(chan struct{})
	go func() {
		if _, err := r.executeJobs(context.Background(), "run", jobs, &BuildResult{}, map[string]*tableStats{}, nil, nil, nil); err != nil {
			t.Errorf("executeJobs: %v", err)
		}
		close(done)
	}()

	// Let the pool saturate, then confirm the ceiling held.
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&peak); got > workers {
		close(release)
		t.Fatalf("peak concurrent jobs = %d, want <= %d", got, workers)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("executeJobs did not finish")
	}
	if got := atomic.LoadInt64(&peak); got == 0 || got > workers {
		t.Fatalf("final peak = %d, want in [1,%d]", got, workers)
	}
}

// TestSchedulerCanceledParentAborts confirms a canceled parent context stops
// scheduling and surfaces as an interruption, not a table failure.
func TestSchedulerCanceledParentAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A pre-canceled context may let a few jobs win the select against ctx.Done
	// before unwinding; the invariant is that the run aborts as an interruption
	// rather than reporting spurious table failures (#641).
	exec := func(context.Context, string, transfer.Job, *BuildResult, map[string]*tableStats, chan<- tableError, *monitor.Controller, transfer.RuntimeTuner, *runtimeAdjustmentRecorder) error {
		return nil
	}

	r := schedulingRunner(2, exec)
	jobs := []transfer.Job{nonPartitionedJob("t1"), partitionJob("B", 1, true), partitionJob("B", 2, false)}

	failures, err := r.executeJobs(ctx, "run", jobs, &BuildResult{}, map[string]*tableStats{}, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if failures != nil {
		t.Fatalf("failures = %+v, want nil on canceled parent", failures)
	}
}
