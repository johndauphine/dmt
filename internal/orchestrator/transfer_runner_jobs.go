package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/transfer"
)

// executeJobs runs jobs with a worker pool.
// For partitioned tables, first partitions are processed before remaining partitions
// to prevent race conditions in partition cleanup logic during idempotent retries.
// Note: Table truncation is handled upfront by preTruncateIfNeeded, not by partitions.
func (r *TransferRunner) executeJobs(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) ([]TableFailure, error) {
	// Separate jobs into two phases:
	// - Phase 1: Non-partitioned jobs + first partitions (no dependencies)
	// - Phase 2: Remaining partitions (wait for first partitions to establish cleanup boundaries)
	var firstPhaseJobs []transfer.Job
	var remainingJobs []transfer.Job

	for _, job := range jobs {
		if job.Partition == nil {
			// Non-partitioned jobs have no race condition concerns
			firstPhaseJobs = append(firstPhaseJobs, job)
		} else if job.Partition.IsFirstPartition {
			firstPhaseJobs = append(firstPhaseJobs, job)
		} else {
			remainingJobs = append(remainingJobs, job)
		}
	}

	logging.Debug("Starting worker pool with %d workers, %d jobs (%d first-phase, %d remaining)",
		r.config.Migration.Workers, len(jobs), len(firstPhaseJobs), len(remainingJobs))

	errCh := make(chan tableError, len(jobs))

	// Phase 1: Process non-partitioned jobs and first partitions
	// These can run in parallel since they're for different tables or establish cleanup boundaries
	if len(firstPhaseJobs) > 0 {
		if err := r.executeJobBatch(ctx, runID, firstPhaseJobs, buildResult, statsMap, errCh, runtimeMonitor, tuner, runtimeAdjustments); err != nil {
			close(errCh)
			return nil, err
		}
	}

	// Phase 2: Process remaining partitions (after first partitions complete)
	if len(remainingJobs) > 0 {
		if err := r.executeJobBatch(ctx, runID, remainingJobs, buildResult, statsMap, errCh, runtimeMonitor, tuner, runtimeAdjustments); err != nil {
			close(errCh)
			return nil, err
		}
	}

	close(errCh)

	// Collect failures
	return r.collectFailures(errCh)
}

// executeJobBatch runs a batch of jobs with the worker pool.
func (r *TransferRunner) executeJobBatch(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) error {
	sem := make(chan struct{}, r.config.Migration.Workers)
	var wg sync.WaitGroup
	var ctxErr error

loop:
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			ctxErr = ctx.Err()
			break loop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(j transfer.Job) {
			defer wg.Done()
			defer func() { <-sem }()

			r.executeJob(ctx, runID, j, buildResult, statsMap, errCh, runtimeMonitor, tuner, runtimeAdjustments)
		}(job)
	}

	// Always wait for in-flight goroutines to finish before returning.
	// Returning early would let the caller close errCh while goroutines
	// still send to it, causing "send on closed channel" panic.
	wg.Wait()
	return ctxErr
}

// executeJob runs a single job with retry logic.
func (r *TransferRunner) executeJob(ctx context.Context, runID string, j transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) {
	// Report active job metrics for runtime monitoring (the rule-based
	// controller's queue-growth and throughput-stable rules read these
	// via the MetricsCollector + tuner.Metrics()).
	if tuner != nil {
		tuner.ReportActiveJobs(1)
		defer tuner.ReportActiveJobs(-1)
	}

	// Mark task as running
	r.state.UpdateTaskStatus(j.TaskID, "running", "")

	// Execute with retry
	maxRetries := r.config.Migration.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	var stats *transfer.TransferStats
	var err error

retryLoop:
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<(attempt-1)) * time.Second
			logging.Warn("Retry %d/%d for %s after %v (error: %v)", attempt, maxRetries, j.Table.Name, backoff, err)
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break retryLoop
			case <-time.After(backoff):
			}
			// Increment AFTER the backoff completes (and ctx wasn't canceled),
			// so the metric reflects retries that actually executed rather than
			// retries scheduled but aborted by cancellation. Also keeps the
			// counter in sync with the "retry attempt about to fire" semantic.
			if tuner != nil {
				tuner.ReportChunkRetry()
			}
		}

		// Per-write-error chunk-size adjuster — deterministic
		// replacement for the AI-driven path removed in PR #195.
		// Halves chunk_size when the write error matches a known
		// structural-limit pattern (MySQL "too many placeholders",
		// MSSQL 2100-parameter cap, max_allowed_packet); returns 0
		// (no adjustment) for other errors so transient failures fall
		// through to the normal retry logic.
		var writeErrAdjuster transfer.WriteErrorAdjuster = monitor.NewRuleWriteErrorAdjuster()
		if runtimeAdjustments != nil {
			writeErrAdjuster = recordingWriteErrorAdjuster{base: writeErrAdjuster, recorder: runtimeAdjustments}
		}
		stats, err = transfer.Execute(ctx, r.sourcePool, r.targetPool, r.config, j, r.progress, tuner, writeErrAdjuster)
		if err == nil {
			break
		}
		if !isRetryableError(err) {
			break
		}
	}

	// Update stats
	ts := statsMap[j.Table.Name]
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if err != nil {
		ts.jobsFailed++
		if tuner != nil {
			tuner.ReportError()
		}
		r.state.UpdateTaskStatus(j.TaskID, "failed", err.Error())
		errCh <- tableError{tableName: j.Table.Name, err: err}

		logging.Error("Table %s failed: %v", j.Table.Name, err)
		r.checkGeographyError(j.Table.Name, err)

		// AI error diagnosis
		r.diagnoseError(ctx, j, err)

		if r.config.Migration.NotifyOnFailure() {
			r.notifier.TableTransferFailed(runID, j.Table.Name, err)
		}
		return
	}

	r.state.UpdateTaskStatus(j.TaskID, "success", "")

	if stats != nil {
		ts.stats.QueryTime += stats.QueryTime
		ts.stats.ScanTime += stats.ScanTime
		ts.stats.WriteTime += stats.WriteTime
		ts.stats.Rows += stats.Rows
		// Report transfer time breakdown for AI monitoring
		if tuner != nil {
			tuner.ReportTransferTime(
				stats.QueryTime.Nanoseconds(),
				stats.ScanTime.Nanoseconds(),
				stats.WriteTime.Nanoseconds(),
				stats.Rows,
			)
		}
		// Forward row count to the runtime monitor's collector so
		// the controller's throughput-stable rule has fresh data.
		if runtimeMonitor != nil {
			runtimeMonitor.UpdateRowsProcessed(r.progress.Current())
		}
	}
	ts.jobsComplete++

	// Check if all jobs for this table are complete
	if ts.jobsComplete == buildResult.TableJobCounts[j.Table.Name] && ts.jobsFailed == 0 {
		taskKey := fmt.Sprintf("transfer:%s.%s", j.Table.Schema, j.Table.Name)
		r.state.MarkTaskComplete(runID, taskKey)
		r.progress.TableComplete()

		// Update sync timestamp
		if _, hasDateFilter := buildResult.TableDateFilters[j.Table.Name]; hasDateFilter {
			if watermark, ok := buildResult.TableSyncWatermarks[j.Table.Name]; ok {
				if err := r.state.UpdateSyncTimestamp(j.Table.Schema, j.Table.Name, r.config.Target.Schema, watermark); err != nil {
					logging.Warn("Failed to update sync timestamp for %s: %v", j.Table.Name, err)
				} else {
					logging.Debug("Updated sync timestamp for %s to %v", j.Table.Name, watermark.Format(time.RFC3339Nano))
				}
			}
		}
	}
}
