package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/notify"
	"github.com/johndauphine/dmt/internal/pipeline"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

// TransferRunner executes transfer jobs with a worker pool.
type TransferRunner struct {
	sourcePool     pool.SourcePool
	targetPool     pool.TargetPool
	state          checkpoint.StateBackend
	config         *config.Config
	progress       *progress.Tracker
	notifier       notify.Provider
	targetMode     TargetModeStrategy
	errorDiagnoser *driver.AIErrorDiagnoser
}

// NewTransferRunner creates a new TransferRunner.
func NewTransferRunner(
	sourcePool pool.SourcePool,
	targetPool pool.TargetPool,
	state checkpoint.StateBackend,
	cfg *config.Config,
	prog *progress.Tracker,
	notifier notify.Provider,
	targetMode TargetModeStrategy,
	errorDiagnoser *driver.AIErrorDiagnoser,
) *TransferRunner {
	return &TransferRunner{
		sourcePool:     sourcePool,
		targetPool:     targetPool,
		state:          state,
		config:         cfg,
		progress:       prog,
		notifier:       notifier,
		targetMode:     targetMode,
		errorDiagnoser: errorDiagnoser,
	}
}

// RunResult contains the outcome of a transfer run.
type RunResult struct {
	TableStats    map[string]*transfer.TransferStats
	TableFailures []TableFailure
}

// tableStats tracks stats for a single table (internal).
type tableStats struct {
	mu           sync.Mutex
	stats        *transfer.TransferStats
	jobsComplete int
	jobsFailed   int
}

// tableError represents a table transfer failure.
type tableError struct {
	tableName string
	err       error
}

// Run executes the transfer jobs and returns the results.
func (r *TransferRunner) Run(ctx context.Context, runID string, buildResult *BuildResult, tables []source.Table, resume bool) (*RunResult, error) {
	jobs := buildResult.Jobs

	// Initialize progress
	logging.Debug("Created %d jobs, calculating total rows", len(jobs))
	var totalRows int64
	for _, j := range jobs {
		if j.Partition != nil {
			totalRows += j.Partition.RowCount
		} else {
			totalRows += j.Table.RowCount
		}
	}
	logging.Debug("Setting progress total to %d rows", totalRows)
	r.progress.SetTotal(totalRows)

	// Stats collection per table
	statsMap := make(map[string]*tableStats)
	for _, t := range tables {
		statsMap[t.Name] = &tableStats{stats: &transfer.TransferStats{}}
	}

	// Pre-truncate partitioned tables (if needed)
	if err := r.preTruncateIfNeeded(ctx, jobs, resume); err != nil {
		return nil, err
	}

	// Setup AI-driven monitoring if enabled (from global secrets)
	var aiMonitor *monitor.AIMonitor
	aiAdjustEnabled := false
	aiAdjustInterval := 30 * time.Second // Default
	if secretsCfg, err := secrets.Load(); err == nil {
		defaults := secretsCfg.GetMigrationDefaults()
		if defaults.AIAdjust != nil {
			aiAdjustEnabled = *defaults.AIAdjust
		}
		if defaults.AIAdjustInterval != "" {
			if d, err := time.ParseDuration(defaults.AIAdjustInterval); err == nil {
				aiAdjustInterval = d
			}
		}
	}
	if aiAdjustEnabled {
		typeMapper, err := driver.GetAITypeMapper()
		if err == nil && typeMapper != nil {
			// Type-assert to AITypeMapper
			if aiMapper, ok := typeMapper.(*driver.AITypeMapper); ok {
				// Create a placeholder pipeline for the monitor (it will be updated per-job)
				p := pipeline.New(nil, nil, pipeline.Config{})
				aiMonitor = monitor.NewAIMonitor(p, aiMapper, aiAdjustInterval)

				// Set connection limits from config for AI guardrails
				aiMonitor.SetConnectionLimits(
					r.config.Migration.MaxSourceConnections,
					r.config.Migration.MaxTargetConnections,
				)

				// Set state backend for persistent history
				aiMonitor.SetStateBackend(r.state, runID)

				// Start monitoring in background
				monitorCtx, cancelMonitor := context.WithCancel(ctx)
				defer cancelMonitor()
				go aiMonitor.Start(monitorCtx)

				logging.Debug("AI-driven parameter adjustment enabled (interval: %v)", aiAdjustInterval)
			} else {
				logging.Debug("AI adjustment requested but type assertion failed")
			}
		} else {
			logging.Debug("AI adjustment requested but AI not configured: %v", err)
		}
	}

	// Execute jobs with worker pool
	failures, err := r.executeJobs(ctx, runID, jobs, buildResult, statsMap, aiMonitor)
	if err != nil {
		return nil, err
	}

	r.progress.Finish()

	// Log pool stats
	r.logPoolStats()

	// Log transfer profile
	r.logTransferProfile(tables, statsMap)

	// Build result
	result := &RunResult{
		TableStats:    make(map[string]*transfer.TransferStats),
		TableFailures: failures,
	}
	for name, ts := range statsMap {
		result.TableStats[name] = ts.stats
	}

	return result, nil
}

// preTruncateIfNeeded truncates partitioned tables before transfer.
func (r *TransferRunner) preTruncateIfNeeded(ctx context.Context, jobs []transfer.Job, resume bool) error {
	if resume || !r.targetMode.ShouldTruncateBeforeTransfer() {
		return nil
	}

	// Collect unique table names that need truncation
	tablesToTruncate := make(map[string]bool)
	for _, j := range jobs {
		if j.Partition != nil {
			tablesToTruncate[j.Table.Name] = true
		}
	}

	if len(tablesToTruncate) == 0 {
		return nil
	}

	logging.Debug("Pre-truncating %d partitioned tables in parallel...", len(tablesToTruncate))
	var truncWg sync.WaitGroup
	truncErrs := make(chan error, len(tablesToTruncate))

	for tableName := range tablesToTruncate {
		truncWg.Add(1)
		go func(tname string) {
			defer truncWg.Done()
			if err := r.targetPool.TruncateTable(ctx, r.config.Target.Schema, tname); err != nil {
				truncErrs <- fmt.Errorf("pre-truncating table %s: %w", tname, err)
			}
		}(tableName)
	}

	truncWg.Wait()
	close(truncErrs)

	if err := <-truncErrs; err != nil {
		return err
	}

	return nil
}

// executeJobs runs jobs with a worker pool.
// For partitioned tables, first partitions are processed before remaining partitions
// to prevent race conditions in partition cleanup logic during idempotent retries.
// Note: Table truncation is handled upfront by preTruncateIfNeeded, not by partitions.
func (r *TransferRunner) executeJobs(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, aiMonitor *monitor.AIMonitor) ([]TableFailure, error) {
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
		if err := r.executeJobBatch(ctx, runID, firstPhaseJobs, buildResult, statsMap, errCh, aiMonitor); err != nil {
			close(errCh)
			return nil, err
		}
	}

	// Phase 2: Process remaining partitions (after first partitions complete)
	if len(remainingJobs) > 0 {
		if err := r.executeJobBatch(ctx, runID, remainingJobs, buildResult, statsMap, errCh, aiMonitor); err != nil {
			close(errCh)
			return nil, err
		}
	}

	close(errCh)

	// Collect failures
	return r.collectFailures(errCh)
}

// executeJobBatch runs a batch of jobs with the worker pool.
func (r *TransferRunner) executeJobBatch(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, aiMonitor *monitor.AIMonitor) error {
	sem := make(chan struct{}, r.config.Migration.Workers)
	var wg sync.WaitGroup

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(j transfer.Job) {
			defer wg.Done()
			defer func() { <-sem }()

			r.executeJob(ctx, runID, j, buildResult, statsMap, errCh, aiMonitor)
		}(job)
	}

	wg.Wait()
	return nil
}

// executeJob runs a single job with retry logic.
func (r *TransferRunner) executeJob(ctx context.Context, runID string, j transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, aiMonitor *monitor.AIMonitor) {
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
		}

		stats, err = transfer.Execute(ctx, r.sourcePool, r.targetPool, r.config, j, r.progress)
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
		r.state.UpdateTaskStatus(j.TaskID, "failed", err.Error())
		errCh <- tableError{tableName: j.Table.Name, err: err}

		logging.Error("Table %s failed: %v", j.Table.Name, err)
		r.checkGeographyError(j.Table.Name, err)

		// AI error diagnosis
		r.diagnoseError(ctx, j, err)

		r.notifier.TableTransferFailed(runID, j.Table.Name, err)
		return
	}

	r.state.UpdateTaskStatus(j.TaskID, "success", "")

	if stats != nil {
		ts.stats.QueryTime += stats.QueryTime
		ts.stats.ScanTime += stats.ScanTime
		ts.stats.WriteTime += stats.WriteTime
		ts.stats.Rows += stats.Rows
		// Update row counter for AI monitoring
		if aiMonitor != nil {
			aiMonitor.UpdateRowsProcessed(r.progress.Current())
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
			if syncTime, ok := buildResult.TableSyncStartTimes[j.Table.Name]; ok {
				if err := r.state.UpdateSyncTimestamp(j.Table.Schema, j.Table.Name, r.config.Target.Schema, syncTime); err != nil {
					logging.Warn("Failed to update sync timestamp for %s: %v", j.Table.Name, err)
				} else {
					logging.Debug("Updated sync timestamp for %s to %v", j.Table.Name, syncTime.Format(time.RFC3339))
				}
			}
		}
	}
}

// checkGeographyError logs a hint for geography/geometry errors.
func (r *TransferRunner) checkGeographyError(tableName string, err error) {
	errStr := err.Error()
	if strings.Contains(errStr, "Invalid operator for data type") &&
		(strings.Contains(errStr, "geography") || strings.Contains(errStr, "geometry")) {
		logging.Warn("HINT: Table %s contains geography/geometry columns which cannot be compared in MERGE statements.", tableName)
		logging.Warn("      Use 'target_mode: drop_recreate' or exclude this table with 'exclude_tables'.")
	}
}

// diagnoseError uses AI to analyze the error and provide suggestions.
func (r *TransferRunner) diagnoseError(ctx context.Context, j transfer.Job, err error) {
	if r.errorDiagnoser == nil {
		return
	}

	// Build error context with table information
	errCtx := &driver.ErrorContext{
		ErrorMessage: err.Error(),
		TableName:    j.Table.Name,
		TableSchema:  j.Table.Schema,
		SourceDBType: r.config.Source.Type,
		TargetDBType: r.config.Target.Type,
		TargetMode:   r.config.Migration.TargetMode,
	}

	// Add column info from the job's table
	if j.Table.Columns != nil {
		errCtx.Columns = make([]driver.Column, len(j.Table.Columns))
		for i, col := range j.Table.Columns {
			errCtx.Columns[i] = driver.Column{
				Name:       col.Name,
				DataType:   col.DataType,
				MaxLength:  col.MaxLength,
				Precision:  col.Precision,
				Scale:      col.Scale,
				IsNullable: col.IsNullable,
				IsIdentity: col.IsIdentity,
			}
		}
	}

	// Call AI diagnosis (non-blocking, use short timeout)
	diagCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	diagnosis, diagErr := r.errorDiagnoser.Diagnose(diagCtx, errCtx)
	if diagErr != nil {
		logging.Debug("AI error diagnosis unavailable: %v", diagErr)
		return
	}

	// Emit the diagnosis (TUI will format as box, CLI falls back to logging)
	driver.EmitDiagnosis(diagnosis)
}

// collectFailures gathers and deduplicates table failures.
func (r *TransferRunner) collectFailures(errCh <-chan tableError) ([]TableFailure, error) {
	failedTables := make(map[string]error)

	for te := range errCh {
		if errors.Is(te.err, context.Canceled) || errors.Is(te.err, context.DeadlineExceeded) {
			return nil, context.Canceled
		}
		if _, exists := failedTables[te.tableName]; !exists {
			failedTables[te.tableName] = te.err
			r.progress.TableFailed()
		}
	}

	var failures []TableFailure
	for tableName, err := range failedTables {
		failures = append(failures, TableFailure{TableName: tableName, Error: err})
	}

	return failures, nil
}

// logPoolStats logs connection pool statistics.
func (r *TransferRunner) logPoolStats() {
	if !logging.IsDebug() {
		return
	}

	logging.Debug("\nConnection Pool Usage:")
	logging.Debug("  Source %s", r.sourcePool.PoolStats())
	logging.Debug("  Target %s", r.targetPool.PoolStats())
}

// logTransferProfile logs per-table transfer statistics.
func (r *TransferRunner) logTransferProfile(tables []source.Table, statsMap map[string]*tableStats) {
	if !logging.IsDebug() {
		return
	}

	logging.Debug("\nTransfer Profile (per table):")
	logging.Debug("------------------------------")

	var totalQuery, totalScan, totalWrite time.Duration
	for _, t := range tables {
		ts := statsMap[t.Name]
		if ts.stats.Rows > 0 {
			logging.Debug("%-25s %s", t.Name, ts.stats.String())
			totalQuery += ts.stats.QueryTime
			totalScan += ts.stats.ScanTime
			totalWrite += ts.stats.WriteTime
		}
	}

	totalTime := totalQuery + totalScan + totalWrite
	if totalTime > 0 {
		logging.Debug("------------------------------")
		logging.Debug("%-25s query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%)",
			"TOTAL",
			totalQuery.Seconds(), float64(totalQuery)/float64(totalTime)*100,
			totalScan.Seconds(), float64(totalScan)/float64(totalTime)*100,
			totalWrite.Seconds(), float64(totalWrite)/float64(totalTime)*100)
	}
}
