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
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

// TransferRunner executes transfer jobs with a worker pool.
type TransferRunner struct {
	sourcePool pool.SourcePool
	targetPool pool.TargetPool
	state      checkpoint.StateBackend
	config     *config.Config
	progress   *progress.Tracker
	notifier   notify.Provider
	targetMode TargetModeStrategy
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
) *TransferRunner {
	return &TransferRunner{
		sourcePool: sourcePool,
		targetPool: targetPool,
		state:      state,
		config:     cfg,
		progress:   prog,
		notifier:   notifier,
		targetMode: targetMode,
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

	// Setup AI-driven monitoring if enabled
	var aiMonitor *monitor.AIMonitor
	if r.config.Migration.AIAdjust {
		typeMapper, err := driver.GetAITypeMapper()
		if err == nil && typeMapper != nil {
			// Type-assert to AITypeMapper
			if aiMapper, ok := typeMapper.(*driver.AITypeMapper); ok {
				// Create a placeholder pipeline for the monitor (it will be updated per-job)
				p := pipeline.New(nil, nil, pipeline.Config{})
				interval := 30 * time.Second // Default
				if r.config.Migration.AIAdjustInterval != "" {
					if d, err := time.ParseDuration(r.config.Migration.AIAdjustInterval); err == nil {
						interval = d
					}
				}
				aiMonitor = monitor.NewAIMonitor(p, aiMapper, interval)

				// Set connection limits from config for AI guardrails
				maxSource := r.config.Migration.MaxMssqlConnections
				if r.config.Source.Type == "postgres" {
					maxSource = r.config.Migration.MaxPgConnections
				}
				maxTarget := r.config.Migration.MaxPgConnections
				if r.config.Target.Type == "mssql" {
					maxTarget = r.config.Migration.MaxMssqlConnections
				}
				aiMonitor.SetConnectionLimits(maxSource, maxTarget)

				// Set state backend for persistent history
				aiMonitor.SetStateBackend(r.state, runID)

				// Start monitoring in background
				monitorCtx, cancelMonitor := context.WithCancel(ctx)
				defer cancelMonitor()
				go aiMonitor.Start(monitorCtx)

				logging.Info("AI-driven parameter adjustment enabled (interval: %v)", interval)
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
func (r *TransferRunner) executeJobs(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, aiMonitor *monitor.AIMonitor) ([]TableFailure, error) {
	logging.Debug("Starting worker pool with %d workers, %d jobs", r.config.Migration.Workers, len(jobs))

	sem := make(chan struct{}, r.config.Migration.Workers)
	var wg sync.WaitGroup

	errCh := make(chan tableError, len(jobs))

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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
	close(errCh)

	// Collect failures
	return r.collectFailures(errCh)
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
