package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/notify"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	// memBudget is the in-flight byte budget shared across every concurrent
	// table pipeline for this run (#617). Created once in Run; nil disables
	// byte-based admission control.
	memBudget *transfer.MemBudget

	// execJob runs one job. It defaults to (*TransferRunner).executeJob and is
	// overridable in tests so executeJobs' partition-dependency scheduling can
	// be exercised without a live database (#648).
	execJob jobExecutor
}

// jobExecutor runs a single transfer job, returning its terminal error.
type jobExecutor func(ctx context.Context, runID string, j transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) error

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
	TableStats      map[string]*transfer.TransferStats
	TableFailures   []TableFailure
	ChunkRetryCount int // Cumulative count of transient chunk retries across the run
	// RuntimeAdjusted is true when the runtime controller or write-error
	// adjuster applied any parameter change during the run (#451).
	RuntimeAdjusted bool
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

	// Stamp resume status on every job so the transfer layer can branch on
	// it without re-deriving from progress state. Specifically, ROW_NUMBER
	// pagination uses this to enable idempotent-on-dup writes for ALL
	// partitions on resume — including partitions that crashed before
	// their first checkpoint flush (resumeLastPK == nil), which would
	// otherwise replay already-committed rows on a plain INSERT and fail
	// with duplicate-PK errors (#227 codex follow-up).
	// One shared in-flight byte budget for the whole run (#617). Every job
	// carries the same pointer, so concurrent table pipelines divide it by
	// contention rather than each claiming a static Workers-split slice.
	r.memBudget = transfer.NewMemBudget(transfer.PipelineMemBudgetBytes(r.config))

	for i := range jobs {
		jobs[i].IsResume = resume
		jobs[i].MemBudget = r.memBudget
	}

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

	// Always create the runtime tuner so chunk-retry / error / queue-depth
	// metrics flow into RunResult and downstream into ai_tuning_history. The
	// runtime controller (which makes runtime adjustments) only attaches if
	// AIAdjust is enabled and an AI mapper is configured, but the tuner's
	// metrics are useful for the smartconfig history feedback loop even
	// without runtime adjustment enabled.
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{
		ChunkSize:            r.config.Migration.ChunkSize,
		ReadAheadBuffers:     r.config.Migration.ReadAheadBuffers,
		ParallelReaders:      r.config.Migration.ParallelReaders,
		WriteAheadWriters:    r.config.Migration.WriteAheadWriters,
		CheckpointFrequency:  r.config.Migration.CheckpointFrequency,
		UpsertMergeChunkSize: r.config.Migration.UpsertMergeChunkSize,
	})
	runtimeAdjustments := newRuntimeAdjustmentRecorder(r.state, runID)

	// Setup runtime parameter adjustment via the rule-based controller
	// (#172). Replaced the AI-driven monitor in PR 172b. The controller
	// reads the same MetricsCollector + tuner the AI loop used and
	// applies adjustments per a fixed rule set; no LLM round-trip.
	// Config knob is migration.runtime_tuning (#211 rename from
	// ai_adjust; the legacy name is still parsed and warns).
	var runtimeMonitor *monitor.Controller
	adjustEnabled := false
	adjustInterval := 5 * time.Second
	if r.config.Migration.RuntimeTuning != nil {
		adjustEnabled = *r.config.Migration.RuntimeTuning
	}
	if r.config.Migration.RuntimeTuningInterval != "" {
		if d, err := time.ParseDuration(r.config.Migration.RuntimeTuningInterval); err == nil {
			adjustInterval = d
		}
	}
	if adjustEnabled {
		collector := monitor.NewMetricsCollector(tuner, adjustInterval)

		// Carry the probe-derived hard cap from applyTuning into the
		// controller so growth and shrink rules can't push chunk_size
		// past the target's protocol limit mid-migration (#166).
		// Without this, MySQL targets with a default 4MB @@max_allowed_packet
		// could exceed the packet via runtime growth even when the
		// initial chunk_size was packet-safe. Codex review on #166.
		controllerOpts := monitor.ControllerOptions{
			// MaxWAW left at default (8). The RuleWriteErrorAdjuster
			// constructed below catches chunk-too-big errors by
			// halving on write error, which is the fast-feedback
			// safety net; MaxChunkSize is the proactive cap.
			RunID:                runID,
			NextAdjustmentNumber: runtimeAdjustments.nextNumber,
			AdjustmentRecorder:   runtimeAdjustments.record,
		}
		if hardCap := r.config.Migration.TargetHardChunkLimit; hardCap > 0 {
			controllerOpts.MaxChunkSize = hardCap
			// Clamp the memory-pressure shrink floor under the cap so
			// the controller can't paradoxically raise chunk_size to
			// monitor.DefaultMinChunkSize when that exceeds the packet
			// limit. Referencing the exported constant keeps this
			// in sync with the controller's actual default (Copilot
			// review on #166).
			if hardCap < monitor.DefaultMinChunkSize {
				controllerOpts.MinChunkSize = hardCap
			}
		}
		runtimeMonitor = monitor.NewController(tuner, collector, adjustInterval, controllerOpts)

		monitorCtx, cancelMonitor := context.WithCancel(ctx)
		defer cancelMonitor()
		go runtimeMonitor.Start(monitorCtx)

		// Push live row counts to the collector at sub-tick frequency.
		// Without this, the collector only sees row updates after each
		// job completes — for long single-table jobs, throughput
		// snapshots stay at 0/unchanged for the whole job and the
		// throughput-stable rule can never fire (Codex review on PR
		// #195). Polling at adjustInterval/3 means the collector has
		// at least 3 fresh row-count samples per controller tick.
		rowPollInterval := adjustInterval / 3
		if rowPollInterval < 1*time.Second {
			rowPollInterval = 1 * time.Second
		}
		go func() {
			t := time.NewTicker(rowPollInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					runtimeMonitor.UpdateRowsProcessed(r.progress.Current())
				case <-monitorCtx.Done():
					return
				}
			}
		}()

		logging.Debug("rule-based runtime adjustment enabled (interval: %v, row poll: %v)",
			adjustInterval, rowPollInterval)
	}

	// Execute jobs with worker pool
	failures, err := r.executeJobs(ctx, runID, jobs, buildResult, statsMap, runtimeMonitor, tuner, runtimeAdjustments)
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
	if tuner != nil {
		result.ChunkRetryCount = tuner.Metrics().ChunkRetryCount
	}
	result.RuntimeAdjusted = runtimeAdjustments.applied()

	return result, nil
}

func (o *Orchestrator) buildTransferJobs(ctx context.Context, runID string, tables []source.Table) (*BuildResult, error) {
	builder := NewJobBuilder(o.sourcePool, o.state, o.config)
	buildResult, err := builder.Build(ctx, runID, tables)
	if err != nil {
		return nil, fmt.Errorf("building jobs: %w", err)
	}
	return buildResult, nil
}

func (o *Orchestrator) transferAll(ctx context.Context, runID string, buildResult *BuildResult, tables []source.Table, resume bool) ([]TableFailure, error) {
	// Execute jobs using TransferRunner. Error diagnosis runs through the
	// deterministic catalog in internal/driver/errordiag (#173); the
	// former AI-driven diagnoser was removed to avoid sending error
	// messages (which routinely contain row data) to a third-party LLM.
	runner := NewTransferRunner(
		o.sourcePool,
		o.targetPool,
		o.state,
		o.config,
		o.progress,
		o.notifier,
		o.targetMode,
	)

	result, err := runner.Run(ctx, runID, buildResult, tables, resume)
	if err != nil {
		return nil, err
	}

	// Stash chunk retry count so the orchestrator can persist it with the run's
	// final tuning result. Read by the UpdateTuningResult call sites in Run/Resume.
	o.lastChunkRetryCount = result.ChunkRetryCount
	o.lastRunAdjusted = result.RuntimeAdjusted

	return result.TableFailures, nil
}

// firstPartitionGate coordinates one table's partitions (#648). done is closed
// when the table's first partition finishes; failed records whether it
// established the pre-transfer cleanup boundary that later partitions depend on.
type firstPartitionGate struct {
	done   chan struct{}
	failed atomic.Bool
}

// executeJobs schedules every transfer job with per-table partition
// dependencies (#648). Non-partitioned jobs and each table's first partition
// start immediately, bounded only by the global worker semaphore. A table's
// dependent partitions (p2+) wait for that same table's first partition, then
// either run or — if the first partition failed — are suppressed so a table
// already known to have failed does not keep writing more partial data.
//
// This replaces the former two global phases, where every non-partitioned job
// and every table's first partition had to finish before ANY table's remaining
// partitions could start: one slow unrelated table stalled partition
// parallelism across the whole migration. The cleanup dependency is
// table-local, so it is now expressed table-locally.
//
// Note: table truncation is handled upfront by preTruncateIfNeeded, not here.
func (r *TransferRunner) executeJobs(ctx context.Context, runID string, jobs []transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) ([]TableFailure, error) {
	// One gate per table that actually has a first partition. Non-partitioned
	// tables never gate anything, so they get no entry.
	gates := make(map[string]*firstPartitionGate)
	for _, job := range jobs {
		if job.Partition != nil && job.Partition.IsFirstPartition {
			gates[job.Table.Name] = &firstPartitionGate{done: make(chan struct{})}
		}
	}

	logging.Debug("Scheduling %d jobs with %d workers (%d partition-gated tables)",
		len(jobs), r.config.Migration.Workers, len(gates))

	execJob := r.execJob
	if execJob == nil {
		execJob = r.executeJob
	}

	errCh := make(chan tableError, len(jobs))
	sem := make(chan struct{}, r.config.Migration.Workers)
	var wg sync.WaitGroup

	for _, job := range jobs {
		job := job
		gate := gates[job.Table.Name]
		dependent := job.Partition != nil && !job.Partition.IsFirstPartition

		wg.Add(1)
		go func() {
			defer wg.Done()

			// A dependent partition waits on its own table's first partition
			// BEFORE taking a worker slot, so a blocked dependent never occupies
			// bounded write concurrency, and it is suppressed if that first
			// partition failed.
			if dependent && gate != nil {
				select {
				case <-ctx.Done():
					return
				case <-gate.done:
				}
				if gate.failed.Load() {
					logging.Warn("Skipping %s partition %d: its first partition failed",
						job.Table.Name, job.Partition.PartitionID)
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			err := execJob(ctx, runID, job, buildResult, statsMap, errCh, runtimeMonitor, tuner, runtimeAdjustments)

			// Release (or suppress) this table's dependent partitions once its
			// first partition has resolved the cleanup boundary.
			if gate != nil && job.Partition != nil && job.Partition.IsFirstPartition {
				if err != nil {
					gate.failed.Store(true)
				}
				close(gate.done)
			}
		}()
	}

	// Always wait for in-flight goroutines before closing errCh: an early close
	// would let a still-running job send on a closed channel.
	wg.Wait()
	close(errCh)

	// Collect failures; collectFailures maps a canceled parent context to an
	// aborted run (#641).
	return r.collectFailures(ctx, errCh)
}

// executeJob runs a single job with retry logic. It returns the job's terminal
// error (nil on success) in addition to reporting failures on errCh, so the
// scheduler can release or suppress a table's dependent partitions based on its
// first partition's outcome (#648).
func (r *TransferRunner) executeJob(ctx context.Context, runID string, j transfer.Job, buildResult *BuildResult, statsMap map[string]*tableStats, errCh chan<- tableError, runtimeMonitor *monitor.Controller, tuner transfer.RuntimeTuner, runtimeAdjustments *runtimeAdjustmentRecorder) error {
	// Report active job metrics for runtime monitoring (the rule-based
	// controller's queue-growth and throughput-stable rules read these
	// via the MetricsCollector + tuner.Metrics()).
	if tuner != nil {
		tuner.ReportActiveJobs(1)
		defer tuner.ReportActiveJobs(-1)
	}

	// Mark task as running
	if err := r.updateTaskStatus(j, "running", ""); err != nil {
		errCh <- tableError{tableName: j.Table.Name, err: err}
		return err
	}

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
		attemptJob := j
		if attempt > 0 {
			attemptJob.ReplayPossible = true
		}
		stats, err = transfer.Execute(ctx, r.sourcePool, r.targetPool, r.config, attemptJob, r.progress, tuner, writeErrAdjuster)
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
		failureErr := err
		if stateErr := r.updateTaskStatus(j, "failed", err.Error()); stateErr != nil {
			failureErr = errors.Join(err, stateErr)
		}
		errCh <- tableError{tableName: j.Table.Name, err: failureErr}

		logging.Error("Table %s failed: %v", j.Table.Name, err)
		r.checkGeographyError(j.Table.Name, err)

		// AI error diagnosis
		r.diagnoseError(ctx, j, err)

		if r.config.Migration.NotifyOnFailure() {
			r.notifier.TableTransferFailed(runID, j.Table.Name, err)
		}
		return failureErr
	}

	if stateErr := r.updateTaskStatus(j, "success", ""); stateErr != nil {
		ts.jobsFailed++
		errCh <- tableError{tableName: j.Table.Name, err: stateErr}
		return stateErr
	}

	if stats != nil {
		ts.stats.QueryTime += stats.QueryTime
		ts.stats.ScanTime += stats.ScanTime
		ts.stats.WriteTime += stats.WriteTime
		ts.stats.Rows += stats.Rows
		// Report transfer time breakdown for runtime controllering
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
		if stateErr := r.markTransferTaskComplete(runID, j, buildResult); stateErr != nil {
			ts.jobsFailed++
			errCh <- tableError{tableName: j.Table.Name, err: stateErr}
			return stateErr
		}
		r.progress.TableComplete()
	}
	return nil
}

func (r *TransferRunner) updateTaskStatus(j transfer.Job, status, errorMessage string) error {
	err := r.state.UpdateTaskStatus(j.TaskID, status, errorMessage)
	return checkpoint.RequiredWrite(fmt.Sprintf("marking task %d for table %s.%s %s", j.TaskID, j.Table.Schema, j.Table.Name, status), err)
}

func (r *TransferRunner) markTransferTaskComplete(runID string, j transfer.Job, buildResult *BuildResult) error {
	var watermark *time.Time
	if buildResult != nil {
		if value, ok := buildResult.TableSyncWatermarks[j.Table.Name]; ok {
			watermark = &value
		}
	}
	targetSchema := ""
	if r.config != nil {
		targetSchema = r.config.Target.Schema
	}
	err := checkpoint.CompleteTransferTask(r.state, runID, checkpoint.TransferTaskIdentity{Schema: j.Table.Schema, Table: j.Table.Name}, targetSchema, watermark)
	return checkpoint.RequiredWrite(fmt.Sprintf("atomically completing transfer task for table %s.%s", j.Table.Schema, j.Table.Name), err)
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

// diagnoseError analyzes a transfer error and emits a diagnosis through
// the deterministic catalog (#173). Pattern-matched diagnoses are
// suggestions, not corrections — emitting one never changes control
// flow; the underlying error continues to propagate to the caller.
//
// The caller's ctx is forwarded so a canceled/timed-out transfer stays
// silent rather than emitting a misleading "no diagnosis available"
// box (driver.DiagnoseError returns nil when ctx is already done).
func (r *TransferRunner) diagnoseError(ctx context.Context, j transfer.Job, err error) {
	errCtx := &driver.ErrorContext{
		ErrorMessage: err.Error(),
		TableName:    j.Table.Name,
		TableSchema:  j.Table.Schema,
		SourceDBType: r.config.Source.Type,
		TargetDBType: r.config.Target.Type,
		TargetMode:   r.config.Migration.TargetMode,
	}

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

	if diag := driver.DiagnoseError(ctx, errCtx); diag != nil {
		driver.EmitDiagnosis(diag)
	}
}

// collectFailures gathers and deduplicates table failures. A child operation
// may return context.Canceled or DeadlineExceeded while the parent run remains
// healthy (for example, a PostgreSQL COPY sub-batch timeout). Only the parent
// context decides whether the whole run was interrupted (#641).
func (r *TransferRunner) collectFailures(ctx context.Context, errCh <-chan tableError) ([]TableFailure, error) {
	failedTables := make(map[string]error)
	var requiredWriteErrors []error

	for te := range errCh {
		if checkpoint.IsRequiredWriteError(te.err) {
			requiredWriteErrors = append(requiredWriteErrors, te.err)
		}
		if _, exists := failedTables[te.tableName]; !exists {
			failedTables[te.tableName] = te.err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(requiredWriteErrors) > 0 {
		return nil, errors.Join(requiredWriteErrors...)
	}

	var failures []TableFailure
	for tableName, err := range failedTables {
		failures = append(failures, TableFailure{TableName: tableName, Error: err})
		r.progress.TableFailed()
	}

	return failures, nil
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
