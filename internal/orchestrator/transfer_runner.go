package orchestrator

import (
	"context"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/monitor"
	"github.com/johndauphine/dmt/internal/notify"
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
	TableStats      map[string]*transfer.TransferStats
	TableFailures   []TableFailure
	ChunkRetryCount int // Cumulative count of transient chunk retries across the run
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
	for i := range jobs {
		jobs[i].IsResume = resume
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
	// AI monitor (which makes runtime adjustments) only attaches if
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

		// Carry the probe-derived hard cap from applyAITuning into the
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
	failures, err := r.executeJobs(ctx, runID, jobs, buildResult, statsMap, runtimeMonitor, tuner)
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

	return result, nil
}
