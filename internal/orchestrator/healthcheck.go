package orchestrator

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
	"github.com/shirou/gopsutil/v3/mem"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// HealthCheck tests connectivity to source and target databases.
// Runs source and target checks in parallel with independent timeouts to prevent
// one slow connection from causing the other to fail with "context deadline exceeded".
func (o *Orchestrator) HealthCheck(ctx context.Context) (*HealthCheckResult, error) {
	result := &HealthCheckResult{
		Timestamp:    time.Now().Format(time.RFC3339),
		SourceDBType: o.sourcePool.DBType(),
		TargetDBType: o.targetPool.DBType(),
	}

	// Use a per-check timeout of 30 seconds.
	const checkTimeout = 30 * time.Second

	// Run source and target checks in parallel to:
	// 1. Give each check its own 30-second budget
	// 2. Preserve context cancellation (SIGINT, etc.)
	// 3. Complete in max(source, target) time instead of source + target
	var wg sync.WaitGroup
	wg.Add(2)

	// Source check goroutine
	go func() {
		defer wg.Done()
		sourceStart := time.Now()
		sourceCtx, sourceCancel := context.WithTimeout(ctx, checkTimeout)
		defer sourceCancel()

		if db := o.sourcePool.DB(); db != nil {
			if err := db.PingContext(sourceCtx); err != nil {
				// Ping errors from go-mssqldb / pgx / go-sql-driver/mysql
				// can include the DSN. Scrub before this string lands in
				// the JSON output or the TUI (#231).
				result.SourceError = logging.Scrub(err.Error())
			} else {
				result.SourceConnected = true
				// Get table count
				tables, err := o.sourcePool.ExtractSchema(sourceCtx, o.config.Source.Schema)
				if err == nil {
					result.SourceTableCount = len(tables)
				}
			}
		}
		result.SourceLatencyMs = time.Since(sourceStart).Milliseconds()
	}()

	// Target check goroutine
	go func() {
		defer wg.Done()
		targetStart := time.Now()
		targetCtx, targetCancel := context.WithTimeout(ctx, checkTimeout)
		defer targetCancel()

		if err := o.targetPool.Ping(targetCtx); err != nil {
			// Same DSN-leak class as the source-side ping above (#231).
			result.TargetError = logging.Scrub(err.Error())
		} else {
			result.TargetConnected = true
		}
		result.TargetLatencyMs = time.Since(targetStart).Milliseconds()
	}()

	wg.Wait()

	result.Healthy = result.SourceConnected && result.TargetConnected

	// Run preflight only when both pings succeeded. Driver-side checks
	// assume the connection is live; running them against a dead pool
	// would just emit a flurry of "could not read X" findings that
	// duplicate the connection error the operator already sees.
	if result.Healthy {
		pf := o.collectPreFlightFindings(ctx)
		pf = applyPreFlightSkips(pf, preFlightSkipSet(o.config.Migration.SkipPreflight))
		result.PreFlightFindings = pf.Findings
		result.PreFlightAborted = pf.Aborted
		if pf.Aborted {
			result.Healthy = false
		}
	}
	return result, nil
}

// DryRun performs a migration preview without transferring data.
func (o *Orchestrator) DryRun(ctx context.Context) (*DryRunResult, error) {
	logging.Info("Performing dry run (no data will be transferred)...")

	if err := o.runPreFlight(ctx); err != nil {
		return nil, err
	}

	// Extract schema
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		return nil, fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)
	schemaReport, err := o.reportSchemaDrift(tables, false)
	if err != nil {
		return nil, err
	}

	result := &DryRunResult{
		SourceType:              o.config.Source.Type,
		TargetType:              o.config.Target.Type,
		SourceSchema:            o.config.Source.Schema,
		TargetSchema:            o.config.Target.Schema,
		Workers:                 o.config.Migration.Workers,
		ChunkSize:               o.config.Migration.ChunkSize,
		TargetMode:              o.config.Migration.TargetMode,
		TotalTables:             len(tables),
		SchemaContractDecisions: o.schemaEvolution().LastContractDecisions(),
	}
	if o.opts.EnableAISchemaAdvisor && schemaReport.HasChanges() {
		reviewCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		result.AISchemaAdvisor = o.ReviewSchemaDriftWithAI(reviewCtx, schemaReport, tables, false)
	}

	// Calculate estimated memory
	bufferMem := int64(o.config.Migration.Workers) *
		int64(o.config.Migration.ReadAheadBuffers) *
		int64(o.config.Migration.ChunkSize) *
		500 // bytes per row estimate
	result.EstimatedMemMB = bufferMem / (1024 * 1024)

	// Analyze each table
	for _, t := range tables {
		rowCount, err := o.sourcePool.GetRowCount(ctx, o.config.Source.Schema, t.Name)
		if err != nil {
			logging.Warn("Failed to get row count for %s.%s: %v (assuming 0)", o.config.Source.Schema, t.Name, err)
			rowCount = 0
		}
		result.TotalRows += rowCount

		// Determine pagination method
		paginationMethod := "full_table"
		partitions := 1
		hasPK := len(t.PKColumns) > 0

		if hasPK {
			if len(t.PKColumns) == 1 && isIntegerType(t.PKColumns[0].DataType) {
				paginationMethod = "keyset"
			} else {
				paginationMethod = "row_number"
			}

			// Estimate partitions for large tables
			if rowCount > int64(o.config.Migration.LargeTableThreshold) {
				partitions = o.config.Migration.MaxPartitions
			}
		}

		result.Tables = append(result.Tables, DryRunTable{
			Name:             t.Name,
			RowCount:         rowCount,
			PaginationMethod: paginationMethod,
			Partitions:       partitions,
			HasPK:            hasPK,
			Columns:          len(t.Columns),
		})
	}

	result.EstimatedRowsPerSecond = o.estimateDryRunRowsPerSecond()
	if result.EstimatedRowsPerSecond > 0 && result.TotalRows > 0 {
		result.EstimatedDurationSeconds = float64(result.TotalRows) / float64(result.EstimatedRowsPerSecond)
	}

	deletePreview, err := o.previewDeleteReconciliation(tables, time.Now())
	if err != nil {
		return nil, err
	}
	if err := o.countDeleteReconciliationCandidates(ctx, tables, deletePreview); err != nil {
		return nil, err
	}
	result.DeleteReconciliation = deletePreview

	return result, nil
}

func (o *Orchestrator) estimateDryRunRowsPerSecond() int64 {
	history, err := o.state.GetTuningHistory(5, o.sourcePool.DBType(), o.targetPool.DBType())
	if err != nil {
		logging.Debug("dry-run: failed to load throughput history: %v", err)
		return 0
	}

	var sum float64
	var count int64
	for _, record := range history {
		if record.FinalThroughput <= 0 {
			continue
		}
		sum += record.FinalThroughput
		count++
	}
	if count == 0 {
		return 0
	}
	return int64(sum / float64(count))
}

// isIntegerType checks if a data type is an integer type.
func isIntegerType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	intTypes := []string{"int", "bigint", "smallint", "tinyint", "integer", "int4", "int8", "int2"}
	for _, t := range intTypes {
		if strings.Contains(dataType, t) {
			return true
		}
	}
	return false
}

// GetSystemBasedSuggestions returns tuning suggestions based only on system resources.
// Use this when no database connections are available. Tries AI first, falls back to defaults.
func GetSystemBasedSuggestions(cfg *config.Config) *driver.SmartConfigSuggestions {
	suggestions := &driver.SmartConfigSuggestions{}

	cores := runtime.NumCPU()
	memGB := 8 // default assumption
	availableMemoryMB := int64(memGB) * 1024

	// Get memory info if available
	if v, err := mem.VirtualMemory(); err == nil {
		memGB = int(v.Total / (1024 * 1024 * 1024))
		availableMemoryMB = int64(v.Available / (1024 * 1024))
	}

	// Build a tuning.Input directly — offline path has no source schema, so
	// AvgRowBytes defaults to 500 (mirrors calculateAvgRowSize's fallback
	// when no row stats are available, #157). The deterministic tuner
	// handles the rest, including the per-target chunk-byte anchor (#166).
	in := tuning.Input{
		CPUCores:           cores,
		MemoryGB:           memGB,
		AvailableMemoryMB:  availableMemoryMB,
		MaxMemoryMB:        cfg.Migration.MaxMemoryMB,
		Platform:           driver.DetectPlatform(),
		SourceDBType:       cfg.Source.Type,
		TargetDBType:       cfg.Target.Type,
		TargetMode:         cfg.Migration.TargetMode,
		AvgRowBytes:        500,
		ForceExplore:       cfg.Migration.Explore,
		ExplorationEpsilon: driver.ExplorationEpsilon(cfg.Migration.ExploreMode),
	}
	profile := tuning.DriverProfile{Name: cfg.Target.Type, BaselineWAW: 2}
	if d, err := driver.Get(cfg.Target.Type); err == nil {
		defaults := d.Defaults()
		profile.BaselineWAW = defaults.WriteAheadWriters
		profile.ScaleWritersWithCores = defaults.ScaleWritersWithCores
		profile.OptimumBulkChunkBytes = defaults.OptimumBulkChunkBytes
		profile.HardChunkLimit = d.HardChunkLimit(in.AvgRowBytes)
	}
	out := tuning.Tune(in, profile, nil, tuning.DBTuning{})

	suggestions.Workers = out.Workers
	suggestions.ChunkSizeRecommendation = out.ChunkSize
	suggestions.ReadAheadBuffers = out.ReadAheadBuffers
	suggestions.WriteAheadWriters = out.WriteAheadWriters
	suggestions.ParallelReaders = out.ParallelReaders
	suggestions.MaxPartitions = out.MaxPartitions
	suggestions.LargeTableThreshold = out.LargeTableThreshold
	suggestions.MaxSourceConnections = out.MaxSourceConnections
	suggestions.MaxTargetConnections = out.MaxTargetConnections
	suggestions.UpsertMergeChunkSize = out.UpsertMergeChunkSize
	suggestions.CheckpointFrequency = out.CheckpointFrequency
	suggestions.MaxRetries = out.MaxRetries
	suggestions.EstimatedMemMB = out.EstimatedMemMB
	suggestions.Reasoning = out.Reasoning
	return suggestions
}

// stateHistoryAdapter adapts checkpoint.StateBackend to driver.TuningHistoryProvider.
type stateHistoryAdapter struct {
	state checkpoint.StateBackend
}

// GetRuntimeAdjustments returns recent runtime adjustments from migrations.
func (a *stateHistoryAdapter) GetRuntimeAdjustments(limit int) ([]checkpoint.RuntimeAdjustmentRecord, error) {
	return a.state.GetRuntimeAdjustments(limit)
}

// GetTuningHistory returns recent tuning recommendations from analyze.
func (a *stateHistoryAdapter) GetTuningHistory(limit int, sourceType, targetType string) ([]checkpoint.TuningRecord, error) {
	return a.state.GetTuningHistory(limit, sourceType, targetType)
}

// SaveTuningRecord saves a tuning recommendation for future reference.
func (a *stateHistoryAdapter) SaveTuningRecord(record checkpoint.TuningRecord) error {
	return a.state.SaveTuningRecord(record)
}

// UpdateTuningResult updates the most recent tuning record with final migration throughput.
func (a *stateHistoryAdapter) UpdateTuningResult(throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error {
	return a.state.UpdateTuningResult(throughput, durationSecs, chunkRetryCount, adjustedAtRuntime)
}

// AnalyzeConfig analyzes the source database and emits tuning
// recommendations. The flow is deterministic — smartconfig (#175/#179)
// produces parameter suggestions from regression + history, and
// dbtuning (#172) emits per-DB recommendations from a hardcoded
// catalog. No AI is required.
func (o *Orchestrator) AnalyzeConfig(ctx context.Context, schema string) (*driver.SmartConfigSuggestions, error) {
	// Target-only mode: provide limited analysis (no schema, but still tuning recommendations)
	if o.sourcePool == nil && o.targetPool != nil {
		logging.Debug("Analyzing target database only (source unavailable)...")

		suggestions := &driver.SmartConfigSuggestions{}

		// Apply sensible defaults based on system resources
		o.applySystemDefaults(suggestions)

		// Add target database tuning recommendations
		o.addDatabaseTuningRecommendations(ctx, suggestions)

		return suggestions, nil
	}

	// Source is required for full schema analysis
	if o.sourcePool == nil {
		return nil, fmt.Errorf("source database connection required for analysis")
	}

	logging.Debug("Analyzing source database for configuration suggestions...")

	// Create the smart config analyzer
	analyzer := driver.NewSmartConfigAnalyzer(o.sourcePool.DB(), o.sourcePool.DBType())

	// Set up history provider using the state backend for learning from past completed migrations.
	if o.state != nil {
		analyzer.SetHistoryProvider(&stateHistoryAdapter{state: o.state})
	}

	// Set target database type for more accurate recommendations
	if o.targetPool != nil {
		analyzer.SetTargetDBType(o.targetPool.DBType())

		// Probe target for runtime values that affect chunk_size
		// selection (#166). MySQL surfaces @@max_allowed_packet here;
		// PG and MSSQL return empty probes. Failures degrade
		// gracefully — analyzer falls back to the static
		// HardChunkLimit (0 today on all drivers).
		if td, err := driver.Get(o.targetPool.DBType()); err == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
			analyzer.SetTargetProbe(td.ProbeTarget(probeCtx, o.targetPool.DB()))
			probeCancel()
		}
	}

	// Capture effective DB tuning for regime classification (#144). Without
	// this the analyze path's prompt has no current-tuning baseline and every
	// trajectory row's regime collapses to same_regime regardless of tuning
	// differences.
	if o.targetPool != nil {
		tuning := captureDBTuning(ctx, o.sourcePool.DB(), o.targetPool.DB(),
			o.sourcePool.DBType(), o.targetPool.DBType())
		analyzer.SetCurrentTuning(driver.DBTuningSnapshot{
			TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
			TargetSyncCommit:        tuning.TargetSyncCommit,
			TargetFsync:             tuning.TargetFsync,
			TargetFullPageWrites:    tuning.TargetFullPageWrites,
			TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
			TargetWALLevel:          tuning.TargetWALLevel,
			SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
		})
	}

	// Wire workload identity (#215) so analyze can read the same
	// identity-scoped history that migration runs persist.
	//
	// The source schema comes from the `schema` parameter that the
	// caller passed to Analyze — that's what actually gets analyzed,
	// so it's the right identity value. Fall back to cfg.Source.Schema
	// only when the caller passed an empty
	// string (Copilot review on PR #223).
	sourceSchema := schema
	if sourceSchema == "" {
		sourceSchema = o.config.Source.Schema
	}
	analyzer.SetWorkloadIdentity(
		o.config.Source.Host, o.config.Source.Port,
		o.config.Source.Database, sourceSchema,
		o.config.Target.Host, o.config.Target.Port,
		o.config.Target.Database, o.config.Target.Schema,
	)

	// Run analysis
	suggestions, err := analyzer.Analyze(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("analyzing config: %w", err)
	}

	// Analyze is advisory. Do not persist synthetic rows to
	// ai_tuning_history; that table is training data from completed
	// migrations with real throughput, retry, and duration measurements.

	// Add database tuning recommendations using the same AI mapper
	o.addDatabaseTuningRecommendations(ctx, suggestions)

	return suggestions, nil
}

// applySystemDefaults applies sensible defaults based on system resources.
func (o *Orchestrator) applySystemDefaults(suggestions *driver.SmartConfigSuggestions) {
	cores := runtime.NumCPU()

	// Workers: CPU cores minus 2 for OS, minimum 2
	workers := cores - 2
	if workers < 2 {
		workers = 2
	}

	suggestions.Workers = workers
	suggestions.ChunkSizeRecommendation = 50000
	suggestions.ReadAheadBuffers = 4
	suggestions.WriteAheadWriters = 2
	suggestions.ParallelReaders = 2
	suggestions.MaxPartitions = workers
	suggestions.LargeTableThreshold = 1000000
	suggestions.MaxSourceConnections = workers + 4
	suggestions.MaxTargetConnections = workers*2 + 4
	suggestions.UpsertMergeChunkSize = 5000
	suggestions.CheckpointFrequency = 20
	suggestions.MaxRetries = 3
}

// addDatabaseTuningRecommendations adds source and target database
// tuning recommendations from the deterministic catalog (#172).
// Previously this required an AI mapper; the catalog is now hardcoded
// per driver and no AI is consulted, so the call always runs.
func (o *Orchestrator) addDatabaseTuningRecommendations(ctx context.Context, suggestions *driver.SmartConfigSuggestions) {
	// Schema statistics for recommendations
	stats := dbtuning.SchemaStatistics{
		TotalTables:     suggestions.TotalTables,
		TotalRows:       suggestions.TotalRows,
		AvgRowSizeBytes: suggestions.AvgRowSizeBytes,
		EstimatedMemMB:  suggestions.EstimatedMemMB,
	}

	// HostMemoryMB describes the DB *server's* RAM, not dmt's host.
	// Only pass dmt's host RAM when the DB connection is local;
	// otherwise rules that size memory from it would emit recommendations
	// against the wrong machine (Codex review on #172).
	dmtHost := dbtuning.DetectSystemInfo()
	sourceSys := dbtuning.SystemInfo{CPUCores: dmtHost.CPUCores}
	targetSys := dbtuning.SystemInfo{CPUCores: dmtHost.CPUCores}
	if isLocalDBHost(o.config.Source.Host) {
		sourceSys.HostMemoryMB = dmtHost.HostMemoryMB
	}
	if isLocalDBHost(o.config.Target.Host) {
		targetSys.HostMemoryMB = dmtHost.HostMemoryMB
	}

	// Run source and target analysis concurrently for better performance
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Analyze source database tuning (concurrent)
	if o.sourcePool != nil && o.sourcePool.DB() != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Independent timeout for source analysis
			sourceCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			logging.Debug("Analyzing source database configuration...")
			sourceTuning, err := dbtuning.Analyze(
				sourceCtx,
				o.sourcePool.DB(),
				o.sourcePool.DBType(),
				"source",
				stats,
				sourceSys,
			)

			mu.Lock()
			if err != nil {
				logging.Warn("Failed to analyze source database tuning: %v", err)
				// Set fallback tuning so output is consistent
				suggestions.SourceTuning = &dbtuning.DatabaseTuning{
					DatabaseType:    o.sourcePool.DBType(),
					Role:            "source",
					TuningPotential: "unknown",
					EstimatedImpact: fmt.Sprintf("Analysis failed: %v", err),
				}
			} else {
				suggestions.SourceTuning = sourceTuning
				if sourceTuning.TuningPotential != "unknown" {
					logging.Info("Source tuning: %s potential (%s)", sourceTuning.TuningPotential, sourceTuning.EstimatedImpact)
				}
			}
			mu.Unlock()
		}()
	} else {
		// No source pool available
		suggestions.SourceTuning = &dbtuning.DatabaseTuning{
			DatabaseType:    "unknown",
			Role:            "source",
			TuningPotential: "unknown",
			EstimatedImpact: "Source database not available for analysis",
		}
	}

	// Analyze target database tuning using AI-driven approach (concurrent)
	if o.targetPool != nil && o.targetPool.DB() != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Independent timeout for target analysis
			targetCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			logging.Debug("Analyzing target database configuration...")
			targetTuning, err := dbtuning.Analyze(
				targetCtx,
				o.targetPool.DB(),
				o.targetPool.DBType(),
				"target",
				stats,
				targetSys,
			)

			mu.Lock()
			if err != nil {
				logging.Warn("Failed to analyze target database tuning: %v", err)
				// Set fallback tuning so output is consistent
				suggestions.TargetTuning = &dbtuning.DatabaseTuning{
					DatabaseType:    o.targetPool.DBType(),
					Role:            "target",
					TuningPotential: "unknown",
					EstimatedImpact: fmt.Sprintf("Analysis failed: %v", err),
				}
			} else {
				suggestions.TargetTuning = targetTuning
				if targetTuning.TuningPotential != "unknown" {
					logging.Info("Target tuning: %s potential (%s)", targetTuning.TuningPotential, targetTuning.EstimatedImpact)
				}
			}
			mu.Unlock()
		}()
	} else {
		// No target pool available
		suggestions.TargetTuning = &dbtuning.DatabaseTuning{
			DatabaseType:    "unknown",
			Role:            "target",
			TuningPotential: "unknown",
			EstimatedImpact: "Target database not available for analysis",
		}
	}

	// Wait for all analyses to complete before returning
	wg.Wait()
}

// isLocalDBHost reports whether the given config host string points at
// the same machine dmt is running on. Used to decide whether dmt's
// host RAM (gopsutil) is a meaningful proxy for the DB server's RAM
// for sizing recommendations. Conservative — when in doubt, returns
// false (resulting in no RAM-based recommendation rather than a
// confidently wrong one).
func isLocalDBHost(host string) bool {
	if host == "" {
		// Empty host typically means "connect via socket / pipe", which
		// requires the DB to be local. Treat as local.
		return true
	}
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	if h, err := os.Hostname(); err == nil && strings.EqualFold(host, h) {
		return true
	}
	return false
}
