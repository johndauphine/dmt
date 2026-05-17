package orchestrator

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

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
