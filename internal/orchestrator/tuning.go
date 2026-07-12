package orchestrator

import (
	"context"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"strings"
	"time"
)

// applyTuning runs the deterministic tuner (internal/tuning) to set
// migration parameters before transfer begins. Only overrides
// formula-computed values (where Original* == 0), never user-specified
// values. Function name kept for now; rename pending in a follow-up
// cleanup (#175 PR1 minimized public-API churn). Falls back to baseline
// on any failure (logged at Debug/Warn level).
func (o *Orchestrator) applyTuning(ctx context.Context) {
	// An Orchestrator may be reused for resume segments. Never let a failed
	// or manual analysis inherit a row or tier created by an earlier segment.
	o.lastTuningRowID = 0
	o.lastTuningTier = ""

	// Coarse switch (#461): migration.tuning: manual disables pre-run
	// parameter derivation entirely — user values and formula defaults
	// rule. Distinct from migration.runtime_tuning, which controls the
	// mid-run rule-based controller.
	if o.config.Migration.Tuning == "manual" {
		logging.Info("Tuning disabled (migration.tuning: manual) — using configured values and formula defaults")
		logging.Info("Tuning provenance: %s", o.config.TuningProvenanceSummary())
		return
	}

	logging.Debug("Running parameter tuning...")

	// One line of parameter ownership (#461), deferred so it fires on
	// every exit path — including the fallback-to-formula-defaults
	// return when analysis fails (codex review). Pinned values silently
	// disable tuning for that knob; surface them at INFO on every run
	// instead of burying provenance in debug output.
	defer func() {
		logging.Info("Tuning provenance: %s", o.config.TuningProvenanceSummary())
	}()

	// Create analyzer (same pattern as AnalyzeConfig in healthcheck.go).
	analyzer := driver.NewSmartConfigAnalyzer(o.sourcePool.DB(), o.sourcePool.DBType())
	envelope := o.config.AutoConfig().MemoryEnvelope
	analyzer.SetMemoryEnvelope(envelope.CapacityMB, envelope.AvailableMB, envelope.BudgetMB)

	// Scope the analyzer to the post-filter table set (#241). Both Run
	// and Resume have already applied include/exclude filters into
	// o.tables by the time we get here; pinning the same scope on the
	// analyzer keeps the packet cap and memory-budget math aligned with
	// what the run will actually transfer. Without this, an excluded
	// wide table still drives the global @@max_allowed_packet cap and
	// clamps chunk_size for the narrow tables that DO ship.
	if names := tableNamesForTuning(o.tables); len(names) > 0 {
		analyzer.SetTableNameFilter(names)
	}

	// Set up history provider for learning from past runs
	if o.state != nil {
		analyzer.SetHistoryProvider(&stateHistoryAdapter{state: o.state})
	}

	// Override-cost advice (#461): tell the tuner which WAW value is
	// pinned so it can compare the pin against measured history bins.
	for _, name := range o.config.PinnedTunables() {
		switch name {
		case config.TunableWriteAheadWriters:
			analyzer.SetPinnedWriteAheadWriters(o.config.Migration.WriteAheadWriters)
		case config.TunableParallelReaders:
			analyzer.SetPinnedParallelReaders(o.config.Migration.ParallelReaders)
		case config.TunableReadAheadBuffers:
			analyzer.SetPinnedReadAheadBuffers(o.config.Migration.ReadAheadBuffers)
		}
	}

	// Set target DB type and migration mode for cross-engine awareness
	if o.targetPool != nil {
		analyzer.SetTargetDBType(o.targetPool.DBType())

		// Probe target for runtime values that affect chunk_size
		// selection (#166). MySQL surfaces @@max_allowed_packet here
		// so the tuner's HardChunkLimit reflects the live cap rather
		// than the static 0; without this the migration path could
		// pick a chunk_size that exceeds the packet limit and crash
		// mid-transfer. PG/MSSQL return empty probes.
		if td, err := driver.Get(o.targetPool.DBType()); err == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
			analyzer.SetTargetProbe(td.ProbeTarget(probeCtx, o.targetPool.DB()))
			probeCancel()
		}
	}
	analyzer.SetTargetMode(o.config.Migration.TargetMode)

	// Capture the probe-derived hard cap so transfer_runner can carry it
	// into the runtime controller. Without this, runtime growth rules can
	// push chunk_size above the packet limit and crash mid-migration
	// (Codex review on #166). Set after Analyze runs (below) so the
	// uncapped row size is populated.
	defer func() {
		if o.targetPool != nil {
			o.config.Migration.TargetHardChunkLimit = analyzer.TargetHardChunkLimit()
		}
	}()

	// Wire exploration policy (#179): --explore flag forces a planned-grid
	// pick this run; ExploreMode controls steady-state ε strength.
	analyzer.SetExploration(o.config.Migration.Explore, o.config.Migration.ExploreMode)

	// Wire workload identity (#215). Together these form the tuple the
	// Tier 1 exact-identity classifier uses to find historically-
	// comparable runs. Values come verbatim from cfg.Source / cfg.Target —
	// the user wrote them, the user understands them. Stored without
	// normalization so the SQL equality match is what the user expects.
	analyzer.SetWorkloadIdentity(
		o.config.Source.Host, o.config.Source.Port,
		o.config.Source.Database, o.config.Source.Schema,
		o.config.Target.Host, o.config.Target.Port,
		o.config.Target.Database, o.config.Target.Schema,
	)

	// Capture effective DB tuning at run start (#144). Used for both:
	//   (a) smartconfig prompt regime classification (per trajectory row)
	//   (b) persisting on the saved TuningRecord so future runs can
	//       classify against THIS run.
	// Best-effort: failures yield zero-valued fields, which the render path
	// and classifier treat as "unknown".
	tuning := captureDBTuning(ctx, o.sourcePool.DB(), o.targetPool.DB(),
		o.config.Source.Type, o.config.Target.Type)
	analyzer.SetCurrentTuning(driver.DBTuningSnapshot{
		TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
		TargetSyncCommit:        tuning.TargetSyncCommit,
		TargetFsync:             tuning.TargetFsync,
		TargetFullPageWrites:    tuning.TargetFullPageWrites,
		TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
		TargetWALLevel:          tuning.TargetWALLevel,
		SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
	})

	// Deterministic tuning is fast (no network round-trip). 30s is generous
	// for the SQL probing inside Analyze (getTables + per-table date-column
	// detection); leave headroom for slow source DBs.
	analyzeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	suggestions, err := analyzer.Analyze(analyzeCtx, o.config.Source.Schema)
	if err != nil {
		logging.Warn("Tuning analysis failed, using formula defaults: %v", err)
		return
	}
	o.lastTuningTier = suggestions.Tier

	// Apply suggestions only where user didn't specify values
	changes := o.config.ApplyTunerSuggestions(suggestions)
	if len(changes) > 0 {
		logging.Info("Tuning applied %d parameter(s):", len(changes))
		for _, c := range changes {
			logging.Info("  %s: %d -> %d", c.Name, c.OldValue, c.NewValue)
		}
	} else {
		logging.Info("Tuning: no changes (recommendation matches current config)")
	}
	// Always log the tuner's reasoning. The deterministic tuner
	// guarantees a non-empty Reasoning + Tier on every run (#202 — silence
	// hid which tier picked when the value matched the baseline anchor).
	// sanitizeForLog guards against multi-line content breaking log parsers.
	logging.Info("Tuning reasoning [%s]: %s",
		suggestions.Tier, sanitizeForLog(suggestions.Reasoning))

	// Measured override-cost findings (#461) — one INFO line each so a
	// pinned knob that history says is costing throughput is visible on
	// every run, next to the provenance line that shows the pin.
	for _, advice := range suggestions.PinnedAdvice {
		logging.Info("Tuning override advice: %s", sanitizeForLog(advice))
	}

	// Save tuning history with actual params used (after user overrides).
	// `tuning` was captured earlier so the smartconfig prompt could classify
	// against it; same snapshot is persisted here so future runs see this
	// run's effective tuning when classifying their own trajectory rows.
	o.lastTuningRowID = analyzer.SaveTuningWithActualParams(driver.ActualParams{
		Workers:           o.config.Migration.Workers,
		ChunkSize:         o.config.Migration.ChunkSize,
		ReadAheadBuffers:  o.config.Migration.ReadAheadBuffers,
		WriteAheadWriters: o.config.Migration.WriteAheadWriters,
		ParallelReaders:   o.config.Migration.ParallelReaders,
		MaxPartitions:     o.config.Migration.MaxPartitions,
		// #144 regime fields. Platform comes from gopsutil-detected runtime
		// (already in AutoTuneInput); the DB-tuning fields come from
		// captureDBTuning above.
		TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
		TargetSyncCommit:        tuning.TargetSyncCommit,
		TargetFsync:             tuning.TargetFsync,
		TargetFullPageWrites:    tuning.TargetFullPageWrites,
		TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
		TargetWALLevel:          tuning.TargetWALLevel,
		SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
	})
}

// sanitizeForLog flattens AI-supplied strings to a single line before logging.
// Newlines, carriage returns, and tabs become spaces; runs of whitespace
// collapse. Without this, multi-paragraph reasoning from the model would
// produce log entries where the second+ lines lack timestamps/levels and
// break log parsers (issue #143 / PR #151 review).
func sanitizeForLog(s string) string {
	r := strings.NewReplacer("\n", " ", "\r", " ", "\t", " ")
	flat := r.Replace(s)
	// Collapse repeated spaces.
	for strings.Contains(flat, "  ") {
		flat = strings.ReplaceAll(flat, "  ", " ")
	}
	return strings.TrimSpace(flat)
}

func (o *Orchestrator) recordSuccessfulTuningResult(totalRows int64, transferDuration time.Duration) {
	if o.state == nil {
		return
	}
	// No row means tuning was manual, analysis or persistence failed, or the
	// backend does not persist tuning history. Never guess a global row.
	if o.lastTuningRowID == 0 {
		return
	}
	transferDurationSecs := transferDuration.Seconds()
	if transferDurationSecs <= 0 {
		return
	}

	transferThroughput := float64(totalRows) / transferDurationSecs
	if err := o.state.UpdateTuningResult(o.lastTuningRowID, transferThroughput, transferDurationSecs, o.lastChunkRetryCount, o.lastRunAdjusted); err != nil {
		logging.Debug("Failed to update AI tuning result: %v", err)
	}
}
