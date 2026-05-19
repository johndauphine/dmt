package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/source"
)

// Run executes a new migration.
func (o *Orchestrator) Run(ctx context.Context) (runErr error) {
	// Use provided run ID or generate a new one
	runID := o.opts.RunID
	if runID == "" {
		runID = uuid.New().String()[:8]
	}
	startTime := time.Now()

	// #229 observability: stamp run_id + source/target DB type + an
	// initial phase ("starting") onto every log line, metric label,
	// and trace span for the rest of this run. Without the initial
	// phase value, the Info("Starting migration run...") lines below
	// would log with `phase=<missing>` until the first setPhase call
	// (Copilot review). Cleared via defer so process-resident state
	// (TUI, tests) doesn't leak attributes from a prior run into the next.
	logging.WithFields(map[string]any{
		"run_id":    runID,
		"source_db": o.sourcePool.DBType(),
		"target_db": o.targetPool.DBType(),
		"phase":     "starting",
	})
	o.metrics.RunStarted(runID, o.sourcePool.DBType(), o.targetPool.DBType())
	// Clear any AI-fallback counts left over from a prior run in the
	// same process (TUI, sidecar) so this run's `dmt status` numbers
	// reflect *this* run only. Prometheus counters are intentionally
	// monotonic and are not cleared here (#176).
	observability.ResetFallbackState()
	// Register a checkpoint-backed sink so RecordFallback writes also
	// land in the state file. Cross-process `dmt status` reads from
	// the state file, so without persistence the Airflow polling case
	// (separate-process status) would always see empty (#176, codex
	// review).
	observability.SetFallbackSink(newFallbackSink(o.state, runID))
	defer observability.SetFallbackSink(nil)
	defer logging.ClearBaseAttrs()
	defer o.metrics.RunComplete(runID)

	// One root span per run; phase spans below attach as children (#229).
	// No-op when OTLP isn't configured.
	runCtx, runSpan := observability.Tracer().StartSpan(ctx, "dmt.run",
		"run_id", runID, "source_db", o.sourcePool.DBType(), "target_db", o.targetPool.DBType())
	o.traceCtx = runCtx
	defer func() {
		o.endPhaseSpan()
		runSpan.End()
		o.traceCtx = nil
	}()

	// #235 audit log: per-run immutable NDJSON. Set up before any other
	// run-side work so a panic in the early phases is still recorded.
	// The deferred close emits run_complete (with final status pulled
	// from a captured named return value below) and chmods the file
	// to 0444. Compliance auditors get a self-describing file on
	// every exit path — success, partial, error, even a panic.
	o.openAuditor(runID, false /*resume*/)
	defer func() {
		rec := recover()
		status, errStr, resumable := classifyRunOutcome(runErr, rec)
		o.auditEvent("run_complete", map[string]any{
			"status":      status,
			"error":       errStr,
			"duration_ms": time.Since(startTime).Milliseconds(),
		})
		// Resumable interruption (Ctrl-C / context cancel) MUST leave
		// the audit file writable so the eventual `dmt resume` can
		// append its own resume_start / resume_complete events.
		// CloseResumable() skips the chmod-0444 step; a successful or
		// hard-failed run gets the usual lockdown (Codex review on #235).
		if resumable {
			if err := o.auditor.CloseResumable(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		} else {
			if err := o.auditor.Close(); err != nil {
				logging.Warn("audit close: %v", err)
			}
		}
		if rec != nil {
			// re-raise after the audit event has been written to the
			// OS (tamper-evident mode additionally fsyncs); the OS may
			// still hold dirty pages but the audit record is no longer
			// in dmt's process memory only.
			panic(rec)
		}
	}()
	o.auditEvent("run_start", map[string]any{
		"operator":    operatorLabel(),
		"dmt_version": versionString(),
		"source": map[string]any{
			"driver":   o.sourcePool.DBType(),
			"host":     o.config.Source.Host,
			"database": o.config.Source.Database,
			"schema":   o.config.Source.Schema,
		},
		"target": map[string]any{
			"driver":   o.targetPool.DBType(),
			"host":     o.config.Target.Host,
			"database": o.config.Target.Database,
			"schema":   o.config.Target.Schema,
		},
		"config_hash": computeConfigHash(o.config),
	})

	logging.Info("Starting migration run: %s", runID)
	logging.Info("Migration: %s -> %s", o.sourcePool.DBType(), o.targetPool.DBType())

	// Log comprehensive configuration dump
	logging.Debug("%s", o.config.DebugDump())

	// Set runtime memory limit using Go's soft limit mechanism
	// This tells the GC to work harder to stay under the limit
	effectiveMemMB := o.config.AutoConfig().EffectiveMaxMemoryMB
	if effectiveMemMB > 0 {
		memLimitBytes := effectiveMemMB * 1024 * 1024
		debug.SetMemoryLimit(memLimitBytes)
		logging.Debug("Runtime memory limit set to %d MB (Go GC soft limit)", effectiveMemMB)
	}

	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}

	// Preflight (phase 0): verify the environment satisfies the assumptions
	// downstream phases make — privileges, version floor, encoding, connection
	// headroom. Fail loud and early rather than minutes into a partial run
	// (#228). Run AFTER CreateRun so the failed-preflight outcome shows up
	// in the run history; the operator can grep `dmt history` for it later.
	o.setPhase("preflight")
	logging.Debug("Running preflight checks...")
	if err := o.runPreFlight(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Extract schema
	o.setPhase("extracting_schema")
	logging.Debug("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Load source metadata for drift reporting and post-transfer DDL.
	o.loadSchemaMetadata(ctx, tables)

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(runID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}
	schemaDriftReport, err := o.reportSchemaDrift(tables, true)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	o.tables = tables
	o.progress.SetTablesTotal(len(tables))
	logging.Debug("Found %d tables", len(tables))

	// Apply AI-recommended parameters (if AI is available)
	o.applyAITuning(ctx)

	// Refine memory settings based on actual row sizes from database stats
	tableRowSizes := make([]config.TableRowSize, len(tables))
	for i, t := range tables {
		tableRowSizes[i] = config.TableRowSize{
			Name:             t.Name,
			RowCount:         t.RowCount,
			EstimatedRowSize: t.EstimatedRowSize,
		}
	}
	if adjusted, changes := o.config.RefineSettingsForRowSizes(tableRowSizes); adjusted {
		logging.Info("%s", changes)
	} else if changes != "" {
		logging.Debug("%s", changes)
	}

	// Persist the post-tuning config so `dmt history --run <id>` reflects what actually ran.
	if err := o.state.UpdateRunConfig(runID, o.config.Sanitized()); err != nil {
		logging.Warn("failed to persist post-tuning config: %v", err)
	}

	// Print pagination strategy summary
	keysetCount := 0
	rowNumberCount := 0
	for _, t := range tables {
		if t.SupportsKeysetPagination() {
			keysetCount++
		} else if t.HasPK() {
			rowNumberCount++
		}
	}
	logging.Debug("Pagination: %d keyset, %d ROW_NUMBER, %d no PK",
		keysetCount, rowNumberCount, len(tables)-keysetCount-rowNumberCount)

	// Send start notification
	if o.config.Migration.NotifyOnStart() {
		o.notifier.MigrationStarted(runID, o.config.Source.Database, o.config.Target.Database, len(tables))
	}

	// Create target schema and tables
	o.setPhase("creating_tables")
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	// Prepare target tables using the appropriate strategy
	if err := o.targetMode.PrepareTables(ctx, tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	if o.shouldApplySchemaEvolution(schemaDriftReport) {
		o.setPhase("schema_evolution")
		o.state.UpdatePhase(runID, "schema_evolution")
		if err := o.applySchemaEvolution(ctx, schemaDriftReport, tables); err != nil {
			o.state.CompleteRun(runID, "failed", err.Error())
			o.notifyFailure(runID, err, time.Since(startTime))
			return err
		}
	}

	// Transfer data
	o.setPhase("transfer")
	logging.Debug("Transferring data...")
	o.state.UpdatePhase(runID, "transferring")
	transferStart := time.Now()
	tableFailures, err := o.transferAll(ctx, runID, tables, false)
	transferDuration := time.Since(transferStart)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(runID) // Reset running tasks to pending
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("transferring data: %w", err)
	}

	// Log summary of table failures (individual failures already logged)
	if len(tableFailures) > 0 {
		logging.Warn("%d table(s) failed during transfer:", len(tableFailures))
		for _, f := range tableFailures {
			logging.Warn("  - %s: %v", f.TableName, f.Error)
		}
	}

	// Filter out failed tables for finalize/validate
	failedTableNames := make(map[string]bool)
	for _, f := range tableFailures {
		failedTableNames[f.TableName] = true
	}
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (only for successful tables)
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	o.state.UpdatePhase(runID, "finalizing")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}

	if err := o.reconcileDeletesIfDue(ctx, runID, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("delete reconciliation: %w", err)
	}

	// Validate (only for successful tables)
	o.setPhase("validating")
	logging.Debug("Validating...")
	o.state.UpdatePhase(runID, "validating")
	o.tables = successTables // Update for validation
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Debug("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Calculate stats for notification
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range successTables {
		totalRows += t.RowCount
	}
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	partialErr := false
	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		o.state.CompleteRun(runID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifyCompletionWithErrors(runID, startTime, duration,
			len(successTables), len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Migration completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			len(successTables), len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		o.state.CompleteRun(runID, "success", "")
		o.captureSchemaSnapshots(runID, tables)
		o.notifyCompletion(runID, startTime, duration, len(tables), totalRows, throughput)
		logging.Info("Migration complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tables), totalRows, duration.Round(time.Second), throughput)
		o.auditEvent("validation_complete", map[string]any{
			"tables":     len(tables),
			"rows_total": totalRows,
		})
	}

	// Record transfer-only throughput in AI tuning history for future learning
	// transferDuration captured right after transferAll returns (excludes schema
	// extraction, DDL creation, finalization, and validation)
	transferDurationSecs := transferDuration.Seconds()
	var transferThroughput float64
	if transferDurationSecs > 0 {
		transferThroughput = float64(totalRows) / transferDurationSecs
	}
	if err := o.state.UpdateAITuningResult(transferThroughput, transferDurationSecs, o.lastChunkRetryCount); err != nil {
		logging.Debug("Failed to update AI tuning result: %v", err)
	}

	// Log identifier changes for PostgreSQL targets
	if o.config.Target.Type == "postgres" {
		o.logPGIdentifierChanges(tables)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
}
