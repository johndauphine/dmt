package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/orchestrator/schemaevolution"
	"time"
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

	leaseBackend, lease, err := o.acquireMigrationLease(nil)
	if err != nil {
		return fmt.Errorf("acquiring migration lease: %w", err)
	}
	if err := o.state.CreateRun(runID, o.config.Source.Schema, o.config.Target.Schema, o.config.Sanitized(), o.runProfile, o.runConfig); err != nil {
		return releaseUnboundMigrationLease(leaseBackend, lease, fmt.Errorf("creating run: %w", err))
	}
	if err := bindMigrationLease(leaseBackend, runID, lease); err != nil {
		return releaseUnboundMigrationLease(leaseBackend, lease, err)
	}
	ownedCtx, leaseSession, err := o.startMigrationLease(ctx, leaseBackend, lease, runID)
	if err != nil {
		return releaseUnboundMigrationLease(leaseBackend, lease, err)
	}
	ctx = ownedCtx
	defer func() {
		runErr = mergeLeaseSessionError(runErr, leaseSession)
	}()

	// Preflight (phase 0): verify the environment satisfies the assumptions
	// downstream phases make — privileges, version floor, encoding, connection
	// headroom. Fail loud and early rather than minutes into a partial run
	// (#228). Run AFTER CreateRun so the failed-preflight outcome shows up
	// in the run history; the operator can grep `dmt history` for it later.
	o.setPhase("preflight")
	logging.Debug("Running preflight checks...")
	if err := o.runPreFlight(ctx, false); err != nil {
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
	tables, err = o.effectiveTablesForSchemaEvolution(schemaDriftReport, tables)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	o.tables = tables
	o.progress.SetTablesTotal(len(tables))
	logging.Debug("Found %d tables", len(tables))

	// Fail before any DDL if source identifiers collide under PostgreSQL
	// sanitization — otherwise drop_recreate would silently destroy a
	// colliding table's data (#553).
	if err := o.enforcePGIdentifierCollisionGate(tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Apply AI-recommended parameters (if AI is available)
	o.applyTuning(ctx)

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
	tupleCount := 0
	rowNumberCount := 0
	for _, t := range tables {
		switch {
		case t.SupportsKeysetPagination():
			keysetCount++
		case driver.TupleKeysetRoutable(&t, o.sourcePool.DBType()):
			tupleCount++
		case t.HasPK():
			rowNumberCount++
		}
	}
	logging.Debug("Pagination: %d keyset, %d tuple keyset, %d ROW_NUMBER, %d no PK",
		keysetCount, tupleCount, rowNumberCount, len(tables)-keysetCount-tupleCount-rowNumberCount)

	// Send start notification
	if o.config.Migration.NotifyOnStart() {
		o.notifier.MigrationStarted(runID, o.config.Source.Database, o.config.Target.Database, len(tables))
	}

	// Durable task creation is part of the transfer protocol. Build every
	// job before the first target mutation so a checkpoint failure cannot
	// leave dropped/truncated target tables without resumable tasks (#645).
	buildResult, err := o.buildTransferJobs(ctx, runID, tables)
	if err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Create target schema and tables
	o.setPhase("creating_tables")
	if err := o.targetPool.CreateSchema(ctx, o.config.Target.Schema); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("creating schema: %w", err)
	}

	if err := o.applySchemaContractTableEvolution(ctx, schemaDriftReport, tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	// Prepare target tables using the appropriate strategy
	if err := o.targetMode.PrepareTables(ctx, tables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return err
	}

	transferSchemaDriftReport := schemaevolution.FilterDriftReportForTables(schemaDriftReport, tables)
	if o.shouldApplySchemaEvolution(transferSchemaDriftReport) {
		o.setPhase("schema_evolution")
		o.state.UpdatePhase(runID, "schema_evolution")
		if err := o.applySchemaEvolution(ctx, transferSchemaDriftReport, tables); err != nil {
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
	tableFailures, err := o.transferAll(ctx, runID, buildResult, tables, false)
	transferDuration := time.Since(transferStart)
	if err != nil {
		// A required checkpoint failure is an interrupted durability
		// transition, not a terminal data failure. Leave the run incomplete so
		// the operator can repair state storage and resume safely (#645).
		if checkpoint.IsRequiredWriteError(err) {
			o.notifyFailure(runID, err, time.Since(startTime))
			return fmt.Errorf("transferring data: %w", err)
		}
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if stateErr := o.markRunAsResumedRequired(runID); stateErr != nil {
				return fmt.Errorf("transferring data: %w", errors.Join(err, stateErr))
			}
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
	successTables := finalizableTables(tables, failedTableNames)

	// Finalize (only for successful tables)
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	o.state.UpdatePhase(runID, "finalizing")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(runID, "failed", err.Error())
		o.notifyFailure(runID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}
	o.finalizeSchemaContractTableEvolution(ctx, schemaDriftReport, successTables)

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

	// Calculate stats for notification. Use checkpointed rows-done
	// (the same source as the run-summary box), not the source-side
	// RowCount estimates — those come from stats-based fast counts and
	// under-report (#498: the mysql estimate was 768K rows short). The
	// progress tracker is not suitable either: it re-counts replayed
	// chunks on retry.
	duration := time.Since(startTime)
	totalRows, rowsErr := o.transferredRowsFromState(runID)
	if rowsErr != nil || totalRows == 0 {
		// State unavailable (or nothing transferred this run) — fall
		// back to the per-table estimates rather than reporting zero.
		totalRows = 0
		for _, t := range successTables {
			totalRows += t.RowCount
		}
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
		if err := o.completePartialRunRequired(runID, fmt.Sprintf("%d tables failed", len(tableFailures))); err != nil {
			o.notifyFailure(runID, err, time.Since(startTime))
			return err
		}
		o.notifyCompletionWithErrors(runID, startTime, duration,
			len(successTables), len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Migration completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			len(successTables), len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		if err := o.completeRunRequired(runID, "success", ""); err != nil {
			o.notifyFailure(runID, err, time.Since(startTime))
			return err
		}
		o.captureSchemaSnapshotsForReport(runID, schemaDriftReport, tables)
		o.notifyCompletion(runID, startTime, duration, len(tables), totalRows, throughput)
		logging.Info("Migration complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tables), totalRows, duration.Round(time.Second), throughput)
		o.auditEvent("validation_complete", map[string]any{
			"tables":     len(tables),
			"rows_total": totalRows,
		})
		// Record transfer-only throughput in tuning history only for full
		// success. Partial runs can have misleading early throughput and should
		// not steer future smartconfig recommendations.
		o.recordSuccessfulTuningResult(totalRows, transferDuration)
	}

	// Log identifier changes for PostgreSQL targets
	if o.targetIsPostgres() {
		o.logPGIdentifierChanges(tables)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
}

const (
	defaultRunHeartbeatInterval = 30 * time.Second
	defaultRunHeartbeatTTL      = 15 * time.Minute
)

func (o *Orchestrator) runHeartbeatInterval() time.Duration {
	if o.opts.RunHeartbeatInterval > 0 {
		return o.opts.RunHeartbeatInterval
	}
	return defaultRunHeartbeatInterval
}

func (o *Orchestrator) runHeartbeatTTL() time.Duration {
	if o.opts.RunHeartbeatTTL > 0 {
		return o.opts.RunHeartbeatTTL
	}
	return defaultRunHeartbeatTTL
}

func (o *Orchestrator) validateResumeHeartbeat(run *checkpoint.Run, now time.Time) error {
	if run == nil {
		return nil
	}
	// A partial status is a completed attempt with a durable, recoverable
	// outcome. Its heartbeat is historical, not evidence of a live writer; the
	// target lease remains the authoritative ownership check. The stale/fresh
	// heartbeat guard exists for interrupted status=running rows only.
	if run.Status != "" && run.Status != "running" {
		return nil
	}

	lastHeartbeat := run.LastHeartbeat
	if lastHeartbeat.IsZero() {
		lastHeartbeat = run.StartedAt
	}
	ttl := o.runHeartbeatTTL()
	age := now.Sub(lastHeartbeat)
	// A pre-lease process cannot be fenced by a new generation because it does
	// not know how to verify one. Never attach to a legacy run whose heartbeat
	// still indicates a live writer, even with --force-resume.
	if run.LeaseGeneration == 0 && age <= ttl {
		return fmt.Errorf("incomplete legacy run %s still has a fresh heartbeat: last heartbeat %s (%s ago, TTL %s); wait for the heartbeat to become stale and verify the original process has stopped before resuming",
			run.ID,
			lastHeartbeat.UTC().Format(time.RFC3339),
			age.Round(time.Second),
			ttl.Round(time.Second))
	}
	if o.opts.ForceResume {
		return nil
	}
	if age <= ttl {
		return nil
	}

	return fmt.Errorf("incomplete run %s has a stale heartbeat: last heartbeat %s (%s ago, TTL %s). Verify no migration process is still running, then use --force-resume to override",
		run.ID,
		lastHeartbeat.UTC().Format(time.RFC3339),
		age.Round(time.Second),
		ttl.Round(time.Second))
}
