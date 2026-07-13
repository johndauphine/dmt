package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/orchestrator/schemaevolution"
	"github.com/johndauphine/dmt/internal/source"
	"reflect"
	"time"
)

// abandonResumeAttempt records a pre-transfer resume failure but leaves the run
// resumable — it does NOT mark the run 'failed'. Reserved for environmental or
// transient failures (preflight, schema extraction) the operator is expected to
// fix and retry; marking the run terminal/non-resumable would orphan all
// checkpointed progress (#566, #643).
func (o *Orchestrator) abandonResumeAttempt(runID string, err error, startTime time.Time) {
	o.notifyFailure(runID, err, time.Since(startTime))
}

// Resume continues an interrupted migration
func (o *Orchestrator) Resume(ctx context.Context) (resumeErr error) {
	// Restore the pre-safety nominal chunk before comparing the current config
	// with the run snapshot. applyTuning repeats this as an idempotent backstop,
	// but resume compatibility is decided before tuning begins.
	o.config.BeginRuntimeChunkSizeProjection()

	leaseState, err := o.migrationLeaseBackend()
	if err != nil {
		return err
	}
	run, err := leaseState.GetLastIncompleteRunForTarget(o.migrationTarget())
	if err != nil {
		return fmt.Errorf("finding incomplete run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("no incomplete run found - use 'run' to start a new migration")
	}
	if err := o.validateResumeHeartbeat(run, time.Now()); err != nil {
		return err
	}
	leaseBackend, lease, err := o.acquireMigrationLease(run)
	if err != nil {
		return fmt.Errorf("acquiring migration lease for run %s: %w", run.ID, err)
	}
	if err := bindMigrationLease(leaseBackend, run.ID, lease); err != nil {
		return releaseUnboundMigrationLease(leaseBackend, lease, err)
	}
	ownedCtx, leaseSession, err := o.startMigrationLease(ctx, leaseBackend, lease, run.ID)
	if err != nil {
		return releaseUnboundMigrationLease(leaseBackend, lease, err)
	}
	ctx = ownedCtx
	o.validationRunID = run.ID
	defer func() { o.validationRunID = "" }()
	defer func() {
		resumeErr = mergeLeaseSessionError(resumeErr, leaseSession)
	}()

	// Check if this incomplete run has been superseded by a later successful run
	superseded, err := o.state.HasSuccessfulRunAfter(run)
	if err != nil {
		return fmt.Errorf("checking for superseding runs: %w", err)
	}
	if superseded {
		// Mark the old incomplete run as failed since it's obsolete
		o.state.CompleteRun(run.ID, "failed", "superseded by later successful migration")
		return fmt.Errorf("incomplete run %s is obsolete - a later migration with the same schemas completed successfully. Use 'run' to start a new migration", run.ID)
	}

	// Validate config hash if stored (prevents resuming with different config).
	// configVerified is true only when a stored hash is present AND matches the
	// current config — positive proof that the table set / target mode we're
	// about to run is the one that created the target. It gates the
	// backup-acknowledgment suppression below (#623): a missing hash (legacy
	// run) or a --force-resume past a mismatch can't prove ownership of the
	// current target tables, so those resumes keep the gate.
	configVerified := false
	if run.ConfigHash != "" {
		currentHash := computeConfigHash(o.config)
		if run.ConfigHash == currentHash {
			configVerified = true
		} else {
			if !o.opts.ForceResume {
				return fmt.Errorf("config changed since run started (hash %s != %s), use --force-resume to override",
					run.ConfigHash, currentHash)
			}
			warnings, err := validateForceResumeConfigCompatibility(run, o.config)
			if err != nil {
				return err
			}
			logging.Warn("--force-resume overriding config hash mismatch for run %s (stored %s != current %s)",
				run.ID, run.ConfigHash, currentHash)
			for _, warning := range warnings {
				logging.Warn("--force-resume config drift: %s", warning)
			}
		}
	}

	startTime := time.Now()

	// #229 observability: same per-run attribute decoration as Run(),
	// including the initial "starting" phase so early log lines carry
	// a phase attr (Copilot review). resume=true differentiates the
	// two flows in log queries.
	logging.WithFields(map[string]any{
		"run_id":    run.ID,
		"source_db": o.sourcePool.DBType(),
		"target_db": o.targetPool.DBType(),
		"resume":    true,
		"phase":     "starting",
	})
	o.metrics.RunStarted(run.ID, o.sourcePool.DBType(), o.targetPool.DBType())
	// Same per-process counter reset as Run() (#176). A resume is a new
	// status window — counts from the original Run() call live in the
	// Prometheus counter and audit log, not in the per-process map.
	observability.ResetFallbackState()
	observability.SetFallbackSink(newFallbackSink(o.state, run.ID))
	defer observability.SetFallbackSink(nil)
	defer logging.ClearBaseAttrs()
	defer o.metrics.RunComplete(run.ID)

	// One root span per Resume(), same shape as Run() but with resume=true.
	runCtx, runSpan := observability.Tracer().StartSpan(ctx, "dmt.resume",
		"run_id", run.ID, "source_db", o.sourcePool.DBType(), "target_db", o.targetPool.DBType(), "resume", true)
	o.traceCtx = runCtx
	defer func() {
		o.endPhaseSpan()
		runSpan.End()
		o.traceCtx = nil
	}()

	// #235 audit log: resume runs append to the same audit file the
	// original Run() opened (same run_id). The audit-dir code path is
	// idempotent — if the file is 0444 from a Close() in the earlier
	// crash, OpenFile fails and the auditor degrades to disabled. We
	// log a warning but don't fail the resume — compliance is best-
	// effort here; the original Run()'s record remains intact.
	o.openAuditor(run.ID, true /*resume*/)
	defer func() {
		rec := recover()
		status, errStr, resumable := classifyRunOutcome(resumeErr, rec)
		o.auditEvent("resume_complete", map[string]any{
			"status":      status,
			"error":       errStr,
			"duration_ms": time.Since(startTime).Milliseconds(),
		})
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
			panic(rec)
		}
	}()
	o.auditEvent("resume_start", map[string]any{
		"operator":            operatorLabel(),
		"dmt_version":         versionString(),
		"original_started_at": run.StartedAt.UTC().Format(time.RFC3339),
	})

	logging.Info("Resuming run: %s (started %s)", run.ID, run.StartedAt.Format(time.RFC3339))

	// Preflight (phase 0) — same gate as Run(). A resume can fail if the
	// environment changed between runs (privileges revoked, version
	// downgraded, target unreachable). Operator can opt out per-check with
	// --skip-preflight if they know what they're doing. #228.
	o.setPhase("preflight")
	logging.Debug("Running preflight checks...")
	// Suppress the backup-acknowledgment gate only when the config is verified
	// AND this run created the target tables (reached transfer) — then the
	// target's contents are this run's own output and resuming is the
	// acknowledgment (#623). A run killed before transfer, an unverified/legacy
	// config, or a drifted --force-resume still faces the gate and can't silently
	// drop_recreate over pre-existing, unacknowledged target data. And even when
	// the gate does fire on a resume, its remedy now names --skip-preflight
	// backup, so the operator is never dead-ended on a flag `resume` lacks.
	resumeOwnsTarget := configVerified && o.runReachedTransfer(run.ID)
	if err := o.runPreFlight(ctx, resumeOwnsTarget); err != nil {
		// A pre-transfer preflight failure is environmental (target
		// unreachable, privileges revoked, connection headroom held by
		// another process) — the operator is expected to fix it and retry.
		// Leave the run resumable rather than marking it failed (#566).
		o.abandonResumeAttempt(run.ID, err, startTime)
		return err
	}

	// Reset any running tasks to pending
	if err := o.markRunAsResumedRequired(run.ID); err != nil {
		return err
	}

	// Extract schema (needed to know all tables)
	logging.Debug("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		// Pre-transfer schema extraction failure is typically transient
		// (source briefly unreachable). Keep the run resumable rather than
		// marking it failed and orphaning checkpointed progress (#566).
		o.abandonResumeAttempt(run.ID, err, startTime)
		return fmt.Errorf("extracting schema: %w", err)
	}
	o.loadSchemaMetadata(ctx, tables)

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(run.ID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}
	schemaDriftReport, err := o.reportSchemaDrift(tables, false)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}
	tables, err = o.effectiveTablesForSchemaEvolution(schemaDriftReport, tables)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	o.tables = tables
	logging.Debug("Found %d tables in source", len(tables))

	// Fail before touching the target if source identifiers collide under
	// PostgreSQL sanitization (#553) — same hard gate as the initial run.
	if err := o.enforcePGIdentifierCollisionGate(tables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Apply AI-recommended parameters (if AI is available)
	o.applyTuning(ctx)

	// Persist the post-tuning config so `dmt history --run <id>` reflects what actually ran.
	if err := o.state.UpdateRunConfig(run.ID, o.config.Sanitized()); err != nil {
		logging.Warn("failed to persist post-tuning config: %v", err)
	}

	// Get tables that were successfully transferred in the previous run
	completedTables, err := o.state.GetCompletedTables(run.ID)
	if err != nil {
		return fmt.Errorf("getting completed tables: %w", err)
	}

	// Check target row counts to determine which tables need re-transfer.
	// The selection helper is deliberately isolated so the partial-run contract
	// can prove a successful table is skipped while a failed peer is scheduled.
	tablesToTransfer, skippedTables := selectResumeTables(
		tables,
		completedTables,
		func(table source.Table) string {
			return checkpoint.TransferTaskKeyForBackend(o.state, checkpoint.TransferTaskIdentity{
				Schema: table.Schema,
				Table:  table.Name,
			})
		},
		func(table source.Table) (int64, error) {
			return o.targetPool.GetRowCount(ctx, o.config.Target.Schema, table.Name)
		},
	)

	if len(skippedTables) > 0 {
		logging.Debug("Skipping %d already-complete tables: %v", len(skippedTables), skippedTables)
	}

	// A SQL Server snapshot can survive a crash after the last table completed
	// but before finalization marked the run successful. Reattach it even when
	// no tables remain so this process owns and drops the surviving snapshot on
	// every completion/error path. A missing snapshot still fails closed.
	strictEpoch, err := o.beginMigrationSnapshotEpoch(ctx, run.ID, true)
	if err != nil {
		o.abandonResumeAttempt(run.ID, err, startTime)
		return err
	}
	if strictEpoch != nil {
		defer func() {
			strictEpoch.Close()
			logging.Info("strict_consistency migration snapshot epoch released after %s", strictEpoch.Age())
		}()
	}

	if len(tablesToTransfer) == 0 {
		logging.Info("All tables already transferred - completing migration")
		o.tables = tables // Use all tables for finalize/validate

		// Finalize
		o.setPhase("finalizing")
		logging.Debug("Finalizing...")
		if err := o.targetMode.Finalize(ctx, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("finalizing: %w", err)
		}
		o.finalizeSchemaContractTableEvolution(ctx, schemaDriftReport, tables)

		if err := o.reconcileDeletesIfDue(ctx, run.ID, tables); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return fmt.Errorf("delete reconciliation: %w", err)
		}

		// Validate
		o.setPhase("validating")
		logging.Debug("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		if err := o.completeRunRequired(run.ID, "success", ""); err != nil {
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}
		o.captureSchemaSnapshotsForReport(run.ID, schemaDriftReport, tables)
		logging.Info("Resume complete!")
		return nil
	}

	logging.Debug("Resuming transfer of %d tables", len(tablesToTransfer))
	// Build and durably persist all tasks before target preparation, which may
	// truncate or create tables. A state failure must leave the target untouched.
	buildResult, err := o.buildTransferJobs(ctx, run.ID, tablesToTransfer, snapshotPlanningPool(o.sourcePool, strictEpoch))
	if err != nil {
		o.abandonResumeAttempt(run.ID, err, startTime)
		return err
	}
	if err := o.resetResumeTableTasksRequired(buildResult, tablesToTransfer); err != nil {
		o.abandonResumeAttempt(run.ID, err, startTime)
		return err
	}

	// For tables that need transfer, ensure target tables exist
	// Check for chunk-level progress to avoid unnecessary truncation
	progressSaver := checkpoint.NewProgressSaver(o.state)
	for _, t := range tablesToTransfer {
		taskID, idOK := buildResult.TableTaskIDs[t.Name]
		taskKey, keyOK := buildResult.TableTaskKeys[t.Name]
		if !idOK || !keyOK || taskID <= 0 || taskKey == "" {
			err := fmt.Errorf("missing durable table task for %s", t.FullName())
			o.abandonResumeAttempt(run.ID, err, startTime)
			return err
		}
		if err := o.prepareResumeTargetTable(ctx, run.ID, t, taskID, taskKey, schemaDriftReport, progressSaver); err != nil {
			// Target preparation runs before any data is moved this resume and
			// touches the target (row counts, truncate, create table/PK). A
			// cancellation (Ctrl+C) or transient failure here must leave the run
			// resumable — marking it failed would orphan all checkpointed
			// progress, and it's asymmetric with the same interruption handled
			// during transfer below (#566). Tasks were already reset to pending
			// by MarkRunAsResumed above.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logging.Info("Migration interrupted during target preparation - run 'resume' to continue")
				return err
			}
			o.abandonResumeAttempt(run.ID, err, startTime)
			return err
		}
	}

	// Transfer only the incomplete tables. Capture the checkpointed row count
	// BEFORE this resume moves anything so the summary/tuning throughput counts
	// only rows moved this resume, not the cumulative full-migration total that
	// persists in transfer_progress from the original run (#565).
	rowsBeforeResume, _ := o.transferredRowsFromState(run.ID)
	o.setPhase("transfer")
	logging.Debug("Transferring data...")
	transferStart := time.Now()
	tableFailures, err := o.transferAll(ctx, run.ID, buildResult, tablesToTransfer, true, strictEpoch)
	transferDuration := time.Since(transferStart)
	if err != nil {
		if checkpoint.IsRequiredWriteError(err) {
			o.abandonResumeAttempt(run.ID, err, startTime)
			return fmt.Errorf("transferring data: %w", err)
		}
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if stateErr := o.markRunAsResumedRequired(run.ID); stateErr != nil {
				return fmt.Errorf("transferring data: %w", errors.Join(err, stateErr))
			}
			logging.Info("Migration interrupted - run 'resume' to continue")
			return fmt.Errorf("transferring data: %w", err)
		}
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
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

	// Finalize (uses successful tables for constraints)
	o.tables = successTables
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
	}
	o.finalizeSchemaContractTableEvolution(ctx, schemaDriftReport, successTables)

	if err := o.reconcileDeletesIfDue(ctx, run.ID, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("delete reconciliation: %w", err)
	}

	// Validate successful tables
	o.setPhase("validating")
	logging.Debug("Validating...")
	if err := o.Validate(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Sample validation if enabled
	if o.config.Migration.SampleValidation {
		logging.Debug("Running sample validation...")
		if err := o.validateSamples(ctx); err != nil {
			logging.Warn("Warning: sample validation failed: %v", err)
		}
	}

	// Count rows actually transferred this resume from checkpoint state (the
	// same source as the run-summary box), not the source-side RowCount
	// estimates — those are stats-based, under-report (#498), and count each
	// table's FULL size even when the resume only moved a remaining fraction,
	// skewing the throughput persisted into ai_tuning_history (#565). Mirror
	// Run()'s #498 handling exactly.
	duration := time.Since(startTime)
	totalRows := o.summaryRowsTransferred(run.ID, rowsBeforeResume, func() int64 {
		// Fallback only: state unavailable or no progress recorded this resume.
		var est int64
		for _, t := range tablesToTransfer {
			if !failedTableNames[t.Name] {
				est += t.RowCount
			}
		}
		return est
	})
	throughput := float64(totalRows) / duration.Seconds()

	// Determine final status and send appropriate notification
	successCount := 0
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			successCount++
		}
	}

	partialErr := false
	if len(tableFailures) > 0 {
		// Partial success
		failureNames := make([]string, len(tableFailures))
		for i, f := range tableFailures {
			failureNames[i] = f.TableName
		}
		if err := o.completePartialRunRequired(run.ID, fmt.Sprintf("%d tables failed", len(tableFailures))); err != nil {
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}
		o.notifyCompletionWithErrors(run.ID, startTime, duration,
			successCount, len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Resume completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			successCount, len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		if err := o.completeRunRequired(run.ID, "success", ""); err != nil {
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}
		o.captureSchemaSnapshotsForReport(run.ID, schemaDriftReport, tables)
		o.notifyCompletion(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
		logging.Info("Resume complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tablesToTransfer), totalRows, duration.Round(time.Second), throughput)
		// Record transfer-only throughput in tuning history only for a clean
		// resume. Partial resumes can overstate early throughput and should not
		// become training examples.
		o.recordSuccessfulTuningResult(totalRows, transferDuration)
	}

	// Log identifier changes for PostgreSQL targets
	if o.targetIsPostgres() {
		o.logPGIdentifierChanges(tablesToTransfer)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
}

func selectResumeTables(
	tables []source.Table,
	completedTables map[string]bool,
	taskKey func(source.Table) string,
	targetRowCount func(source.Table) (int64, error),
) (toTransfer []source.Table, skipped []string) {
	for _, table := range tables {
		if completedTables[taskKey(table)] {
			count, err := targetRowCount(table)
			if err == nil && count == table.RowCount {
				skipped = append(skipped, table.Name)
				continue
			}
		}
		toTransfer = append(toTransfer, table)
	}
	return toTransfer, skipped
}

// AbandonResume explicitly removes the newest recoverable run for the
// configured target from automatic resume selection. It acquires and binds the
// same target lease as Run/Resume, so a live owner cannot be abandoned out from
// underneath its data-path writes.
func (o *Orchestrator) AbandonResume(reason string) (*checkpoint.Run, error) {
	leaseState, err := o.migrationLeaseBackend()
	if err != nil {
		return nil, err
	}
	run, err := leaseState.GetLastIncompleteRunForTarget(o.migrationTarget())
	if err != nil {
		return nil, fmt.Errorf("finding resumable run: %w", err)
	}
	if run == nil {
		return nil, fmt.Errorf("no resumable run found for the configured target")
	}
	leaseBackend, lease, err := o.acquireMigrationLease(run)
	if err != nil {
		return nil, fmt.Errorf("acquiring migration lease for run %s: %w", run.ID, err)
	}
	if err := bindMigrationLease(leaseBackend, run.ID, lease); err != nil {
		return nil, releaseUnboundMigrationLease(leaseBackend, lease, err)
	}
	if err := o.state.AbandonRun(run.ID, reason); err != nil {
		return nil, releaseUnboundMigrationLease(
			leaseBackend,
			lease,
			checkpoint.RequiredWrite(fmt.Sprintf("abandoning run %s", run.ID), err),
		)
	}
	if err := leaseBackend.ReleaseMigrationLease(lease); err != nil {
		return nil, checkpoint.RequiredWrite("releasing migration lease", err)
	}
	abandoned, err := o.state.GetRunByID(run.ID)
	if err != nil {
		return nil, fmt.Errorf("reading abandoned run %s: %w", run.ID, err)
	}
	return abandoned, nil
}

func (o *Orchestrator) resetResumeTableTasksRequired(buildResult *BuildResult, tables []source.Table) error {
	// Validate the complete plan before writing any resets. All resets still
	// happen before target preparation, so a later failure leaves target state
	// untouched and every successful reset safely pending.
	for _, table := range tables {
		taskID, idOK := buildResult.TableTaskIDs[table.Name]
		taskKey, keyOK := buildResult.TableTaskKeys[table.Name]
		if !idOK || !keyOK || taskID <= 0 || taskKey == "" {
			return checkpoint.RequiredWrite(
				fmt.Sprintf("validating durable table task for %s", table.FullName()),
				fmt.Errorf("missing planned aggregate task"),
			)
		}
	}
	for _, table := range tables {
		taskID := buildResult.TableTaskIDs[table.Name]
		if err := o.state.UpdateTaskStatus(taskID, "pending", ""); err != nil {
			return checkpoint.RequiredWrite(fmt.Sprintf("resetting aggregate task %d for %s to pending", taskID, table.FullName()), err)
		}
	}
	return nil
}

func (o *Orchestrator) prepareResumeTargetTable(
	ctx context.Context,
	runID string,
	t source.Table,
	taskID int64,
	taskKey string,
	schemaDriftReport drift.Report,
	progressSaver *checkpoint.ProgressSaver,
) error {
	exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("checking table %s: %w", t.Name, err)
	}
	if !exists {
		if err := validateResumeMissingTargetTable(t, o.config.Migration, schemaDriftReport); err != nil {
			return err
		}
		// Table doesn't exist - clear any stale progress before creating it.
		// If cleanup fails, leaving the target missing is safer than creating
		// an empty target beside stale partition checkpoints (#266).
		if err := o.clearResumeProgress(runID, taskKey, taskID, t.Name); err != nil {
			return err
		}
		if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
			return fmt.Errorf("creating table %s: %w", t.Name, err)
		}
		// Idempotent-INSERT-on-resume depends on the PK existing on the
		// target. AI-generated CREATE TABLE DDL usually includes the PK
		// inline, but CreatePrimaryKey is idempotent (no-op if PK exists)
		// so call it defensively when re-creating a missing table on resume.
		if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
			return fmt.Errorf("ensuring PK on %s: %w", t.Name, err)
		}
		return nil
	}

	// Table exists - check if we have saved chunk progress.
	lastPK, rowsDone, _, err := progressSaver.GetProgress(taskID)
	if err != nil {
		return fmt.Errorf("getting progress for %s: %w", t.Name, err)
	}

	// Match the partitioning decision made in job_builder.go:
	// large + HasPK is partitioned (keyset for single-int PK,
	// ROW_NUMBER otherwise).
	isPartitioned := t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasPK()
	expectedRows, hasProgress, err := o.expectedResumeRows(
		runID, taskKey, isPartitioned, lastPK, rowsDone,
	)
	if err != nil {
		return err
	}

	if !hasProgress {
		if isPartitioned || o.config.Migration.TargetMode == "upsert" {
			return nil
		}
		// No chunk progress - truncate to ensure clean re-transfer.
		if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
			return fmt.Errorf("truncating table %s: %w", t.Name, err)
		}
		return nil
	}

	// Have saved progress - verify target row count matches it. In upsert mode
	// rowsDone can include updates and target-only retained rows are valid, so
	// a smaller target count is not proof that a destructive restart is safe.
	targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("getting row count for %s: %w", t.Name, err)
	}
	if targetCount < expectedRows && o.config.Migration.TargetMode != "upsert" {
		logging.Warn("  Warning: %s has %d rows but expected %d - restarting transfer",
			t.Name, targetCount, expectedRows)
		if err := o.clearResumeProgress(runID, taskKey, taskID, t.Name); err != nil {
			return err
		}
		if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
			return fmt.Errorf("truncating table %s: %w", t.Name, err)
		}
	}
	// If target has >= expectedRows, resume from saved progress.
	return nil
}

func validateResumeMissingTargetTable(table source.Table, migration config.MigrationConfig, report drift.Report) error {
	if migration.TargetMode == "upsert" && !table.HasPK() {
		return fmt.Errorf(
			"upsert resume cannot create missing target table %s: source table has no primary key",
			table.FullName(),
		)
	}
	if !migration.SchemaContractEnabled() ||
		migration.SchemaContractTablesMode() == config.SchemaContractEvolve ||
		!schemaevolution.TableAddedInReport(report, table) {
		return nil
	}
	return fmt.Errorf(
		"schema contract tables=%s will not create added table %s during resume",
		migration.SchemaContractTablesMode(),
		table.FullName(),
	)
}

func (o *Orchestrator) clearResumeProgress(runID, taskKey string, taskID int64, tableName string) error {
	var err error
	if identity, ok := checkpoint.ParseTransferTaskKey(taskKey); ok {
		err = checkpoint.ClearTransferPartitionProgress(o.state, runID, identity.Schema, identity.Table)
	} else {
		err = o.state.ClearPartitionTransferProgress(runID, taskKey)
	}
	if err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("clearing partition progress for %s", tableName), err)
	}
	if err := o.state.ClearTransferProgress(taskID); err != nil {
		return checkpoint.RequiredWrite(fmt.Sprintf("clearing transfer progress for %s task %d", tableName, taskID), err)
	}
	return nil
}

func (o *Orchestrator) expectedResumeRows(
	runID, taskKey string,
	isPartitioned bool,
	tableLastPK any,
	tableRowsDone int64,
) (int64, bool, error) {
	expectedRows := tableRowsDone
	hasProgress := tableLastPK != nil

	if !isPartitioned {
		return expectedRows, hasProgress, nil
	}

	var summary checkpoint.PartitionProgressSummary
	var err error
	if identity, ok := checkpoint.ParseTransferTaskKey(taskKey); ok {
		summary, err = checkpoint.GetTransferPartitionProgressSummary(o.state, runID, identity.Schema, identity.Table)
	} else {
		summary, err = o.state.GetPartitionTransferProgressSummary(runID, taskKey)
	}
	if err != nil {
		return 0, false, fmt.Errorf("getting partition progress for %s: %w", taskKey, err)
	}
	if !summary.HasProgress() {
		return expectedRows, hasProgress, nil
	}
	if !hasProgress || summary.RowsDone > expectedRows {
		expectedRows = summary.RowsDone
	}
	return expectedRows, true, nil
}

func validateForceResumeConfigCompatibility(run *checkpoint.Run, current *config.Config) ([]string, error) {
	if run == nil || current == nil {
		return nil, nil
	}

	var warnings []string
	var forbidden []string

	addForbidden := func(field string, oldValue, newValue any) {
		if !reflect.DeepEqual(oldValue, newValue) {
			forbidden = append(forbidden, fmt.Sprintf("%s changed from %v to %v", field, oldValue, newValue))
		}
	}
	addWarning := func(field string, oldValue, newValue any) {
		if !reflect.DeepEqual(oldValue, newValue) {
			warnings = append(warnings, fmt.Sprintf("%s changed from %v to %v", field, oldValue, newValue))
		}
	}

	// These schema fields are always available on both SQLite and file state,
	// even when the backend cannot persist the full config snapshot.
	addForbidden("source.schema", run.SourceSchema, current.Source.Schema)
	addForbidden("target.schema", run.TargetSchema, current.Target.Schema)

	if run.Config == "" {
		if len(forbidden) > 0 {
			return warnings, forceResumeCompatibilityError(run.ID, forbidden)
		}
		return append(warnings, "stored config snapshot unavailable; only source/target schema drift could be checked"), nil
	}

	var original config.Config
	if err := json.Unmarshal([]byte(run.Config), &original); err != nil {
		return warnings, fmt.Errorf("cannot validate --force-resume config compatibility for run %s: stored config snapshot is invalid: %w", run.ID, err)
	}

	addForbidden("source.type", original.Source.Type, current.Source.Type)
	addForbidden("source.host", original.Source.Host, current.Source.Host)
	addForbidden("source.port", original.Source.Port, current.Source.Port)
	addForbidden("source.database", original.Source.Database, current.Source.Database)
	addForbidden("source.schema", original.Source.Schema, current.Source.Schema)
	addForbidden("source.user", original.Source.User, current.Source.User)

	addForbidden("target.type", original.Target.Type, current.Target.Type)
	addForbidden("target.host", original.Target.Host, current.Target.Host)
	addForbidden("target.port", original.Target.Port, current.Target.Port)
	addForbidden("target.database", original.Target.Database, current.Target.Database)
	addForbidden("target.schema", original.Target.Schema, current.Target.Schema)
	addForbidden("target.user", original.Target.User, current.Target.User)

	addForbidden("migration.target_mode", original.Migration.TargetMode, current.Migration.TargetMode)

	addWarning("migration.include_tables", original.Migration.IncludeTables, current.Migration.IncludeTables)
	addWarning("migration.exclude_tables", original.Migration.ExcludeTables, current.Migration.ExcludeTables)
	addWarning("migration.chunk_size", original.Migration.ChunkSize, current.Migration.ChunkSize)
	addWarning("migration.max_partitions", original.Migration.MaxPartitions, current.Migration.MaxPartitions)
	addWarning("migration.large_table_threshold", original.Migration.LargeTableThreshold, current.Migration.LargeTableThreshold)
	addWarning("migration.parallel_readers", original.Migration.ParallelReaders, current.Migration.ParallelReaders)
	addWarning("source.chunk_size", original.Source.ChunkSize, current.Source.ChunkSize)
	addWarning("target.chunk_size", original.Target.ChunkSize, current.Target.ChunkSize)
	addWarning("migration.date_updated_columns", original.Migration.DateUpdatedColumns, current.Migration.DateUpdatedColumns)

	if len(forbidden) > 0 {
		return warnings, forceResumeCompatibilityError(run.ID, forbidden)
	}
	return warnings, nil
}

func forceResumeCompatibilityError(runID string, changes []string) error {
	return fmt.Errorf("--force-resume refused for run %s because incompatible config fields changed: %v. Start a new run or restore the original config", runID, changes)
}
