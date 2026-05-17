package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/source"
)

// Resume continues an interrupted migration
func (o *Orchestrator) Resume(ctx context.Context) (resumeErr error) {
	run, err := o.state.GetLastIncompleteRun()
	if err != nil {
		return fmt.Errorf("finding incomplete run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("no incomplete run found - use 'run' to start a new migration")
	}

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

	// Validate config hash if stored (prevents resuming with different config)
	if run.ConfigHash != "" && !o.opts.ForceResume {
		currentHash := computeConfigHash(o.config)
		if run.ConfigHash != currentHash {
			return fmt.Errorf("config changed since run started (hash %s != %s), use --force-resume to override",
				run.ConfigHash, currentHash)
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
	if err := o.runPreFlight(ctx); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return err
	}

	// Reset any running tasks to pending
	if err := o.state.MarkRunAsResumed(run.ID); err != nil {
		return fmt.Errorf("resetting tasks: %w", err)
	}

	// Extract schema (needed to know all tables)
	logging.Debug("Extracting schema...")
	tables, err := o.sourcePool.ExtractSchema(ctx, o.config.Source.Schema)
	if err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("extracting schema: %w", err)
	}

	// Apply table filters
	tables = o.filterTables(tables)
	if len(tables) == 0 {
		o.state.CompleteRun(run.ID, "failed", "no tables to migrate after applying filters")
		return fmt.Errorf("no tables to migrate after applying filters")
	}

	o.tables = tables
	logging.Debug("Found %d tables in source", len(tables))

	// Apply AI-recommended parameters (if AI is available)
	o.applyAITuning(ctx)

	// Persist the post-tuning config so `dmt history --run <id>` reflects what actually ran.
	if err := o.state.UpdateRunConfig(run.ID, o.config.Sanitized()); err != nil {
		logging.Warn("failed to persist post-tuning config: %v", err)
	}

	// Get tables that were successfully transferred in the previous run
	completedTables, err := o.state.GetCompletedTables(run.ID)
	if err != nil {
		return fmt.Errorf("getting completed tables: %w", err)
	}

	// Check target row counts to determine which tables need re-transfer
	var tablesToTransfer []source.Table
	var skippedTables []string

	for _, t := range tables {
		// Check if table was marked complete AND has correct row count
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		if completedTables[taskKey] {
			// Verify row count matches
			targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
			if err == nil && targetCount == t.RowCount {
				skippedTables = append(skippedTables, t.Name)
				continue
			}
		}
		tablesToTransfer = append(tablesToTransfer, t)
	}

	if len(skippedTables) > 0 {
		logging.Debug("Skipping %d already-complete tables: %v", len(skippedTables), skippedTables)
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

		// Validate
		o.setPhase("validating")
		logging.Debug("Validating...")
		if err := o.Validate(ctx); err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			o.notifyFailure(run.ID, err, time.Since(startTime))
			return err
		}

		o.state.CompleteRun(run.ID, "success", "")
		duration := time.Since(startTime)
		if err := o.state.UpdateAITuningResult(0, duration.Seconds(), o.lastChunkRetryCount); err != nil {
			logging.Debug("Failed to update AI tuning result: %v", err)
		}
		logging.Info("Resume complete!")
		return nil
	}

	logging.Debug("Resuming transfer of %d tables", len(tablesToTransfer))

	// For tables that need transfer, ensure target tables exist
	// Check for chunk-level progress to avoid unnecessary truncation
	progressSaver := checkpoint.NewProgressSaver(o.state)
	for _, t := range tablesToTransfer {
		taskKey := fmt.Sprintf("transfer:%s.%s", t.Schema, t.Name)
		taskID, _ := o.state.CreateTask(run.ID, "transfer", taskKey)

		exists, err := o.targetPool.TableExists(ctx, o.config.Target.Schema, t.Name)
		if err != nil {
			o.state.CompleteRun(run.ID, "failed", err.Error())
			return fmt.Errorf("checking table %s: %w", t.Name, err)
		}
		if !exists {
			// Table doesn't exist - clear any stale progress before creating it.
			// If cleanup fails, leaving the target missing is safer than creating
			// an empty target beside stale partition checkpoints (#266).
			if err := o.clearResumeProgress(run.ID, taskKey, taskID, t.Name); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return err
			}
			if err := o.targetPool.CreateTable(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("creating table %s: %w", t.Name, err)
			}
			// Idempotent-INSERT-on-resume depends on the PK existing on the
			// target. AI-generated CREATE TABLE DDL usually includes the PK
			// inline, but CreatePrimaryKey is idempotent (no-op if PK exists)
			// so call it defensively when re-creating a missing table on resume.
			if err := o.targetPool.CreatePrimaryKey(ctx, &t, o.config.Target.Schema); err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("ensuring PK on %s: %w", t.Name, err)
			}
		} else {
			// Table exists - check if we have saved chunk progress
			lastPK, rowsDone, err := progressSaver.GetProgress(taskID)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("getting progress for %s: %w", t.Name, err)
			}

			// Match the partitioning decision made in job_builder.go:
			// large + HasPK is partitioned (keyset for single-int PK,
			// ROW_NUMBER otherwise).
			isPartitioned := t.IsLarge(o.config.Migration.LargeTableThreshold) && t.HasPK()
			expectedRows, hasProgress, err := o.expectedResumeRows(
				run.ID, taskKey, isPartitioned, lastPK, rowsDone,
			)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return err
			}

			if !hasProgress {
				if isPartitioned {
					continue
				}
				// No chunk progress - truncate to ensure clean re-transfer.
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
				continue
			}

			// Have saved progress - verify target row count matches it.
			// If target has fewer rows than saved progress, data was lost;
			// clear all table + partition progress and start fresh.
			targetCount, err := o.targetPool.GetRowCount(ctx, o.config.Target.Schema, t.Name)
			if err != nil {
				o.state.CompleteRun(run.ID, "failed", err.Error())
				return fmt.Errorf("getting row count for %s: %w", t.Name, err)
			}
			if targetCount < expectedRows {
				logging.Warn("  Warning: %s has %d rows but expected %d - restarting transfer",
					t.Name, targetCount, expectedRows)
				if err := o.clearResumeProgress(run.ID, taskKey, taskID, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return err
				}
				if err := o.targetPool.TruncateTable(ctx, o.config.Target.Schema, t.Name); err != nil {
					o.state.CompleteRun(run.ID, "failed", err.Error())
					return fmt.Errorf("truncating table %s: %w", t.Name, err)
				}
			}
			// If target has >= expectedRows, resume from saved progress.
		}
	}

	// Transfer only the incomplete tables
	o.setPhase("transfer")
	logging.Debug("Transferring data...")
	transferStart := time.Now()
	tableFailures, err := o.transferAll(ctx, run.ID, tablesToTransfer, true)
	transferDuration := time.Since(transferStart)
	if err != nil {
		// If context was canceled (Ctrl+C), leave run as "running" so resume works
		// but reset any "running" tasks to "pending" so status shows correctly
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			o.state.MarkRunAsResumed(run.ID) // Reset running tasks to pending
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
	var successTables []source.Table
	for _, t := range tables {
		if !failedTableNames[t.Name] {
			successTables = append(successTables, t)
		}
	}

	// Finalize (uses successful tables for constraints)
	o.tables = successTables
	o.setPhase("finalizing")
	logging.Debug("Finalizing...")
	if err := o.targetMode.Finalize(ctx, successTables); err != nil {
		o.state.CompleteRun(run.ID, "failed", err.Error())
		o.notifyFailure(run.ID, err, time.Since(startTime))
		return fmt.Errorf("finalizing: %w", err)
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

	// Calculate stats - only count rows from tables we attempted to transfer that succeeded
	duration := time.Since(startTime)
	var totalRows int64
	for _, t := range tablesToTransfer {
		if !failedTableNames[t.Name] {
			totalRows += t.RowCount
		}
	}
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
		o.state.CompleteRun(run.ID, "partial", fmt.Sprintf("%d tables failed", len(tableFailures)))
		o.notifier.MigrationCompletedWithErrors(run.ID, startTime, duration,
			successCount, len(tableFailures), totalRows, throughput, failureNames)
		logging.Warn("Resume completed with errors: %d tables succeeded, %d tables failed, %d rows in %s (%.0f rows/sec)",
			successCount, len(tableFailures), totalRows, duration.Round(time.Second), throughput)
		partialErr = !o.config.Migration.AllowPartial
	} else {
		// Full success
		o.state.CompleteRun(run.ID, "success", "")
		o.notifier.MigrationCompleted(run.ID, startTime, duration, len(tablesToTransfer), totalRows, throughput)
		logging.Info("Resume complete: %d tables, %d rows in %s (%.0f rows/sec)",
			len(tablesToTransfer), totalRows, duration.Round(time.Second), throughput)
	}

	// Record transfer-only throughput in AI tuning history for future learning
	// transferDuration captured right after transferAll returns
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
		o.logPGIdentifierChanges(tablesToTransfer)
	}

	if partialErr {
		return &PartialMigrationError{Failed: tableFailures}
	}
	return nil
}
