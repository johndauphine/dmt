package transfer

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/target"
)

// Execute runs a transfer job using the optimal pagination strategy.
// If tuner is non-nil, runtime parameters (writer count, checkpoint frequency,
// upsert merge chunk size) are read dynamically and writer scaling is applied
// at chunk boundaries.
func Execute(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	prog *progress.Tracker,
	tuner RuntimeTuner,
	writeErrorAdjuster ...WriteErrorAdjuster,
) (*TransferStats, error) {
	// Extract optional AI adjuster
	var adjuster WriteErrorAdjuster
	if len(writeErrorAdjuster) > 0 {
		adjuster = writeErrorAdjuster[0]
	}

	// A strict snapshot is table-scoped. Partition jobs would each open their
	// own transaction and therefore could not represent one stable table view.
	// JobBuilder prevents newly planned strict jobs from being partitioned, and
	// this runtime guard protects legacy checkpoints and direct callers too.
	if cfg.Migration.StrictConsistency && job.Partition != nil {
		return nil, fmt.Errorf("strict_consistency does not support partitioned jobs for %s; rebuild the run with one unpartitioned job per table", job.Table.FullName())
	}

	// Track table start/end for accurate progress display
	prog.StartTable(job.Table.Name)
	defer prog.EndTable(job.Table.Name)

	// Check for saved progress (chunk-level resume)
	var resumeLastPK any
	var resumeRowsDone int64
	var resumeRanges []resumeRange
	var resumeCompositeRangeState string
	if job.Saver != nil && job.TaskID > 0 {
		var rangeState string
		var err error
		resumeLastPK, resumeRowsDone, rangeState, err = job.Saver.GetProgress(job.TaskID)
		if err != nil {
			logging.Warn("Failed to load checkpoint for %s: %v", job.Table.Name, err)
		}
		if resumeLastPK != nil {
			logging.Debug("Resuming %s at row %d (checkpoint: %v)", job.Table.Name, resumeRowsDone, resumeLastPK)
			job.ReplayPossible = true
		}
		// Per-range watermarks (#464): only meaningful on the keyset
		// path; legacy rows (and ROW_NUMBER tasks) have none.
		if job.Table.SupportsKeysetPagination() {
			if resumeRanges = decodeKeysetRangeState(rangeState); len(resumeRanges) > 0 {
				logging.Debug("Resuming %s with %d per-range watermarks", job.Table.Name, len(resumeRanges))
			}
		}
		// Composite keyset persists its int64-preserving watermark tuple in
		// the range_state column (#616).
		resumeCompositeRangeState = rangeState

		// Incremental upsert resume replays the whole changed-row window from
		// the start instead of continuing from the positional cursor (#647). A
		// row updated behind the saved PK would otherwise be skipped, and its
		// update permanently lost once the fenced watermark advanced past it.
		// Upsert is idempotent, so replaying "updated_at > T0" is safe and
		// transfers every changed row exactly once logically.
		if cfg.Migration.TargetMode == "upsert" && job.DateFilter != nil {
			if resumeLastPK != nil || len(resumeRanges) > 0 || resumeCompositeRangeState != "" {
				logging.Debug("Incremental resume for %s: replaying changed-row window from start (#647)", job.Table.Name)
			}
			resumeLastPK = nil
			resumeRowsDone = 0
			resumeRanges = nil
			resumeCompositeRangeState = ""
			job.ReplayPossible = true
		}
	}

	// A strict-consistency transfer is one stable table view, not merely a
	// different table hint. Start and pin the source transaction before any
	// target preparation so an unsupported source fails without truncating data.
	// The transaction remains open through every page and is released on return.
	if cfg.Migration.StrictConsistency {
		strictCtx, releaseSnapshot, err := beginStrictSourceSnapshot(ctx, srcPool, job.Table)
		if err != nil {
			return nil, err
		}
		ctx = strictCtx
		defer releaseSnapshot()

		// Full-table strict jobs validate against this exact count rather than a
		// later live source count. Incremental DateFilter jobs intentionally
		// skip it: their target contains a window, not the whole table (#664).
		if job.DateFilter == nil && job.Saver != nil && job.TaskID > 0 {
			snapshotCount, err := strictSnapshotRowCount(ctx, srcPool, job.Table)
			if err != nil {
				return nil, err
			}
			saver, ok := job.Saver.(interface {
				SaveStrictSnapshotRowCount(taskID, rowCount int64) error
			})
			if !ok {
				return nil, fmt.Errorf("strict_consistency task %d cannot persist its snapshot row count", job.TaskID)
			}
			if err := saver.SaveStrictSnapshotRowCount(job.TaskID, snapshotCount); err != nil {
				return nil, fmt.Errorf("persisting strict_consistency snapshot row count for %s: %w", job.Table.FullName(), err)
			}
		}
	}

	// Handle truncation based on job type (skip if resuming or in upsert mode)
	// Upsert mode: no truncation needed, upserts are idempotent
	if cfg.Migration.TargetMode != "upsert" {
		if resumeLastPK == nil {
			if job.Partition == nil {
				// Non-partitioned table: truncate here (no race possible).
				// A missing table is benign (defensive — the table is created
				// before transfer); any other failure (permission denied, lock
				// timeout) must be surfaced, not swallowed, or stale rows cause
				// confusing duplicate-key errors mid-transfer (#619).
				if err := tgtPool.TruncateTable(ctx, cfg.Target.Schema, job.Table.Name); err != nil {
					if isTableNotFoundError(err) {
						logging.Debug("Truncate skipped for %s: table not found (proceeding)", job.Table.Name)
					} else {
						logging.Warn("Truncate failed for %s before transfer: %v (stale rows may surface as duplicate-key errors)", job.Table.Name, err)
					}
				}
			} else {
				// A fresh partitioned table was already truncated by the
				// orchestrator. Range cleanup is required only when a retry or
				// cross-process resume may replay committed rows (#644).
				if job.Table.SupportsKeysetPagination() && (job.ReplayPossible || job.IsResume) {
					if err := cleanupPartitionDataGeneric(ctx, tgtPool, cfg.Target.Schema, &job); err != nil {
						return nil, fmt.Errorf("partition replay cleanup for %s: %w", job.Table.Name, err)
					}
				}
			}
		} else if job.Table.SupportsKeysetPagination() {
			// Chunk-level resume: delete any rows written after the last
			// saved checkpoint. With per-range watermarks (#464) the
			// delete is scoped per incomplete range — ranges a previous
			// segment completed are left untouched instead of being
			// deleted and re-transferred.
			if len(resumeRanges) > 0 {
				for _, rr := range resumeRanges {
					if rr.complete || rr.lastPK == nil {
						continue
					}
					if err := cleanupPartialData(ctx, tgtPool, cfg.Target.Schema, job.Table.Name, job.Table.PrimaryKey[0], rr.lastPK, rr.maxPK); err != nil {
						return nil, fmt.Errorf("resume cleanup for %s: %w", job.Table.Name, err)
					}
				}
			} else {
				var maxPK any
				if job.Partition != nil {
					maxPK = job.Partition.MaxPK
				}
				if err := cleanupPartialData(ctx, tgtPool, cfg.Target.Schema, job.Table.Name, job.Table.PrimaryKey[0], resumeLastPK, maxPK); err != nil {
					return nil, fmt.Errorf("resume cleanup for %s: %w", job.Table.Name, err)
				}
			}
		}
	}

	// Build column list
	cols := make([]string, len(job.Table.Columns))
	targetCols := make([]string, len(job.Table.Columns))
	colTypes := make([]string, len(job.Table.Columns))
	colSRIDs := make([]int, len(job.Table.Columns))

	// Only sanitize identifiers when target is PostgreSQL
	isPGTarget := tgtPool.DBType() == "postgres"

	for i, c := range job.Table.Columns {
		cols[i] = c.Name
		if isPGTarget {
			targetCols[i] = target.SanitizePGIdentifier(c.Name)
		} else {
			targetCols[i] = c.Name // Preserve original case for MSSQL
		}
		colTypes[i] = strings.ToLower(c.DataType)
		colSRIDs[i] = c.SRID // SRID from source schema (0 = unset)
	}

	// Sanitize table name for target (only for PostgreSQL)
	var targetTableName string
	if isPGTarget {
		targetTableName = target.SanitizePGIdentifier(job.Table.Name)
	} else {
		targetTableName = job.Table.Name // Preserve original case for MSSQL
	}

	// Choose pagination strategy.
	if job.Table.SupportsKeysetPagination() {
		return executeKeysetPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, resumeRanges, targetTableName, tuner, adjuster)
	}

	// Tuple-safe PKs not owned by the legacy parallel keyset path page via
	// tuple keyset instead of the slow ROW_NUMBER fallback (#616/#629) — but
	// only on engines whose primary keys are unique (SupportsCompositeKeyset);
	// on non-unique-key engines like ClickHouse a duplicate tuple split across
	// a chunk boundary could be skipped, so those keep ROW_NUMBER. A
	// ValueConverter on any PK column also forces ROW_NUMBER: the watermark is
	// extracted after converters run, and a rewritten value may no longer
	// match the source column (#629). The type-exact watermark tuple is
	// restored from the range_state column (the legacy last_pk column
	// round-trips through float64 and would lose BIGINT precision).
	srcDialect := driver.GetDialect(srcPool.DBType())
	if driver.TupleKeysetRoutable(&job.Table, srcPool.DBType()) &&
		!convertersTouchPK(srcDialect, cols, colTypes, tgtPool.DBType(), job.Table.PrimaryKey) {
		var resumeTuple []any
		tupleResumeRowsDone := resumeRowsDone
		if resumeLastPK != nil {
			resumeTuple = decodeCompositeTuple(resumeCompositeRangeState)
			if resumeTuple == nil {
				// Fall back to the (float64) last_pk tuple if range_state is
				// absent or malformed.
				if t, ok := resumeLastPK.([]any); ok {
					resumeTuple = t
				}
			}
			if resumeTuple == nil {
				// Foreign checkpoints (for example pre-#629 ROW_NUMBER row
				// offsets) are replayed from the start. Their rows_done value
				// is not a tuple watermark position, so carrying it forward
				// would overcount replay progress.
				tupleResumeRowsDone = 0
			}
		}
		return executeCompositeKeysetPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeTuple, tupleResumeRowsDone, targetTableName, tuner, adjuster)
	}

	// Fall back to ROW_NUMBER pagination for non-integer composite PKs,
	// single non-integer PKs, or no PK.
	return executeRowNumberPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName, tuner, adjuster)
}
