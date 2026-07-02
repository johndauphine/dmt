package transfer

import (
	"context"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
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

	// Track table start/end for accurate progress display
	prog.StartTable(job.Table.Name)
	defer prog.EndTable(job.Table.Name)

	// Check for saved progress (chunk-level resume)
	var resumeLastPK any
	var resumeRowsDone int64
	var resumeRanges []resumeRange
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
	}

	// Handle truncation based on job type (skip if resuming or in upsert mode)
	// Upsert mode: no truncation needed, upserts are idempotent
	if cfg.Migration.TargetMode != "upsert" {
		if resumeLastPK == nil {
			if job.Partition == nil {
				// Non-partitioned table: truncate here (no race possible)
				if err := tgtPool.TruncateTable(ctx, cfg.Target.Schema, job.Table.Name); err != nil {
					// Ignore truncate errors (table might not exist)
				}
			} else {
				// Partitioned table: already truncated in orchestrator, just cleanup for idempotent retry
				if job.Table.SupportsKeysetPagination() {
					if err := cleanupPartitionDataGeneric(ctx, tgtPool, cfg.Target.Schema, &job); err != nil {
						logging.Warn("Partition cleanup failed for %s: %v", job.Table.Name, err)
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
						logging.Warn("Resume cleanup failed for %s: %v", job.Table.Name, err)
					}
				}
			} else {
				var maxPK any
				if job.Partition != nil {
					maxPK = job.Partition.MaxPK
				}
				if err := cleanupPartialData(ctx, tgtPool, cfg.Target.Schema, job.Table.Name, job.Table.PrimaryKey[0], resumeLastPK, maxPK); err != nil {
					logging.Warn("Resume cleanup failed for %s: %v", job.Table.Name, err)
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

	// Choose pagination strategy
	if job.Table.SupportsKeysetPagination() {
		return executeKeysetPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, resumeRanges, targetTableName, tuner, adjuster)
	}

	// Fall back to ROW_NUMBER pagination for composite/varchar PKs or no PK
	return executeRowNumberPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName, tuner, adjuster)
}
