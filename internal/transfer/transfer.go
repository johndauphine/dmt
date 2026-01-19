package transfer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/target"
)

// Buffer sizing constants for parallel reader coordination.
// These values prevent cascading deadlocks when multiple readers produce faster than the consumer can process.
const (
	// chunkChanBufferMultiplier scales the chunk channel buffer with the number of readers.
	// A multiplier of 50 provides enough headroom for burst production while keeping
	// memory usage reasonable (each chunk holds rows from a database query).
	chunkChanBufferMultiplier = 50

	// chunkChanMinBuffer is the minimum chunk channel buffer size for parallel readers.
	// This ensures adequate buffering even with small configured buffer sizes.
	chunkChanMinBuffer = 500
)

// ProgressSaver is an interface for saving transfer progress
type ProgressSaver interface {
	SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error
	GetProgress(taskID int64) (lastPK any, rowsDone int64, err error)
}

// DateFilter is an alias for driver.DateFilter for backward compatibility
type DateFilter = driver.DateFilter

// Job represents a data transfer job
type Job struct {
	Table      source.Table
	Partition  *source.Partition
	TaskID     int64         // For chunk-level resume
	Saver      ProgressSaver // For saving progress (nil to disable)
	DateFilter *DateFilter   // Optional date filter for incremental sync (upsert mode)
}

// chunkResult holds a chunk of data for the read-ahead pipeline
type chunkResult struct {
	rows      [][]any
	lastPK    any
	rowNum    int64 // for ROW_NUMBER pagination progress tracking
	readerID  int
	seq       int64
	queryTime time.Duration
	scanTime  time.Duration
	readEnd   time.Time // when this chunk finished reading
	err       error
	done      bool // signals end of data
}

type writeJob struct {
	rows     [][]any
	lastPK   any
	rowNum   int64
	readerID int
	seq      int64
}

type writeAck struct {
	readerID int
	seq      int64
	lastPK   any
	rowNum   int64
}

type readerCheckpointState struct {
	lastPK    any
	lastPKInt int64
	maxPKInt  int64
	maxOK     bool
	complete  bool
	nextSeq   int64
	pending   map[int64]writeAck
}

// writeResult holds the result of a parallel write operation
type writeResult struct {
	writeTime time.Duration
	rowCount  int64
	err       error
}

// TransferStats tracks timing statistics for profiling
type TransferStats struct {
	QueryTime time.Duration
	ScanTime  time.Duration
	WriteTime time.Duration
	Rows      int64
}

// pkRange represents a primary key range for parallel reading
type pkRange struct {
	minPK any // inclusive (start from > minPK)
	maxPK any // inclusive (read up to <= maxPK)
}

// splitPKRange divides a PK range into n sub-ranges for parallel reading
// Note: minPK should be the actual minimum PK value; this function handles
// the decrement needed for the > comparison in WHERE clauses
func splitPKRange(minPK, maxPK any, n int) []pkRange {
	if n <= 1 {
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	// Convert to int64 for range splitting
	var minVal, maxVal int64
	switch v := minPK.(type) {
	case int:
		minVal = int64(v)
	case int32:
		minVal = int64(v)
	case int64:
		minVal = v
	default:
		// Can't split non-integer PKs, use single range
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	switch v := maxPK.(type) {
	case int:
		maxVal = int64(v)
	case int32:
		maxVal = int64(v)
	case int64:
		maxVal = v
	default:
		return []pkRange{{minPK: minPK, maxPK: maxPK}}
	}

	// Calculate range size per reader
	totalRange := maxVal - minVal
	if totalRange <= 0 {
		return []pkRange{{minPK: minPK, maxPK: maxPK}}
	}

	rangeSize := totalRange / int64(n)
	if rangeSize < 1 {
		rangeSize = 1
		n = int(totalRange) // Reduce readers if range is small
	}

	ranges := make([]pkRange, 0, n)
	for i := 0; i < n; i++ {
		var rangeMin, rangeMax int64
		if i == 0 {
			rangeMin = minVal - 1 // First range: start before minVal for > comparison
		} else {
			rangeMin = minVal + int64(i)*rangeSize // Subsequent ranges: start at boundary
		}
		rangeMax = minVal + int64(i+1)*rangeSize
		if i == n-1 {
			rangeMax = maxVal // Last reader gets remainder
		}
		ranges = append(ranges, pkRange{
			minPK: rangeMin,
			maxPK: rangeMax,
		})
	}

	return ranges
}

func (s *TransferStats) String() string {
	total := s.QueryTime + s.ScanTime + s.WriteTime
	if total == 0 {
		return "no data"
	}
	return fmt.Sprintf("query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%), rows=%d",
		s.QueryTime.Seconds(), float64(s.QueryTime)/float64(total)*100,
		s.ScanTime.Seconds(), float64(s.ScanTime)/float64(total)*100,
		s.WriteTime.Seconds(), float64(s.WriteTime)/float64(total)*100,
		s.Rows)
}

// Execute runs a transfer job using the optimal pagination strategy
func Execute(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	prog *progress.Tracker,
) (*TransferStats, error) {
	// Track table start/end for accurate progress display
	prog.StartTable(job.Table.Name)
	defer prog.EndTable(job.Table.Name)

	// Check for saved progress (chunk-level resume)
	var resumeLastPK any
	var resumeRowsDone int64
	if job.Saver != nil && job.TaskID > 0 {
		var err error
		resumeLastPK, resumeRowsDone, err = job.Saver.GetProgress(job.TaskID)
		if err != nil {
			logging.Warn("Failed to load checkpoint for %s: %v", job.Table.Name, err)
		}
		if resumeLastPK != nil {
			logging.Debug("Resuming %s at row %d (checkpoint: %v)", job.Table.Name, resumeRowsDone, resumeLastPK)
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
			// Chunk-level resume: delete any rows beyond the saved lastPK
			// This handles partial data written after the last saved checkpoint
			var maxPK any
			if job.Partition != nil {
				maxPK = job.Partition.MaxPK
			}
			if err := cleanupPartialData(ctx, tgtPool, cfg.Target.Schema, job.Table.Name, job.Table.PrimaryKey[0], resumeLastPK, maxPK); err != nil {
				logging.Warn("Resume cleanup failed for %s: %v", job.Table.Name, err)
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
		return executeKeysetPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName)
	}

	// Fall back to ROW_NUMBER pagination for composite/varchar PKs or no PK
	return executeRowNumberPagination(ctx, srcPool, tgtPool, cfg, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName)
}

// cleanupPartitionData removes any existing data for a partition's PK range (idempotent retry) - PostgreSQL version
func cleanupPartitionData(ctx context.Context, pgPool *pgxpool.Pool, schema string, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	pkCol := target.SanitizePGIdentifier(job.Table.PrimaryKey[0])
	tableName := target.SanitizePGIdentifier(job.Table.Name)

	query := fmt.Sprintf(
		`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
		schema, tableName, pkCol, pkCol,
	)

	_, err := pgPool.Exec(ctx, query, job.Partition.MinPK, job.Partition.MaxPK)
	return err
}

// cleanupPartitionDataGeneric removes partition data using the appropriate pool interface
func cleanupPartitionDataGeneric(ctx context.Context, tgtPool pool.TargetPool, schema string, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	pkCol := job.Table.PrimaryKey[0]

	// Build query and args based on target type
	var query string
	var args []any

	switch tgtPool.DBType() {
	case "postgres":
		// PostgreSQL target - sanitize identifiers and use $N parameters
		sanitizedPK := target.SanitizePGIdentifier(pkCol)
		sanitizedTable := target.SanitizePGIdentifier(job.Table.Name)
		query = fmt.Sprintf(
			`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
			schema, sanitizedTable, sanitizedPK, sanitizedPK,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	case "mysql":
		// MySQL target - use backtick identifiers and ? positional parameters
		query = fmt.Sprintf(
			"DELETE FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` <= ?",
			schema, job.Table.Name, pkCol, pkCol,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	case "oracle":
		// Oracle target - use double-quote identifiers and :N positional parameters
		query = fmt.Sprintf(
			`DELETE FROM "%s"."%s" WHERE "%s" >= :1 AND "%s" <= :2`,
			schema, job.Table.Name, pkCol, pkCol,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	default:
		// SQL Server target - use bracket identifiers and @p parameters
		query = fmt.Sprintf(
			`DELETE FROM [%s].[%s] WHERE [%s] >= @p1 AND [%s] <= @p2`,
			schema, job.Table.Name, pkCol, pkCol,
		)
		args = []any{sql.Named("p1", job.Partition.MinPK), sql.Named("p2", job.Partition.MaxPK)}
	}

	_, err := tgtPool.ExecRaw(ctx, query, args...)
	return err
}

// cleanupPartialData removes rows beyond the saved lastPK for chunk-level resume
func cleanupPartialData(ctx context.Context, tgtPool pool.TargetPool, schema, tableName, pkCol string, lastPK any, maxPK any) error {
	var deleteQuery string
	var args []any

	switch tgtPool.DBType() {
	case "postgres":
		// PostgreSQL target - sanitize identifiers
		sanitizedPK := target.SanitizePGIdentifier(pkCol)
		sanitizedTable := target.SanitizePGIdentifier(tableName)

		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM %s.%q WHERE %q > $1 AND %q <= $2`,
				schema, sanitizedTable, sanitizedPK, sanitizedPK)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM %s.%q WHERE %q > $1`,
				schema, sanitizedTable, sanitizedPK)
			args = []any{lastPK}
		}
	case "mysql":
		// MySQL target - use backtick identifiers and ? positional parameters
		if maxPK != nil {
			deleteQuery = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` > ? AND `%s` <= ?",
				schema, tableName, pkCol, pkCol)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` > ?",
				schema, tableName, pkCol)
			args = []any{lastPK}
		}
	case "oracle":
		// Oracle target - use double-quote identifiers and :N positional parameters
		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" > :1 AND "%s" <= :2`,
				schema, tableName, pkCol, pkCol)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" > :1`,
				schema, tableName, pkCol)
			args = []any{lastPK}
		}
	default:
		// SQL Server target
		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM [%s].[%s] WHERE [%s] > @p1 AND [%s] <= @p2`,
				schema, tableName, pkCol, pkCol)
			args = []any{sql.Named("p1", lastPK), sql.Named("p2", maxPK)}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM [%s].[%s] WHERE [%s] > @p1`,
				schema, tableName, pkCol)
			args = []any{sql.Named("p1", lastPK)}
		}
	}

	rowsAffected, err := tgtPool.ExecRaw(ctx, deleteQuery, args...)
	if err != nil {
		return err
	}
	if rowsAffected > 0 {
		logging.Debug("Removed %d stale rows from %s beyond pk=%v", rowsAffected, tableName, lastPK)
	}
	return nil
}

func parseResumeRowNum(lastPK any) (int64, bool) {
	if lastPK == nil {
		return 0, false
	}
	switch v := lastPK.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func parseNumericPK(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// executeKeysetPagination uses WHERE pk > last_pk for efficient pagination
// with async read-ahead pipelining to overlap reads and writes
func executeKeysetPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
	targetTableName string,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}
	pkCol := job.Table.PrimaryKey[0]

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %s", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)
	chunkSize := cfg.Migration.ChunkSize

	// Get PK range for parallel readers
	var minPKVal, maxPKVal any
	if job.Partition != nil {
		minPKVal = job.Partition.MinPK
		maxPKVal = job.Partition.MaxPK
	} else {
		// For non-partitioned tables, get min and max PK
		minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s %s",
			srcDialect.QuoteIdentifier(pkCol), srcDialect.QuoteIdentifier(pkCol),
			srcDialect.QualifyTable(job.Table.Schema, job.Table.Name), tableHint)
		err := db.QueryRowContext(ctx, minMaxQuery).Scan(&minPKVal, &maxPKVal)
		if err != nil || minPKVal == nil {
			return stats, nil // Empty table
		}
	}

	// Use resume point if available
	if resumeLastPK != nil {
		minPKVal = resumeLastPK
	}

	// Find PK column index
	pkIdx := 0
	for i, c := range cols {
		if c == pkCol {
			pkIdx = i
			break
		}
	}

	// Determine number of parallel readers
	numReaders := cfg.Migration.ParallelReaders
	if numReaders < 1 {
		numReaders = 1
	}

	// Create buffered channel for read-ahead pipeline
	// With parallel readers, use a larger buffer to prevent readers from blocking
	// when the consumer is temporarily slow (e.g., waiting for writers).
	bufferSize := cfg.Migration.ReadAheadBuffers
	if bufferSize < 0 {
		bufferSize = 0
	}
	if numReaders > 1 {
		// Scale buffer with number of readers to prevent deadlock
		bufferSize = bufferSize * numReaders * chunkChanBufferMultiplier
		if bufferSize < chunkChanMinBuffer {
			bufferSize = chunkChanMinBuffer
		}
	}
	chunkChan := make(chan chunkResult, bufferSize)

	// Split PK range for parallel readers
	pkRanges := splitPKRange(minPKVal, maxPKVal, numReaders)
	// Start parallel reader goroutines
	var readerWg sync.WaitGroup
	for readerID, pkr := range pkRanges {
		readerWg.Add(1)
		go func(readerID int, rangeMinPK, rangeMaxPK any) {
			defer readerWg.Done()

			lastPK := rangeMinPK
			seq := int64(0)

			for {
				select {
				case <-ctx.Done():
					chunkChan <- chunkResult{err: ctx.Err()}
					return
				default:
				}

				// Always use bounded query for parallel readers
				query := srcDialect.BuildKeysetQuery(colList, pkCol, job.Table.Schema, job.Table.Name, tableHint, true, job.DateFilter)
				args := srcDialect.BuildKeysetArgs(lastPK, rangeMaxPK, chunkSize, true, job.DateFilter)

				// Time the query
				queryStart := time.Now()
				rows, err := db.QueryContext(ctx, query, args...)
				queryTime := time.Since(queryStart)
				if err != nil {
					chunkChan <- chunkResult{err: fmt.Errorf("keyset query: %w", err)}
					return
				}

				// Time the scan
				scanStart := time.Now()
				chunk, _, err := scanRows(rows, cols, colTypes)
				rows.Close()
				scanTime := time.Since(scanStart)
				if err != nil {
					chunkChan <- chunkResult{err: fmt.Errorf("scanning rows: %w", err)}
					return
				}

				if len(chunk) == 0 {
					return // This reader is done
				}

				// Update lastPK for next iteration
				lastPK = chunk[len(chunk)-1][pkIdx]

				chunkChan <- chunkResult{
					rows:      chunk,
					lastPK:    lastPK,
					readerID:  readerID,
					seq:       seq,
					queryTime: queryTime,
					scanTime:  scanTime,
					readEnd:   time.Now(),
				}
				seq++

				if len(chunk) < chunkSize {
					return // This reader is done
				}
			}
		}(readerID, pkr.minPK, pkr.maxPK)
	}

	// Close chunkChan when all readers are done
	go func() {
		readerWg.Wait()
		logging.Debug("All %d parallel readers finished, closing chunkChan (len=%d)", numReaders, len(chunkChan))
		close(chunkChan)
	}()

	// Get partition ID for staging table naming
	var partitionID *int
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
	}

	// Create writer pool
	numWriters := cfg.Migration.WriteAheadWriters
	if numWriters < 1 {
		numWriters = 1
	}

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:           numWriters,
		BufferSize:           bufferSize,
		UseUpsert:            cfg.Migration.TargetMode == "upsert",
		UpsertMergeChunkSize: cfg.Migration.UpsertMergeChunkSize,
		TargetSchema:         cfg.Target.Schema,
		TargetTable:          targetTableName,
		TargetCols:           targetCols,
		ColTypes:             colTypes,
		ColSRIDs:             colSRIDs,
		TargetPKCols:         buildTargetPKCols(job.Table.PrimaryKey, tgtPool),
		PartitionID:          partitionID,
		TgtPool:              tgtPool,
		Prog:                 prog,
		EnableAck:            job.Saver != nil && job.TaskID > 0,
	})

	// Setup checkpoint coordinator
	checkpointCoord := newKeysetCheckpointCoordinator(job, pkRanges, resumeRowsDone, wp.TotalWrittenPtr(), cfg.Migration.CheckpointFrequency)
	if checkpointCoord != nil {
		wp.startAckProcessor(checkpointCoord.onAck)
	}

	wp.start()

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	totalTransferred := resumeRowsDone
	chunkCount := 0
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var lastPK any
	var loopErr error

	// Process chunks and dispatch writes
chunkLoop:
	for result := range chunkChan {
		if result.err != nil {
			loopErr = result.err
			wp.Cancel()
			break
		}
		if result.done {
			break
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		lastPK = result.lastPK

		// Calculate overlap: if this chunk was ready before last write ended, we had overlap
		receiveTime := time.Now()
		if !lastWriteEnd.IsZero() && !result.readEnd.IsZero() && result.readEnd.Before(lastWriteEnd) {
			overlap := lastWriteEnd.Sub(result.readEnd)
			totalOverlap += overlap
		}
		lastWriteEnd = time.Now()

		// Dispatch to write pool
		if !wp.submit(writeJob{
			rows:     result.rows,
			lastPK:   result.lastPK,
			readerID: result.readerID,
			seq:      result.seq,
		}) {
			if err := wp.error(); err != nil {
				loopErr = fmt.Errorf("writing chunk: %w", err)
			} else {
				loopErr = ctx.Err()
			}
			break chunkLoop
		}

		// Log overlap stats periodically
		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			logging.Debug("Pipeline %s: %d chunks, overlap=%v, wait=%v, buffers=%d, writers=%d",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	logging.Debug("Consumer loop finished, calling wp.wait()")

	// Wait for writers to finish
	wp.wait()
	logging.Debug("wp.wait() completed")

	if loopErr != nil {
		return stats, loopErr
	}

	// Check for write errors
	if err := wp.error(); err != nil {
		return stats, fmt.Errorf("writing chunk: %w", err)
	}

	// Aggregate stats
	stats.WriteTime = wp.writeTime()
	totalTransferred += wp.written()
	stats.Rows = totalTransferred

	// Save final progress
	if job.Saver != nil && job.TaskID > 0 && lastPK != nil {
		finalLastPK := lastPK
		if checkpointCoord != nil {
			finalLastPK = checkpointCoord.finalCheckpoint(lastPK)
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalLastPK, totalTransferred, job.Table.RowCount); err != nil {
			logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
		}
	}

	return stats, nil
}

// executeRowNumberPagination uses ROW_NUMBER for composite/varchar PKs
// with async read-ahead pipelining to overlap reads and writes
func executeRowNumberPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
	targetTableName string,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %q", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)

	// Build ORDER BY clause from PK columns
	// Tables without PK cannot be migrated safely - fail fast
	if len(job.Table.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key - cannot guarantee data correctness with ROW_NUMBER pagination. "+
			"Add a primary key to the table or exclude it from migration", job.Table.FullName())
	}

	pkCols := make([]string, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		pkCols[i] = srcDialect.QuoteIdentifier(pk)
	}
	orderBy := strings.Join(pkCols, ", ")

	chunkSize := cfg.Migration.ChunkSize

	// Determine row range for this job
	var startRow, endRow int64
	if job.Partition != nil && job.Partition.EndRow > 0 {
		// Partitioned: use partition boundaries
		startRow = job.Partition.StartRow
		endRow = job.Partition.EndRow
	} else {
		// Non-partitioned: process entire table
		startRow = 0
		endRow = job.Table.RowCount
	}

	// Resume from saved progress if available
	initialRowNum := startRow
	if resumeRowNum, ok := parseResumeRowNum(resumeLastPK); ok {
		initialRowNum = resumeRowNum
	}
	if initialRowNum < startRow {
		initialRowNum = startRow
	}
	if initialRowNum > endRow {
		initialRowNum = endRow
	}

	// Create buffered channel for read-ahead pipeline
	bufferSize := cfg.Migration.ReadAheadBuffers
	if bufferSize < 0 {
		bufferSize = 0
	}
	chunkChan := make(chan chunkResult, bufferSize)

	// Start reader goroutine
	go func() {
		defer close(chunkChan)
		rowNum := initialRowNum
		seq := int64(0)

		for rowNum < endRow {
			select {
			case <-ctx.Done():
				chunkChan <- chunkResult{err: ctx.Err()}
				return
			default:
			}

			// Adjust chunk size if near end of partition
			effectiveChunkSize := chunkSize
			if rowNum+int64(chunkSize) > endRow {
				effectiveChunkSize = int(endRow - rowNum)
			}

			// ROW_NUMBER pagination with direction-aware syntax
			query := srcDialect.BuildRowNumberQuery(colList, orderBy, job.Table.Schema, job.Table.Name, tableHint, job.DateFilter)
			args := srcDialect.BuildRowNumberArgs(rowNum, effectiveChunkSize, job.DateFilter)

			// Time the query
			queryStart := time.Now()
			rows, err := db.QueryContext(ctx, query, args...)
			queryTime := time.Since(queryStart)
			if err != nil {
				chunkChan <- chunkResult{err: fmt.Errorf("row_number query: %w", err)}
				return
			}

			// Time the scan
			scanStart := time.Now()
			chunk, _, err := scanRows(rows, cols, colTypes)
			rows.Close()
			scanTime := time.Since(scanStart)
			if err != nil {
				chunkChan <- chunkResult{err: fmt.Errorf("scanning rows: %w", err)}
				return
			}

			if len(chunk) == 0 {
				chunkChan <- chunkResult{done: true}
				return
			}

			// Update rowNum for progress tracking
			newRowNum := rowNum + int64(len(chunk))

			chunkChan <- chunkResult{
				rows:      chunk,
				rowNum:    newRowNum,
				readerID:  0,
				seq:       seq,
				queryTime: queryTime,
				scanTime:  scanTime,
				readEnd:   time.Now(),
			}
			seq++

			rowNum = newRowNum

			if len(chunk) < effectiveChunkSize {
				chunkChan <- chunkResult{done: true}
				return
			}
		}
		chunkChan <- chunkResult{done: true}
	}()

	// Get partition ID and row count for staging table naming and checkpointing
	var partitionID *int
	var partitionRows int64
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		partitionRows = job.Partition.RowCount
	} else {
		partitionRows = job.Table.RowCount
	}

	// Create writer pool
	numWriters := cfg.Migration.WriteAheadWriters
	if numWriters < 1 {
		numWriters = 1
	}

	enableAck := job.Saver != nil && job.TaskID > 0
	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:           numWriters,
		BufferSize:           bufferSize,
		UseUpsert:            cfg.Migration.TargetMode == "upsert",
		UpsertMergeChunkSize: cfg.Migration.UpsertMergeChunkSize,
		TargetSchema:         cfg.Target.Schema,
		TargetTable:          targetTableName,
		TargetCols:           targetCols,
		ColTypes:             colTypes,
		ColSRIDs:             colSRIDs,
		TargetPKCols:         buildTargetPKCols(job.Table.PrimaryKey, tgtPool),
		PartitionID:          partitionID,
		TgtPool:              tgtPool,
		Prog:                 prog,
		EnableAck:            enableAck,
	})

	// Setup ROW_NUMBER checkpoint handler
	checkpointFreq := cfg.Migration.CheckpointFrequency
	if checkpointFreq <= 0 {
		checkpointFreq = 10
	}
	lastCheckpointRowNum := initialRowNum

	if enableAck {
		expectedSeq := int64(0)
		pending := make(map[int64]writeAck)
		completedChunks := 0

		wp.startAckProcessor(func(ack writeAck) {
			if ack.seq != expectedSeq {
				pending[ack.seq] = ack
				return
			}
			for {
				lastCheckpointRowNum = ack.rowNum
				completedChunks++
				if completedChunks%checkpointFreq == 0 {
					rowsDone := resumeRowsDone + wp.written()
					if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, lastCheckpointRowNum, rowsDone, partitionRows); err != nil {
						logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
					}
				}
				expectedSeq++
				next, ok := pending[expectedSeq]
				if !ok {
					break
				}
				delete(pending, expectedSeq)
				ack = next
			}
		})
	}

	wp.start()

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	chunkCount := 0
	totalTransferred := resumeRowsDone
	var currentRowNum int64
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var loopErr error

	// Process chunks and dispatch writes
chunkLoop:
	for result := range chunkChan {
		if result.err != nil {
			loopErr = result.err
			wp.Cancel()
			break
		}
		if result.done {
			break
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		currentRowNum = result.rowNum

		// Calculate overlap: if this chunk was ready before last write ended, we had overlap
		receiveTime := time.Now()
		if !lastWriteEnd.IsZero() && !result.readEnd.IsZero() && result.readEnd.Before(lastWriteEnd) {
			overlap := lastWriteEnd.Sub(result.readEnd)
			totalOverlap += overlap
		}
		lastWriteEnd = time.Now()

		// Dispatch to write pool
		if !wp.submit(writeJob{
			rows:     result.rows,
			rowNum:   result.rowNum,
			readerID: result.readerID,
			seq:      result.seq,
		}) {
			if err := wp.error(); err != nil {
				loopErr = fmt.Errorf("writing chunk: %w", err)
			} else {
				loopErr = ctx.Err()
			}
			break chunkLoop
		}

		// Log overlap stats periodically
		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			logging.Debug("Pipeline %s: %d chunks, overlap=%v, wait=%v, buffers=%d, writers=%d",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	logging.Debug("Consumer loop finished, calling wp.wait()")

	// Wait for writers to finish
	wp.wait()
	logging.Debug("wp.wait() completed")

	if loopErr != nil {
		return stats, loopErr
	}

	// Check for write errors
	if err := wp.error(); err != nil {
		return stats, fmt.Errorf("writing chunk: %w", err)
	}

	// Aggregate stats
	stats.WriteTime = wp.writeTime()
	totalTransferred += wp.written()
	stats.Rows = totalTransferred

	// Save final progress
	if job.Saver != nil && job.TaskID > 0 {
		finalRowNum := currentRowNum
		if enableAck {
			finalRowNum = lastCheckpointRowNum
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalRowNum, totalTransferred, partitionRows); err != nil {
			logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
		}
	}

	return stats, nil
}

// scanRows scans database rows into a slice of values with proper type handling.
func scanRows(rows *sql.Rows, cols, colTypes []string) ([][]any, any, error) {
	numCols := len(cols)
	// Result slice grows as needed; we primarily optimize by reusing the pointers slice per row.
	var result [][]any
	var lastPK any

	// Reuse pointers slice to avoid allocation per row
	ptrs := make([]any, numCols)

	for rows.Next() {
		row := make([]any, numCols)
		for i := range row {
			ptrs[i] = &row[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}

		// Process values for PostgreSQL compatibility
		for i, val := range row {
			row[i] = processValue(val, colTypes[i])
		}

		result = append(result, row)
	}

	if len(result) > 0 {
		// lastPK is derived after the loop from the last row (first column assumed to be PK)
		lastPK = result[len(result)-1][0]
	}

	return result, lastPK, rows.Err()
}

// processValue handles type conversions for PostgreSQL compatibility
func processValue(val any, colType string) any {
	if val == nil {
		return nil
	}

	// Handle Oracle NUMBER type - convert to string to preserve precision
	// This must happen before other type checks since godror.Number can represent any numeric type
	if converted, handled := processOracleValue(val); handled {
		return converted
	}

	switch colType {
	case "binary", "varbinary", "image":
		// Convert binary data to hex format for bytea
		switch v := val.(type) {
		case []byte:
			if len(v) == 0 {
				return nil
			}
			return v // pgx handles []byte directly
		}
	case "uniqueidentifier":
		// Handle UUID conversion
		switch v := val.(type) {
		case []byte:
			if len(v) == 16 {
				// SQL Server GUID to PostgreSQL UUID
				return formatUUID(v)
			}
			return string(v)
		case string:
			return v
		}
	case "bit":
		// Convert bit to boolean
		switch v := val.(type) {
		case bool:
			return v
		case int64:
			return v != 0
		case int:
			return v != 0
		}
	case "datetime", "datetime2", "smalldatetime":
		// Ensure proper timestamp format
		switch v := val.(type) {
		case time.Time:
			// Handle SQL Server minimum datetime (1753-01-01)
			if v.Year() < 1 {
				return nil
			}
			return v
		}
	case "datetimeoffset":
		// Handle datetimeoffset with timezone
		switch v := val.(type) {
		case time.Time:
			if v.Year() < 1 {
				return nil
			}
			return v
		}
	}

	return val
}

// formatUUID converts SQL Server GUID bytes to UUID string
func formatUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	// SQL Server stores GUIDs in mixed-endian format
	// Convert to standard UUID format
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], // time_low (reversed)
		b[5], b[4], // time_mid (reversed)
		b[7], b[6], // time_hi_and_version (reversed)
		b[8], b[9], // clock_seq
		b[10], b[11], b[12], b[13], b[14], b[15]) // node
}

// decrementPK returns a value that is less than the given PK value
func decrementPK(pk any) any {
	switch v := pk.(type) {
	case int64:
		return v - 1
	case int32:
		return v - 1
	case int:
		return v - 1
	default:
		return pk
	}
}

func writeChunk(ctx context.Context, pgPool *pgxpool.Pool, schema, table string, cols []string, rows [][]any) error {
	conn, err := pgPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Disable statement timeout for this operation
	_, err = conn.Exec(ctx, "SET statement_timeout = 0")
	if err != nil {
		return fmt.Errorf("setting statement timeout: %w", err)
	}

	// Use COPY for bulk insert
	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{schema, table},
		cols,
		pgx.CopyFromRows(rows),
	)

	return err
}

// writeChunkGeneric writes a chunk of data using the appropriate target pool
func writeChunkGeneric(ctx context.Context, tgtPool pool.TargetPool, schema, table string, cols []string, rows [][]any) error {
	return tgtPool.WriteBatch(ctx, pool.WriteBatchOptions{
		Schema:  schema,
		Table:   table,
		Columns: cols,
		Rows:    rows,
	})
}

// writeChunkUpsertWithWriter writes a chunk using high-performance staging table approach.
// This uses per-writer staging tables for isolation and better parallelism:
// - PostgreSQL: TEMP table + COPY + INSERT...ON CONFLICT
// - MSSQL: #temp table + bulk insert + MERGE WITH (TABLOCK)
// colTypes is passed to skip geography/geometry from change detection in MSSQL MERGE
// colSRIDs is passed for geography/geometry SRID in STGeomFromText conversion (PG→MSSQL)
func writeChunkUpsertWithWriter(ctx context.Context, tgtPool pool.TargetPool, schema, table string,
	cols []string, colTypes []string, colSRIDs []int, pkCols []string, rows [][]any, writerID int, partitionID *int) error {
	return tgtPool.UpsertBatch(ctx, pool.UpsertBatchOptions{
		Schema:      schema,
		Table:       table,
		Columns:     cols,
		ColumnTypes: colTypes,
		ColumnSRIDs: colSRIDs,
		PKColumns:   pkCols,
		Rows:        rows,
		WriterID:    writerID,
		PartitionID: partitionID,
	})
}

// ValidateBinaryData ensures binary data is properly formatted
func ValidateBinaryData(data []byte) []byte {
	if data == nil || len(data) == 0 {
		return nil
	}
	return data
}

// FormatBytea formats binary data for PostgreSQL bytea column
func FormatBytea(data []byte) string {
	if data == nil || len(data) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("\\x")
	buf.WriteString(hex.EncodeToString(data))
	return buf.String()
}
