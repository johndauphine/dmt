package transfer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/pool"
	"github.com/johndauphine/mssql-pg-migrate/internal/progress"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/target"
)

// ProgressSaver is an interface for saving transfer progress
type ProgressSaver interface {
	SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error
	GetProgress(taskID int64) (lastPK any, rowsDone int64, err error)
}

// Job represents a data transfer job
type Job struct {
	Table     source.Table
	Partition *source.Partition
	TaskID    int64         // For chunk-level resume
	Saver     ProgressSaver // For saving progress (nil to disable)
}

// chunkResult holds a chunk of data for the read-ahead pipeline
type chunkResult struct {
	rows      [][]any
	lastPK    any
	rowNum    int64 // for ROW_NUMBER pagination progress tracking
	queryTime time.Duration
	scanTime  time.Duration
	readEnd   time.Time // when this chunk finished reading
	err       error
	done      bool // signals end of data
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

// dbSyntax provides direction-aware SQL syntax helpers
type dbSyntax struct {
	dbType string
}

func newDBSyntax(dbType string) dbSyntax {
	return dbSyntax{dbType: dbType}
}

// quoteIdent quotes an identifier based on database type
func (s dbSyntax) quoteIdent(name string) string {
	if s.dbType == "postgres" {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// qualifiedTable returns schema.table with proper quoting
func (s dbSyntax) qualifiedTable(schema, table string) string {
	return s.quoteIdent(schema) + "." + s.quoteIdent(table)
}

// columnList returns a quoted, comma-separated list of columns
func (s dbSyntax) columnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = s.quoteIdent(c)
	}
	return strings.Join(quoted, ", ")
}

// tableHint returns the table hint (WITH (NOLOCK) for SQL Server, empty for PostgreSQL)
func (s dbSyntax) tableHint(strictConsistency bool) string {
	if s.dbType == "postgres" || strictConsistency {
		return ""
	}
	return "WITH (NOLOCK)"
}

// buildKeysetQuery builds a keyset pagination query for the given database type
func (s dbSyntax) buildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool) string {
	if s.dbType == "postgres" {
		if hasMaxPK {
			return fmt.Sprintf(`
				SELECT %s FROM %s
				WHERE %s > $1 AND %s <= $2
				ORDER BY %s
				LIMIT $3
			`, cols, s.qualifiedTable(schema, table), s.quoteIdent(pkCol), s.quoteIdent(pkCol), s.quoteIdent(pkCol))
		}
		return fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s > $1
			ORDER BY %s
			LIMIT $2
		`, cols, s.qualifiedTable(schema, table), s.quoteIdent(pkCol), s.quoteIdent(pkCol))
	}

	// SQL Server syntax
	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT TOP (@limit) %s
			FROM %s %s
			WHERE [%s] > @lastPK AND [%s] <= @maxPK
			ORDER BY [%s]
		`, cols, s.qualifiedTable(schema, table), tableHint, pkCol, pkCol, pkCol)
	}
	return fmt.Sprintf(`
		SELECT TOP (@limit) %s
		FROM %s %s
		WHERE [%s] > @lastPK
		ORDER BY [%s]
	`, cols, s.qualifiedTable(schema, table), tableHint, pkCol, pkCol)
}

// buildRowNumberQuery builds a ROW_NUMBER pagination query for the given database type
func (s dbSyntax) buildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string {
	if s.dbType == "postgres" {
		return fmt.Sprintf(`
			WITH numbered AS (
				SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
				FROM %s
			)
			SELECT %s FROM numbered
			WHERE __rn > $1 AND __rn <= $2
			ORDER BY __rn
		`, cols, orderBy, s.qualifiedTable(schema, table), cols)
	}

	// SQL Server syntax
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s %s
		)
		SELECT %s FROM numbered
		WHERE __rn > @rowNum AND __rn <= @rowNumEnd
		ORDER BY __rn
	`, cols, orderBy, s.qualifiedTable(schema, table), tableHint, cols)
}

// buildArgs builds query arguments for the given database type
func (s dbSyntax) buildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool) []any {
	if s.dbType == "postgres" {
		if hasMaxPK {
			return []any{lastPK, maxPK, limit}
		}
		return []any{lastPK, limit}
	}

	// SQL Server uses named parameters
	if hasMaxPK {
		return []any{
			sql.Named("limit", limit),
			sql.Named("lastPK", lastPK),
			sql.Named("maxPK", maxPK),
		}
	}
	return []any{
		sql.Named("limit", limit),
		sql.Named("lastPK", lastPK),
	}
}

// buildRowNumberArgs builds query arguments for ROW_NUMBER pagination
func (s dbSyntax) buildRowNumberArgs(rowNum int64, limit int) []any {
	if s.dbType == "postgres" {
		return []any{rowNum, rowNum + int64(limit)}
	}
	return []any{
		sql.Named("rowNum", rowNum),
		sql.Named("rowNumEnd", rowNum+int64(limit)),
	}
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
	// Update progress bar with current table name
	prog.SetTable(job.Table.Name)

	// Check for saved progress (chunk-level resume)
	var resumeLastPK any
	var resumeRowsDone int64
	if job.Saver != nil && job.TaskID > 0 {
		var err error
		resumeLastPK, resumeRowsDone, err = job.Saver.GetProgress(job.TaskID)
		if err != nil {
			fmt.Printf("Warning: loading progress for %s: %v\n", job.Table.Name, err)
		}
		if resumeLastPK != nil {
			fmt.Printf("Resuming %s from chunk (lastPK=%v, rows=%d)\n", job.Table.Name, resumeLastPK, resumeRowsDone)
		}
	}

	// Handle truncation based on job type (skip if resuming)
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
					fmt.Printf("Warning: cleanup partition data for %s: %v\n", job.Table.Name, err)
				}
			}
		}
	} else if job.Table.SupportsKeysetPagination() {
		// Chunk-level resume: delete any rows beyond the saved lastPK
		// This handles partial data written after the last saved checkpoint
		if err := cleanupPartialData(ctx, tgtPool, cfg.Target.Schema, job.Table.Name, job.Table.PrimaryKey[0], resumeLastPK); err != nil {
			fmt.Printf("Warning: cleaning up partial data for %s: %v\n", job.Table.Name, err)
		}
	}

	// Build column list
	cols := make([]string, len(job.Table.Columns))
	colTypes := make([]string, len(job.Table.Columns))
	for i, c := range job.Table.Columns {
		cols[i] = c.Name
		colTypes[i] = strings.ToLower(c.DataType)
	}

	// Choose pagination strategy
	if job.Table.SupportsKeysetPagination() {
		return executeKeysetPagination(ctx, srcPool, tgtPool, cfg, job, cols, colTypes, prog, resumeLastPK, resumeRowsDone)
	}

	// Fall back to ROW_NUMBER pagination for composite/varchar PKs or no PK
	return executeRowNumberPagination(ctx, srcPool, tgtPool, cfg, job, cols, colTypes, prog, resumeRowsDone)
}

// cleanupPartitionData removes any existing data for a partition's PK range (idempotent retry) - PostgreSQL version
func cleanupPartitionData(ctx context.Context, pgPool *pgxpool.Pool, schema string, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	pkCol := job.Table.PrimaryKey[0]
	query := fmt.Sprintf(
		`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
		schema, job.Table.Name, pkCol, pkCol,
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

	// Check target type and use appropriate method
	switch p := tgtPool.(type) {
	case *target.Pool:
		// PostgreSQL target
		query := fmt.Sprintf(
			`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
			schema, job.Table.Name, pkCol, pkCol,
		)
		_, err := p.Pool().Exec(ctx, query, job.Partition.MinPK, job.Partition.MaxPK)
		return err
	case *target.MSSQLPool:
		// SQL Server target
		query := fmt.Sprintf(
			`DELETE FROM [%s].[%s] WHERE [%s] >= @p1 AND [%s] <= @p2`,
			schema, job.Table.Name, pkCol, pkCol,
		)
		_, err := p.DB().ExecContext(ctx, query,
			sql.Named("p1", job.Partition.MinPK),
			sql.Named("p2", job.Partition.MaxPK))
		return err
	default:
		return fmt.Errorf("unsupported target pool type: %T", tgtPool)
	}
}

// cleanupPartialData removes rows beyond the saved lastPK for chunk-level resume
func cleanupPartialData(ctx context.Context, tgtPool pool.TargetPool, schema, tableName, pkCol string, lastPK any) error {
	switch p := tgtPool.(type) {
	case *target.Pool:
		// PostgreSQL target
		deleteQuery := fmt.Sprintf(`DELETE FROM %s.%q WHERE %q > $1`,
			schema, tableName, pkCol)
		result, err := p.Pool().Exec(ctx, deleteQuery, lastPK)
		if err != nil {
			return err
		}
		if result.RowsAffected() > 0 {
			fmt.Printf("Cleaned up %d partial rows from %s (pk > %v)\n", result.RowsAffected(), tableName, lastPK)
		}
		return nil
	case *target.MSSQLPool:
		// SQL Server target
		deleteQuery := fmt.Sprintf(`DELETE FROM [%s].[%s] WHERE [%s] > @p1`,
			schema, tableName, pkCol)
		result, err := p.DB().ExecContext(ctx, deleteQuery, sql.Named("p1", lastPK))
		if err != nil {
			return err
		}
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			fmt.Printf("Cleaned up %d partial rows from %s (pk > %v)\n", rowsAffected, tableName, lastPK)
		}
		return nil
	default:
		return fmt.Errorf("unsupported target pool type: %T", tgtPool)
	}
}

// executeKeysetPagination uses WHERE pk > last_pk for efficient pagination
// with async read-ahead pipelining to overlap reads and writes
func executeKeysetPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, colTypes []string,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}
	pkCol := job.Table.PrimaryKey[0]

	// Use direction-aware syntax
	syntax := newDBSyntax(srcPool.DBType())
	colList := syntax.columnList(cols)
	tableHint := syntax.tableHint(cfg.Migration.StrictConsistency)
	chunkSize := cfg.Migration.ChunkSize

	// Get PK range for parallel readers
	var minPKVal, maxPKVal any
	if job.Partition != nil {
		minPKVal = job.Partition.MinPK
		maxPKVal = job.Partition.MaxPK
	} else {
		// For non-partitioned tables, get min and max PK
		minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s %s",
			syntax.quoteIdent(pkCol), syntax.quoteIdent(pkCol),
			syntax.qualifiedTable(job.Table.Schema, job.Table.Name), tableHint)
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

	// Create buffered channel for read-ahead pipeline
	bufferSize := cfg.Migration.ReadAheadBuffers
	if bufferSize < 0 {
		bufferSize = 0
	}
	chunkChan := make(chan chunkResult, bufferSize)

	// Determine number of parallel readers
	numReaders := cfg.Migration.ParallelReaders
	if numReaders < 1 {
		numReaders = 1
	}

	// Split PK range for parallel readers
	pkRanges := splitPKRange(minPKVal, maxPKVal, numReaders)
	actualReaders := len(pkRanges)

	// Start parallel reader goroutines
	var readerWg sync.WaitGroup
	for _, pkr := range pkRanges {
		readerWg.Add(1)
		go func(rangeMinPK, rangeMaxPK any) {
			defer readerWg.Done()
			lastPK := rangeMinPK

			for {
				select {
				case <-ctx.Done():
					chunkChan <- chunkResult{err: ctx.Err()}
					return
				default:
				}

				// Always use bounded query for parallel readers
				query := syntax.buildKeysetQuery(colList, pkCol, job.Table.Schema, job.Table.Name, tableHint, true)
				args := syntax.buildKeysetArgs(lastPK, rangeMaxPK, chunkSize, true)

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
					queryTime: queryTime,
					scanTime:  scanTime,
					readEnd:   time.Now(),
				}

				if len(chunk) < chunkSize {
					return // This reader is done
				}
			}
		}(pkr.minPK, pkr.maxPK)
	}

	// Close chunkChan when all readers are done
	go func() {
		readerWg.Wait()
		close(chunkChan)
	}()

	_ = actualReaders // Used for logging

	// Parallel writers setup
	numWriters := cfg.Migration.WriteAheadWriters
	if numWriters < 1 {
		numWriters = 1
	}

	// Channel for dispatching write jobs to worker pool
	writeJobChan := make(chan [][]any, bufferSize)

	// Shared state for parallel writers
	var totalWriteTime int64 // atomic, nanoseconds
	var totalWritten int64   // atomic, rows written
	var writeErr atomic.Pointer[error]
	var writerWg sync.WaitGroup

	// Writer cancellation context
	writerCtx, cancelWriters := context.WithCancel(ctx)
	defer cancelWriters()

	// Start write workers
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for rows := range writeJobChan {
				select {
				case <-writerCtx.Done():
					return
				default:
				}

				writeStart := time.Now()
				if err := writeChunkGeneric(writerCtx, tgtPool, cfg.Target.Schema, job.Table.Name, cols, rows); err != nil {
					writeErr.CompareAndSwap(nil, &err)
					cancelWriters()
					return
				}
				writeDuration := time.Since(writeStart)
				atomic.AddInt64(&totalWriteTime, int64(writeDuration))

				// Update progress atomically
				rowCount := int64(len(rows))
				atomic.AddInt64(&totalWritten, rowCount)
				prog.Add(rowCount)
			}
		}()
	}

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	totalTransferred := resumeRowsDone
	chunkCount := 0
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var lastPK any

	// Process chunks and dispatch writes
	for result := range chunkChan {
		if result.err != nil {
			cancelWriters()
			close(writeJobChan)
			return stats, result.err
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
		select {
		case writeJobChan <- result.rows:
		case <-writerCtx.Done():
			close(writeJobChan)
			if err := writeErr.Load(); err != nil {
				return stats, fmt.Errorf("writing chunk: %w", *err)
			}
			return stats, writerCtx.Err()
		}

		// Log overlap stats periodically
		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			fmt.Printf("  [pipeline] %s chunk=%d overlap=%v chanWait=%v buffers=%d writers=%d\n",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	// Close write job channel and wait for writers to finish
	close(writeJobChan)
	writerWg.Wait()

	// Check for write errors
	if err := writeErr.Load(); err != nil {
		return stats, fmt.Errorf("writing chunk: %w", *err)
	}

	// Aggregate stats
	stats.WriteTime = time.Duration(atomic.LoadInt64(&totalWriteTime))
	totalTransferred += atomic.LoadInt64(&totalWritten)
	stats.Rows = totalTransferred

	// Save final progress
	if job.Saver != nil && job.TaskID > 0 && lastPK != nil {
		var partID *int
		if job.Partition != nil {
			partID = &job.Partition.PartitionID
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partID, lastPK, totalTransferred, job.Table.RowCount); err != nil {
			fmt.Printf("Warning: saving progress for %s: %v\n", job.Table.Name, err)
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
	cols, colTypes []string,
	prog *progress.Tracker,
	resumeRowsDone int64,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}

	// Use direction-aware syntax
	syntax := newDBSyntax(srcPool.DBType())
	colList := syntax.columnList(cols)
	tableHint := syntax.tableHint(cfg.Migration.StrictConsistency)

	// Build ORDER BY clause from PK columns
	// Tables without PK cannot be migrated safely - fail fast
	if len(job.Table.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key - cannot guarantee data correctness with ROW_NUMBER pagination. "+
			"Add a primary key to the table or exclude it from migration", job.Table.FullName())
	}

	pkCols := make([]string, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		pkCols[i] = syntax.quoteIdent(pk)
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
	if resumeRowsDone > 0 {
		initialRowNum = resumeRowsDone
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
			query := syntax.buildRowNumberQuery(colList, orderBy, job.Table.Schema, job.Table.Name, tableHint)
			args := syntax.buildRowNumberArgs(rowNum, effectiveChunkSize)

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
				queryTime: queryTime,
				scanTime:  scanTime,
				readEnd:   time.Now(),
			}

			rowNum = newRowNum

			if len(chunk) < effectiveChunkSize {
				chunkChan <- chunkResult{done: true}
				return
			}
		}
		chunkChan <- chunkResult{done: true}
	}()

	// Parallel writers setup
	numWriters := cfg.Migration.WriteAheadWriters
	if numWriters < 1 {
		numWriters = 1
	}

	// Channel for dispatching write jobs to worker pool
	writeJobChan := make(chan [][]any, bufferSize)

	// Shared state for parallel writers
	var totalWriteTime int64 // atomic, nanoseconds
	var totalWritten int64   // atomic, rows written
	var writeErr atomic.Pointer[error]
	var writerWg sync.WaitGroup

	// Writer cancellation context
	writerCtx, cancelWriters := context.WithCancel(ctx)
	defer cancelWriters()

	// Start write workers
	for i := 0; i < numWriters; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for rows := range writeJobChan {
				select {
				case <-writerCtx.Done():
					return
				default:
				}

				writeStart := time.Now()
				if err := writeChunkGeneric(writerCtx, tgtPool, cfg.Target.Schema, job.Table.Name, cols, rows); err != nil {
					writeErr.CompareAndSwap(nil, &err)
					cancelWriters()
					return
				}
				writeDuration := time.Since(writeStart)
				atomic.AddInt64(&totalWriteTime, int64(writeDuration))

				// Update progress atomically
				rowCount := int64(len(rows))
				atomic.AddInt64(&totalWritten, rowCount)
				prog.Add(rowCount)
			}
		}()
	}

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	chunkCount := 0
	totalTransferred := int64(0)
	var currentRowNum int64
	var totalOverlap time.Duration
	var lastWriteEnd time.Time

	// Process chunks and dispatch writes
	for result := range chunkChan {
		if result.err != nil {
			cancelWriters()
			close(writeJobChan)
			return stats, result.err
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
		select {
		case writeJobChan <- result.rows:
		case <-writerCtx.Done():
			close(writeJobChan)
			if err := writeErr.Load(); err != nil {
				return stats, fmt.Errorf("writing chunk: %w", *err)
			}
			return stats, writerCtx.Err()
		}

		// Log overlap stats periodically
		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			fmt.Printf("  [pipeline] %s chunk=%d overlap=%v chanWait=%v buffers=%d writers=%d\n",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	// Close write job channel and wait for writers to finish
	close(writeJobChan)
	writerWg.Wait()

	// Check for write errors
	if err := writeErr.Load(); err != nil {
		return stats, fmt.Errorf("writing chunk: %w", *err)
	}

	// Aggregate stats
	stats.WriteTime = time.Duration(atomic.LoadInt64(&totalWriteTime))
	totalTransferred += atomic.LoadInt64(&totalWritten)
	stats.Rows = totalTransferred

	// Save final progress
	if job.Saver != nil && job.TaskID > 0 {
		var partID *int
		var partitionRows int64
		if job.Partition != nil {
			partID = &job.Partition.PartitionID
			partitionRows = job.Partition.RowCount
		} else {
			partitionRows = job.Table.RowCount
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partID, currentRowNum, totalTransferred, partitionRows); err != nil {
			fmt.Printf("Warning: saving progress for %s: %v\n", job.Table.Name, err)
		}
	}

	return stats, nil
}

// scanRows scans database rows into a slice of values with proper type handling
func scanRows(rows *sql.Rows, cols, colTypes []string) ([][]any, any, error) {
	numCols := len(cols)
	var result [][]any
	var lastPK any

	for rows.Next() {
		row := make([]any, numCols)
		ptrs := make([]any, numCols)
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
		if len(result) > 0 {
			lastPK = result[len(result)-1][0] // Assume first column is PK for keyset
		}
	}

	return result, lastPK, rows.Err()
}

// processValue handles type conversions for PostgreSQL compatibility
func processValue(val any, colType string) any {
	if val == nil {
		return nil
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
	switch p := tgtPool.(type) {
	case *target.Pool:
		// PostgreSQL target - use COPY
		return writeChunk(ctx, p.Pool(), schema, table, cols, rows)
	case *target.MSSQLPool:
		// SQL Server target - use BULK INSERT or batch INSERT
		return p.WriteChunk(ctx, schema, table, cols, rows)
	default:
		return fmt.Errorf("unsupported target pool type: %T", tgtPool)
	}
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
