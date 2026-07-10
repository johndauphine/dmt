package transfer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// executeKeysetPagination uses WHERE pk > last_pk for efficient pagination
// with async read-ahead pipelining to overlap reads and writes. It sets up
// the keyset-specific pieces — PK boundary discovery, range splitting,
// per-range resume (#464), and the checkpoint coordinator — and delegates
// the shared pipeline to runPipeline (#614).
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
	resumeRanges []resumeRange,
	targetTableName string,
	tuner RuntimeTuner,
	writeErrorAdjuster WriteErrorAdjuster,
) (*TransferStats, error) {
	db := sourceQueryerForJob(ctx, srcPool.DB(), job)
	pkCol := job.Table.PrimaryKey[0]

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %s", srcPool.DBType())
	}
	strictStrategyName, strictStrategy, err := resolveStrictReaderStrategy(srcDialect)
	if err != nil {
		return nil, err
	}
	if job.StrictSnapshotEpoch != nil && job.StrictSnapshotEpoch.DBType() == "mssql" {
		strictStrategyName = strictParallelMigrationEpoch
		strictStrategy = migrationEpochReaderStrategy{}
	}
	if cfg.Migration.StrictConsistency && strictStrategy != nil && sourceQueryerFactoryForJob(ctx, job) == nil {
		strictStrategyName = strictParallelNone
		strictStrategy = nil
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	// Source-dialect value normalization, resolved once per transfer (#477).
	valueConvs := srcDialect.ValueConverters(colTypes, tgtPool.DBType())
	convIdx := buildConvIdx(valueConvs)
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)

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
		if err != nil {
			return nil, fmt.Errorf("keyset boundary query: %w", err)
		}
		if minPKVal == nil {
			return &TransferStats{}, nil // Empty table
		}
	}

	// Use resume point if available. When per-range watermarks were
	// persisted (#464), they carry the resume points instead — the single
	// watermark is only the cross-range safe minimum and would restart
	// faster readers' completed work.
	if resumeLastPK != nil && len(resumeRanges) == 0 {
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

	numReaders, joinSnapshotReaders, readerCountClamped := strictKeysetReaderPlan(
		cfg.Migration.StrictConsistency,
		strictStrategy,
		cfg.Migration.ParallelReaders,
		cfg.Migration.MaxSourceConnections,
	)
	if readerCountClamped && strictStrategy != nil {
		switch strictStrategyName {
		case strictParallelMigrationEpoch:
			logging.Warn("Table %s: strict_consistency clamped parallel_readers from %d to %d to honor max_source_connections=%d", job.Table.Name, cfg.Migration.ParallelReaders, numReaders, cfg.Migration.MaxSourceConnections)
		case strictParallelLockWindow, strictParallelTableSharedLock:
			logging.Warn("Table %s: strict_consistency clamped parallel_readers from %d to %d because the %s coordinator reserves one of max_source_connections=%d", job.Table.Name, cfg.Migration.ParallelReaders, numReaders, strictStrategyName, cfg.Migration.MaxSourceConnections)
		default:
			logging.Warn("Table %s: strict_consistency clamped parallel_readers from %d to %d because the lead snapshot transaction reserves one of max_source_connections=%d", job.Table.Name, cfg.Migration.ParallelReaders, numReaders, cfg.Migration.MaxSourceConnections)
		}
	}
	if cfg.Migration.StrictConsistency && strictStrategy != nil && !joinSnapshotReaders && cfg.Migration.MaxSourceConnections > 0 {
		switch strictStrategyName {
		case strictParallelLockWindow:
			logging.Warn("Table %s: strict_consistency uses one pinned source session because max_source_connections=%d leaves no connection for both the lock-window coordinator and a reader", job.Table.Name, cfg.Migration.MaxSourceConnections)
		case strictParallelTableSharedLock:
			logging.Warn("Table %s: strict_consistency uses one serializable reader because max_source_connections=%d leaves no connection for both the shared-lock coordinator and a reader", job.Table.Name, cfg.Migration.MaxSourceConnections)
		default:
			logging.Warn("Table %s: strict_consistency uses its lead snapshot transaction as the sole reader because max_source_connections=%d leaves no connection for an imported snapshot reader", job.Table.Name, cfg.Migration.MaxSourceConnections)
		}
	}

	var queryerForWorker sourceQueryerFactory
	if joinSnapshotReaders {
		queryerForWorker = sourceQueryerFactoryForJob(ctx, job)
		if queryerForWorker == nil {
			if strictStrategyName == strictParallelExportedSnapshot {
				return nil, fmt.Errorf("strict_consistency PostgreSQL snapshot reader factory is unavailable")
			}
			return nil, fmt.Errorf("strict_consistency %s reader factory is unavailable", strictStrategyName)
		}
	}

	// Split PK range into work-stealing sub-ranges (#615) — unless a
	// previous segment persisted per-range watermarks (#464), in which
	// case those ranges (and their completion flags) are restored
	// verbatim, whatever their count.
	var pkRanges []pkRange
	var rangeCompleted []bool
	if len(resumeRanges) > 0 {
		pkRanges, rangeCompleted = restoredPKRanges(resumeRanges, minPKVal)
	} else {
		numRanges := keysetWorkRangeCount(minPKVal, maxPKVal, numReaders, cfg.Migration.ChunkSize)
		pkRanges = splitPKRange(minPKVal, maxPKVal, numRanges, resumeLastPK == nil)
	}

	producer := &keysetProducer{
		db:               db,
		queryerForWorker: queryerForWorker,
		dialect:          srcDialect,
		colList:          colList,
		tableHint:        tableHint,
		job:              job,
		pkCol:            pkCol,
		pkIdx:            pkIdx,
		valueConvs:       valueConvs,
		convIdx:          convIdx,
		numCols:          len(cols),
		pkRanges:         pkRanges,
		rangeCompleted:   rangeCompleted,
		numReaders:       numReaders,
	}

	var coord *keysetCheckpointCoordinator
	return runPipeline(ctx, pipelineConfig{
		cfg:             cfg,
		job:             job,
		tgtPool:         tgtPool,
		prog:            prog,
		tuner:           tuner,
		adjuster:        writeErrorAdjuster,
		producer:        producer,
		targetTableName: targetTableName,
		targetCols:      targetCols,
		colTypes:        colTypes,
		colSRIDs:        colSRIDs,
		resumeRowsDone:  resumeRowsDone,
		newAckHandler: func(cb tunerCallbacks, saver ProgressSaver) func(writeAck) {
			coord = newKeysetCheckpointCoordinator(saver, job, pkRanges, rangeCompleted, resumeRowsDone, cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		saveFinal: func(last chunkResult, totalTransferred int64) (bool, error) {
			if job.Saver == nil || job.TaskID <= 0 || last.lastPK == nil {
				return false, nil
			}
			var partitionID *int
			if job.Partition != nil {
				partitionID = &job.Partition.PartitionID
			}
			finalLastPK := coord.finalCheckpoint(last.lastPK)
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalLastPK, totalTransferred, job.Table.RowCount, coord.rangeState()); err != nil {
				return false, err
			}
			return true, nil
		},
	})
}

// strictKeysetReaderPlan returns the keyset worker count and whether each
// worker must join the resolved shared-view strategy. Strict engines without
// a strategy retain their existing one-transaction, one-reader path. When the
// connection budget cannot fit both the strategy reservation and one worker,
// the coordinator queryer remains the sole reader.
func strictKeysetReaderPlan(strict bool, strategy strictReaderStrategy, requested, maxSourceConnections int) (readers int, joinSnapshotReaders, clamped bool) {
	if requested < 1 {
		requested = 1
	}
	if !strict || strategy == nil {
		if strict {
			return 1, false, requested > 1
		}
		return requested, false, false
	}
	reserved := strategy.joinBudget()
	if maxSourceConnections > 0 && maxSourceConnections <= reserved {
		return 1, false, requested > 1
	}
	if maxSourceConnections > reserved && requested > maxSourceConnections-reserved {
		return maxSourceConnections - reserved, true, true
	}
	return requested, true, false
}

// Work-stealing range sizing (#615). A static one-range-per-reader split
// finishes at straggler speed when PKs are skewed (bulk-deleted ID spans,
// snowflake IDs): one reader owns most rows while the rest exit early.
// Oversubscribing the split and letting readers pull the next unfinished
// range bounds the imbalance to one sub-range's worth of work.
const (
	// rangeOversubscription is the target sub-range count per reader.
	rangeOversubscription = 8
	// maxWorkRanges caps the split so the persisted per-range watermark
	// blob (#464 range_state) stays small (~40 bytes/range).
	maxWorkRanges = 256
)

// keysetWorkRangeCount decides how many sub-ranges to split [minPK, maxPK]
// into. Small or dense-narrow tables stay at numReaders ranges — a
// sub-range should hold several chunks, or stealing just shreds batching
// into short final pages. Non-numeric bounds fall back to numReaders
// (splitPKRange collapses those to a single range anyway).
func keysetWorkRangeCount(minPK, maxPK any, numReaders, chunkSize int) int {
	if numReaders <= 1 {
		return 1
	}
	maxRanges := numReaders * rangeOversubscription
	if maxRanges > maxWorkRanges {
		maxRanges = maxWorkRanges
	}
	minVal, okMin := parseNumericPK(minPK)
	maxVal, okMax := parseNumericPK(maxPK)
	if !okMin || !okMax || maxVal <= minVal || chunkSize <= 0 {
		return numReaders
	}
	// Keep each sub-range ≥ ~4 chunk-widths of PK distance. For dense PKs
	// this approximates 4 chunks of rows; sparse PKs get finer ranges,
	// which only means quicker steals.
	const minChunksPerRange = 4
	est := pkRangeDistance(minVal, maxVal) / (uint64(chunkSize) * minChunksPerRange)
	if est < uint64(numReaders) {
		return numReaders
	}
	if est < uint64(maxRanges) {
		return int(est)
	}
	return maxRanges
}

// keysetProducer runs numReaders goroutines that pull PK sub-ranges from a
// shared work queue (#615), each paginating its current range with an
// explicit first-page lower-bound operator and pk <= range_max.
type keysetProducer struct {
	db               sourceQueryer
	queryerForWorker sourceQueryerFactory
	dialect          driver.Dialect
	colList          string
	tableHint        string
	job              Job
	pkCol            string
	pkIdx            int
	valueConvs       []func(any) any
	convIdx          []int
	numCols          int

	pkRanges       []pkRange
	rangeCompleted []bool
	numReaders     int

	// rangesPerWorker[i] / rowsPerWorker[i] account for the sub-ranges and
	// rows worker i handled (#615 observability). Under a static split one
	// worker would own ~all rows of a skewed table; work-stealing spreads a
	// heavy band's sub-ranges across idle workers, so no single worker's
	// row share approaches the whole. Sized numReaders in produce; each
	// goroutine owns its own index (no races); read after produce returns.
	rangesPerWorker []int64
	rowsPerWorker   []int64
}

func (p *keysetProducer) readerCount() int { return p.numReaders }

// produce starts numReaders goroutines pulling range IDs from a shared
// work queue (#615) and blocks until they all finish. Ranges a previous
// run segment completed keep their slot (the coordinator indexes states
// by range ID) but are never queued (#464). A reader that hits an error
// or cancellation stops pulling; the consumer's shutdown cancels the rest.
func (p *keysetProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	queue := make(chan int, len(p.pkRanges))
	for rangeID := range p.pkRanges {
		if p.rangeCompleted != nil && p.rangeCompleted[rangeID] {
			continue
		}
		queue <- rangeID
	}
	close(queue)

	p.rangesPerWorker = make([]int64, p.numReaders)
	p.rowsPerWorker = make([]int64, p.numReaders)
	var readerWg sync.WaitGroup
	for i := 0; i < p.numReaders; i++ {
		readerWg.Add(1)
		go func(workerID int) {
			defer readerWg.Done()
			queryer := p.db
			if p.queryerForWorker != nil {
				var release func()
				var err error
				queryer, release, err = p.queryerForWorker(ctx, workerID)
				if err != nil {
					sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("starting keyset reader %d: %w", workerID, err)})
					return
				}
				defer release()
			}
			for rangeID := range queue {
				// Single writer per index — each goroutine owns its slot.
				p.rangesPerWorker[workerID]++
				pkr := p.pkRanges[rangeID]
				rows, ok := p.readRange(ctx, queryer, env, out, rangeID, pkr)
				p.rowsPerWorker[workerID] += rows
				if !ok {
					return
				}
			}
		}(i)
	}
	readerWg.Wait()
}

// readRange pages one PK range from the source, sending each chunk to out.
// Chunks carry the range ID (in chunkResult.readerID) so the checkpoint
// coordinator tracks watermarks per range regardless of which reader
// processed it. Returns false when the reader should stop pulling further
// ranges (error sent or context cancelled), true when the range is done.
// The first return is the number of rows read from this range.
func (p *keysetProducer) readRange(ctx context.Context, queryer sourceQueryer, env pipelineEnv, out chan<- chunkResult, rangeID int, pkr pkRange) (int64, bool) {
	lastPK := pkr.minPK
	inclusiveLowerBound := pkr.minInclusive
	seq := int64(0)
	var rowsRead int64

	for {
		select {
		case <-ctx.Done():
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return rowsRead, false
		default:
		}

		// Memory pressure check — pause if heap is above threshold
		if !env.memGuard.waitIfNeeded(ctx) {
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return rowsRead, false
		}

		// Read chunk_size dynamically so guardrail reductions take effect immediately
		chunkSize := env.chunkSize()

		// Always use bounded query for parallel readers
		query := p.dialect.BuildKeysetQuery(p.colList, p.pkCol, p.job.Table.Schema, p.job.Table.Name, p.tableHint, true, inclusiveLowerBound, p.job.DateFilter)
		args := p.dialect.BuildKeysetArgs(lastPK, pkr.maxPK, chunkSize, true, p.job.DateFilter)

		// Time the query
		queryStart := time.Now()
		rows, err := queryer.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("keyset query: %w", err)})
			return rowsRead, false
		}

		// Time the scan
		scanStart := time.Now()
		chunk, chunkBytes, err := scanRows(rows, p.numCols, p.valueConvs, p.convIdx)
		rows.Close()
		scanTime := time.Since(scanStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
			return rowsRead, false
		}

		if len(chunk) == 0 {
			return rowsRead, true // Range exhausted
		}
		rowsRead += int64(len(chunk))

		// Reserve this chunk's measured bytes against the shared budget
		// before it enters the pipeline (#617). A single blocking acquire —
		// the reader holds no other reservation while it waits, so writers
		// draining and releasing always let it proceed (no hold-and-wait).
		reserved, ok := env.acquireMem(ctx, chunkBytes)
		if !ok {
			// The wait was aborted. Propagate a genuine reader cancellation;
			// on a writer-pool failure (reader ctx still live) just stop —
			// wp.error() carries the real error (#617 codex review).
			if err := ctx.Err(); err != nil {
				sendChunkOrCancel(ctx, out, chunkResult{err: err})
			}
			return rowsRead, false
		}

		if logging.IsDebug() {
			logging.Debug("Range[%d]: chunk #%d read %d rows (query=%v, scan=%v)", rangeID, seq, len(chunk), queryTime, scanTime)
		}
		// Update lastPK for next iteration
		lastPK = chunk[len(chunk)-1][p.pkIdx]
		inclusiveLowerBound = false

		var sendStart time.Time
		if logging.IsDebug() {
			sendStart = time.Now()
		}
		if !sendChunkOrCancel(ctx, out, chunkResult{
			rows:      chunk,
			lastPK:    lastPK,
			readerID:  rangeID,
			seq:       seq,
			bytes:     reserved,
			queryTime: queryTime,
			scanTime:  scanTime,
			readEnd:   time.Now(),
		}) {
			return rowsRead, false
		}
		if logging.IsDebug() {
			if sendWait := time.Since(sendStart); sendWait > 500*time.Millisecond {
				logging.Debug("Range[%d]: blocked %v sending chunk #%d to chunkChan (len=%d, cap=%d)",
					rangeID, sendWait, seq, len(out), cap(out))
			}
		}
		seq++

		if len(chunk) < chunkSize {
			return rowsRead, true // Range exhausted
		}
	}
}
