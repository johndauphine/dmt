package transfer

import (
	"context"
	"database/sql"
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
	db := srcPool.DB()
	pkCol := job.Table.PrimaryKey[0]

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %s", srcPool.DBType())
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

	numReaders := cfg.Migration.ParallelReaders
	if numReaders < 1 {
		numReaders = 1
	}

	// Split PK range for parallel readers — unless a previous segment
	// persisted per-range watermarks (#464), in which case those ranges
	// (and their completion flags) are restored verbatim.
	var pkRanges []pkRange
	var rangeCompleted []bool
	if len(resumeRanges) > 0 {
		pkRanges, rangeCompleted = restoredPKRanges(resumeRanges, minPKVal)
	} else {
		pkRanges = splitPKRange(minPKVal, maxPKVal, numReaders)
	}

	producer := &keysetProducer{
		db:             db,
		dialect:        srcDialect,
		colList:        colList,
		tableHint:      tableHint,
		job:            job,
		pkCol:          pkCol,
		pkIdx:          pkIdx,
		valueConvs:     valueConvs,
		convIdx:        convIdx,
		numCols:        len(cols),
		pkRanges:       pkRanges,
		rangeCompleted: rangeCompleted,
		numReaders:     numReaders,
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
		newAckHandler: func(wp *writerPool, cb tunerCallbacks) func(writeAck) {
			coord = newKeysetCheckpointCoordinator(job, pkRanges, rangeCompleted, resumeRowsDone, wp.TotalWrittenPtr(), cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		saveFinal: func(last chunkResult, totalTransferred int64) {
			if job.Saver == nil || job.TaskID <= 0 || last.lastPK == nil {
				return
			}
			var partitionID *int
			if job.Partition != nil {
				partitionID = &job.Partition.PartitionID
			}
			finalLastPK := coord.finalCheckpoint(last.lastPK)
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalLastPK, totalTransferred, job.Table.RowCount, coord.rangeState()); err != nil {
				logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
			}
		},
	})
}

// keysetProducer runs one reader goroutine per incomplete PK range, each
// paginating with WHERE pk > last_pk AND pk <= range_max.
type keysetProducer struct {
	db         *sql.DB
	dialect    driver.Dialect
	colList    string
	tableHint  string
	job        Job
	pkCol      string
	pkIdx      int
	valueConvs []func(any) any
	convIdx    []int
	numCols    int

	pkRanges       []pkRange
	rangeCompleted []bool
	numReaders     int
}

func (p *keysetProducer) readerCount() int { return p.numReaders }

// produce starts the parallel readers and blocks until they all finish.
// Ranges a previous run segment completed keep their slot (the coordinator
// indexes states by readerID) but spawn no reader (#464).
func (p *keysetProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	var readerWg sync.WaitGroup
	for readerID, pkr := range p.pkRanges {
		if p.rangeCompleted != nil && p.rangeCompleted[readerID] {
			continue
		}
		readerWg.Add(1)
		go func(readerID int, rangeMinPK, rangeMaxPK any) {
			defer readerWg.Done()
			p.readRange(ctx, env, out, readerID, rangeMinPK, rangeMaxPK)
		}(readerID, pkr.minPK, pkr.maxPK)
	}
	readerWg.Wait()
}

// readRange pages one PK range from the source, sending each chunk to out.
func (p *keysetProducer) readRange(ctx context.Context, env pipelineEnv, out chan<- chunkResult, readerID int, rangeMinPK, rangeMaxPK any) {
	lastPK := rangeMinPK
	seq := int64(0)

	for {
		select {
		case <-ctx.Done():
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return
		default:
		}

		// Memory pressure check — pause if heap is above threshold
		if !env.memGuard.waitIfNeeded(ctx) {
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return
		}

		// Read chunk_size dynamically so guardrail reductions take effect immediately
		chunkSize := env.chunkSize()

		// Always use bounded query for parallel readers
		query := p.dialect.BuildKeysetQuery(p.colList, p.pkCol, p.job.Table.Schema, p.job.Table.Name, p.tableHint, true, p.job.DateFilter)
		args := p.dialect.BuildKeysetArgs(lastPK, rangeMaxPK, chunkSize, true, p.job.DateFilter)

		// Time the query
		queryStart := time.Now()
		rows, err := p.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("keyset query: %w", err)})
			return
		}

		// Time the scan
		scanStart := time.Now()
		chunk, _, err := scanRows(rows, p.numCols, p.valueConvs, p.convIdx)
		rows.Close()
		scanTime := time.Since(scanStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
			return
		}

		if len(chunk) == 0 {
			return // This reader is done
		}

		if logging.IsDebug() {
			logging.Debug("Reader[%d]: chunk #%d read %d rows (query=%v, scan=%v)", readerID, seq, len(chunk), queryTime, scanTime)
		}
		// Update lastPK for next iteration
		lastPK = chunk[len(chunk)-1][p.pkIdx]

		var sendStart time.Time
		if logging.IsDebug() {
			sendStart = time.Now()
		}
		if !sendChunkOrCancel(ctx, out, chunkResult{
			rows:      chunk,
			lastPK:    lastPK,
			readerID:  readerID,
			seq:       seq,
			queryTime: queryTime,
			scanTime:  scanTime,
			readEnd:   time.Now(),
		}) {
			return
		}
		if logging.IsDebug() {
			if sendWait := time.Since(sendStart); sendWait > 500*time.Millisecond {
				logging.Debug("Reader[%d]: blocked %v sending chunk #%d to chunkChan (len=%d, cap=%d)",
					readerID, sendWait, seq, len(out), cap(out))
			}
		}
		seq++

		if len(chunk) < chunkSize {
			return // This reader is done
		}
	}
}
