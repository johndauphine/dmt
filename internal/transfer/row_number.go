package transfer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// executeRowNumberPagination uses ROW_NUMBER for composite/varchar PKs
// with async read-ahead pipelining to overlap reads and writes. It sets up
// the ROW_NUMBER-specific pieces — ORDER BY construction, row-range resume,
// idempotent replay routing (#227/#540), and the checkpoint coordinator —
// and delegates the shared pipeline to runPipeline (#614).
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
	tuner RuntimeTuner,
	writeErrorAdjuster WriteErrorAdjuster,
) (*TransferStats, error) {
	db := sourceQueryerForJob(ctx, srcPool.DB(), job)

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %q", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	// Source-dialect value normalization, resolved once per transfer (#477).
	valueConvs := srcDialect.ValueConverters(colTypes, tgtPool.DBType())
	convIdx := buildConvIdx(valueConvs)
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

	// Determine row range for this job. The final ROW_NUMBER range is
	// intentionally unbounded so stats-estimated RowCount undercounts cannot
	// drop tail rows.
	var startRow, endRow int64
	boundedEnd := false
	if job.Partition != nil && job.Partition.EndRow > 0 {
		// Partitioned: use partition boundaries
		startRow = job.Partition.StartRow
		endRow = job.Partition.EndRow
		boundedEnd = job.Partition.EndRow < job.Table.RowCount
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
	if boundedEnd && initialRowNum > endRow {
		initialRowNum = endRow
	}

	// Get partition ID and row count for staging table naming and checkpointing
	var partitionID *int
	var partitionRows int64
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		partitionRows = job.Partition.RowCount
	} else {
		partitionRows = job.Table.RowCount
	}

	// #227/#540: on ROW_NUMBER replay, route plain inserts through the
	// idempotent-on-dup path so replayed committed rows become no-ops.
	idempotentOnDup := (job.IsResume || job.ReplayPossible) && cfg.Migration.TargetMode != "upsert" && job.Table.HasPK()
	if idempotentOnDup {
		partitionStr := "single"
		if partitionID != nil {
			partitionStr = fmt.Sprintf("p%d", *partitionID)
		}
		logging.Debug("ROW_NUMBER resume for %s: enabling idempotent-on-dup writer (start=%d, resume=%d, partition=%s)",
			job.Table.Name, startRow, initialRowNum, partitionStr)
	}

	producer := &rowNumberProducer{
		db:            db,
		dialect:       srcDialect,
		colList:       colList,
		orderBy:       orderBy,
		tableHint:     tableHint,
		job:           job,
		valueConvs:    valueConvs,
		convIdx:       convIdx,
		numCols:       len(cols),
		initialRowNum: initialRowNum,
		endRow:        endRow,
		boundedEnd:    boundedEnd,
	}

	var coord *rowNumberCheckpointCoordinator
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
		idempotentOnDup: idempotentOnDup,
		resumeRowsDone:  resumeRowsDone,
		newAckHandler: func(cb tunerCallbacks, saver ProgressSaver) func(writeAck) {
			coord = newRowNumberCheckpointCoordinator(saver, job, partitionID, partitionRows, initialRowNum, resumeRowsDone, cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		saveFinal: func(last chunkResult, totalTransferred int64) (bool, error) {
			if job.Saver == nil || job.TaskID <= 0 {
				return false, nil
			}
			finalRowNum := coord.finalRowNum(last.rowNum)
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalRowNum, totalTransferred, partitionRows, ""); err != nil {
				return false, err
			}
			return true, nil
		},
	})
}

// rowNumberProducer runs a single reader that pages the table in PK order
// via the dialect's ROW_NUMBER query. Composite/varchar PKs give no range
// to split, so there is exactly one reader.
type rowNumberProducer struct {
	db         sourceQueryer
	dialect    driver.Dialect
	colList    string
	orderBy    string
	tableHint  string
	job        Job
	valueConvs []func(any) any
	convIdx    []int
	numCols    int

	initialRowNum int64
	endRow        int64
	boundedEnd    bool
}

func (p *rowNumberProducer) readerCount() int { return 1 }

// produce pages the row range from the source, sending each chunk to out,
// and blocks until the range is exhausted (or the context is cancelled).
func (p *rowNumberProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	rowNum := p.initialRowNum
	seq := int64(0)

	for !p.boundedEnd || rowNum < p.endRow {
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

		// Adjust chunk size if near end of partition
		effectiveChunkSize := chunkSize
		if p.boundedEnd && rowNum+int64(chunkSize) > p.endRow {
			effectiveChunkSize = int(p.endRow - rowNum)
		}
		if effectiveChunkSize <= 0 {
			sendChunkOrCancel(ctx, out, chunkResult{done: true})
			return
		}

		// ROW_NUMBER pagination with direction-aware syntax
		query := p.dialect.BuildRowNumberQuery(p.colList, p.orderBy, p.job.Table.Schema, p.job.Table.Name, p.tableHint, p.job.DateFilter)
		args := p.dialect.BuildRowNumberArgs(rowNum, effectiveChunkSize, p.job.DateFilter)

		// Time the query
		queryStart := time.Now()
		rows, err := p.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("row_number query: %w", err)})
			return
		}

		// Time the scan
		scanStart := time.Now()
		chunk, chunkBytes, err := scanRows(rows, p.numCols, p.valueConvs, p.convIdx)
		rows.Close()
		scanTime := time.Since(scanStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
			return
		}

		if len(chunk) == 0 {
			sendChunkOrCancel(ctx, out, chunkResult{done: true})
			return
		}

		// Reserve this chunk's measured bytes against the shared budget
		// before it enters the pipeline (#617). Single blocking acquire while
		// holding no other reservation, so it can never hold-and-wait.
		reserved, ok := env.acquireMem(ctx, chunkBytes)
		if !ok {
			// Propagate a real reader cancellation; on writer-pool failure
			// (reader ctx still live) stop quietly — wp.error() carries it.
			if err := ctx.Err(); err != nil {
				sendChunkOrCancel(ctx, out, chunkResult{err: err})
			}
			return
		}

		// Update rowNum for progress tracking
		newRowNum := rowNum + int64(len(chunk))

		var sendStart time.Time
		if logging.IsDebug() {
			sendStart = time.Now()
		}
		if !sendChunkOrCancel(ctx, out, chunkResult{
			rows:      chunk,
			rowNum:    newRowNum,
			readerID:  0,
			seq:       seq,
			bytes:     reserved,
			queryTime: queryTime,
			scanTime:  scanTime,
			readEnd:   time.Now(),
		}) {
			return
		}
		if logging.IsDebug() {
			if sendWait := time.Since(sendStart); sendWait > 500*time.Millisecond {
				logging.Debug("Reader[0]: blocked %v sending chunk #%d to chunkChan (ROW_NUMBER, len=%d, cap=%d)",
					sendWait, seq, len(out), cap(out))
			}
		}
		seq++

		rowNum = newRowNum

		if len(chunk) < effectiveChunkSize {
			sendChunkOrCancel(ctx, out, chunkResult{done: true})
			return
		}
	}
	sendChunkOrCancel(ctx, out, chunkResult{done: true})
}
