package transfer

import (
	"context"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/target"
)

// writerPool wraps pool.WriterPool with transfer-specific functionality.
type writerPool struct {
	*pool.WriterPool
	tgtPool                pool.TargetPool
	targetSchema           string
	targetTable            string
	targetCols             []string
	colTypes               []string
	colSRIDs               []int
	targetPKCols           []string
	partitionID            *int
	useUpsert              bool
	upsertMergeChunkSizeFn func() int
	batchSizeFn            func() int // dynamic writer batch size (for target DB sub-batching)

	// AI-driven error recovery
	tuner      RuntimeTuner
	aiAdjuster WriteErrorAdjuster
	tableName  string // original table name (for tuner per-table overrides)
}

// writerPoolConfig holds the configuration for creating a writer pool.
type writerPoolConfig struct {
	NumWriters             int
	BufferSize             int
	JobBufferSize          int   // computed by pool.CalculateJobBufferSize
	UseUpsert              bool
	UpsertMergeChunkSizeFn func() int
	BatchSizeFn            func() int // dynamic writer batch size
	TargetSchema           string
	TargetTable            string
	TargetCols             []string
	ColTypes               []string
	ColSRIDs               []int
	TargetPKCols           []string
	PartitionID            *int
	TgtPool                pool.TargetPool
	Prog                   *progress.Tracker
	EnableAck              bool // Whether to enable ack channel for checkpointing

	// AI-driven error recovery
	Tuner      RuntimeTuner
	AIAdjuster WriteErrorAdjuster
	TableName  string
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	// Default upsert merge chunk size callback if not specified
	upsertFn := cfg.UpsertMergeChunkSizeFn
	if upsertFn == nil {
		upsertFn = func() int { return 5000 }
	}

	// Default batch size callback (returns 0 = let writer use its default)
	batchFn := cfg.BatchSizeFn
	if batchFn == nil {
		batchFn = func() int { return 0 }
	}

	wp := &writerPool{
		tgtPool:                cfg.TgtPool,
		targetSchema:           cfg.TargetSchema,
		targetTable:            cfg.TargetTable,
		targetCols:             cfg.TargetCols,
		colTypes:               cfg.ColTypes,
		colSRIDs:               cfg.ColSRIDs,
		targetPKCols:           cfg.TargetPKCols,
		partitionID:            cfg.PartitionID,
		useUpsert:              cfg.UseUpsert,
		upsertMergeChunkSizeFn: upsertFn,
		batchSizeFn:            batchFn,
		tuner:                  cfg.Tuner,
		aiAdjuster:             cfg.AIAdjuster,
		tableName:              cfg.TableName,
	}

	// Create the base writer pool with our write function
	wp.WriterPool = pool.NewWriterPool(ctx, pool.WriterPoolConfig{
		NumWriters:    cfg.NumWriters,
		BufferSize:    cfg.BufferSize,
		JobBufferSize: cfg.JobBufferSize,
		WriteFunc:     wp.executeWrite,
		Prog:          cfg.Prog,
		EnableAck:     cfg.EnableAck,
	})

	return wp
}

// executeWrite handles both regular writes and upserts with chunking.
// On failure, it asks the AI for a new chunk_size and retries with smaller sub-batches.
func (wp *writerPool) executeWrite(ctx context.Context, writerID int, rows [][]any) error {
	batchSize := wp.batchSizeFn()
	var err error
	if wp.useUpsert {
		err = wp.executeUpsertWithChunking(ctx, writerID, rows, batchSize)
	} else {
		err = writeChunkGeneric(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable, wp.targetCols, rows, batchSize)
	}

	if err == nil {
		return nil
	}

	// Try AI-driven chunk size adjustment for error recovery
	newChunkSize := wp.evaluateAndAdjust(ctx, rows, err)
	if newChunkSize <= 0 || newChunkSize >= len(rows) {
		return err // AI says not a chunk_size issue, or can't help
	}

	logging.Info("Retrying table %s with chunk_size=%d (was %d rows)", wp.tableName, newChunkSize, len(rows))

	// Retry with smaller sub-batches
	return wp.retryWithChunkSize(ctx, writerID, rows, newChunkSize)
}

// evaluateAndAdjust asks the AI adjuster to recommend a new batch_size for the error.
// Returns the recommended size, or 0 if the error is not batch-size related.
func (wp *writerPool) evaluateAndAdjust(ctx context.Context, rows [][]any, writeErr error) int {
	if wp.aiAdjuster == nil {
		return 0
	}

	errCtx := WriteErrorContext{
		TableName:    wp.tableName,
		ColumnCount:  len(wp.targetCols),
		ChunkSize:    len(rows),
		RowCount:     len(rows),
		ErrorMessage: writeErr.Error(),
		TargetDBType: wp.tgtPool.DBType(),
	}

	newSize := wp.aiAdjuster.EvaluateWriteError(ctx, errCtx)
	if newSize > 0 && wp.tuner != nil {
		// Set both chunk size (for reader retry batching) and batch size (for writer sub-batching)
		wp.tuner.SetTableChunkSize(wp.tableName, newSize)
		wp.tuner.SetTableBatchSize(wp.tableName, newSize)
	}
	return newSize
}

// retryWithChunkSize re-batches the failed rows into smaller sub-batches and writes them.
func (wp *writerPool) retryWithChunkSize(ctx context.Context, writerID int, rows [][]any, chunkSize int) error {
	batchSize := wp.batchSizeFn()
	for i := 0; i < len(rows); i += chunkSize {
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[i:end]
		var err error
		if wp.useUpsert {
			err = writeChunkUpsertWithWriter(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable,
				wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, chunk, writerID, wp.partitionID, batchSize)
		} else {
			err = writeChunkGeneric(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable, wp.targetCols, chunk, batchSize)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// executeUpsertWithChunking executes an upsert job, chunking if necessary based on UpsertMergeChunkSize.
// Chunking reduces memory pressure on the target database when processing large batches.
func (wp *writerPool) executeUpsertWithChunking(ctx context.Context, writerID int, rows [][]any, batchSize int) error {
	mergeChunkSize := wp.upsertMergeChunkSizeFn()
	if mergeChunkSize <= 0 {
		mergeChunkSize = 5000
	}

	logging.Debug("executeUpsertWithChunking: rows=%d, mergeChunkSize=%d", len(rows), mergeChunkSize)

	// If batch is small enough, execute directly
	if len(rows) <= mergeChunkSize {
		return writeChunkUpsertWithWriter(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable,
			wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, rows, writerID, wp.partitionID, batchSize)
	}

	// Chunk the rows to reduce memory pressure on target database
	numChunks := (len(rows) + mergeChunkSize - 1) / mergeChunkSize
	logging.Debug("Upsert chunking: %d rows into %d chunks of max %d rows", len(rows), numChunks, mergeChunkSize)

	for i := 0; i < len(rows); i += mergeChunkSize {
		end := i + mergeChunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[i:end]
		if err := writeChunkUpsertWithWriter(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable,
			wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, chunk, writerID, wp.partitionID, batchSize); err != nil {
			return err
		}
	}

	return nil
}

// submit sends a write job to the pool. Returns false if context is cancelled.
func (wp *writerPool) submit(job writeJob) bool {
	return wp.WriterPool.Submit(pool.WriteJob{
		Rows:     job.rows,
		ReaderID: job.readerID,
		Seq:      job.seq,
		LastPK:   job.lastPK,
		RowNum:   job.rowNum,
	})
}

// wait closes the job channel and waits for all workers to complete.
func (wp *writerPool) wait() {
	wp.WriterPool.Wait()
}

// error returns any write error that occurred.
func (wp *writerPool) error() error {
	return wp.WriterPool.Error()
}

// writeTime returns the total time spent writing.
func (wp *writerPool) writeTime() time.Duration {
	return wp.WriterPool.WriteTime()
}

// written returns the total rows written.
func (wp *writerPool) written() int64 {
	return wp.WriterPool.Written()
}

// acks returns the ack channel for checkpoint coordination.
func (wp *writerPool) acks() <-chan pool.WriteAck {
	return wp.WriterPool.Acks()
}

// startAckProcessor starts a goroutine to process acks with the given handler.
func (wp *writerPool) startAckProcessor(handler func(writeAck)) {
	wp.WriterPool.StartAckProcessor(func(ack pool.WriteAck) {
		handler(writeAck{
			readerID: ack.ReaderID,
			seq:      ack.Seq,
			lastPK:   ack.LastPK,
			rowNum:   ack.RowNum,
		})
	})
}

// start begins the writer worker goroutines.
func (wp *writerPool) start() {
	wp.WriterPool.Start()
}

// buildTargetPKCols sanitizes PK columns for the target database.
func buildTargetPKCols(pkCols []string, tgtPool pool.TargetPool) []string {
	isPGTarget := tgtPool.DBType() == "postgres"
	targetPKCols := make([]string, len(pkCols))
	for i, pk := range pkCols {
		if isPGTarget {
			targetPKCols[i] = target.SanitizePGIdentifier(pk)
		} else {
			targetPKCols[i] = pk
		}
	}
	return targetPKCols
}
