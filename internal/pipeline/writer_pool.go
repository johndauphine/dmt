package pipeline

import (
	"context"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// writerPool wraps pool.WriterPool with pipeline-specific functionality.
type writerPool struct {
	*pool.WriterPool
	writer                 driver.Writer
	targetSchema           string
	targetTable            string
	targetCols             []string
	colTypes               []string
	colSRIDs               []int
	targetPKCols           []string
	partitionID            *int
	useUpsert              bool
	upsertMergeChunkSizeFn func() int // Function to get current upsert merge chunk size
}

// writerPoolConfig holds the configuration for creating a writer pool.
type writerPoolConfig struct {
	NumWriters             int
	BufferSize             int
	UseUpsert              bool
	TargetSchema           string
	TargetTable            string
	TargetCols             []string
	ColTypes               []string
	ColSRIDs               []int
	TargetPKCols           []string
	PartitionID            *int
	Writer                 driver.Writer
	Prog                   *progress.Tracker
	EnableAck              bool       // Whether to enable ack channel for checkpointing
	UpsertMergeChunkSizeFn func() int // Function to get current upsert merge chunk size (for dynamic tuning)
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	wp := &writerPool{
		writer:                 cfg.Writer,
		targetSchema:           cfg.TargetSchema,
		targetTable:            cfg.TargetTable,
		targetCols:             cfg.TargetCols,
		colTypes:               cfg.ColTypes,
		colSRIDs:               cfg.ColSRIDs,
		targetPKCols:           cfg.TargetPKCols,
		partitionID:            cfg.PartitionID,
		useUpsert:              cfg.UseUpsert,
		upsertMergeChunkSizeFn: cfg.UpsertMergeChunkSizeFn,
	}

	// Create the base writer pool with our write function
	wp.WriterPool = pool.NewWriterPool(ctx, pool.WriterPoolConfig{
		NumWriters: cfg.NumWriters,
		BufferSize: cfg.BufferSize,
		WriteFunc:  wp.executeWrite,
		Prog:       cfg.Prog,
		EnableAck:  cfg.EnableAck,
	})

	return wp
}

// executeWrite handles both regular writes and upserts with chunking.
func (wp *writerPool) executeWrite(ctx context.Context, writerID int, rows [][]any) error {
	if wp.useUpsert {
		return wp.executeUpsertJob(ctx, writerID, rows)
	}
	return wp.writer.WriteBatch(ctx, driver.WriteBatchOptions{
		Schema:  wp.targetSchema,
		Table:   wp.targetTable,
		Columns: wp.targetCols,
		Rows:    rows,
	})
}

// executeUpsertJob executes an upsert job, chunking if necessary based on UpsertMergeChunkSize.
func (wp *writerPool) executeUpsertJob(ctx context.Context, writerID int, rows [][]any) error {
	// Get current merge chunk size (supports mid-migration tuning)
	mergeChunkSize := 5000 // default
	if wp.upsertMergeChunkSizeFn != nil {
		mergeChunkSize = wp.upsertMergeChunkSizeFn()
		if mergeChunkSize <= 0 {
			mergeChunkSize = 5000
		}
	}

	logging.Debug("executeUpsertJob: rows=%d, mergeChunkSize=%d", len(rows), mergeChunkSize)

	// If batch is small enough, execute directly
	if len(rows) <= mergeChunkSize {
		return wp.writer.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Schema:      wp.targetSchema,
			Table:       wp.targetTable,
			Columns:     wp.targetCols,
			ColumnTypes: wp.colTypes,
			ColumnSRIDs: wp.colSRIDs,
			PKColumns:   wp.targetPKCols,
			Rows:        rows,
			WriterID:    writerID,
			PartitionID: wp.partitionID,
		})
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
		if err := wp.writer.UpsertBatch(ctx, driver.UpsertBatchOptions{
			Schema:      wp.targetSchema,
			Table:       wp.targetTable,
			Columns:     wp.targetCols,
			ColumnTypes: wp.colTypes,
			ColumnSRIDs: wp.colSRIDs,
			PKColumns:   wp.targetPKCols,
			Rows:        chunk,
			WriterID:    writerID,
			PartitionID: wp.partitionID,
		}); err != nil {
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
