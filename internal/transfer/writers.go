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
	tgtPool              pool.TargetPool
	targetSchema         string
	targetTable          string
	targetCols           []string
	colTypes             []string
	colSRIDs             []int
	targetPKCols         []string
	partitionID          *int
	useUpsert            bool
	upsertMergeChunkSize int
}

// writerPoolConfig holds the configuration for creating a writer pool.
type writerPoolConfig struct {
	NumWriters           int
	BufferSize           int
	UseUpsert            bool
	UpsertMergeChunkSize int
	TargetSchema         string
	TargetTable          string
	TargetCols           []string
	ColTypes             []string
	ColSRIDs             []int
	TargetPKCols         []string
	PartitionID          *int
	TgtPool              pool.TargetPool
	Prog                 *progress.Tracker
	EnableAck            bool // Whether to enable ack channel for checkpointing
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	// Default upsert merge chunk size if not specified
	upsertMergeChunkSize := cfg.UpsertMergeChunkSize
	if upsertMergeChunkSize <= 0 {
		upsertMergeChunkSize = 5000
	}

	wp := &writerPool{
		tgtPool:              cfg.TgtPool,
		targetSchema:         cfg.TargetSchema,
		targetTable:          cfg.TargetTable,
		targetCols:           cfg.TargetCols,
		colTypes:             cfg.ColTypes,
		colSRIDs:             cfg.ColSRIDs,
		targetPKCols:         cfg.TargetPKCols,
		partitionID:          cfg.PartitionID,
		useUpsert:            cfg.UseUpsert,
		upsertMergeChunkSize: upsertMergeChunkSize,
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
		return wp.executeUpsertWithChunking(ctx, writerID, rows)
	}
	return writeChunkGeneric(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable, wp.targetCols, rows)
}

// executeUpsertWithChunking executes an upsert job, chunking if necessary based on UpsertMergeChunkSize.
// Chunking reduces memory pressure on the target database when processing large batches.
func (wp *writerPool) executeUpsertWithChunking(ctx context.Context, writerID int, rows [][]any) error {
	mergeChunkSize := wp.upsertMergeChunkSize

	logging.Debug("executeUpsertWithChunking: rows=%d, mergeChunkSize=%d", len(rows), mergeChunkSize)

	// If batch is small enough, execute directly
	if len(rows) <= mergeChunkSize {
		return writeChunkUpsertWithWriter(ctx, wp.tgtPool, wp.targetSchema, wp.targetTable,
			wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, rows, writerID, wp.partitionID)
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
			wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, chunk, writerID, wp.partitionID); err != nil {
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
