package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
	"github.com/johndauphine/mssql-pg-migrate/internal/progress"
)

// writerPool manages a pool of parallel write workers.
type writerPool struct {
	// Configuration
	numWriters   int
	bufferSize   int
	useUpsert    bool
	targetSchema string
	targetTable  string
	targetCols   []string
	colTypes     []string
	colSRIDs     []int
	targetPKCols []string
	partitionID  *int
	writer       driver.Writer
	prog         *progress.Tracker

	// Channels
	jobChan chan writeJob
	ackChan chan writeAck

	// State
	totalWriteTime int64 // atomic, nanoseconds
	totalWritten   int64 // atomic, rows written
	writeErr       atomic.Pointer[error]

	// Synchronization
	writerWg sync.WaitGroup
	ackWg    sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// writerPoolConfig holds the configuration for creating a writer pool.
type writerPoolConfig struct {
	NumWriters   int
	BufferSize   int
	UseUpsert    bool
	TargetSchema string
	TargetTable  string
	TargetCols   []string
	ColTypes     []string
	ColSRIDs     []int
	TargetPKCols []string
	PartitionID  *int
	Writer       driver.Writer
	Prog         *progress.Tracker
	EnableAck    bool // Whether to enable ack channel for checkpointing
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	writerCtx, cancel := context.WithCancel(ctx)

	wp := &writerPool{
		numWriters:   cfg.NumWriters,
		bufferSize:   cfg.BufferSize,
		useUpsert:    cfg.UseUpsert,
		targetSchema: cfg.TargetSchema,
		targetTable:  cfg.TargetTable,
		targetCols:   cfg.TargetCols,
		colTypes:     cfg.ColTypes,
		colSRIDs:     cfg.ColSRIDs,
		targetPKCols: cfg.TargetPKCols,
		partitionID:  cfg.PartitionID,
		writer:       cfg.Writer,
		prog:         cfg.Prog,
		jobChan:      make(chan writeJob, cfg.BufferSize),
		ctx:          writerCtx,
		cancel:       cancel,
	}

	if cfg.EnableAck {
		wp.ackChan = make(chan writeAck, cfg.BufferSize)
	}

	return wp
}

// start begins the writer worker goroutines.
func (wp *writerPool) start() {
	for i := 0; i < wp.numWriters; i++ {
		writerID := i
		wp.writerWg.Add(1)
		go wp.worker(writerID)
	}
}

// worker is the main write worker goroutine.
func (wp *writerPool) worker(writerID int) {
	defer wp.writerWg.Done()

	for job := range wp.jobChan {
		select {
		case <-wp.ctx.Done():
			return
		default:
		}

		writeStart := time.Now()
		var err error
		if wp.useUpsert {
			err = wp.writer.UpsertBatch(wp.ctx, driver.UpsertBatchOptions{
				Schema:      wp.targetSchema,
				Table:       wp.targetTable,
				Columns:     wp.targetCols,
				ColumnTypes: wp.colTypes,
				ColumnSRIDs: wp.colSRIDs,
				PKColumns:   wp.targetPKCols,
				Rows:        job.rows,
				WriterID:    writerID,
				PartitionID: wp.partitionID,
			})
		} else {
			err = wp.writer.WriteBatch(wp.ctx, driver.WriteBatchOptions{
				Schema:  wp.targetSchema,
				Table:   wp.targetTable,
				Columns: wp.targetCols,
				Rows:    job.rows,
			})
		}

		if err != nil {
			wp.writeErr.CompareAndSwap(nil, &err)
			wp.cancel()
			return
		}

		writeDuration := time.Since(writeStart)
		atomic.AddInt64(&wp.totalWriteTime, int64(writeDuration))

		rowCount := int64(len(job.rows))
		atomic.AddInt64(&wp.totalWritten, rowCount)
		wp.prog.Add(rowCount)

		if wp.ackChan != nil {
			wp.ackChan <- writeAck{
				readerID: job.readerID,
				seq:      job.seq,
				lastPK:   job.lastPK,
				rowNum:   job.rowNum,
			}
		}
	}
}

// submit sends a write job to the pool. Returns false if context is cancelled.
func (wp *writerPool) submit(job writeJob) bool {
	select {
	case wp.jobChan <- job:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

// wait closes the job channel and waits for all workers to complete.
func (wp *writerPool) wait() {
	close(wp.jobChan)
	wp.writerWg.Wait()
	if wp.ackChan != nil {
		close(wp.ackChan)
		wp.ackWg.Wait()
	}
}

// error returns any write error that occurred.
func (wp *writerPool) error() error {
	if err := wp.writeErr.Load(); err != nil {
		return *err
	}
	return nil
}

// writeTime returns the total time spent writing.
func (wp *writerPool) writeTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&wp.totalWriteTime))
}

// written returns the total rows written.
func (wp *writerPool) written() int64 {
	return atomic.LoadInt64(&wp.totalWritten)
}

// startAckProcessor starts a goroutine to process acks with the given handler.
func (wp *writerPool) startAckProcessor(handler func(writeAck)) {
	if wp.ackChan == nil {
		return
	}
	wp.ackWg.Add(1)
	go func() {
		defer wp.ackWg.Done()
		for ack := range wp.ackChan {
			handler(ack)
		}
	}()
}
