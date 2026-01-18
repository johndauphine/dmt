package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/progress"
)

// workerState tracks the state of a single worker goroutine.
type workerState struct {
	id     int
	active bool
	cancel context.CancelFunc
}

// writerPool manages a pool of parallel write workers.
type writerPool struct {
	// Configuration
	numWriters             int
	bufferSize             int
	useUpsert              bool
	targetSchema           string
	targetTable            string
	targetCols             []string
	colTypes               []string
	colSRIDs               []int
	targetPKCols           []string
	partitionID            *int
	writer                 driver.Writer
	prog                   *progress.Tracker
	upsertMergeChunkSizeFn func() int // Function to get current upsert merge chunk size

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

	// Dynamic worker management
	workersMu sync.RWMutex
	workers   []*workerState // Track individual worker states for scaling
	started   bool           // Whether start() has been called
}

// writerPoolConfig holds the configuration for creating a writer pool.
type writerPoolConfig struct {
	NumWriters            int
	BufferSize            int
	UseUpsert             bool
	TargetSchema          string
	TargetTable           string
	TargetCols            []string
	ColTypes              []string
	ColSRIDs              []int
	TargetPKCols          []string
	PartitionID           *int
	Writer                driver.Writer
	Prog                  *progress.Tracker
	EnableAck             bool       // Whether to enable ack channel for checkpointing
	UpsertMergeChunkSizeFn func() int // Function to get current upsert merge chunk size (for dynamic tuning)
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	writerCtx, cancel := context.WithCancel(ctx)

	wp := &writerPool{
		numWriters:             cfg.NumWriters,
		bufferSize:             cfg.BufferSize,
		useUpsert:              cfg.UseUpsert,
		targetSchema:           cfg.TargetSchema,
		targetTable:            cfg.TargetTable,
		targetCols:             cfg.TargetCols,
		colTypes:               cfg.ColTypes,
		colSRIDs:               cfg.ColSRIDs,
		targetPKCols:           cfg.TargetPKCols,
		partitionID:            cfg.PartitionID,
		writer:                 cfg.Writer,
		prog:                   cfg.Prog,
		upsertMergeChunkSizeFn: cfg.UpsertMergeChunkSizeFn,
		jobChan:                make(chan writeJob, cfg.BufferSize),
		ctx:                    writerCtx,
		cancel:                 cancel,
		workers:                make([]*workerState, 0, cfg.NumWriters),
		started:                false,
	}

	if cfg.EnableAck {
		wp.ackChan = make(chan writeAck, cfg.BufferSize)
	}

	return wp
}

// start begins the writer worker goroutines.
func (wp *writerPool) start() {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()

	if wp.started {
		return // Already started
	}

	for i := 0; i < wp.numWriters; i++ {
		writerID := i
		wp.writerWg.Add(1)

		// Create worker state
		workerCtx, cancel := context.WithCancel(wp.ctx)
		ws := &workerState{
			id:     writerID,
			active: true,
			cancel: cancel,
		}
		wp.workers = append(wp.workers, ws)

		go wp.workerWithContext(writerID, workerCtx)
	}
	wp.started = true
}

// workerWithContext is the main write worker goroutine with context support.
func (wp *writerPool) workerWithContext(writerID int, workerCtx context.Context) {
	defer wp.writerWg.Done()

	for job := range wp.jobChan {
		// Check if worker should exit
		select {
		case <-workerCtx.Done():
			return
		default:
		}

		wp.executeJob(writerID, job)
	}
}

// executeJob executes a single write job.
func (wp *writerPool) executeJob(writerID int, job writeJob) {
	writeStart := time.Now()
	var err error
	if wp.useUpsert {
		err = wp.executeUpsertJob(writerID, job.rows)
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

// executeUpsertJob executes an upsert job, chunking if necessary based on UpsertMergeChunkSize.
func (wp *writerPool) executeUpsertJob(writerID int, rows [][]any) error {
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
		return wp.writer.UpsertBatch(wp.ctx, driver.UpsertBatchOptions{
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
		if err := wp.writer.UpsertBatch(wp.ctx, driver.UpsertBatchOptions{
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

// ScaleWorkers adjusts the number of active workers. Can increase or decrease
// the number of workers at runtime. Returns an error if scaling is invalid.
// Workers are scaled between chunks, not mid-chunk.
func (wp *writerPool) ScaleWorkers(newCount int) error {
	if newCount < 1 {
		return fmt.Errorf("worker count must be at least 1, got %d", newCount)
	}

	if newCount > 128 {
		return fmt.Errorf("worker count too high: %d (max 128)", newCount)
	}

	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()

	currentCount := len(wp.workers)

	if newCount == currentCount {
		return nil // No change needed
	}

	if newCount > currentCount {
		// Add new workers
		for i := currentCount; i < newCount; i++ {
			workerCtx, cancel := context.WithCancel(wp.ctx)
			ws := &workerState{
				id:     i,
				active: true,
				cancel: cancel,
			}
			wp.workers = append(wp.workers, ws)

			wp.writerWg.Add(1)
			go wp.workerWithContext(i, workerCtx)
		}
	} else {
		// Remove workers: mark them as inactive and cancel their contexts
		// They will exit gracefully when they finish current job
		for i := newCount; i < currentCount; i++ {
			if i < len(wp.workers) {
				wp.workers[i].active = false
				wp.workers[i].cancel()
			}
		}
		// Trim the workers slice
		wp.workers = wp.workers[:newCount]
	}

	wp.numWriters = newCount
	return nil
}

// GetWorkerCount returns the current number of workers.
func (wp *writerPool) GetWorkerCount() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return len(wp.workers)
}
