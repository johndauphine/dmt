// Package pool provides shared infrastructure for database pools and parallel writer management.
package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/progress"
)

// defaultPipelineMemoryMB is the default memory budget for pipeline buffers (jobChan)
// when no explicit budget is provided. Each buffered job holds a full chunk of row data,
// so this budget limits how much data can be queued between readers and writers.
const defaultPipelineMemoryMB = 2048 // 2GB

// CalculateJobBufferSize computes the jobChan buffer size from a memory budget.
// This ensures the pipeline adapts to actual row sizes — narrow-row tables (Votes at
// 37 bytes/row) get large buffers for throughput, while wide-row tables (Posts at
// 2290 bytes/row) get small buffers to prevent memory balloon.
//
// Parameters:
//   - pipelineMemoryMB: memory budget for buffered jobs (0 = use default 2GB)
//   - chunkSize: rows per chunk
//   - estimatedRowBytes: average bytes per row (0 = assume 1KB)
//   - numWriters: number of writer goroutines (used as minimum)
//
// Returns the number of jobs (chunks) that can be buffered.
func CalculateJobBufferSize(pipelineMemoryMB int, chunkSize int, estimatedRowBytes int64, numWriters int) int {
	if pipelineMemoryMB <= 0 {
		pipelineMemoryMB = defaultPipelineMemoryMB
	}
	if chunkSize <= 0 {
		chunkSize = 50000
	}
	if estimatedRowBytes <= 0 {
		estimatedRowBytes = 1024 // conservative default
	}

	bytesPerJob := int64(chunkSize) * estimatedRowBytes
	budgetBytes := int64(pipelineMemoryMB) * 1024 * 1024
	bufferSize := int(budgetBytes / bytesPerJob)

	// Minimum: numWriters + 1 to prevent deadlock (each writer needs a pending job,
	// plus one slot for the consumer to submit without blocking).
	minBuffer := numWriters + 1
	if bufferSize < minBuffer {
		bufferSize = minBuffer
	}

	return bufferSize
}

// WriteJob represents a batch of rows to write.
type WriteJob struct {
	Rows     [][]any
	ReaderID int
	Seq      int64
	LastPK   any
	RowNum   int64
}

// WriteAck represents an acknowledgment that a write job completed.
type WriteAck struct {
	ReaderID int
	Seq      int64
	LastPK   any
	RowNum   int64
}

// WriteFunc is the function signature for executing a write operation.
// It receives the writer ID and the rows to write.
type WriteFunc func(ctx context.Context, writerID int, rows [][]any) error

// workerState tracks the state of a single worker goroutine.
type workerState struct {
	id     int
	active bool
	cancel context.CancelFunc
}

// WriterPool manages a pool of parallel write workers.
// It provides the common infrastructure for both pipeline and transfer packages.
type WriterPool struct {
	// Configuration
	numWriters int
	bufferSize int
	writeFunc  WriteFunc
	prog       *progress.Tracker

	// Channels
	jobChan chan WriteJob
	ackChan chan WriteAck

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
	started   bool           // Whether Start() has been called
}

// WriterPoolConfig holds the configuration for creating a writer pool.
type WriterPoolConfig struct {
	NumWriters    int
	BufferSize    int // read-ahead buffer size (used for ack channel scaling)
	JobBufferSize int // jobChan buffer size — computed by CalculateJobBufferSize. 0 = max(numWriters*50, 100).
	WriteFunc     WriteFunc
	Prog          *progress.Tracker
	EnableAck     bool // Whether to enable ack channel for checkpointing
}

// NewWriterPool creates a new writer pool with the given configuration.
func NewWriterPool(ctx context.Context, cfg WriterPoolConfig) *WriterPool {
	writerCtx, cancel := context.WithCancel(ctx)

	// Use caller-computed job buffer size, or a safe default.
	// The caller should use CalculateJobBufferSize() to derive this from memory
	// budget, chunk size, and estimated row size — ensuring the pipeline adapts
	// to actual data characteristics instead of using magic multipliers.
	jobBufferSize := cfg.JobBufferSize
	if jobBufferSize <= 0 {
		// Safe default when no memory-aware sizing is provided.
		// Needs enough headroom so burst reads don't stall writers.
		jobBufferSize = cfg.NumWriters * 50
		if jobBufferSize < 100 {
			jobBufferSize = 100
		}
	}
	if jobBufferSize < cfg.NumWriters+1 {
		jobBufferSize = cfg.NumWriters + 1 // prevent deadlock
	}
	logging.Debug("WriterPool: creating jobChan with buffer size %d (writers=%d, bufferSize=%d)",
		jobBufferSize, cfg.NumWriters, cfg.BufferSize)

	wp := &WriterPool{
		numWriters: cfg.NumWriters,
		bufferSize: cfg.BufferSize,
		writeFunc:  cfg.WriteFunc,
		prog:       cfg.Prog,
		jobChan:    make(chan WriteJob, jobBufferSize),
		ctx:        writerCtx,
		cancel:     cancel,
		workers:    make([]*workerState, 0, cfg.NumWriters),
		started:    false,
	}

	if cfg.EnableAck {
		// Ack channel: acks are tiny (PK value + sequence number), so size
		// generously relative to job buffer. Checkpoint saves can temporarily
		// slow ack processing, but writers use non-blocking sends with fallback.
		ackBufferSize := jobBufferSize * 4
		if ackBufferSize < 1000 {
			ackBufferSize = 1000
		}
		logging.Debug("WriterPool: creating ackChan with buffer size %d", ackBufferSize)
		wp.ackChan = make(chan WriteAck, ackBufferSize)
	}

	return wp
}

// Start begins the writer worker goroutines.
func (wp *WriterPool) Start() {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()

	if wp.started {
		return // Already started
	}

	for i := 0; i < wp.numWriters; i++ {
		writerID := i
		wp.writerWg.Add(1)

		// Create worker state with individual context
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
func (wp *WriterPool) workerWithContext(writerID int, workerCtx context.Context) {
	defer wp.writerWg.Done()

	for job := range wp.jobChan {
		writeStart := time.Now()
		err := wp.writeFunc(wp.ctx, writerID, job.Rows)

		if err != nil {
			wp.writeErr.CompareAndSwap(nil, &err)
			wp.cancel()
			return
		}

		writeDuration := time.Since(writeStart)
		atomic.AddInt64(&wp.totalWriteTime, int64(writeDuration))

		rowCount := int64(len(job.Rows))
		atomic.AddInt64(&wp.totalWritten, rowCount)
		if wp.prog != nil {
			wp.prog.Add(rowCount)
		}

		if wp.ackChan != nil {
			// Non-blocking send with context check to prevent deadlock.
			// If ackChan is full, we skip the ack rather than blocking the writer.
			// This is safe because checkpoint coordination handles out-of-order and
			// missing acks gracefully - the checkpoint just won't advance past this point.
			select {
			case wp.ackChan <- WriteAck{
				ReaderID: job.ReaderID,
				Seq:      job.Seq,
				LastPK:   job.LastPK,
				RowNum:   job.RowNum,
			}:
				// Ack sent successfully
			case <-wp.ctx.Done():
				// Context cancelled, exit worker
				return
			default:
				// Channel full, skip this ack (checkpoint won't advance past this point).
				// This should be rare with the large buffer, but prevents deadlock.
				// Log at debug level to help diagnose checkpoint issues if they occur.
				logging.Debug("Ack channel full, skipping ack for reader %d seq %d (checkpoint may not advance)", job.ReaderID, job.Seq)
			}
		}

		// Check if worker should exit (for scaling down).
		// This check is AFTER processing to ensure a job already pulled from
		// the channel is never dropped — the worker finishes it before exiting.
		select {
		case <-workerCtx.Done():
			return
		default:
		}
	}
}

// Submit sends a write job to the pool. Returns false if context is cancelled.
func (wp *WriterPool) Submit(job WriteJob) bool {
	select {
	case wp.jobChan <- job:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

// Wait closes the job channel and waits for all workers to complete.
func (wp *WriterPool) Wait() {
	logging.Debug("WriterPool.Wait: closing jobChan (len=%d)", len(wp.jobChan))
	close(wp.jobChan)
	logging.Debug("WriterPool.Wait: waiting for %d writers to finish", wp.numWriters)
	wp.writerWg.Wait()
	logging.Debug("WriterPool.Wait: all writers finished")
	if wp.ackChan != nil {
		logging.Debug("WriterPool.Wait: closing ackChan (len=%d)", len(wp.ackChan))
		close(wp.ackChan)
		logging.Debug("WriterPool.Wait: waiting for ack processor")
		wp.ackWg.Wait()
		logging.Debug("WriterPool.Wait: ack processor finished")
	}
}

// Error returns any write error that occurred.
func (wp *WriterPool) Error() error {
	if err := wp.writeErr.Load(); err != nil {
		return *err
	}
	return nil
}

// WriteTime returns the total time spent writing.
func (wp *WriterPool) WriteTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&wp.totalWriteTime))
}

// Written returns the total rows written.
func (wp *WriterPool) Written() int64 {
	return atomic.LoadInt64(&wp.totalWritten)
}

// TotalWrittenPtr returns a pointer to the total written counter for external access.
func (wp *WriterPool) TotalWrittenPtr() *int64 {
	return &wp.totalWritten
}

// Acks returns the ack channel for checkpoint coordination.
func (wp *WriterPool) Acks() <-chan WriteAck {
	return wp.ackChan
}

// StartAckProcessor starts a goroutine to process acks with the given handler.
func (wp *WriterPool) StartAckProcessor(handler func(WriteAck)) {
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

// Context returns the writer pool's context.
func (wp *WriterPool) Context() context.Context {
	return wp.ctx
}

// Cancel cancels the writer pool's context.
func (wp *WriterPool) Cancel() {
	wp.cancel()
}

// NumWriters returns the configured number of workers.
func (wp *WriterPool) NumWriters() int {
	return wp.numWriters
}

// ScaleWorkers adjusts the number of active workers at runtime.
// Can increase or decrease the number of workers.
// Workers are scaled between chunks, not mid-chunk.
func (wp *WriterPool) ScaleWorkers(newCount int) error {
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

// GetWorkerCount returns the current number of active workers.
func (wp *WriterPool) GetWorkerCount() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return len(wp.workers)
}
