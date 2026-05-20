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

// PipelineBufferConfig contains the parameters needed to compute pipeline buffer sizes.
// All values come from system detection, user config, or per-table metadata.
type PipelineBufferConfig struct {
	MemoryBudgetMB   int64 // Memory available for the pipeline (from EffectiveMaxMemoryMB minus connection overhead)
	ChunkSize        int   // Rows per batch
	RowBytes         int64 // Bytes per row for this specific table (from Table.GoHeapBytesPerRow)
	NumWriters       int   // Writer goroutines (each holds one chunk being written)
	NumReaders       int   // Parallel reader goroutines
	ReadAheadBuffers int   // Configured read-ahead buffers
}

// PipelineBufferSizes holds the computed buffer depths for both pipeline channels.
type PipelineBufferSizes struct {
	ChunkChanDepth int // Buffer depth for the reader→consumer channel
	JobChanDepth   int // Buffer depth for the consumer→writer channel
}

// maxBufferDepth caps the total number of chunks buffered across both channels.
// Unbounded depth wastes memory on narrow-row tables (e.g., 40 bytes/row ×
// 50K chunk = 2MB/chunk → thousands of slots in a large budget). Beyond ~200
// chunks the pipeline is always I/O-bound, and extra buffering only increases
// memory footprint without improving throughput.
const maxBufferDepth = 200

// CalculatePipelineBuffers derives buffer depths for both chunkChan and jobChan
// from a shared memory budget.
//
// The full pipeline memory model:
//
//	total_in_flight = chunkChanDepth + jobChanDepth + numWriters  (in chunks)
//	total_memory    = total_in_flight * chunkSize * rowBytes      (in bytes)
//
// Writers always hold one chunk each (non-negotiable). The remaining budget is
// split between chunkChan (reader side) and jobChan (writer side).
func CalculatePipelineBuffers(cfg PipelineBufferConfig) PipelineBufferSizes {
	bytesPerChunk := int64(cfg.ChunkSize) * cfg.RowBytes
	numReaders := cfg.NumReaders
	if numReaders < 1 {
		numReaders = 1
	}

	// Minimum safe values to prevent deadlock
	minJobDepth := cfg.NumWriters + 1 // each writer + 1 for consumer to submit
	minChunkDepth := numReaders       // default: each reader can produce 1 chunk without blocking
	// Allow a completely unbuffered reader→consumer channel when there is
	// exactly one reader and read-ahead buffering is explicitly disabled.
	if numReaders == 1 && cfg.ReadAheadBuffers == 0 {
		minChunkDepth = 0
	}

	if bytesPerChunk <= 0 || cfg.MemoryBudgetMB <= 0 {
		return PipelineBufferSizes{
			ChunkChanDepth: minChunkDepth,
			JobChanDepth:   minJobDepth,
		}
	}

	budgetBytes := cfg.MemoryBudgetMB * 1024 * 1024

	// Total chunk slots that fit in memory, capped to prevent excessive buffering
	// on narrow-row tables where thousands of tiny chunks would fit in budget.
	totalSlots := int(budgetBytes / bytesPerChunk)
	if totalSlots > maxBufferDepth+cfg.NumWriters {
		totalSlots = maxBufferDepth + cfg.NumWriters
	}

	// Writers always hold one chunk each — subtract those first
	available := totalSlots - cfg.NumWriters
	if available < minChunkDepth+minJobDepth {
		return PipelineBufferSizes{
			ChunkChanDepth: minChunkDepth,
			JobChanDepth:   minJobDepth,
		}
	}

	// Split remaining slots: give readers enough to stay busy, rest goes to job queue.
	// Readers need at least numReaders × readAheadBuffers slots to pipeline reads,
	// but cap at half the available budget so the job queue gets its share.
	chunkDepth := numReaders * cfg.ReadAheadBuffers
	if chunkDepth > available/2 {
		chunkDepth = available / 2
	}
	if chunkDepth < minChunkDepth {
		chunkDepth = minChunkDepth
	}

	jobDepth := available - chunkDepth
	if jobDepth < minJobDepth {
		jobDepth = minJobDepth
	}

	return PipelineBufferSizes{
		ChunkChanDepth: chunkDepth,
		JobChanDepth:   jobDepth,
	}
}

// CalculateJobBufferSize is a convenience wrapper for callers that only need jobChan depth.
// Prefer CalculatePipelineBuffers for new code.
func CalculateJobBufferSize(cfg PipelineBufferConfig) int {
	return CalculatePipelineBuffers(cfg).JobChanDepth
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
	BufferSize    int // read-ahead buffer size
	JobBufferSize int // jobChan buffer size — must be computed by CalculateJobBufferSize
	WriteFunc     WriteFunc
	Prog          *progress.Tracker
	EnableAck     bool // Whether to enable ack channel for checkpointing
}

// NewWriterPool creates a new writer pool with the given configuration.
func NewWriterPool(ctx context.Context, cfg WriterPoolConfig) *WriterPool {
	writerCtx, cancel := context.WithCancel(ctx)

	// JobBufferSize must be computed by the caller via CalculateJobBufferSize.
	// Enforce the minimum to prevent deadlock.
	jobBufferSize := cfg.JobBufferSize
	if jobBufferSize < cfg.NumWriters+1 {
		jobBufferSize = cfg.NumWriters + 1
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
		// Ack buffer depth = job buffer + writers in flight.
		// Each writer produces one ack per completed job, and the ack processor
		// runs continuously. This ensures the buffer can hold all pending acks
		// even if checkpoint saves temporarily pause processing.
		ackBufferSize := jobBufferSize + cfg.NumWriters
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

		if logging.IsDebug() && writeDuration > 0 {
			logging.Debug("Writer[%d]: wrote %d rows in %v (%.0f rows/sec)",
				writerID, len(job.Rows), writeDuration,
				float64(len(job.Rows))/writeDuration.Seconds())
		}

		rowCount := int64(len(job.Rows))
		atomic.AddInt64(&wp.totalWritten, rowCount)
		if wp.prog != nil {
			wp.prog.Add(rowCount)
		}

		if wp.ackChan != nil {
			ack := WriteAck{
				ReaderID: job.ReaderID,
				Seq:      job.Seq,
				LastPK:   job.LastPK,
				RowNum:   job.RowNum,
			}
			select {
			case wp.ackChan <- ack:
			case <-wp.ctx.Done():
				return
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
	// Try non-blocking send first to detect when jobChan is full.
	select {
	case wp.jobChan <- job:
		return true
	case <-wp.ctx.Done():
		return false
	default:
		// jobChan is full — consumer will block until a writer finishes.
		if logging.IsDebug() {
			stallStart := time.Now()
			select {
			case wp.jobChan <- job:
				logging.Debug("WriterPool.Submit: jobChan stall %v (cap=%d)", time.Since(stallStart), cap(wp.jobChan))
				return true
			case <-wp.ctx.Done():
				return false
			}
		}
		// Non-debug: just block.
		select {
		case wp.jobChan <- job:
			return true
		case <-wp.ctx.Done():
			return false
		}
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
