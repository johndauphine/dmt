// Package pool provides shared infrastructure for database pools and parallel writer management.
package pool

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/progress"
)

// PipelineBufferConfig contains the parameters needed to compute pipeline buffer sizes.
// All values come from system detection, user config, or per-table metadata.
type PipelineBufferConfig struct {
	MemoryBudgetMB    int64 // Legacy whole-MiB memory budget; ignored when MemoryBudgetBytes is positive
	MemoryBudgetBytes int64 // Exact memory available to this pipeline (preferred when a shared budget is divided across jobs)
	ChunkSize         int   // Rows per batch
	RowBytes          int64 // Bytes per row for this specific table (from Table.GoHeapBytesPerRow)
	NumWriters        int   // Writer goroutines (each holds one chunk being written)
	NumReaders        int   // Parallel reader goroutines
	ReadAheadBuffers  int   // Configured read-ahead buffers
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

// maxWriterPoolSize is both the runtime scaling ceiling and the writer count
// reserved by the ordered-ack submission window. Keeping one shared bound
// prevents checkpoint ordering from starving writers after an allowed upscale.
const maxWriterPoolSize = 128

// writerEncodeAmplification models the driver-side write path holding a
// second copy of a chunk while encoding it (#465): MySQL materializes a
// multi-value INSERT string roughly the size of the data, and the
// staging-table upsert paths buffer similarly. Counting each writer as two
// chunk slots keeps the budget honest for the worst common case; engines
// that stream (PG COPY) simply leave a little slack.
const writerEncodeAmplification = 2

// consumerDispatchSlots accounts for the chunk removed from chunkChan while
// the runner is blocked handing it to a full jobChan. Once the receive frees a
// chunkChan slot, a reader can fill that slot before the consumer's submit
// completes, so this handoff chunk is additional live inventory rather than an
// alias of either channel depth.
const consumerDispatchSlots = 1

// CalculatePipelineBuffers derives buffer depths for both chunkChan and jobChan
// from a shared memory budget.
//
// The full pipeline memory model (#465 — the complete in-flight inventory):
//
//	total_in_flight = chunkChanDepth + jobChanDepth               (queued)
//	                + numReaders                                  (chunk accumulating in scanRows before it occupies a slot)
//	                + numWriters × writerEncodeAmplification      (chunk being written + driver-side encode copy)
//	                + consumerDispatchSlots                       (chunk moving between the two channels)
//	total_memory    = total_in_flight * chunkSize * rowBytes      (in bytes)
//
// Checkpoint acknowledgements retain only pagination metadata, not whole row
// chunks. WriterPool bounds their count with ackSlots independently of the
// measured-byte budget.
//
// Depths are computed once from the chunk size at pipeline start. A caller
// that changes chunk size mid-flight must cap it against the fixed depths via
// SafePipelineChunkSize (or retain another hard backstop); a shrink simply
// leaves slack. The reader/writer overhead slots are reserved first; the
// remaining budget is split between chunkChan (reader side) and jobChan
// (writer side).
func CalculatePipelineBuffers(cfg PipelineBufferConfig) PipelineBufferSizes {
	bytesPerChunk := saturatingPositiveProduct64(int64(cfg.ChunkSize), cfg.RowBytes)
	budgetBytes := pipelineMemoryBudgetBytes(cfg)
	numReaders := cfg.NumReaders
	if numReaders < 1 {
		numReaders = 1
	}
	numWriters := cfg.NumWriters
	if numWriters < 0 {
		numWriters = 0
	}

	// Minimum safe values to prevent deadlock
	minJobDepth := saturatingNonNegativeIntAdd(numWriters, 1) // each writer + 1 for consumer to submit
	minChunkDepth := numReaders                               // default: each reader can produce 1 chunk without blocking
	// Allow a completely unbuffered reader→consumer channel when there is
	// exactly one reader and read-ahead buffering is explicitly disabled.
	if numReaders == 1 && cfg.ReadAheadBuffers == 0 {
		minChunkDepth = 0
	}

	if bytesPerChunk <= 0 || budgetBytes <= 0 {
		return PipelineBufferSizes{
			ChunkChanDepth: minChunkDepth,
			JobChanDepth:   minJobDepth,
		}
	}

	// Slots consumed outside the two channels: each writer holds a chunk
	// plus its encode copy; each reader accumulates one chunk in scanRows
	// before it ever occupies a chunkChan slot (#465).
	overheadSlots := saturatingNonNegativeIntAdd(
		saturatingNonNegativeIntMultiply(numWriters, writerEncodeAmplification),
		numReaders,
	)
	overheadSlots = saturatingNonNegativeIntAdd(overheadSlots, consumerDispatchSlots)

	// Total chunk slots that fit in memory, capped to prevent excessive buffering
	// on narrow-row tables where thousands of tiny chunks would fit in budget.
	slots64 := budgetBytes / bytesPerChunk
	maxInt := int(^uint(0) >> 1)
	totalSlots := maxInt
	if slots64 <= int64(maxInt) {
		totalSlots = int(slots64)
	}
	maxTotalSlots := saturatingNonNegativeIntAdd(maxBufferDepth, overheadSlots)
	if totalSlots > maxTotalSlots {
		totalSlots = maxTotalSlots
	}

	// Reserve the overhead slots first
	available := totalSlots - overheadSlots
	if available < saturatingNonNegativeIntAdd(minChunkDepth, minJobDepth) {
		return PipelineBufferSizes{
			ChunkChanDepth: minChunkDepth,
			JobChanDepth:   minJobDepth,
		}
	}

	// Split remaining slots: give readers enough to stay busy, rest goes to job queue.
	// Readers need at least numReaders × readAheadBuffers slots to pipeline reads,
	// but cap at half the available budget so the job queue gets its share.
	readAheadBuffers := cfg.ReadAheadBuffers
	if readAheadBuffers < 0 {
		readAheadBuffers = 0
	}
	chunkDepth := saturatingNonNegativeIntMultiply(numReaders, readAheadBuffers)
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

// SafePipelineChunkSize returns the largest row count whose complete live
// inventory fits inside totalBudgetBytes across concurrentPipelines identical
// pipelines. The channel depths must be the fixed depths that the pipeline
// will actually use; readers and writers account for scan accumulation and
// driver-side encoding respectively. Malformed inputs return 0. When even one
// row per live slot exceeds the budget, the one-row minimum-progress fallback
// is returned.
//
// Dividing one positive factor at a time is equivalent to dividing by their
// product, but cannot overflow even for adversarial direct callers.
func SafePipelineChunkSize(totalBudgetBytes int64, concurrentPipelines int, cfg PipelineBufferConfig, sizes PipelineBufferSizes) int64 {
	rows, _ := SafePipelineChunkSizeDetail(totalBudgetBytes, concurrentPipelines, cfg, sizes)
	return rows
}

// SafePipelineChunkSizeDetail is SafePipelineChunkSize plus an explicit flag
// for the minimum-progress exception. minimumExceedsBudget is true only when
// the complete inventory for one row per chunk is itself larger than the
// budget; callers should surface that state rather than describing one row as
// fitting.
func SafePipelineChunkSizeDetail(totalBudgetBytes int64, concurrentPipelines int, cfg PipelineBufferConfig, sizes PipelineBufferSizes) (rows int64, minimumExceedsBudget bool) {
	if totalBudgetBytes <= 0 || concurrentPipelines <= 0 || cfg.RowBytes <= 0 {
		return 0, false
	}

	numReaders := cfg.NumReaders
	if numReaders < 1 {
		numReaders = 1
	}
	numWriters := cfg.NumWriters
	if numWriters < 0 {
		numWriters = 0
	}
	chunkDepth := sizes.ChunkChanDepth
	if chunkDepth < 0 {
		chunkDepth = 0
	}
	jobDepth := sizes.JobChanDepth
	if jobDepth < 0 {
		jobDepth = 0
	}

	liveSlots := saturatingNonNegativeIntAdd(chunkDepth, jobDepth)
	liveSlots = saturatingNonNegativeIntAdd(liveSlots, numReaders)
	liveSlots = saturatingNonNegativeIntAdd(
		liveSlots,
		saturatingNonNegativeIntMultiply(numWriters, writerEncodeAmplification),
	)
	liveSlots = saturatingNonNegativeIntAdd(liveSlots, consumerDispatchSlots)
	if liveSlots <= 0 {
		return 0, false
	}

	rows = totalBudgetBytes / cfg.RowBytes
	rows /= int64(concurrentPipelines)
	rows /= int64(liveSlots)
	if rows < 1 {
		return 1, true
	}
	return rows, false
}

func pipelineMemoryBudgetBytes(cfg PipelineBufferConfig) int64 {
	if cfg.MemoryBudgetBytes > 0 {
		return cfg.MemoryBudgetBytes
	}
	return saturatingPositiveProduct64(cfg.MemoryBudgetMB, 1024*1024)
}

func saturatingPositiveProduct64(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func saturatingNonNegativeIntAdd(left, right int) int {
	if left < 0 {
		left = 0
	}
	if right < 0 {
		right = 0
	}
	maxInt := int(^uint(0) >> 1)
	if left > maxInt-right {
		return maxInt
	}
	return left + right
}

func saturatingNonNegativeIntMultiply(left, right int) int {
	if left <= 0 || right <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if left > maxInt/right {
		return maxInt
	}
	return left * right
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
	Bytes    int64 // in-flight memory reserved for this chunk, released on completion (#617)
}

// WriteAck represents an acknowledgment that a write job completed.
type WriteAck struct {
	ReaderID int
	Seq      int64
	LastPK   any
	RowNum   int64
	// Rows is the chunk's row count. Checkpoint coordinators accumulate
	// rows_done from acks applied in sequence order — not from the pool's
	// write-attempt counter — so persisted progress never counts rows beyond
	// the watermark saved alongside it, which a retry would replay and
	// count again (#632).
	Rows int64
}

// AckRelease reports ordered-ack slots made releasable by one callback. Jobs
// may be greater than one when filling a sequence gap drains buffered
// successors. Chunk byte reservations are released earlier, immediately after
// successful ack delivery, and are deliberately independent of this count.
type AckRelease struct {
	Jobs int
}

// WriteFunc is the function signature for executing a write operation.
// It receives the writer ID and the rows to write.
type WriteFunc func(ctx context.Context, writerID int, rows [][]any) error

// workerState tracks the state of a single worker goroutine.
type workerState struct {
	id      int
	cancel  context.CancelFunc
	done    chan struct{}
	busy    atomic.Bool
	retired atomic.Bool
}

// WriterPool manages a pool of parallel write workers.
// It provides the common infrastructure for both pipeline and transfer packages.
type WriterPool struct {
	// Configuration
	numWriters int
	bufferSize int
	writeFunc  WriteFunc
	prog       *progress.Tracker
	onComplete func(bytes int64) // release hook for in-flight memory budget (#617)

	// Channels
	jobChan chan WriteJob
	ackChan chan WriteAck
	// ackSlots bounds acknowledgements that have been submitted but not yet
	// applied in sequence order, independently of the optional byte budget.
	// A slot is acquired before job submission, so the missing earlier job that
	// can unblock a full reorder window is always already admitted.
	ackSlots chan struct{}

	// State
	totalWriteTime int64 // atomic, nanoseconds
	totalWritten   int64 // atomic, rows written
	liveWorkers    int64 // atomic, worker goroutines currently alive (incl. draining)
	writeErr       atomic.Pointer[error]

	// Synchronization
	writerWg sync.WaitGroup
	ackWg    sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc

	// Dynamic worker management
	scaleMu         sync.Mutex   // Serializes complete scale transitions, including idle retirement waits
	workersMu       sync.RWMutex // Guards worker-management fields below
	workers         []*workerState
	nextWorkerID    int  // Monotonic ID source; never reused by replacements
	retiringWorkers int  // Retired goroutines still finishing or acknowledging cancellation
	started         bool // Whether Start() has been called
	closing         bool // Wait has begun; worker exits must not spawn replacements
}

// WriterPoolConfig holds the configuration for creating a writer pool.
type WriterPoolConfig struct {
	NumWriters    int
	BufferSize    int // read-ahead buffer size
	JobBufferSize int // jobChan buffer size — must be computed by CalculateJobBufferSize
	WriteFunc     WriteFunc
	Prog          *progress.Tracker
	EnableAck     bool // Whether to enable ack channel for checkpointing

	// OnComplete, if set, releases a job's in-flight memory reservation
	// (#617). It fires immediately on a write error (so a failed chunk cannot
	// wedge a tight budget), after the write when acknowledgements are disabled,
	// or after successful ack delivery when they are enabled. A worker that exits
	// while ack delivery is blocked skips it; the transfer runner's residual
	// release frees that reservation, so each chunk is freed exactly once.
	OnComplete func(bytes int64)
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
		onComplete: cfg.OnComplete,
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
		ackWindowWriters := cfg.NumWriters
		if ackWindowWriters < maxWriterPoolSize {
			ackWindowWriters = maxWriterPoolSize
		}
		ackWindowSize := jobBufferSize + ackWindowWriters
		logging.Debug("WriterPool: creating ordered-ack window with %d slots", ackWindowSize)
		wp.ackSlots = make(chan struct{}, ackWindowSize)
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
		wp.startWorkerLocked()
	}
	wp.started = true
}

// startWorkerLocked spawns one worker goroutine and records its state. Callers
// must hold workersMu.
func (wp *WriterPool) startWorkerLocked() {
	writerID := wp.nextWorkerID
	wp.nextWorkerID++

	// Each worker has an individual context so a scale-down can retire it
	// without touching the shared pool context.
	workerCtx, cancel := context.WithCancel(wp.ctx)
	ws := &workerState{
		id:     writerID,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	wp.workers = append(wp.workers, ws)

	// Count the worker at admission, before publishing the goroutine. Start and
	// ScaleWorkers are synchronous pool operations; incrementing only after the
	// scheduler ran the goroutine left a brief, observable live-count of zero
	// immediately after a successful start/downscale.
	atomic.AddInt64(&wp.liveWorkers, 1)
	wp.writerWg.Add(1)
	go wp.workerWithContext(ws, workerCtx)
}

// workerWithContext is the main write worker goroutine with context support.
//
// The receive is guarded by workerCtx so a retired (scaled-down) worker exits
// *before* dequeuing another job rather than only after finishing the next one
// (#642). A job already pulled from jobChan is always processed to completion
// exactly once — the retirement check happens between jobs, never mid-chunk —
// so a downscale never drops or double-writes an in-flight chunk.
func (wp *WriterPool) workerWithContext(ws *workerState, workerCtx context.Context) {
	defer func() {
		atomic.AddInt64(&wp.liveWorkers, -1)
		wp.workerExited(ws)
		close(ws.done)
		wp.writerWg.Done()
	}()

	for {
		// Retire before taking new work. This bounds post-downscale concurrency
		// to the requested writer count: a canceled worker cannot consume a job
		// a surviving worker should handle.
		select {
		case <-workerCtx.Done():
			return
		default:
		}

		var job WriteJob
		var ok bool
		select {
		case <-workerCtx.Done():
			return
		case job, ok = <-wp.jobChan:
			if !ok {
				return // jobChan closed by Wait(): graceful shutdown.
			}
		}

		ws.busy.Store(true)
		keepRunning := wp.processJob(ws.id, job)
		ws.busy.Store(false)
		if !keepRunning {
			return
		}
	}
}

// workerExited removes one retired goroutine from the draining count and, when
// an earlier upscale was held back by those drainers, starts only enough
// replacements to reach the desired ceiling. The active+retiring count is the
// admission invariant; it prevents a rapid 4→1→4 from creating seven live
// writers while the original three finish their in-flight chunks (#642).
func (wp *WriterPool) workerExited(ws *workerState) {
	wp.workersMu.Lock()
	defer wp.workersMu.Unlock()
	if ws.retired.Load() {
		wp.retiringWorkers--
	}
	if wp.closing || !wp.started || wp.ctx.Err() != nil {
		return
	}
	for len(wp.workers)+wp.retiringWorkers < wp.numWriters {
		wp.startWorkerLocked()
	}
}

// processJob executes one write job. It returns false when the worker must
// exit: either the write failed (the pool is canceled) or the pool is aborting
// while the ack is still pending. It returns true when the job completed and
// the worker may take another.
func (wp *WriterPool) processJob(writerID int, job WriteJob) bool {
	writeStart := time.Now()
	err := wp.writeFunc(wp.ctx, writerID, job.Rows)

	if err != nil {
		// Release the failed chunk's reservation immediately (#617). With
		// a tight or oversized-clamped budget the failed chunk may hold
		// the last free bytes; freeing it here lets a reader blocked in
		// acquireMem proceed far enough for the consumer to observe the
		// cancel and unwind, instead of wedging the pipeline.
		if wp.onComplete != nil {
			wp.onComplete(job.Bytes)
		}
		wp.releaseAckSlots(1)
		wp.writeErr.CompareAndSwap(nil, &err)
		wp.cancel()
		return false
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
			Rows:     rowCount,
		}
		select {
		case wp.ackChan <- ack:
			// Delivery transfers the small checkpoint metadata to the bounded
			// ordered-ack window; the writer no longer owns the full row chunk.
			// Release its measured-byte reservation now instead of tying row
			// payload capacity to a possibly missing earlier sequence.
			if wp.onComplete != nil {
				wp.onComplete(job.Bytes)
			}
		case <-wp.ctx.Done():
			// Aborting — the runner's residual release frees this chunk's
			// still-held reservation, so no explicit release here.
			wp.releaseAckSlots(1)
			return false
		}
	}

	// Without checkpoint acks, the worker is the last owner and releases the
	// reservation after the successful write. With acks, the successful-delivery
	// branch above already released it; ackSlots separately bounds retained
	// checkpoint metadata until ordered application.
	if wp.ackChan == nil && wp.onComplete != nil {
		wp.onComplete(job.Bytes)
	}
	return true
}

// Submit sends a write job to the pool. Returns false if context is cancelled.
func (wp *WriterPool) Submit(job WriteJob) bool {
	if !wp.acquireAckSlot() {
		return false
	}
	if wp.submit(job) {
		return true
	}
	wp.releaseAckSlots(1)
	return false
}

func (wp *WriterPool) acquireAckSlot() bool {
	if wp.ackSlots == nil {
		return true
	}
	select {
	case wp.ackSlots <- struct{}{}:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

func (wp *WriterPool) releaseAckSlots(count int) {
	if wp.ackSlots == nil || count <= 0 {
		return
	}
	for range count {
		<-wp.ackSlots
	}
}

func (wp *WriterPool) submit(job WriteJob) bool {
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
	wp.workersMu.Lock()
	wp.closing = true
	writers := wp.numWriters
	wp.workersMu.Unlock()
	logging.Debug("WriterPool.Wait: closing jobChan (len=%d)", len(wp.jobChan))
	close(wp.jobChan)
	logging.Debug("WriterPool.Wait: waiting for %d desired writers plus drainers to finish", writers)
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

// Acks returns the ack channel for checkpoint coordination.
func (wp *WriterPool) Acks() <-chan WriteAck {
	return wp.ackChan
}

// StartAckProcessor starts a goroutine to process acks with the given handler.
func (wp *WriterPool) StartAckProcessor(handler func(WriteAck)) {
	wp.startAckProcessor(func(ack WriteAck) AckRelease {
		handler(ack)
		return AckRelease{Jobs: 1}
	})
}

// StartOrderedAckProcessor starts an ack processor whose handler reports the
// job count whose slots became releasable after applying this ack. It may cover
// successors when filling an earlier sequence gap, or be zero when the ack
// must remain pending. This bounds out-of-order checkpoint metadata without
// retaining the corresponding full row-chunk reservation.
func (wp *WriterPool) StartOrderedAckProcessor(handler func(WriteAck) AckRelease) {
	wp.startAckProcessor(handler)
}

func (wp *WriterPool) startAckProcessor(handler func(WriteAck) AckRelease) {
	if wp.ackChan == nil {
		return
	}
	wp.ackWg.Add(1)
	go func() {
		defer wp.ackWg.Done()
		for ack := range wp.ackChan {
			released := handler(ack)
			wp.releaseAckSlots(released.Jobs)
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

// NumWriters returns the desired ceiling set by the latest completed
// ScaleWorkers call (or the initial config). During a rapid upscale while old
// workers are draining, GetWorkerCount may be lower until replacements can
// start without exceeding this ceiling.
func (wp *WriterPool) NumWriters() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return wp.numWriters
}

// ScaleWorkers adjusts the number of active workers at runtime. Workers are
// scaled between chunks, never mid-chunk.
//
// A downscale waits for every idle retiree to acknowledge cancellation before
// returning, closing the cancel-vs-ready-job select race: work submitted after
// return can only be dequeued by survivors. A worker already inside WriteFunc
// is allowed to drain asynchronously, exactly once. A later upscale accounts
// for those drainers and starts replacements only as they exit, so rapid
// 4→1→4 never exceeds four live writers.
func (wp *WriterPool) ScaleWorkers(newCount int) error {
	if newCount < 1 {
		return fmt.Errorf("worker count must be at least 1, got %d", newCount)
	}

	if newCount > maxWriterPoolSize {
		return fmt.Errorf("worker count too high: %d (max %d)", newCount, maxWriterPoolSize)
	}

	wp.scaleMu.Lock()
	defer wp.scaleMu.Unlock()

	wp.workersMu.Lock()
	if wp.closing {
		wp.workersMu.Unlock()
		return fmt.Errorf("writer pool is closing")
	}
	if !wp.started {
		wp.numWriters = newCount
		wp.workersMu.Unlock()
		return nil
	}

	currentCount := len(wp.workers)

	if newCount == currentCount {
		wp.numWriters = newCount
		wp.workersMu.Unlock()
		return nil // No change needed
	}

	if newCount > currentCount {
		wp.numWriters = newCount
		for len(wp.workers)+wp.retiringWorkers < newCount {
			wp.startWorkerLocked()
		}
		wp.workersMu.Unlock()
		return nil
	}

	// Downscale: cancel the tail workers and count them until their goroutines
	// exit. Busy retirees drain asynchronously. Idle retirees are joined below
	// so none can remain parked in the cancel-vs-job select after this method
	// returns.
	idleDone := make([]<-chan struct{}, 0, currentCount-newCount)
	for i := newCount; i < currentCount; i++ {
		ws := wp.workers[i]
		ws.retired.Store(true)
		wp.retiringWorkers++
		ws.cancel()
		if !ws.busy.Load() {
			idleDone = append(idleDone, ws.done)
		}
	}
	// Trim with a fixed cap so a later append allocates a fresh array rather
	// than aliasing the retired worker entries.
	wp.workers = wp.workers[:newCount:newCount]
	wp.numWriters = newCount
	wp.workersMu.Unlock()

	for _, done := range idleDone {
		<-done
	}
	return nil
}

// GetWorkerCount returns active, non-retired worker slots that can accept new
// jobs. It may be lower than NumWriters while an upscale waits for retired
// in-flight writers to exit without breaching the desired live ceiling.
func (wp *WriterPool) GetWorkerCount() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return len(wp.workers)
}

// GetLiveWorkerCount returns the number of worker goroutines currently alive,
// including any retired by a downscale that are still draining a final
// in-flight write. It equals GetWorkerCount at rest and transiently exceeds it
// while retired workers finish committed chunks. Exposed for metrics and
// concurrency assertions (#642).
func (wp *WriterPool) GetLiveWorkerCount() int {
	return int(atomic.LoadInt64(&wp.liveWorkers))
}

// GetDrainingWorkerCount returns retired worker goroutines that have not yet
// exited. They may only finish work dequeued before the downscale completed;
// they never accept work submitted after ScaleWorkers returns.
func (wp *WriterPool) GetDrainingWorkerCount() int {
	wp.workersMu.RLock()
	defer wp.workersMu.RUnlock()
	return wp.retiringWorkers
}
