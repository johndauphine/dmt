package transfer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/observability"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// This file owns the strategy-independent half of the transfer pipeline:
// buffer sizing, the memory guard, the chunk channel and its reader-context
// cancel discipline (#250), the consumer loop, writer-pool construction and
// tuner-driven scaling, the drain epilogue, stats aggregation, and the final
// progress save. The pagination strategies (keyset.go, row_number.go) supply
// only what genuinely differs: how chunks are produced and how checkpoints
// are keyed (#614).

// pipelineEnv carries the runner-owned facilities reader goroutines need:
// the memory guardrail, the dynamic chunk-size callback, and the in-flight
// byte-budget reservation (#617).
type pipelineEnv struct {
	memGuard  *MemoryGuard
	chunkSize func() int

	// acquireMem reserves a scanned chunk's measured bytes against the shared
	// budget, blocking (backpressuring the reader) until they are available.
	// It returns the bytes actually reserved (to stamp on the chunk so the
	// writer releases the same amount) and false if ctx was cancelled while
	// waiting. With no budget it reserves 0 and returns true immediately.
	//
	// A reader that blocks here holds no reservation — only the one chunk it
	// just scanned — so this is a single, deadlock-free acquire, never a
	// hold-and-wait top-up. As in the pre-epic path, that leaves at most one
	// unadmitted scanned chunk per reader; MemoryGuard and GOMEMLIMIT are the
	// process-level backstops for that transient allocation and driver copies.
	acquireMem func(ctx context.Context, n int64) (int64, bool)
}

// chunkProducer is the strategy half of the pipeline: it owns the reader
// goroutine(s) that page rows out of the source and send them to the
// runner's chunk channel. produce must block until all of its readers have
// finished; the runner closes the channel afterwards.
type chunkProducer interface {
	// readerCount reports the parallel reader count used for pipeline
	// buffer sizing (the configured value, not the number of goroutines
	// actually spawned — resume may skip completed ranges).
	readerCount() int
	produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult)
}

// tunerCallbacks bundles the dynamic-parameter closures shared by both
// strategies. Each reads the tuner snapshot on invocation so runtime
// adjustments (rule-driven, error-driven) take effect on in-flight work.
// Priority everywhere: per-table override → global tuner value → config.
type tunerCallbacks struct {
	chunkSize       func() int
	batchSize       func() int
	upsertChunk     func() int
	checkpointFreq  func() int
	tryScaleWriters func(desired int, upscale bool, apply func() error) (applied bool, observedChunk int, prospectiveCap int, err error)
}

type chunkProjectionReporter interface {
	ReportChunkProjection(requested, effective int)
	ReportBatchProjection(requested, effective int)
}

type writerScaleDeferralReporter interface {
	ReportWriterScaleDeferral()
}

// downwardChunkCap is a race-safe, positive-only minimum. A pipeline's fixed
// channel depths cannot be resized, and writer downscales retire busy workers
// asynchronously. Once an applied concurrency state requires a lower execution
// cap, that cap therefore remains binding for the rest of the pipeline. A
// desired but refused WAW transition does not mutate it. Relaxing an applied
// cap requires a new pipeline (or a future explicit, measured resize protocol),
// not an incidental WAW backoff.
type downwardChunkCap struct {
	value atomic.Int64
}

func newDownwardChunkCap(initial int) *downwardChunkCap {
	cap := &downwardChunkCap{}
	if initial > 0 {
		cap.value.Store(int64(initial))
	}
	return cap
}

func (c *downwardChunkCap) observe(candidate int) int {
	if c == nil {
		return candidate
	}
	for {
		current64 := c.value.Load()
		current := positiveInt64ToInt(current64)
		if candidate <= 0 || (current > 0 && current <= candidate) {
			return current
		}
		if c.value.CompareAndSwap(current64, int64(candidate)) {
			return candidate
		}
	}
}

func (c *downwardChunkCap) current() int {
	if c == nil {
		return 0
	}
	return positiveInt64ToInt(c.value.Load())
}

func newTunerCallbacks(
	cfg *config.Config,
	tuner RuntimeTuner,
	tableName string,
	tableRowBytes int64,
	workers int,
	numReaders int,
	initialWriters int,
	buffers pool.PipelineBufferSizes,
) tunerCallbacks {
	if workers < 1 {
		workers = 1
	}
	if numReaders < 1 {
		numReaders = 1
	}
	if initialWriters < 1 {
		initialWriters = 1
	}

	capDetailForWriters := func(numWriters int) (int, bool) {
		if numWriters < 1 {
			numWriters = 1
		}
		return runtimeTableChunkSizeCapDetail(
			cfg,
			tableRowBytes,
			workers,
			numReaders,
			numWriters,
			buffers,
		)
	}
	capForWriters := func(numWriters int) int {
		cap, _ := capDetailForWriters(numWriters)
		return cap
	}
	protocolCap := 0
	if cfg != nil {
		protocolCap = cfg.Migration.TargetHardChunkLimit
	}
	// The initial policy is not projected through the per-table inventory
	// model. Restoring the pre-epic steady execution policy avoids the strongly
	// WAW-dependent chunk shrink that regressed transfer throughput. The target
	// protocol limit remains independently binding, and a complete-inventory
	// ceiling is committed after any applied writer-count transition.
	capRatchet := newDownwardChunkCap(protocolCap)
	// Serializing reader-size selection with writer upscales closes the
	// transition race where a callback could compute an old, larger cap just
	// before the tuner update but publish/use it after new writers were live.
	// maxReaderChunk is deliberately a lifetime high-water: proving that all
	// older chunks have drained would require a generation-aware queue barrier.
	var sizingMu sync.Mutex
	var scaleMu sync.Mutex
	maxReaderChunk := 0
	writerHighWater := initialWriters
	pendingTransitionCap := 0
	effectiveCap := func() int {
		return minPositiveInt(capRatchet.current(), pendingTransitionCap)
	}
	_, rowWidthKnown := tableSizingRowBytes(cfg, tableRowBytes)
	pipelineMB, _ := pipelineBudgetMB(cfg)
	memoryBoundAvailable := rowWidthKnown && pipelineMB > 0

	cb := tunerCallbacks{
		chunkSize: func() int {
			sizingMu.Lock()
			defer sizingMu.Unlock()
			requested := requestedReaderChunkSize(cfg, tuner, tableName)
			effective := capPositiveInt(requested, effectiveCap())
			if effective > maxReaderChunk {
				maxReaderChunk = effective
			}
			if reporter, ok := tuner.(chunkProjectionReporter); ok {
				reporter.ReportChunkProjection(requested, effective)
			}
			return effective
		},
		batchSize: func() int {
			sizingMu.Lock()
			defer sizingMu.Unlock()
			requested := requestedWriterBatchSize(cfg, tuner, tableName)
			effective := capPositiveInt(requested, effectiveCap())
			if reporter, ok := tuner.(chunkProjectionReporter); ok {
				reporter.ReportBatchProjection(requested, effective)
			}
			return effective
		},
		upsertChunk: func() int { return cfg.Migration.UpsertMergeChunkSize },
		checkpointFreq: func() int {
			f := cfg.Migration.CheckpointFrequency
			if f <= 0 {
				f = 10
			}
			return f
		},
		tryScaleWriters: func(desired int, upscale bool, apply func() error) (bool, int, int, error) {
			scaleMu.Lock()
			defer scaleMu.Unlock()

			if !upscale {
				// A downscale cannot increase inventory. Do not hold sizingMu while
				// ScaleWorkers retires idle workers: a writer may need batchSize
				// (and therefore this mutex) to finish its current call. Once the
				// transition succeeds, activate the complete-inventory ceiling for
				// the lifetime writer high-water. Busy retirees can still be live,
				// so a later rebound must never use a looser desired-writer cap.
				sizingMu.Lock()
				prospectiveCap := capForWriters(writerHighWater)
				observed := maxReaderChunk
				if apply == nil {
					sizingMu.Unlock()
					return false, observed, prospectiveCap, nil
				}
				// Publish a transactional cap before ScaleWorkers can expose an
				// asynchronously draining generation. Reader and writer callbacks
				// consult it during the unlocked apply window.
				pendingTransitionCap = prospectiveCap
				sizingMu.Unlock()
				if err := apply(); err != nil {
					sizingMu.Lock()
					pendingTransitionCap = 0
					sizingMu.Unlock()
					return false, observed, prospectiveCap, err
				}
				sizingMu.Lock()
				capRatchet.observe(prospectiveCap)
				pendingTransitionCap = 0
				observed = maxReaderChunk
				sizingMu.Unlock()
				return true, observed, prospectiveCap, nil
			}

			sizingMu.Lock()
			defer sizingMu.Unlock()
			prospectiveWriters := desired
			if writerHighWater > prospectiveWriters {
				prospectiveWriters = writerHighWater
			}
			prospectiveCap, minimumExceeds := capDetailForWriters(prospectiveWriters)
			if !memoryBoundAvailable || minimumExceeds || prospectiveCap <= 0 || maxReaderChunk > prospectiveCap {
				return false, maxReaderChunk, prospectiveCap, nil
			}
			if apply == nil {
				return false, maxReaderChunk, prospectiveCap, nil
			}
			if err := apply(); err != nil {
				return false, maxReaderChunk, prospectiveCap, err
			}
			// ScaleWorkers has admitted the new live-writer ceiling while sizingMu
			// keeps readers and newly started writers out of their sizing callbacks.
			// Commit the nonincreasing cap and high-water before unlocking; a
			// refused or failed transition leaves the pool and cap unchanged.
			writerHighWater = prospectiveWriters
			capRatchet.observe(prospectiveCap)
			return true, maxReaderChunk, prospectiveCap, nil
		},
	}
	if tuner == nil {
		return cb
	}
	cb.upsertChunk = func() int { return tuner.Snapshot().UpsertMergeChunkSize }
	cb.checkpointFreq = func() int {
		f := tuner.Snapshot().CheckpointFrequency
		if f <= 0 {
			f = 10
		}
		return f
	}
	return cb
}

// pipelineConfig wires one strategy into the shared runner.
type pipelineConfig struct {
	cfg      *config.Config
	job      Job
	tgtPool  pool.TargetPool
	prog     *progress.Tracker
	tuner    RuntimeTuner
	adjuster WriteErrorAdjuster

	producer chunkProducer

	targetTableName string
	targetCols      []string
	colTypes        []string
	colSRIDs        []int

	// idempotentOnDup routes plain inserts through the duplicate-safe
	// path on ROW_NUMBER replay (#227/#540); always false for keyset.
	idempotentOnDup bool

	resumeRowsDone int64

	// newAckHandler builds the strategy's checkpoint ack handler. saver is
	// the async persistence sink the runner owns (#620) — the coordinator
	// uses it for periodic saves. Returning nil disables ack processing.
	// Nil field means no checkpointing at all.
	// The handler returns the job count and total in-flight byte reservation
	// released by this acknowledgement. Both are zero for an out-of-order ack
	// and may include successors when the ack fills a sequence gap.
	newAckHandler func(cb tunerCallbacks, saver ProgressSaver) func(writeAck) ackRelease

	// onRangeDone receives a parallel producer's end-of-range marker. It is
	// deliberately separate from a write ack: a range is checkpoint-complete
	// only after this marker and every earlier write ack have arrived.
	onRangeDone func(readerID int, nextSeq int64)

	// saveFinal persists final progress after a successful drain. It reports
	// whether it wrote the authoritative durable watermark, so a trailing
	// periodic-save failure can be downgraded only when final state superseded
	// it. last is the last data chunk the consumer received (zero value if
	// none).
	saveFinal func(last chunkResult, totalTransferred int64) (saved bool, err error)
}

// runPipeline executes the shared read→buffer→write→ack pipeline for one
// transfer job.
func runPipeline(ctx context.Context, pc pipelineConfig) (*TransferStats, error) {
	cfg, job := pc.cfg, pc.job
	tableName := job.Table.Name
	stats := &TransferStats{}

	// Reader and writer counts are both needed up front to compute
	// pipeline buffer depths from the memory budget.
	numReaders := pc.producer.readerCount()
	if numReaders < 1 {
		numReaders = 1
	}
	numWriters := cfg.Migration.WriteAheadWriters
	readAheadBuffers := cfg.Migration.ReadAheadBuffers
	if pc.tuner != nil {
		snapshot := pc.tuner.Snapshot()
		if tw := snapshot.WriteAheadWriters; tw > 0 {
			numWriters = tw
		}
		if rab := snapshot.ReadAheadBuffers; rab > 0 {
			readAheadBuffers = rab
		}
	}
	if numWriters < 1 {
		numWriters = 1
	}

	// Compute both pipeline buffer depths from the shared memory budget.
	pipelineBufs := calculatePipelineBuffers(cfg, job, tableName, pc.tuner, numWriters, numReaders, readAheadBuffers)
	workers := effectiveRuntimeSizingTuple(cfg, pc.tuner).workers
	cb := newTunerCallbacks(
		cfg,
		pc.tuner,
		tableName,
		job.Table.EstimatedRowSize,
		workers,
		numReaders,
		numWriters,
		pipelineBufs,
	)
	if _, minimumExceeds := runtimeTableChunkSizeCapDetail(
		cfg,
		job.Table.EstimatedRowSize,
		workers,
		numReaders,
		numWriters,
		pipelineBufs,
	); minimumExceeds {
		rowBytes, _ := tableSizingRowBytes(cfg, job.Table.EstimatedRowSize)
		budgetMB, _ := pipelineBudgetMB(cfg)
		logging.Warn("Pipeline %s: the complete-inventory writer-growth model cannot fit one row inside the unified pipeline budget (budget=%d MB, workers=%d, readers=%d, writers=%d, row_width=%d B); runtime writer upscales are disabled for this pipeline while the requested steady policy continues under shared measured-byte admission and MemoryGuard",
			tableName, budgetMB, workers, numReaders, numWriters, rowBytes)
		if job.AuditEvent != nil {
			job.AuditEvent("pipeline_memory_minimum_over_budget", map[string]any{
				"table":                  tableName,
				"budget_mb":              budgetMB,
				"workers":                workers,
				"readers":                numReaders,
				"writers":                numWriters,
				"row_width_bytes":        rowBytes,
				"requested_chunk_size":   requestedReaderChunkSize(cfg, pc.tuner, tableName),
				"writer_growth_disabled": true,
			})
		}
	}
	bufferSize := pipelineBufs.ChunkChanDepth
	chunkChan := make(chan chunkResult, bufferSize)

	// Per-transfer reader context. Cancelling this releases any reader
	// goroutines blocked on `chunkChan <- result` after the consumer
	// stops draining (e.g. on writer failure), and aborts in-flight DB
	// queries via QueryContext so source-side cursors don't linger.
	// Deferred cancel covers all return paths; we also call it
	// explicitly after the consumer loop so the cleanup happens before
	// wp.wait() rather than after the function returns. (#250)
	readerCtx, cancelReaders := context.WithCancel(ctx)
	defer cancelReaders()

	// budgetCtx wakes a reader blocked reserving in-flight bytes when either
	// the readers are cancelled OR the writer pool fails (#617 codex review).
	// Without the writer-failure signal, a reader can wait forever for bytes
	// that only a now-departed writer could release, while the consumer —
	// still ranging on chunkChan — never observes the failure to unwind. A
	// watcher (started once wp exists) links wp's cancellation into this.
	budgetCtx, cancelBudgetWaits := context.WithCancel(readerCtx)
	defer cancelBudgetWaits()

	// Memory guardrail: use the migration-wide guard when the orchestrator
	// provided one; direct callers retain a private guard as a compatibility
	// fallback. The shared path keeps one GC leader across concurrent tables
	// under process-global heap pressure (#666).
	memGuard := memoryGuardForJob(cfg, job)
	// In-flight byte budget (#617). acquiredBytes tracks what this pipeline
	// has reserved but not yet released, so the drain can return the exact
	// residual — covering every chunk abandoned in a channel on an error or
	// cancel path — and guarantee the shared budget always nets back to
	// zero for this table. Successful chunks are released per-chunk by the
	// writer (OnComplete) to keep the budget accurate mid-run.
	budget := job.MemBudget
	metrics := observability.Global()
	var acquiredBytes int64
	env := pipelineEnv{
		memGuard:  memGuard,
		chunkSize: cb.chunkSize,
		acquireMem: func(_ context.Context, n int64) (int64, bool) {
			// Wait on budgetCtx (reader-cancel or writer-failure), not the
			// caller's reader ctx, so a writer failure unblocks the reserve.
			// A disabled budget is a true no-op: do not take clocks or emit
			// misleading zero-value metrics for an admission control that is
			// not configured (#668).
			if budget == nil {
				return 0, true
			}
			waitStart := time.Now()
			got, ok := budget.acquire(budgetCtx, n)
			wait := time.Since(waitStart)
			if wait > 0 {
				if pc.tuner != nil {
					pc.tuner.ReportBudgetWait(wait.Nanoseconds())
				}
				metrics.AddBudgetWait(tableName, wait.Seconds())
				if wait > time.Second {
					logging.Debug("Pipeline %s: reader waited %v for in-flight memory budget", tableName, wait)
				}
			}
			if ok && got > 0 {
				atomic.AddInt64(&acquiredBytes, got)
			}
			return got, ok
		},
	}

	// Run the strategy's readers; close chunkChan when they all finish.
	// producerDone is closed once every reader has returned, so the drain can
	// join them before the residual release — without the join, an acquire
	// that succeeds just before cancellation could add to acquiredBytes after
	// the residual swap and leak that reservation (#617 codex review).
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		pc.producer.produce(readerCtx, env, chunkChan)
		logging.Debug("All %d parallel readers finished for %s, closing chunkChan (len=%d)", numReaders, tableName, len(chunkChan))
		close(chunkChan)
	}()

	// Partition ID for staging table naming and checkpointing.
	var partitionID *int
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
	}

	jobBufSize := pipelineBufs.JobChanDepth
	requestedChunk := requestedReaderChunkSize(cfg, pc.tuner, tableName)
	requestedBatch := requestedWriterBatchSize(cfg, pc.tuner, tableName)
	effectiveChunk := cb.chunkSize()
	effectiveBatch := cb.batchSize()
	if effectiveChunk != requestedChunk || effectiveBatch != requestedBatch {
		logging.Info("Pipeline %s: target protocol limit applied (reader chunk %d -> %d, writer batch %d -> %d, hard limit=%d); persisted chunk_size remains the requested policy",
			tableName, requestedChunk, effectiveChunk, requestedBatch, effectiveBatch,
			cfg.Migration.TargetHardChunkLimit)
	}
	logging.Debug("Pipeline %s: chunkChan=%d, jobChan=%d (effectiveChunk=%d, configuredChunk=%d, rowBytes=%d, writers=%d, readers=%d)",
		tableName, bufferSize, jobBufSize, effectiveChunk, cfg.Migration.ChunkSize, job.Table.EstimatedRowSize, numWriters, numReaders)

	enableAck := job.Saver != nil && job.TaskID > 0

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:             numWriters,
		BufferSize:             bufferSize,
		JobBufferSize:          jobBufSize,
		UseUpsert:              cfg.Migration.TargetMode == "upsert",
		IdempotentOnDup:        pc.idempotentOnDup,
		UpsertMergeChunkSizeFn: cb.upsertChunk,
		BatchSizeFn:            cb.batchSize,
		TargetSchema:           cfg.Target.Schema,
		TargetTable:            pc.targetTableName,
		TargetCols:             pc.targetCols,
		ColTypes:               pc.colTypes,
		ColSRIDs:               pc.colSRIDs,
		TargetPKCols:           buildTargetPKCols(job.Table.PrimaryKey, pc.tgtPool),
		PartitionID:            partitionID,
		TgtPool:                pc.tgtPool,
		Prog:                   pc.prog,
		EnableAck:              enableAck,
		Tuner:                  pc.tuner,
		Adjuster:               pc.adjuster,
		TableName:              tableName,
		BytesPerRow:            job.Table.GoHeapBytesPerRow(), // #229 metrics bytes_total estimate
		OnComplete: func(bytes int64) {
			// Release a chunk's reservation once the writer is done with it,
			// success or error (#617). Decrement the pipeline's running total
			// so the final residual release covers only chunks that never
			// reached a writer.
			if bytes > 0 {
				budget.release(bytes)
				atomic.AddInt64(&acquiredBytes, -bytes)
			}
		},
	})

	// Periodic checkpoints go through an async saver so a slow SaveProgress
	// (SQLite fsync, YAML rewrite) can't stall ack draining and, via the
	// bounded ackChan, backpressure the writers (#620). It is flushed and
	// joined during drain, before the final synchronous save.
	var asyncSv *asyncSaver
	if pc.newAckHandler != nil {
		var handlerSaver ProgressSaver
		if enableAck {
			asyncSv = newAsyncSaver(job.Saver)
			asyncSv.start()
			handlerSaver = asyncSv
		}
		if handler := pc.newAckHandler(cb, handlerSaver); handler != nil {
			wp.startAckProcessor(handler)
		}
	}

	wp.start()

	// Link writer-pool failure into budgetCtx so a reader blocked reserving
	// bytes wakes the moment a writer errors (#617). The watcher exits when
	// budgetCtx is cancelled (the deferred cancelBudgetWaits on every return
	// path), so it never outlives the transfer.
	if budget != nil {
		go func() {
			select {
			case <-wp.Context().Done():
				cancelBudgetWaits()
			case <-budgetCtx.Done():
			}
		}()
	}

	// Main consumer loop — reads from chunkChan, dispatches to write pool.
	totalTransferred := pc.resumeRowsDone
	chunkCount := 0
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var lastResult chunkResult
	var loopErr error
	var lastReportedQueueDepth int // for delta-based queue depth reporting
	var deferredWriterTarget int   // suppress duplicate transition-safety notices

	debugEnabled := logging.IsDebug()
	var chunkWaitStart time.Time
	var totalChunkWait time.Duration  // total time consumer spent waiting for readers
	var totalSubmitWait time.Duration // total time consumer spent blocked on submit (writers full)
	if debugEnabled {
		chunkWaitStart = time.Now()
	}

chunkLoop:
	for {
		var result chunkResult
		select {
		case next, ok := <-chunkChan:
			if !ok {
				break chunkLoop
			}
			result = next
		case <-wp.Context().Done():
			// Writer failure must interrupt the reader side immediately. The
			// producer may be blocked in a source QueryContext and cannot close
			// chunkChan until its reader context is cancelled (#639).
			cancelReaders()
			if err := ctx.Err(); err != nil {
				loopErr = err
			} else if err := wp.error(); err != nil {
				loopErr = fmt.Errorf("writing chunk: %w", err)
			} else {
				loopErr = context.Canceled
			}
			break chunkLoop
		}

		if debugEnabled {
			chunkWait := time.Since(chunkWaitStart)
			totalChunkWait += chunkWait
			if chunkCount > 0 && chunkWait > 500*time.Millisecond {
				logging.Debug("Pipeline %s: consumer waited %v for chunk #%d from readers (chunkChan len=%d)",
					tableName, chunkWait, chunkCount, len(chunkChan))
			}
		}

		if result.err != nil {
			loopErr = result.err
			wp.Cancel()
			break
		}
		if result.done {
			break
		}
		if result.rangeDone {
			if pc.onRangeDone != nil {
				pc.onRangeDone(result.readerID, result.seq)
			}
			continue
		}

		// Report read-ahead queue depth to tuner (delta-based for aggregation)
		if pc.tuner != nil {
			currentQueueDepth := len(chunkChan)
			pc.tuner.ReportQueueDepth(currentQueueDepth - lastReportedQueueDepth)
			lastReportedQueueDepth = currentQueueDepth
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		// Final checkpointing only needs pagination metadata. Keeping rows here
		// would retain the last completed chunk outside the modeled pipeline
		// inventory (and after its MemBudget reservation is released).
		lastResult = result
		lastResult.rows = nil

		// Calculate overlap: if this chunk was ready before last write ended, we had overlap
		receiveTime := time.Now()
		if !lastWriteEnd.IsZero() && !result.readEnd.IsZero() && result.readEnd.Before(lastWriteEnd) {
			overlap := lastWriteEnd.Sub(result.readEnd)
			totalOverlap += overlap
		}
		lastWriteEnd = time.Now()

		// Dispatch to write pool (may block if jobChan is full)
		var submitStart time.Time
		if debugEnabled {
			submitStart = time.Now()
		}
		if !wp.submit(writeJob{
			rows:     result.rows,
			lastPK:   result.lastPK,
			rowNum:   result.rowNum,
			readerID: result.readerID,
			seq:      result.seq,
			bytes:    result.bytes,
		}) {
			if err := wp.error(); err != nil {
				loopErr = fmt.Errorf("writing chunk: %w", err)
			} else {
				loopErr = ctx.Err()
			}
			break chunkLoop
		}
		// Ownership moved to jobChan/the writer. Drop the consumer's alias so
		// the single dispatch slot counted by the pipeline model ends here.
		result.rows = nil
		if debugEnabled {
			totalSubmitWait += time.Since(submitStart)
		}

		// Check for tuner-driven writer scaling at chunk boundaries
		if pc.tuner != nil {
			if desired := pc.tuner.Snapshot().WriteAheadWriters; desired > 0 && desired != numWriters {
				upscale := desired > numWriters
				applied, observedChunk, prospectiveCap, err := cb.tryScaleWriters(desired, upscale, func() error {
					return wp.ScaleWorkers(desired)
				})
				if err != nil {
					logging.Warn("Failed to scale workers: %v", err)
				} else if applied {
					logging.Debug("Scaled writers from %d to %d (tuner)", numWriters, desired)
					numWriters = desired
					deferredWriterTarget = 0
				} else if upscale && deferredWriterTarget != desired {
					if reporter, ok := pc.tuner.(writerScaleDeferralReporter); ok {
						reporter.ReportWriterScaleDeferral()
					}
					logging.Info("Pipeline %s: deferring runtime writer upscale %d -> %d until a new pipeline because the prospective fixed-buffer safety check did not admit it (reader chunk high-water=%d, transition cap=%d); the requested chunk policy remains unchanged",
						tableName, numWriters, desired, observedChunk, prospectiveCap)
					deferredWriterTarget = desired
				}
			} else if desired == numWriters {
				deferredWriterTarget = 0
			}
		}
		// Actual live workers include downscaled goroutines draining their
		// last write, which can briefly exceed the desired tuner value. This
		// is a cheap atomic sample at the existing scaling boundary; it gives
		// dashboards and future controller rules the real writer population.
		metrics.SetLiveWriters(tableName, wp.GetLiveWorkerCount())

		// Log pipeline stats periodically
		if debugEnabled && chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			logging.Debug("Pipeline %s: %d chunks, overlap=%v, dispatch=%v, buffers=%d, writers=%d, chunkWait=%v, submitWait=%v",
				tableName, chunkCount, totalOverlap, waitTime, bufferSize, numWriters, totalChunkWait, totalSubmitWait)
		}

		chunkCount++
		if debugEnabled {
			chunkWaitStart = time.Now()
		}
	}

	// Release any reader goroutines blocked mid-send on chunkChan before
	// wp.wait() blocks the function for in-flight writes. The deferred
	// cancelReaders() at function entry would otherwise only fire after
	// return, leaving readers stuck (and holding source-side cursors)
	// for the entire writer drain. (#250)
	cancelReaders()

	// If the parent context was cancelled while readers were shutting
	// down, sendChunkOrCancel's select can race and silently drop the
	// reader's error chunk (both branches ready). Catch that here so a
	// SIGINT/timeout during transfer can't be reported as a successful
	// migration. (#250 review)
	if loopErr == nil && ctx.Err() != nil {
		loopErr = ctx.Err()
	}

	// Clean up queue depth reporting
	if pc.tuner != nil && lastReportedQueueDepth != 0 {
		pc.tuner.ReportQueueDepth(-lastReportedQueueDepth)
	}

	logging.Debug("Consumer loop finished for %s: %d chunks, chunkWait=%v, submitWait=%v, overlap=%v",
		tableName, chunkCount, totalChunkWait, totalSubmitWait, totalOverlap)

	// Wait for writers to finish
	waitStart := time.Now()
	wp.wait()
	metrics.SetLiveWriters(tableName, wp.GetLiveWorkerCount())
	logging.Debug("wp.wait() completed in %v for %s", time.Since(waitStart), tableName)

	// Return this pipeline's in-flight byte reservation to the shared budget
	// (#617). Join the producers first so acquiredBytes is final: an acquire
	// can succeed just before cancellation and add to the counter after this
	// point, so releasing without the join could leak that reservation and
	// permanently shrink the shared budget. The join is bounded — cancelReaders
	// above cancelled budgetCtx (waking any acquire-blocked reader) and
	// readerCtx (aborting in-flight QueryContext/Rows.Next) — so producers
	// return promptly rather than hanging the drain. Writers already released
	// every chunk they finished; the remainder is exactly the chunks abandoned
	// in a channel on an error/cancel path, released here in one shot so the
	// shared budget nets back to zero for this table regardless of how it ended.
	<-producerDone
	if residual := atomic.SwapInt64(&acquiredBytes, 0); residual > 0 {
		budget.release(residual)
	}

	// Flush and join the async checkpoint saver now that the ack processor
	// has stopped (wp.wait joined it) — no more periodic saves can be
	// posted. Joining here guarantees no stale async write lands after the
	// final synchronous save below. Runs on the error paths too so the
	// saver goroutine never leaks (#620).
	var saverErr error
	if asyncSv != nil {
		saverErr = asyncSv.close()
	}

	if loopErr != nil {
		return stats, errors.Join(loopErr, saverErr)
	}

	// Check for write errors
	if err := wp.error(); err != nil {
		return stats, errors.Join(fmt.Errorf("writing chunk: %w", err), saverErr)
	}

	// Aggregate stats
	stats.WriteTime = wp.writeTime()
	totalTransferred += wp.written()
	stats.Rows = totalTransferred

	// Save final progress (synchronous + durable, through the raw saver).
	// A successful final write supersedes every periodic checkpoint: those are
	// only crash insurance until this terminal watermark is durable (#665).
	finalSaved := false
	if pc.saveFinal != nil {
		var err error
		finalSaved, err = pc.saveFinal(lastResult, totalTransferred)
		if err != nil {
			return stats, errors.Join(fmt.Errorf("saving final checkpoint for %s: %w", tableName, err), saverErr)
		}
	}

	if saverErr != nil {
		if finalSaved {
			consecutiveFailures, lastErr := periodicSaveFailureDetails(asyncSv, saverErr)
			lastError := logging.Scrub(lastErr.Error())
			logging.WarnEvent("checkpoint_periodic_save_degraded",
				"table", tableName,
				"consecutive_failures", consecutiveFailures,
				"last_error", lastError,
			)
			if job.AuditEvent != nil {
				job.AuditEvent("checkpoint_periodic_save_degraded", map[string]any{
					"table":                tableName,
					"consecutive_failures": consecutiveFailures,
					"last_error":           lastError,
				})
			}
			return stats, nil
		}
		return stats, fmt.Errorf("saving periodic checkpoints for %s: %w", tableName, saverErr)
	}

	return stats, nil
}

func memoryGuardForJob(cfg *config.Config, job Job) *MemoryGuard {
	if job.MemGuard != nil {
		return job.MemGuard
	}
	return NewMemoryGuard(MemoryGuardLimitMB(cfg))
}

// periodicSaveFailureDetails exposes asyncSaver's terminal state to the
// runner, which alone knows whether a synchronous final save superseded it.
// asyncSaver is fully joined before this helper is called, but retain its lock
// so this stays correct if that lifecycle ever changes.
func periodicSaveFailureDetails(saver *asyncSaver, fallback error) (int, error) {
	if saver == nil {
		return 0, fallback
	}
	saver.mu.Lock()
	defer saver.mu.Unlock()
	if saver.lastErr == nil {
		return saver.consecFails, fallback
	}
	return saver.consecFails, saver.lastErr
}
