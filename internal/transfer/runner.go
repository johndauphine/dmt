package transfer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
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
	memGuard  *memoryGuard
	chunkSize func() int

	// acquireMem reserves a scanned chunk's measured bytes against the shared
	// budget, blocking (backpressuring the reader) until they are available.
	// It returns the bytes actually reserved (to stamp on the chunk so the
	// writer releases the same amount) and false if ctx was cancelled while
	// waiting. With no budget it reserves 0 and returns true immediately.
	//
	// A reader that blocks here holds no reservation — only the one chunk it
	// just scanned — so this is a single, deadlock-free acquire, never a
	// hold-and-wait top-up. The transient overshoot of at most one chunk per
	// reader is exactly the per-reader scan-accumulation slack the #465
	// buffer model already reserves, with the memory guard as the backstop.
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
	chunkSize      func() int
	batchSize      func() int
	upsertChunk    func() int
	checkpointFreq func() int
}

func newTunerCallbacks(cfg *config.Config, tuner RuntimeTuner, tableName string) tunerCallbacks {
	baseChunkSize := cfg.Migration.ChunkSize
	cb := tunerCallbacks{
		chunkSize:   func() int { return baseChunkSize },
		batchSize:   func() int { return baseChunkSize },
		upsertChunk: func() int { return cfg.Migration.UpsertMergeChunkSize },
		checkpointFreq: func() int {
			f := cfg.Migration.CheckpointFrequency
			if f <= 0 {
				f = 10
			}
			return f
		},
	}
	if tuner == nil {
		return cb
	}
	cb.chunkSize = func() int {
		if cs, ok := tuner.TableChunkSize(tableName); ok && cs > 0 {
			return cs
		}
		if cs := tuner.Snapshot().ChunkSize; cs > 0 {
			return cs
		}
		return baseChunkSize
	}
	// Batch size reaches the writer even though target.chunk_size is set
	// before tuning: per-table override, then global tuner chunk_size.
	cb.batchSize = func() int {
		if bs, ok := tuner.TableBatchSize(tableName); ok && bs > 0 {
			return bs
		}
		if cs := tuner.Snapshot().ChunkSize; cs > 0 {
			return cs
		}
		return baseChunkSize
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
	newAckHandler func(cb tunerCallbacks, saver ProgressSaver) func(writeAck)

	// saveFinal persists final progress after a successful drain. last is
	// the last data chunk the consumer received (zero value if none).
	saveFinal func(last chunkResult, totalTransferred int64)
}

// runPipeline executes the shared read→buffer→write→ack pipeline for one
// transfer job.
func runPipeline(ctx context.Context, pc pipelineConfig) (*TransferStats, error) {
	cfg, job := pc.cfg, pc.job
	tableName := job.Table.Name
	stats := &TransferStats{}
	cb := newTunerCallbacks(cfg, pc.tuner, tableName)

	// Reader and writer counts are both needed up front to compute
	// pipeline buffer depths from the memory budget.
	numReaders := pc.producer.readerCount()
	if numReaders < 1 {
		numReaders = 1
	}
	numWriters := cfg.Migration.WriteAheadWriters
	if pc.tuner != nil {
		if tw := pc.tuner.Snapshot().WriteAheadWriters; tw > 0 {
			numWriters = tw
		}
	}
	if numWriters < 1 {
		numWriters = 1
	}

	// Compute both pipeline buffer depths from the shared memory budget.
	pipelineBufs := calculatePipelineBuffers(cfg, job, tableName, pc.tuner, numWriters, numReaders, cfg.Migration.ReadAheadBuffers)
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

	// Memory guardrail: pause readers when heap exceeds 80% of memory limit.
	// This prevents memory ballooning when actual row sizes exceed static
	// estimates (e.g., TEXT columns with large content vs. the default
	// 256-byte estimate). Apply the same user cap as pipeline buffer sizing.
	guardMemMB := cfg.AutoConfig().EffectiveMaxMemoryMB
	if cfg.Migration.MaxMemoryMB > 0 && cfg.Migration.MaxMemoryMB < guardMemMB {
		guardMemMB = cfg.Migration.MaxMemoryMB
	}
	// In-flight byte budget (#617). acquiredBytes tracks what this pipeline
	// has reserved but not yet released, so the drain can return the exact
	// residual — covering every chunk abandoned in a channel on an error or
	// cancel path — and guarantee the shared budget always nets back to
	// zero for this table. Successful chunks are released per-chunk by the
	// writer (OnComplete) to keep the budget accurate mid-run.
	budget := job.MemBudget
	var acquiredBytes int64
	env := pipelineEnv{
		memGuard:  newMemoryGuard(guardMemMB),
		chunkSize: cb.chunkSize,
		acquireMem: func(_ context.Context, n int64) (int64, bool) {
			// Wait on budgetCtx (reader-cancel or writer-failure), not the
			// caller's reader ctx, so a writer failure unblocks the reserve.
			got, ok := budget.acquire(budgetCtx, n)
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
	logging.Debug("Pipeline %s: chunkChan=%d, jobChan=%d (configChunk=%d, rowBytes=%d, writers=%d, readers=%d)",
		tableName, bufferSize, jobBufSize, cfg.Migration.ChunkSize, job.Table.EstimatedRowSize, numWriters, numReaders)

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

		// Report read-ahead queue depth to tuner (delta-based for aggregation)
		if pc.tuner != nil {
			currentQueueDepth := len(chunkChan)
			pc.tuner.ReportQueueDepth(currentQueueDepth - lastReportedQueueDepth)
			lastReportedQueueDepth = currentQueueDepth
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		lastResult = result

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
		if debugEnabled {
			totalSubmitWait += time.Since(submitStart)
		}

		// Check for tuner-driven writer scaling at chunk boundaries
		if pc.tuner != nil {
			if desired := pc.tuner.Snapshot().WriteAheadWriters; desired > 0 && desired != numWriters {
				if err := wp.ScaleWorkers(desired); err != nil {
					logging.Warn("Failed to scale workers: %v", err)
				} else {
					logging.Debug("Scaled writers from %d to %d (tuner)", numWriters, desired)
					numWriters = desired
				}
			}
		}

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
		return stats, loopErr
	}

	// Check for write errors
	if err := wp.error(); err != nil {
		return stats, fmt.Errorf("writing chunk: %w", err)
	}

	// Aggregate stats
	stats.WriteTime = wp.writeTime()
	totalTransferred += wp.written()
	stats.Rows = totalTransferred

	// Save final progress (synchronous + durable, through the raw saver).
	if pc.saveFinal != nil {
		pc.saveFinal(lastResult, totalTransferred)
	}

	// A persistently broken saver doesn't fail an otherwise-successful data
	// transfer — the rows landed — but it must be loud, since resume would
	// silently restart from an earlier point (#620).
	if saverErr != nil {
		logging.Warn("Checkpoint saver unhealthy during %s: %v (resume may restart from an earlier checkpoint)", tableName, saverErr)
	}

	return stats, nil
}
