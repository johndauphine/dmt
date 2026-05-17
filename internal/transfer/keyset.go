package transfer

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"sync"
	"time"
)

// executeKeysetPagination uses WHERE pk > last_pk for efficient pagination
// with async read-ahead pipelining to overlap reads and writes
func executeKeysetPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
	targetTableName string,
	tuner RuntimeTuner,
	aiAdjuster WriteErrorAdjuster,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}
	pkCol := job.Table.PrimaryKey[0]

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %s", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)
	baseChunkSize := cfg.Migration.ChunkSize

	// chunkSizeFn reads chunk_size dynamically from the tuner so that runtime
	// adjustments (AI-driven, error-driven) take effect on in-flight readers.
	// Priority: per-table override → global tuner value → config default.
	tableName := job.Table.Name
	chunkSizeFn := func() int { return baseChunkSize }
	if tuner != nil {
		chunkSizeFn = func() int {
			if cs, ok := tuner.TableChunkSize(tableName); ok && cs > 0 {
				return cs
			}
			if cs := tuner.Snapshot().ChunkSize; cs > 0 {
				return cs
			}
			return baseChunkSize
		}
	}

	// Get PK range for parallel readers
	var minPKVal, maxPKVal any
	if job.Partition != nil {
		minPKVal = job.Partition.MinPK
		maxPKVal = job.Partition.MaxPK
	} else {
		// For non-partitioned tables, get min and max PK
		minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s %s",
			srcDialect.QuoteIdentifier(pkCol), srcDialect.QuoteIdentifier(pkCol),
			srcDialect.QualifyTable(job.Table.Schema, job.Table.Name), tableHint)
		err := db.QueryRowContext(ctx, minMaxQuery).Scan(&minPKVal, &maxPKVal)
		if err != nil || minPKVal == nil {
			return stats, nil // Empty table
		}
	}

	// Use resume point if available
	if resumeLastPK != nil {
		minPKVal = resumeLastPK
	}

	// Find PK column index
	pkIdx := 0
	for i, c := range cols {
		if c == pkCol {
			pkIdx = i
			break
		}
	}

	// Determine number of parallel readers and writers upfront — both are needed
	// to compute pipeline buffer depths from the memory budget.
	numReaders := cfg.Migration.ParallelReaders
	if numReaders < 1 {
		numReaders = 1
	}
	numWriters := cfg.Migration.WriteAheadWriters
	if tuner != nil {
		if tw := tuner.Snapshot().WriteAheadWriters; tw > 0 {
			numWriters = tw
		}
	}
	if numWriters < 1 {
		numWriters = 1
	}

	// Compute both pipeline buffer depths from the shared memory budget.
	// This replaces the old magic-number multipliers with a proper memory model.
	pipelineBufs := calculatePipelineBuffers(cfg, job, tableName, tuner, numWriters, numReaders, cfg.Migration.ReadAheadBuffers)
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

	// Split PK range for parallel readers
	pkRanges := splitPKRange(minPKVal, maxPKVal, numReaders)

	// Memory guardrail: pause readers when heap exceeds 80% of memory limit.
	// This prevents memory ballooning when actual row sizes exceed static estimates
	// (e.g., TEXT columns with large content vs. the default 256-byte estimate).
	// Apply the same user cap as pipeline buffer sizing.
	guardMemMB := cfg.AutoConfig().EffectiveMaxMemoryMB
	if cfg.Migration.MaxMemoryMB > 0 && cfg.Migration.MaxMemoryMB < guardMemMB {
		guardMemMB = cfg.Migration.MaxMemoryMB
	}
	memGuard := newMemoryGuard(guardMemMB)

	// Start parallel reader goroutines
	var readerWg sync.WaitGroup
	for readerID, pkr := range pkRanges {
		readerWg.Add(1)
		go func(readerID int, rangeMinPK, rangeMaxPK any) {
			defer readerWg.Done()

			lastPK := rangeMinPK
			seq := int64(0)

			for {
				select {
				case <-readerCtx.Done():
					sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: readerCtx.Err()})
					return
				default:
				}

				// Memory pressure check — pause if heap is above threshold
				if !memGuard.waitIfNeeded(readerCtx) {
					sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: readerCtx.Err()})
					return
				}

				// Read chunk_size dynamically so guardrail reductions take effect immediately
				chunkSize := chunkSizeFn()

				// Always use bounded query for parallel readers
				query := srcDialect.BuildKeysetQuery(colList, pkCol, job.Table.Schema, job.Table.Name, tableHint, true, job.DateFilter)
				args := srcDialect.BuildKeysetArgs(lastPK, rangeMaxPK, chunkSize, true, job.DateFilter)

				// Time the query
				queryStart := time.Now()
				rows, err := db.QueryContext(readerCtx, query, args...)
				queryTime := time.Since(queryStart)
				if err != nil {
					sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: fmt.Errorf("keyset query: %w", err)})
					return
				}

				// Time the scan
				scanStart := time.Now()
				chunk, _, err := scanRows(rows, cols, colTypes)
				rows.Close()
				scanTime := time.Since(scanStart)
				if err != nil {
					sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
					return
				}

				if len(chunk) == 0 {
					return // This reader is done
				}

				if logging.IsDebug() {
					logging.Debug("Reader[%d]: chunk #%d read %d rows (query=%v, scan=%v)", readerID, seq, len(chunk), queryTime, scanTime)
				}
				// Update lastPK for next iteration
				lastPK = chunk[len(chunk)-1][pkIdx]

				var sendStart time.Time
				if logging.IsDebug() {
					sendStart = time.Now()
				}
				if !sendChunkOrCancel(readerCtx, chunkChan, chunkResult{
					rows:      chunk,
					lastPK:    lastPK,
					readerID:  readerID,
					seq:       seq,
					queryTime: queryTime,
					scanTime:  scanTime,
					readEnd:   time.Now(),
				}) {
					return
				}
				if logging.IsDebug() {
					if sendWait := time.Since(sendStart); sendWait > 500*time.Millisecond {
						logging.Debug("Reader[%d]: blocked %v sending chunk #%d to chunkChan (len=%d, cap=%d)",
							readerID, sendWait, seq, len(chunkChan), cap(chunkChan))
					}
				}
				seq++

				if len(chunk) < chunkSize {
					return // This reader is done
				}
			}
		}(readerID, pkr.minPK, pkr.maxPK)
	}

	// Close chunkChan when all readers are done
	go func() {
		readerWg.Wait()
		logging.Debug("All %d parallel readers finished, closing chunkChan (len=%d)", numReaders, len(chunkChan))
		close(chunkChan)
	}()

	// Get partition ID for staging table naming
	var partitionID *int
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
	}

	// Build callbacks: if tuner is present, read dynamically; otherwise use static config values
	upsertChunkFn := func() int { return cfg.Migration.UpsertMergeChunkSize }
	checkpointFreqFn := func() int { return cfg.Migration.CheckpointFrequency }
	if tuner != nil {
		upsertChunkFn = func() int { return tuner.Snapshot().UpsertMergeChunkSize }
		checkpointFreqFn = func() int { return tuner.Snapshot().CheckpointFrequency }
	}

	// Build batch size callback: per-table override from tuner, then global
	// tuner chunk_size, then config chunk_size. This ensures AI-tuned values
	// reach the writer even though target.chunk_size is set before AI tuning.
	baseChunkSizeForBatch := cfg.Migration.ChunkSize
	batchSizeFn := func() int { return baseChunkSizeForBatch }
	if tuner != nil {
		batchSizeFn = func() int {
			if bs, ok := tuner.TableBatchSize(tableName); ok && bs > 0 {
				return bs
			}
			if cs := tuner.Snapshot().ChunkSize; cs > 0 {
				return cs
			}
			return baseChunkSizeForBatch
		}
	}

	// Compute job buffer size from memory budget and actual row size.
	// Use the jobChan depth from the same memory-budget calculation that sized chunkChan.
	jobBufSize := pipelineBufs.JobChanDepth
	logging.Debug("Pipeline %s: chunkChan=%d, jobChan=%d (configChunk=%d, rowBytes=%d, writers=%d, readers=%d)",
		job.Table.Name, bufferSize, jobBufSize, cfg.Migration.ChunkSize, job.Table.EstimatedRowSize, numWriters, numReaders)

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:             numWriters,
		BufferSize:             bufferSize,
		JobBufferSize:          jobBufSize,
		UseUpsert:              cfg.Migration.TargetMode == "upsert",
		UpsertMergeChunkSizeFn: upsertChunkFn,
		BatchSizeFn:            batchSizeFn,
		TargetSchema:           cfg.Target.Schema,
		TargetTable:            targetTableName,
		TargetCols:             targetCols,
		ColTypes:               colTypes,
		ColSRIDs:               colSRIDs,
		TargetPKCols:           buildTargetPKCols(job.Table.PrimaryKey, tgtPool),
		PartitionID:            partitionID,
		TgtPool:                tgtPool,
		Prog:                   prog,
		EnableAck:              job.Saver != nil && job.TaskID > 0,
		Tuner:                  tuner,
		AIAdjuster:             aiAdjuster,
		TableName:              job.Table.Name,
		BytesPerRow:            job.Table.GoHeapBytesPerRow(), // #229 metrics bytes_total estimate
	})

	// Setup checkpoint coordinator with dynamic checkpoint frequency
	checkpointCoord := newKeysetCheckpointCoordinator(job, pkRanges, resumeRowsDone, wp.TotalWrittenPtr(), checkpointFreqFn)
	if checkpointCoord != nil {
		wp.startAckProcessor(checkpointCoord.onAck)
	}

	wp.start()

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	totalTransferred := resumeRowsDone
	chunkCount := 0
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var lastPK any
	var loopErr error
	var lastReportedQueueDepth int // for delta-based queue depth reporting

	// Process chunks and dispatch writes
	debugEnabled := logging.IsDebug()
	var chunkWaitStart time.Time
	var totalChunkWait time.Duration  // total time consumer spent waiting for readers
	var totalSubmitWait time.Duration // total time consumer spent blocked on submit (writers full)
	if debugEnabled {
		chunkWaitStart = time.Now()
	}

chunkLoop:
	for result := range chunkChan {
		if debugEnabled {
			chunkWait := time.Since(chunkWaitStart)
			totalChunkWait += chunkWait
			if chunkCount > 0 && chunkWait > 500*time.Millisecond {
				logging.Debug("Pipeline %s: consumer waited %v for chunk #%d from readers (chunkChan len=%d)",
					job.Table.Name, chunkWait, chunkCount, len(chunkChan))
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
		if tuner != nil {
			currentQueueDepth := len(chunkChan)
			tuner.ReportQueueDepth(currentQueueDepth - lastReportedQueueDepth)
			lastReportedQueueDepth = currentQueueDepth
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		lastPK = result.lastPK

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
			readerID: result.readerID,
			seq:      result.seq,
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
		if tuner != nil {
			if desired := tuner.Snapshot().WriteAheadWriters; desired > 0 && desired != numWriters {
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
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters, totalChunkWait, totalSubmitWait)
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
	if tuner != nil && lastReportedQueueDepth != 0 {
		tuner.ReportQueueDepth(-lastReportedQueueDepth)
	}

	logging.Debug("Consumer loop finished for %s: %d chunks, chunkWait=%v, submitWait=%v, overlap=%v",
		job.Table.Name, chunkCount, totalChunkWait, totalSubmitWait, totalOverlap)

	// Wait for writers to finish
	waitStart := time.Now()
	wp.wait()
	logging.Debug("wp.wait() completed in %v for %s", time.Since(waitStart), job.Table.Name)

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

	// Save final progress
	if job.Saver != nil && job.TaskID > 0 && lastPK != nil {
		finalLastPK := lastPK
		if checkpointCoord != nil {
			finalLastPK = checkpointCoord.finalCheckpoint(lastPK)
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalLastPK, totalTransferred, job.Table.RowCount); err != nil {
			logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
		}
	}

	return stats, nil
}
