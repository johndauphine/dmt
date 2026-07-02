package transfer

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"strings"
	"time"
)

// executeRowNumberPagination uses ROW_NUMBER for composite/varchar PKs
// with async read-ahead pipelining to overlap reads and writes
func executeRowNumberPagination(
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
	writeErrorAdjuster WriteErrorAdjuster,
) (*TransferStats, error) {
	db := srcPool.DB()
	stats := &TransferStats{}

	// Use dialect for database-specific SQL syntax
	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %q", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	// Source-dialect value normalization, resolved once per transfer (#477).
	valueConvs := srcDialect.ValueConverters(colTypes, tgtPool.DBType())
	convIdx := buildConvIdx(valueConvs)
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)

	// Build ORDER BY clause from PK columns
	// Tables without PK cannot be migrated safely - fail fast
	if len(job.Table.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key - cannot guarantee data correctness with ROW_NUMBER pagination. "+
			"Add a primary key to the table or exclude it from migration", job.Table.FullName())
	}

	pkCols := make([]string, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		pkCols[i] = srcDialect.QuoteIdentifier(pk)
	}
	orderBy := strings.Join(pkCols, ", ")

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

	// Determine row range for this job. The final ROW_NUMBER range is
	// intentionally unbounded so stats-estimated RowCount undercounts cannot
	// drop tail rows.
	var startRow, endRow int64
	boundedEnd := false
	if job.Partition != nil && job.Partition.EndRow > 0 {
		// Partitioned: use partition boundaries
		startRow = job.Partition.StartRow
		endRow = job.Partition.EndRow
		boundedEnd = job.Partition.EndRow < job.Table.RowCount
	} else {
		// Non-partitioned: process entire table
		startRow = 0
		endRow = job.Table.RowCount
	}

	// Determine writer count upfront — needed for pipeline buffer sizing.
	numWriters := cfg.Migration.WriteAheadWriters
	if tuner != nil {
		if tw := tuner.Snapshot().WriteAheadWriters; tw > 0 {
			numWriters = tw
		}
	}
	if numWriters < 1 {
		numWriters = 1
	}

	// Resume from saved progress if available
	initialRowNum := startRow
	if resumeRowNum, ok := parseResumeRowNum(resumeLastPK); ok {
		initialRowNum = resumeRowNum
	}
	if initialRowNum < startRow {
		initialRowNum = startRow
	}
	if boundedEnd && initialRowNum > endRow {
		initialRowNum = endRow
	}

	// Compute pipeline buffer depths from memory budget (single reader for ROW_NUMBER).
	rnBufs := calculatePipelineBuffers(cfg, job, tableName, tuner, numWriters, 1, cfg.Migration.ReadAheadBuffers)
	bufferSize := rnBufs.ChunkChanDepth
	chunkChan := make(chan chunkResult, bufferSize)

	// Per-transfer reader context — see executeKeysetPagination for the
	// rationale. Same fix shape applied here. (#250)
	readerCtx, cancelReaders := context.WithCancel(ctx)
	defer cancelReaders()

	// Memory guardrail for ROW_NUMBER reader (same cap logic as keyset path)
	guardMemMB := cfg.AutoConfig().EffectiveMaxMemoryMB
	if cfg.Migration.MaxMemoryMB > 0 && cfg.Migration.MaxMemoryMB < guardMemMB {
		guardMemMB = cfg.Migration.MaxMemoryMB
	}
	memGuard := newMemoryGuard(guardMemMB)

	// Start reader goroutine
	go func() {
		defer close(chunkChan)
		rowNum := initialRowNum
		seq := int64(0)

		for !boundedEnd || rowNum < endRow {
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

			// Adjust chunk size if near end of partition
			effectiveChunkSize := chunkSize
			if boundedEnd && rowNum+int64(chunkSize) > endRow {
				effectiveChunkSize = int(endRow - rowNum)
			}
			if effectiveChunkSize <= 0 {
				sendChunkOrCancel(readerCtx, chunkChan, chunkResult{done: true})
				return
			}

			// ROW_NUMBER pagination with direction-aware syntax
			query := srcDialect.BuildRowNumberQuery(colList, orderBy, job.Table.Schema, job.Table.Name, tableHint, job.DateFilter)
			args := srcDialect.BuildRowNumberArgs(rowNum, effectiveChunkSize, job.DateFilter)

			// Time the query
			queryStart := time.Now()
			rows, err := db.QueryContext(readerCtx, query, args...)
			queryTime := time.Since(queryStart)
			if err != nil {
				sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: fmt.Errorf("row_number query: %w", err)})
				return
			}

			// Time the scan
			scanStart := time.Now()
			chunk, _, err := scanRows(rows, len(cols), valueConvs, convIdx)
			rows.Close()
			scanTime := time.Since(scanStart)
			if err != nil {
				sendChunkOrCancel(readerCtx, chunkChan, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
				return
			}

			if len(chunk) == 0 {
				sendChunkOrCancel(readerCtx, chunkChan, chunkResult{done: true})
				return
			}

			// Update rowNum for progress tracking
			newRowNum := rowNum + int64(len(chunk))

			var sendStart time.Time
			if logging.IsDebug() {
				sendStart = time.Now()
			}
			if !sendChunkOrCancel(readerCtx, chunkChan, chunkResult{
				rows:      chunk,
				rowNum:    newRowNum,
				readerID:  0,
				seq:       seq,
				queryTime: queryTime,
				scanTime:  scanTime,
				readEnd:   time.Now(),
			}) {
				return
			}
			if logging.IsDebug() {
				if sendWait := time.Since(sendStart); sendWait > 500*time.Millisecond {
					logging.Debug("Reader[0]: blocked %v sending chunk #%d to chunkChan (ROW_NUMBER, len=%d, cap=%d)",
						sendWait, seq, len(chunkChan), cap(chunkChan))
				}
			}
			seq++

			rowNum = newRowNum

			if len(chunk) < effectiveChunkSize {
				sendChunkOrCancel(readerCtx, chunkChan, chunkResult{done: true})
				return
			}
		}
		sendChunkOrCancel(readerCtx, chunkChan, chunkResult{done: true})
	}()

	// Get partition ID and row count for staging table naming and checkpointing
	var partitionID *int
	var partitionRows int64
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		partitionRows = job.Partition.RowCount
	} else {
		partitionRows = job.Table.RowCount
	}

	// Build callbacks: if tuner is present, read dynamically; otherwise use static config values
	upsertChunkFn := func() int { return cfg.Migration.UpsertMergeChunkSize }
	checkpointFreqFn := func() int {
		f := cfg.Migration.CheckpointFrequency
		if f <= 0 {
			f = 10
		}
		return f
	}
	if tuner != nil {
		upsertChunkFn = func() int { return tuner.Snapshot().UpsertMergeChunkSize }
		checkpointFreqFn = func() int {
			f := tuner.Snapshot().CheckpointFrequency
			if f <= 0 {
				f = 10
			}
			return f
		}
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

	enableAck := job.Saver != nil && job.TaskID > 0

	rnJobBufSize := rnBufs.JobChanDepth

	// #227/#540: on ROW_NUMBER replay, route plain inserts through the
	// idempotent-on-dup path so replayed committed rows become no-ops.
	idempotentOnDup := (job.IsResume || job.ReplayPossible) && cfg.Migration.TargetMode != "upsert" && job.Table.HasPK()

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:             numWriters,
		BufferSize:             bufferSize,
		JobBufferSize:          rnJobBufSize,
		UseUpsert:              cfg.Migration.TargetMode == "upsert",
		IdempotentOnDup:        idempotentOnDup,
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
		EnableAck:              enableAck,
		Tuner:                  tuner,
		Adjuster:               writeErrorAdjuster,
		TableName:              job.Table.Name,
		BytesPerRow:            job.Table.GoHeapBytesPerRow(), // #229 metrics bytes_total estimate
	})

	if idempotentOnDup {
		partitionStr := "single"
		if partitionID != nil {
			partitionStr = fmt.Sprintf("p%d", *partitionID)
		}
		logging.Debug("ROW_NUMBER resume for %s: enabling idempotent-on-dup writer (start=%d, resume=%d, partition=%s)",
			job.Table.Name, startRow, initialRowNum, partitionStr)
	}

	// Setup ROW_NUMBER checkpoint handler
	lastCheckpointRowNum := initialRowNum

	if enableAck {
		expectedSeq := int64(0)
		pending := make(map[int64]writeAck)
		completedChunks := 0

		wp.startAckProcessor(func(ack writeAck) {
			if ack.seq != expectedSeq {
				pending[ack.seq] = ack
				return
			}
			for {
				lastCheckpointRowNum = ack.rowNum
				completedChunks++
				freq := checkpointFreqFn()
				if completedChunks%freq == 0 {
					rowsDone := resumeRowsDone + wp.written()
					if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, lastCheckpointRowNum, rowsDone, partitionRows, ""); err != nil {
						logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
					}
				}
				expectedSeq++
				next, ok := pending[expectedSeq]
				if !ok {
					break
				}
				delete(pending, expectedSeq)
				ack = next
			}
		})
	}

	wp.start()

	// Main consumer loop - reads from chunkChan, dispatches to write pool
	chunkCount := 0
	totalTransferred := resumeRowsDone
	var currentRowNum int64
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var loopErr error
	var lastReportedQueueDepth int // for delta-based queue depth reporting

	// Process chunks and dispatch writes
	debugEnabled := logging.IsDebug()
	var chunkWaitStart time.Time
	var totalChunkWait time.Duration
	var totalSubmitWait time.Duration
	if debugEnabled {
		chunkWaitStart = time.Now()
	}

chunkLoop:
	for result := range chunkChan {
		if debugEnabled {
			chunkWait := time.Since(chunkWaitStart)
			totalChunkWait += chunkWait
			if chunkCount > 0 && chunkWait > 500*time.Millisecond {
				logging.Debug("Pipeline %s: consumer waited %v for chunk #%d from reader (ROW_NUMBER, chunkChan len=%d)",
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
		currentRowNum = result.rowNum

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
			rowNum:   result.rowNum,
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

	// Release the ROW_NUMBER reader if it's blocked mid-send on
	// chunkChan before wp.wait() runs. (#250)
	cancelReaders()

	// Same cancellation-race guard as the keyset path: if the parent
	// ctx fired while the reader was shutting down, surface it as
	// loopErr so the migration isn't reported as successful. (#250 review)
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
	if job.Saver != nil && job.TaskID > 0 {
		finalRowNum := currentRowNum
		if enableAck {
			finalRowNum = lastCheckpointRowNum
		}
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalRowNum, totalTransferred, partitionRows, ""); err != nil {
			logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
		}
	}

	return stats, nil
}
