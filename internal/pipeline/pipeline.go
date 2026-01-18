package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/progress"
)

// Config contains pipeline execution configuration.
type Config struct {
	// ChunkSize is the number of rows per batch.
	ChunkSize int

	// ReadAheadBuffers is the number of chunks to buffer between reader and writer.
	ReadAheadBuffers int

	// ParallelReaders is the number of parallel reader goroutines.
	ParallelReaders int

	// WriteAheadWriters is the number of parallel writer goroutines.
	WriteAheadWriters int

	// TargetMode is "drop_recreate" or "upsert".
	TargetMode string

	// TargetSchema is the target schema name.
	TargetSchema string

	// StrictConsistency enables table hints for consistent reads.
	StrictConsistency bool

	// CheckpointFrequency is how often (in chunks) to save checkpoints.
	CheckpointFrequency int

	// UpsertMergeChunkSize is the batch size for upsert MERGE operations.
	// Smaller values reduce memory pressure on the target database.
	// Only used when TargetMode is "upsert".
	UpsertMergeChunkSize int
}

// ApplyTiming determines when a config update should be applied.
type ApplyTiming int

const (
	// ApplyNextChunk applies the update at the next chunk boundary (within a table)
	ApplyNextChunk ApplyTiming = iota
	// ApplyNextTable applies the update at the next table boundary
	ApplyNextTable
)

// ConfigUpdate represents a requested configuration update with timing information.
type ConfigUpdate struct {
	ChunkSize            *int // Pointer allows nil = "no change"
	ReadAheadBuffers     *int
	ParallelReaders      *int
	WriteAheadWriters    *int
	CheckpointFrequency  *int
	UpsertMergeChunkSize *int
	ApplyAt              ApplyTiming
}

// Pipeline orchestrates data transfer from a Reader to a Writer.
type Pipeline struct {
	reader driver.Reader
	writer driver.Writer

	// Configuration with synchronization for runtime updates
	configMu      sync.RWMutex
	config        Config
	configUpdates chan ConfigUpdate // Channel for safe runtime configuration updates
}

// New creates a new Pipeline with the given reader, writer, and configuration.
func New(reader driver.Reader, writer driver.Writer, cfg Config) *Pipeline {
	// Set defaults
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 10000
	}
	if cfg.ReadAheadBuffers < 0 {
		cfg.ReadAheadBuffers = 4
	}
	if cfg.ParallelReaders < 1 {
		cfg.ParallelReaders = 1
	}
	if cfg.WriteAheadWriters < 1 {
		cfg.WriteAheadWriters = 1
	}
	if cfg.CheckpointFrequency <= 0 {
		cfg.CheckpointFrequency = 10
	}
	if cfg.UpsertMergeChunkSize <= 0 {
		cfg.UpsertMergeChunkSize = 5000 // Default for upsert MERGE operations
	}

	return &Pipeline{
		reader:        reader,
		writer:        writer,
		config:        cfg,
		configUpdates: make(chan ConfigUpdate, 10), // Buffer size 10 allows AI monitor to queue multiple decisions
	}
}

// UpdateConfig requests a configuration update to be applied at the specified timing.
// The request is queued non-blocking; if the channel is full, an error is returned.
func (p *Pipeline) UpdateConfig(update ConfigUpdate) error {
	select {
	case p.configUpdates <- update:
		return nil
	default:
		// Channel is full (timeout or still processing previous update)
		return fmt.Errorf("config update queue full - another update may be pending")
	}
}

// ApplyPendingUpdates checks for any pending configuration updates and applies them.
// This should be called at chunk boundaries for ApplyNextChunk updates,
// and at table boundaries for ApplyNextTable updates.
func (p *Pipeline) ApplyPendingUpdates(atTableBoundary bool) {
	select {
	case update := <-p.configUpdates:
		// Only apply if timing matches
		if atTableBoundary || update.ApplyAt == ApplyNextChunk {
			p.applyConfigUpdate(update)
		} else {
			// Re-queue the update to apply later (at table boundary)
			select {
			case p.configUpdates <- update:
			default:
				// This shouldn't happen with buffer size 1
				logging.Warn("Failed to re-queue config update")
			}
		}
	default:
		// No pending updates
	}
}

// applyConfigUpdate safely applies a configuration update to the current config.
func (p *Pipeline) applyConfigUpdate(update ConfigUpdate) {
	p.configMu.Lock()
	defer p.configMu.Unlock()

	changed := false

	if update.ChunkSize != nil && *update.ChunkSize > 0 {
		p.config.ChunkSize = *update.ChunkSize
		changed = true
	}
	if update.ReadAheadBuffers != nil && *update.ReadAheadBuffers >= 0 {
		p.config.ReadAheadBuffers = *update.ReadAheadBuffers
		changed = true
	}
	if update.ParallelReaders != nil && *update.ParallelReaders >= 1 {
		p.config.ParallelReaders = *update.ParallelReaders
		changed = true
	}
	if update.WriteAheadWriters != nil && *update.WriteAheadWriters >= 1 {
		p.config.WriteAheadWriters = *update.WriteAheadWriters
		changed = true
	}
	if update.CheckpointFrequency != nil && *update.CheckpointFrequency >= 1 {
		p.config.CheckpointFrequency = *update.CheckpointFrequency
		changed = true
	}
	if update.UpsertMergeChunkSize != nil && *update.UpsertMergeChunkSize >= 1000 {
		p.config.UpsertMergeChunkSize = *update.UpsertMergeChunkSize
		changed = true
	}

	if changed {
		logging.Debug("Config updated: chunk_size=%d, readers=%d, writers=%d, buffers=%d, checkpoint_freq=%d, upsert_chunk=%d",
			p.config.ChunkSize, p.config.ParallelReaders, p.config.WriteAheadWriters, p.config.ReadAheadBuffers,
			p.config.CheckpointFrequency, p.config.UpsertMergeChunkSize)
	}
}

// GetConfig returns a copy of the current configuration.
func (p *Pipeline) GetConfig() Config {
	p.configMu.RLock()
	defer p.configMu.RUnlock()
	return p.config
}

// Execute runs a transfer job, returning statistics on completion.
func (p *Pipeline) Execute(ctx context.Context, job Job, prog *progress.Tracker) (*Stats, error) {
	// Track table start/end for accurate progress display
	prog.StartTable(job.Table.Name)
	defer prog.EndTable(job.Table.Name)

	// Check for saved progress (chunk-level resume)
	var resumeLastPK any
	var resumeRowsDone int64
	if job.Saver != nil && job.TaskID > 0 {
		var err error
		resumeLastPK, resumeRowsDone, err = job.Saver.GetProgress(job.TaskID)
		if err != nil {
			logging.Warn("Failed to load checkpoint for %s: %v", job.Table.Name, err)
		}
		if resumeLastPK != nil {
			logging.Debug("Resuming %s at row %d (checkpoint: %v)", job.Table.Name, resumeRowsDone, resumeLastPK)
		}
	}

	// Handle truncation based on job type (skip if resuming or in upsert mode)
	if p.config.TargetMode != "upsert" {
		if resumeLastPK == nil {
			if job.Partition == nil {
				// Non-partitioned table: truncate here
				if err := p.writer.TruncateTable(ctx, p.config.TargetSchema, job.Table.Name); err != nil {
					// Ignore truncate errors (table might not exist)
				}
			} else {
				// Partitioned table: cleanup for idempotent retry
				if len(job.Table.PrimaryKey) == 1 {
					if err := p.cleanupPartitionData(ctx, &job); err != nil {
						logging.Warn("Partition cleanup failed for %s: %v", job.Table.Name, err)
					}
				}
			}
		} else if len(job.Table.PrimaryKey) == 1 {
			// Chunk-level resume: delete any rows beyond the saved lastPK
			var maxPK any
			if job.Partition != nil {
				maxPK = job.Partition.MaxPK
			}
			if err := p.cleanupPartialData(ctx, job.Table.Name, job.Table.PrimaryKey[0], resumeLastPK, maxPK); err != nil {
				logging.Warn("Resume cleanup failed for %s: %v", job.Table.Name, err)
			}
		}
	}

	// Build column lists
	cols := make([]string, len(job.Table.Columns))
	targetCols := make([]string, len(job.Table.Columns))
	colTypes := make([]string, len(job.Table.Columns))
	colSRIDs := make([]int, len(job.Table.Columns))

	for i, c := range job.Table.Columns {
		cols[i] = c.Name
		targetCols[i] = p.sanitizeIdentifier(c.Name)
		colTypes[i] = strings.ToLower(c.DataType)
		colSRIDs[i] = c.SRID
	}

	// Sanitize table name for target
	targetTableName := p.sanitizeIdentifier(job.Table.Name)

	// Choose pagination strategy based on PK structure
	if job.Table.SupportsKeysetPagination() {
		return p.executeKeysetPagination(ctx, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName)
	}

	// Fall back to ROW_NUMBER pagination
	return p.executeRowNumberPagination(ctx, job, cols, targetCols, colTypes, colSRIDs, prog, resumeLastPK, resumeRowsDone, targetTableName)
}

// sanitizeIdentifier returns a sanitized identifier for the target database.
func (p *Pipeline) sanitizeIdentifier(name string) string {
	// PostgreSQL requires lowercase identifiers for case-insensitive matching
	if p.writer.DBType() == "postgres" {
		return strings.ToLower(name)
	}
	return name
}

// isIntegerType returns true if the data type supports keyset pagination.
func isIntegerType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	switch dataType {
	case "int", "int4", "integer", "bigint", "int8", "smallint", "int2", "tinyint":
		return true
	}
	return false
}

// executeKeysetPagination uses WHERE pk > last_pk for efficient pagination.
func (p *Pipeline) executeKeysetPagination(
	ctx context.Context,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
	targetTableName string,
) (*Stats, error) {
	stats := &Stats{}
	pkCol := job.Table.PrimaryKey[0]

	// Get partition ID for staging table naming
	var partitionID *int
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
	}

	// Determine PK range
	var minPKVal, maxPKVal any
	if job.Partition != nil {
		minPKVal = job.Partition.MinPK
		maxPKVal = job.Partition.MaxPK
	} else {
		// For non-partitioned tables, get boundaries from partition query
		partitions, err := p.reader.GetPartitionBoundaries(ctx, &job.Table, 1)
		if err != nil || len(partitions) == 0 {
			return stats, nil // Empty table
		}
		minPKVal = partitions[0].MinPK
		maxPKVal = partitions[0].MaxPK
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

	// Create buffered channel for read-ahead pipeline
	bufferSize := p.config.ReadAheadBuffers
	chunkChan := make(chan chunkResult, bufferSize)

	// Convert DateFilter
	var dateFilter *driver.DateFilter
	if job.DateFilter != nil {
		dateFilter = &driver.DateFilter{
			Column:    job.DateFilter.Column,
			Timestamp: job.DateFilter.Timestamp,
		}
	}

	// Determine number of parallel readers
	numReaders := p.config.ParallelReaders
	pkRanges := splitPKRange(minPKVal, maxPKVal, numReaders)

	// Start parallel reader goroutines
	var readerWg sync.WaitGroup
	for readerID, pkr := range pkRanges {
		readerWg.Add(1)
		go func(readerID int, rangeMinPK, rangeMaxPK any) {
			defer readerWg.Done()

			// Create partition for this reader's range
			partition := &driver.Partition{
				PartitionID: readerID,
				MinPK:       rangeMinPK,
				MaxPK:       rangeMaxPK,
			}

			readOpts := driver.ReadOptions{
				Table:             job.Table,
				Columns:           cols,
				ColumnTypes:       colTypes,
				Partition:         partition,
				ChunkSize:         p.config.ChunkSize,
				DateFilter:        dateFilter,
				TargetDBType:      p.writer.DBType(),
				StrictConsistency: p.config.StrictConsistency,
			}

			batches, err := p.reader.ReadTable(ctx, readOpts)
			if err != nil {
				chunkChan <- chunkResult{err: fmt.Errorf("starting reader: %w", err)}
				return
			}

			seq := int64(0)
			for batch := range batches {
				if batch.Error != nil {
					chunkChan <- chunkResult{err: batch.Error}
					return
				}
				if len(batch.Rows) == 0 {
					continue
				}

				// Get last PK from batch
				lastPK := batch.Rows[len(batch.Rows)-1][pkIdx]

				chunkChan <- chunkResult{
					rows:      batch.Rows,
					lastPK:    lastPK,
					readerID:  readerID,
					seq:       seq,
					queryTime: batch.Stats.QueryTime,
					scanTime:  batch.Stats.ScanTime,
					readEnd:   batch.Stats.ReadEnd,
				}
				seq++
			}
		}(readerID, pkr.minPK, pkr.maxPK)
	}

	// Close chunkChan when all readers are done
	go func() {
		readerWg.Wait()
		close(chunkChan)
	}()

	// Build target PK columns
	targetPKCols := make([]string, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		targetPKCols[i] = p.sanitizeIdentifier(pk)
	}

	// Create writer pool
	numWriters := p.config.WriteAheadWriters
	enableAck := job.Saver != nil && job.TaskID > 0

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:   numWriters,
		BufferSize:   bufferSize,
		UseUpsert:    p.config.TargetMode == "upsert",
		TargetSchema: p.config.TargetSchema,
		TargetTable:  targetTableName,
		TargetCols:   targetCols,
		ColTypes:     colTypes,
		ColSRIDs:     colSRIDs,
		TargetPKCols: targetPKCols,
		PartitionID:  partitionID,
		Writer:       p.writer,
		Prog:         prog,
		EnableAck:    enableAck,
		UpsertMergeChunkSizeFn: func() int {
			return p.GetConfig().UpsertMergeChunkSize
		},
	})

	// Setup checkpoint coordinator with dynamic checkpoint frequency (supports mid-migration tuning)
	checkpointCoord := newKeysetCheckpointCoordinator(job, pkRanges, resumeRowsDone, &wp.totalWritten, func() int {
		return p.GetConfig().CheckpointFrequency
	})
	if checkpointCoord != nil {
		wp.startAckProcessor(checkpointCoord.onAck)
	}

	wp.start()

	// Main consumer loop
	// SYNCHRONIZATION: loopErr is set from the main goroutine only. The channel receive
	// from chunkChan happens-before any access, and wp.wait() ensures all writers complete
	// before we check wp.error(). No race conditions exist.
	totalTransferred := resumeRowsDone
	chunkCount := 0
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var lastPK any
	var loopErr error

chunkLoop:
	for result := range chunkChan {
		if result.err != nil {
			loopErr = result.err
			wp.cancel()
			break
		}
		if result.done {
			break
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		lastPK = result.lastPK

		// Calculate overlap
		receiveTime := time.Now()
		if !lastWriteEnd.IsZero() && !result.readEnd.IsZero() && result.readEnd.Before(lastWriteEnd) {
			overlap := lastWriteEnd.Sub(result.readEnd)
			totalOverlap += overlap
		}
		lastWriteEnd = time.Now()

		// Dispatch to write pool
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

		// Check for and apply pending configuration updates at chunk boundaries
		select {
		case update := <-p.configUpdates:
			// Only apply WriteAheadWriters updates mid-chunk since other params
			// (ChunkSize, ReadAheadBuffers, ParallelReaders) are tied to reader setup
			if update.WriteAheadWriters != nil && *update.WriteAheadWriters > 0 {
				newWriters := *update.WriteAheadWriters
				if err := wp.ScaleWorkers(newWriters); err != nil {
					logging.Warn("Failed to scale workers: %v", err)
				} else {
					logging.Debug("Scaled writers from %d to %d", numWriters, newWriters)
					numWriters = newWriters
				}
			}
		default:
			// No pending updates
		}

		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			logging.Debug("Pipeline %s: %d chunks, overlap=%v, wait=%v, buffers=%d, writers=%d",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	// Wait for writers to finish
	wp.wait()

	if loopErr != nil {
		return stats, loopErr
	}

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

// executeRowNumberPagination uses ROW_NUMBER for composite/varchar PKs.
func (p *Pipeline) executeRowNumberPagination(
	ctx context.Context,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeLastPK any,
	resumeRowsDone int64,
	targetTableName string,
) (*Stats, error) {
	stats := &Stats{}

	// Tables without PK cannot be migrated safely
	if len(job.Table.PrimaryKey) == 0 {
		return nil, fmt.Errorf("table %s has no primary key - cannot guarantee data correctness with ROW_NUMBER pagination", job.Table.Name)
	}

	// Get partition ID and row count
	var partitionID *int
	var partitionRows int64
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		partitionRows = job.Partition.RowCount
	} else {
		partitionRows = job.Table.RowCount
	}

	// Determine row range for this job
	var startRow, endRow int64
	if job.Partition != nil && job.Partition.EndRow > 0 {
		startRow = job.Partition.StartRow
		endRow = job.Partition.EndRow
	} else {
		startRow = 0
		endRow = job.Table.RowCount
	}

	// Resume from saved progress if available
	initialRowNum := startRow
	if resumeRowNum, ok := parseResumeRowNum(resumeLastPK); ok {
		initialRowNum = resumeRowNum
	}
	if initialRowNum < startRow {
		initialRowNum = startRow
	}
	if initialRowNum > endRow {
		initialRowNum = endRow
	}

	// Create buffered channel for read-ahead pipeline
	bufferSize := p.config.ReadAheadBuffers
	chunkChan := make(chan chunkResult, bufferSize)

	// Convert DateFilter
	var dateFilter *driver.DateFilter
	if job.DateFilter != nil {
		dateFilter = &driver.DateFilter{
			Column:    job.DateFilter.Column,
			Timestamp: job.DateFilter.Timestamp,
		}
	}

	// Start reader goroutine
	go func() {
		defer close(chunkChan)

		// Create partition for row-number based reading
		partition := &driver.Partition{
			PartitionID: 0,
			StartRow:    initialRowNum,
			EndRow:      endRow,
			RowCount:    endRow - initialRowNum,
		}

		readOpts := driver.ReadOptions{
			Table:             job.Table,
			Columns:           cols,
			ColumnTypes:       colTypes,
			Partition:         partition,
			ChunkSize:         p.config.ChunkSize,
			DateFilter:        dateFilter,
			TargetDBType:      p.writer.DBType(),
			StrictConsistency: p.config.StrictConsistency,
		}

		batches, err := p.reader.ReadTable(ctx, readOpts)
		if err != nil {
			chunkChan <- chunkResult{err: fmt.Errorf("starting reader: %w", err)}
			return
		}

		seq := int64(0)
		currentRowNum := initialRowNum
		for batch := range batches {
			if batch.Error != nil {
				chunkChan <- chunkResult{err: batch.Error}
				return
			}
			if len(batch.Rows) == 0 {
				continue
			}

			currentRowNum += int64(len(batch.Rows))

			chunkChan <- chunkResult{
				rows:      batch.Rows,
				rowNum:    currentRowNum,
				readerID:  0,
				seq:       seq,
				queryTime: batch.Stats.QueryTime,
				scanTime:  batch.Stats.ScanTime,
				readEnd:   batch.Stats.ReadEnd,
			}
			seq++
		}
	}()

	// Build target PK columns
	targetPKCols := make([]string, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		targetPKCols[i] = p.sanitizeIdentifier(pk)
	}

	// Create writer pool
	numWriters := p.config.WriteAheadWriters
	enableAck := job.Saver != nil && job.TaskID > 0

	wp := newWriterPool(ctx, writerPoolConfig{
		NumWriters:   numWriters,
		BufferSize:   bufferSize,
		UseUpsert:    p.config.TargetMode == "upsert",
		TargetSchema: p.config.TargetSchema,
		TargetTable:  targetTableName,
		TargetCols:   targetCols,
		ColTypes:     colTypes,
		ColSRIDs:     colSRIDs,
		TargetPKCols: targetPKCols,
		PartitionID:  partitionID,
		Writer:       p.writer,
		Prog:         prog,
		EnableAck:    enableAck,
		UpsertMergeChunkSizeFn: func() int {
			return p.GetConfig().UpsertMergeChunkSize
		},
	})

	// Setup ROW_NUMBER checkpoint handler
	lastCheckpointRowNum := initialRowNum

	if enableAck {
		// THREAD SAFETY: These variables are only accessed from the single ack processor
		// goroutine started by startAckProcessor. No mutex needed.
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
				// Read checkpoint frequency dynamically to allow mid-migration tuning
				checkpointFreq := p.GetConfig().CheckpointFrequency
				if completedChunks%checkpointFreq == 0 {
					rowsDone := resumeRowsDone + wp.written()
					if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, lastCheckpointRowNum, rowsDone, partitionRows); err != nil {
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

	// Main consumer loop
	chunkCount := 0
	totalTransferred := resumeRowsDone
	var currentRowNum int64
	var totalOverlap time.Duration
	var lastWriteEnd time.Time
	var loopErr error

chunkLoop:
	for result := range chunkChan {
		if result.err != nil {
			loopErr = result.err
			wp.cancel()
			break
		}
		if result.done {
			break
		}

		stats.QueryTime += result.queryTime
		stats.ScanTime += result.scanTime
		currentRowNum = result.rowNum

		// Calculate overlap
		receiveTime := time.Now()
		if !lastWriteEnd.IsZero() && !result.readEnd.IsZero() && result.readEnd.Before(lastWriteEnd) {
			overlap := lastWriteEnd.Sub(result.readEnd)
			totalOverlap += overlap
		}
		lastWriteEnd = time.Now()

		// Dispatch to write pool
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

		// Check for and apply pending configuration updates at chunk boundaries
		select {
		case update := <-p.configUpdates:
			// Only apply WriteAheadWriters updates mid-chunk since other params
			// (ChunkSize, ReadAheadBuffers, ParallelReaders) are tied to reader setup
			if update.WriteAheadWriters != nil && *update.WriteAheadWriters > 0 {
				newWriters := *update.WriteAheadWriters
				if err := wp.ScaleWorkers(newWriters); err != nil {
					logging.Warn("Failed to scale workers: %v", err)
				} else {
					logging.Debug("Scaled writers from %d to %d", numWriters, newWriters)
					numWriters = newWriters
				}
			}
		default:
			// No pending updates
		}

		if chunkCount > 0 && chunkCount%50 == 0 {
			waitTime := time.Since(receiveTime)
			logging.Debug("Pipeline %s: %d chunks, overlap=%v, wait=%v, buffers=%d, writers=%d",
				job.Table.Name, chunkCount, totalOverlap, waitTime, bufferSize, numWriters)
		}

		chunkCount++
	}

	// Wait for writers to finish
	wp.wait()

	if loopErr != nil {
		return stats, loopErr
	}

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
		if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalRowNum, totalTransferred, partitionRows); err != nil {
			logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
		}
	}

	return stats, nil
}

// cleanupPartitionData removes existing data for a partition's PK range.
func (p *Pipeline) cleanupPartitionData(ctx context.Context, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	// This is handled by the writer through a custom method
	// For now, we'll use the generic cleanup approach
	return nil
}

// cleanupPartialData removes rows beyond the saved lastPK for chunk-level resume.
func (p *Pipeline) cleanupPartialData(ctx context.Context, tableName, pkCol string, lastPK any, maxPK any) error {
	// This would be implemented per-database type
	// For now, we'll leave it to the transfer package
	return nil
}
