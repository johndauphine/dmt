package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/target"
)

// Buffer sizing constants for parallel reader/writer coordination.
// These values prevent cascading deadlocks when readers produce faster than writers consume.
const (
	// jobChanBufferMultiplier scales the job channel buffer with the number of writers.
	// A multiplier of 50 provides enough headroom for burst production while keeping
	// memory usage reasonable (each job holds a chunk of rows).
	jobChanBufferMultiplier = 50

	// jobChanMinBuffer is the minimum job channel buffer size.
	// This ensures adequate buffering even with small configured buffer sizes.
	jobChanMinBuffer = 500

	// ackChanBufferMultiplier scales the ack channel buffer with the number of writers.
	// Uses a higher multiplier (100) than jobChan because acks are tiny (just PK values
	// and sequence numbers) and checkpoint saves can temporarily slow ack processing.
	ackChanBufferMultiplier = 100

	// ackChanMinBuffer is the minimum ack channel buffer size.
	// Higher than jobChan minimum because acks are cheap and we want to avoid
	// any possibility of writers blocking on ack sends.
	ackChanMinBuffer = 1000
)

// writerPool manages a pool of parallel write workers.
// It consolidates the common logic from executeKeysetPagination and executeRowNumberPagination.
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
	tgtPool      pool.TargetPool
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
	TgtPool      pool.TargetPool
	Prog         *progress.Tracker
	EnableAck    bool // Whether to enable ack channel for checkpointing
}

// newWriterPool creates a new writer pool with the given configuration.
func newWriterPool(ctx context.Context, cfg writerPoolConfig) *writerPool {
	writerCtx, cancel := context.WithCancel(ctx)

	// Use a larger buffer for jobChan to prevent consumer from blocking.
	// With parallel readers producing chunks faster than writers can consume,
	// a small buffer causes the consumer to block on submit(), which blocks
	// reading from chunkChan, which blocks readers, causing deadlock.
	jobBufferSize := cfg.BufferSize * cfg.NumWriters * jobChanBufferMultiplier
	if jobBufferSize < jobChanMinBuffer {
		jobBufferSize = jobChanMinBuffer
	}

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
		tgtPool:      cfg.TgtPool,
		prog:         cfg.Prog,
		jobChan:      make(chan writeJob, jobBufferSize),
		ctx:          writerCtx,
		cancel:       cancel,
	}

	if cfg.EnableAck {
		// Use a much larger buffer for ackChan to prevent writers from blocking.
		// With parallel readers, writers can produce acks faster than the ack processor
		// can consume them (especially during checkpoint saves). A small buffer causes
		// a cascading deadlock: writers block → jobChan fills → consumer blocks →
		// chunkChan fills → all readers block.
		// Acks are small (just PK values and sequence numbers), so a large buffer is cheap.
		ackBufferSize := cfg.BufferSize * cfg.NumWriters * ackChanBufferMultiplier
		if ackBufferSize < ackChanMinBuffer {
			ackBufferSize = ackChanMinBuffer
		}
		wp.ackChan = make(chan writeAck, ackBufferSize)
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
			err = writeChunkUpsertWithWriter(wp.ctx, wp.tgtPool, wp.targetSchema, wp.targetTable,
				wp.targetCols, wp.colTypes, wp.colSRIDs, wp.targetPKCols, job.rows, writerID, wp.partitionID)
		} else {
			err = writeChunkGeneric(wp.ctx, wp.tgtPool, wp.targetSchema, wp.targetTable, wp.targetCols, job.rows)
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
			// Non-blocking send with context check to prevent deadlock.
			// If ackChan is full, we skip the ack rather than blocking the writer.
			// This is safe because checkpoint coordination handles out-of-order and
			// missing acks gracefully - the checkpoint just won't advance past this point.
			select {
			case wp.ackChan <- writeAck{
				readerID: job.readerID,
				seq:      job.seq,
				lastPK:   job.lastPK,
				rowNum:   job.rowNum,
			}:
				// Ack sent successfully
			case <-wp.ctx.Done():
				// Context cancelled, exit worker
				return
			default:
				// Channel full, skip this ack (checkpoint won't advance past this point).
				// This should be rare with the large buffer, but prevents deadlock.
				// Log at debug level to help diagnose checkpoint issues if they occur.
				logging.Debug("Ack channel full, skipping ack for reader %d seq %d (checkpoint may not advance)", job.readerID, job.seq)
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
	logging.Debug("writerPool.wait: closing jobChan (len=%d)", len(wp.jobChan))
	close(wp.jobChan)
	logging.Debug("writerPool.wait: waiting for %d writers to finish", wp.numWriters)
	wp.writerWg.Wait()
	logging.Debug("writerPool.wait: all writers finished")
	if wp.ackChan != nil {
		logging.Debug("writerPool.wait: closing ackChan (len=%d)", len(wp.ackChan))
		close(wp.ackChan)
		logging.Debug("writerPool.wait: waiting for ack processor")
		wp.ackWg.Wait()
		logging.Debug("writerPool.wait: ack processor finished")
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

// acks returns the ack channel for checkpoint coordination.
func (wp *writerPool) acks() <-chan writeAck {
	return wp.ackChan
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

// buildTargetPKCols sanitizes PK columns for the target database.
func buildTargetPKCols(pkCols []string, tgtPool pool.TargetPool) []string {
	isPGTarget := tgtPool.DBType() == "postgres"
	targetPKCols := make([]string, len(pkCols))
	for i, pk := range pkCols {
		if isPGTarget {
			targetPKCols[i] = target.SanitizePGIdentifier(pk)
		} else {
			targetPKCols[i] = pk
		}
	}
	return targetPKCols
}
