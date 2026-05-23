package transfer

import (
	"context"
	"sync"
	"sync/atomic"
)

// RuntimeSnapshot holds the current runtime parameters as a point-in-time copy.
type RuntimeSnapshot struct {
	ChunkSize            int
	ReadAheadBuffers     int
	ParallelReaders      int
	WriteAheadWriters    int
	CheckpointFrequency  int
	UpsertMergeChunkSize int
}

// RuntimeUpdate describes a partial update to runtime parameters.
// Nil fields are left unchanged.
type RuntimeUpdate struct {
	ChunkSize            *int
	ReadAheadBuffers     *int
	ParallelReaders      *int
	WriteAheadWriters    *int
	CheckpointFrequency  *int
	UpsertMergeChunkSize *int
}

// RuntimeMetrics holds operational metrics reported by the transfer pipeline.
type RuntimeMetrics struct {
	ActiveJobs      int // Number of concurrently executing transfer jobs
	QueueDepth      int // Aggregate read-ahead queue depth across all active jobs
	ErrorCount      int // Cumulative count of transfer jobs (per-table or per-partition) that exhausted all retries
	ChunkRetryCount int // Cumulative count of chunk retry attempts triggered by transient failures (most succeed on the next attempt; the few that fail terminally are also counted in ErrorCount)

	// Cumulative transfer time breakdown (nanoseconds)
	TotalQueryNs      int64
	TotalScanNs       int64
	TotalWriteNs      int64
	TotalTransferRows int64
}

// WriteErrorContext provides context about a write error for AI-driven chunk size adjustment.
type WriteErrorContext struct {
	TableName    string
	ColumnCount  int
	ChunkSize    int
	RowCount     int
	ErrorMessage string
	TargetDBType string
}

// WriteErrorAdjuster evaluates write errors and recommends chunk_size adjustments.
type WriteErrorAdjuster interface {
	EvaluateWriteError(ctx context.Context, errCtx WriteErrorContext) int
}

// RuntimeTuner provides thread-safe access to runtime migration parameters.
type RuntimeTuner interface {
	Snapshot() RuntimeSnapshot
	Update(RuntimeUpdate) error
	TableChunkSize(tableName string) (int, bool)             // Returns per-table chunk size override if set
	SetTableChunkSize(tableName string, size int)            // Sets per-table chunk size override
	TableBatchSize(tableName string) (int, bool)             // Returns per-table batch size override if set
	SetTableBatchSize(tableName string, size int)            // Sets per-table batch size override
	ReportActiveJobs(delta int)                              // Atomically adjust active job count (+1 on start, -1 on end)
	ReportQueueDepth(delta int)                              // Atomically adjust aggregate queue depth (use deltas per job)
	ReportError()                                            // Atomically increment terminal error count (job exhausted all retries)
	ReportChunkRetry()                                       // Atomically increment chunk retry count (transient failure about to be retried)
	ReportTransferTime(queryNs, scanNs, writeNs, rows int64) // Atomically add cumulative transfer time breakdown
	Metrics() RuntimeMetrics                                 // Read current operational metrics
}

// runtimeTuner is the concrete implementation of RuntimeTuner.
type runtimeTuner struct {
	mu              sync.RWMutex
	snapshot        RuntimeSnapshot
	tableChunkSizes map[string]int // per-table chunk size overrides (reader)
	tableBatchSizes map[string]int // per-table batch size overrides (writer)
	activeJobs      atomic.Int32
	queueDepth      atomic.Int32
	errorCount      atomic.Int32
	chunkRetryCount atomic.Int32

	// Cumulative transfer time breakdown
	totalQueryNs      atomic.Int64
	totalScanNs       atomic.Int64
	totalWriteNs      atomic.Int64
	totalTransferRows atomic.Int64
}

// NewRuntimeTuner creates a RuntimeTuner with the given initial values.
func NewRuntimeTuner(initial RuntimeSnapshot) RuntimeTuner {
	return &runtimeTuner{
		snapshot:        initial,
		tableChunkSizes: make(map[string]int),
		tableBatchSizes: make(map[string]int),
	}
}

// Snapshot returns a copy of the current runtime parameters.
func (rt *runtimeTuner) Snapshot() RuntimeSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.snapshot
}

// TableChunkSize returns the per-table chunk size override if set.
func (rt *runtimeTuner) TableChunkSize(tableName string) (int, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	size, ok := rt.tableChunkSizes[tableName]
	return size, ok
}

// SetTableChunkSize sets a per-table chunk size override.
func (rt *runtimeTuner) SetTableChunkSize(tableName string, size int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tableChunkSizes[tableName] = size
}

// TableBatchSize returns the per-table batch size override if set.
func (rt *runtimeTuner) TableBatchSize(tableName string) (int, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	size, ok := rt.tableBatchSizes[tableName]
	return size, ok
}

// SetTableBatchSize sets a per-table batch size override.
func (rt *runtimeTuner) SetTableBatchSize(tableName string, size int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tableBatchSizes[tableName] = size
}

// ReportActiveJobs atomically adjusts the active job count.
func (rt *runtimeTuner) ReportActiveJobs(delta int) {
	rt.activeJobs.Add(int32(delta))
}

// ReportQueueDepth atomically adjusts the aggregate queue depth.
// Each transfer job should track its own lastReported value and pass deltas:
//
//	current := len(chunkChan)
//	tuner.ReportQueueDepth(current - lastReported)
//	lastReported = current
//
// On job completion: tuner.ReportQueueDepth(-lastReported)
func (rt *runtimeTuner) ReportQueueDepth(delta int) {
	rt.queueDepth.Add(int32(delta))
}

// ReportError atomically increments the terminal error count (a transfer job
// that failed after exhausting all retries). Note: jobs are per-table for
// non-partitioned tables and per-partition for partitioned tables, so this
// counter tracks failed jobs/partitions rather than unique failed tables.
func (rt *runtimeTuner) ReportError() {
	rt.errorCount.Add(1)
}

// ReportChunkRetry atomically increments the chunk retry count. Called once per
// retry attempt that actually executes — i.e., after the backoff completes and
// the context wasn't canceled — for transient errors (deadline exceeded,
// connection reset, etc.) that the orchestrator's retry loop will retry.
// Surfaces as a feedback signal to the AI adjuster so it can correlate
// concurrency / chunk-size choices with transient-stall pressure even when the
// migration ultimately succeeds.
func (rt *runtimeTuner) ReportChunkRetry() {
	rt.chunkRetryCount.Add(1)
}

// ReportTransferTime atomically adds to cumulative transfer time breakdown.
func (rt *runtimeTuner) ReportTransferTime(queryNs, scanNs, writeNs, rows int64) {
	rt.totalQueryNs.Add(queryNs)
	rt.totalScanNs.Add(scanNs)
	rt.totalWriteNs.Add(writeNs)
	rt.totalTransferRows.Add(rows)
}

// Metrics returns the current operational metrics.
func (rt *runtimeTuner) Metrics() RuntimeMetrics {
	qd := int(rt.queueDepth.Load())
	if qd < 0 {
		qd = 0 // Guard against slight negative from concurrent delta reporting
	}
	return RuntimeMetrics{
		ActiveJobs:        int(rt.activeJobs.Load()),
		QueueDepth:        qd,
		ErrorCount:        int(rt.errorCount.Load()),
		ChunkRetryCount:   int(rt.chunkRetryCount.Load()),
		TotalQueryNs:      rt.totalQueryNs.Load(),
		TotalScanNs:       rt.totalScanNs.Load(),
		TotalWriteNs:      rt.totalWriteNs.Load(),
		TotalTransferRows: rt.totalTransferRows.Load(),
	}
}

// Update applies non-nil fields from the update immediately.
func (rt *runtimeTuner) Update(u RuntimeUpdate) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if u.ChunkSize != nil {
		rt.snapshot.ChunkSize = *u.ChunkSize
	}
	if u.ReadAheadBuffers != nil {
		rt.snapshot.ReadAheadBuffers = *u.ReadAheadBuffers
	}
	if u.ParallelReaders != nil {
		rt.snapshot.ParallelReaders = *u.ParallelReaders
	}
	if u.WriteAheadWriters != nil {
		rt.snapshot.WriteAheadWriters = *u.WriteAheadWriters
	}
	if u.CheckpointFrequency != nil {
		rt.snapshot.CheckpointFrequency = *u.CheckpointFrequency
	}
	if u.UpsertMergeChunkSize != nil {
		rt.snapshot.UpsertMergeChunkSize = *u.UpsertMergeChunkSize
	}

	return nil
}
