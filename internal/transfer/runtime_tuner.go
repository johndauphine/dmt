package transfer

import (
	"context"
	"sync"
	"sync/atomic"
)

// RuntimeSnapshot holds the current runtime parameters as a point-in-time copy.
type RuntimeSnapshot struct {
	ChunkSize            int
	Workers              int
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
	// BudgetWaitNs and BudgetWaitCount describe time readers spent waiting for
	// the shared in-flight byte budget. They are controller inputs only: a
	// future monitor rule can use them to distinguish memory-budget starvation
	// from a slow source without changing any tuning behavior here (#668).
	BudgetWaitNs    int64
	BudgetWaitCount int64

	// SafetyProjected reports that a target protocol limit or conditional
	// complete-inventory writer-transition ratchet reduced a requested chunk or
	// batch. ExecutionChunkMin/Max are effective reader chunks observed across
	// pipelines. They remain diagnostics and compatibility metadata, not a
	// replacement action for the steady global policy.
	SafetyProjected   bool
	ExecutionChunkMin int
	ExecutionChunkMax int
	// WriterScaleDeferrals counts pipeline-local WAW upscales that could not be
	// applied because already-emitted chunks exceeded the prospective fixed-
	// buffer cap. It distinguishes the tuner's desired WAW from live execution.
	WriterScaleDeferrals int

	// Cumulative transfer time breakdown (nanoseconds)
	TotalQueryNs      int64
	TotalScanNs       int64
	TotalWriteNs      int64
	TotalTransferRows int64
}

// WriteErrorContext provides context about a write error for rule-based chunk size adjustment.
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
	ReportBudgetWait(waitNs int64)                           // Atomically add a positive reader wait for shared in-flight memory budget
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
	budgetWaitNs    atomic.Int64
	budgetWaitCount atomic.Int64
	safetyProjected atomic.Bool
	executionMin    atomic.Int64
	executionMax    atomic.Int64
	writerDeferrals atomic.Int32

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

// ReportBudgetWait records reader time spent blocked on the migration-wide
// in-flight memory budget. Non-positive durations carry no useful signal and
// are ignored so callers can report a measured duration without extra guards.
func (rt *runtimeTuner) ReportBudgetWait(waitNs int64) {
	if waitNs <= 0 {
		return
	}
	rt.budgetWaitNs.Add(waitNs)
	rt.budgetWaitCount.Add(1)
}

// ReportChunkProjection records one effective reader chunk after a target
// protocol limit or conditional writer-transition ratchet. It is an optional
// concrete-runtime capability rather than part of RuntimeTuner's public
// contract so focused tuner fakes do not need to implement accounting.
func (rt *runtimeTuner) ReportChunkProjection(requested, effective int) {
	if requested <= 0 || effective <= 0 {
		return
	}
	if effective < requested {
		rt.safetyProjected.Store(true)
	}
	observeAtomicPositiveMin(&rt.executionMin, int64(effective))
	observeAtomicPositiveMax(&rt.executionMax, int64(effective))
}

// ReportBatchProjection records a writer-only conditional limit without mixing
// the writer sub-batch into the reader chunk range used by history diagnostics.
func (rt *runtimeTuner) ReportBatchProjection(requested, effective int) {
	if requested > 0 && effective > 0 && effective < requested {
		rt.safetyProjected.Store(true)
	}
}

func (rt *runtimeTuner) ReportWriterScaleDeferral() {
	rt.writerDeferrals.Add(1)
}

func observeAtomicPositiveMin(dst *atomic.Int64, candidate int64) {
	if dst == nil || candidate <= 0 {
		return
	}
	for {
		current := dst.Load()
		if current > 0 && current <= candidate {
			return
		}
		if dst.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func observeAtomicPositiveMax(dst *atomic.Int64, candidate int64) {
	if dst == nil || candidate <= 0 {
		return
	}
	for {
		current := dst.Load()
		if current >= candidate {
			return
		}
		if dst.CompareAndSwap(current, candidate) {
			return
		}
	}
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
		ActiveJobs:           int(rt.activeJobs.Load()),
		QueueDepth:           qd,
		ErrorCount:           int(rt.errorCount.Load()),
		ChunkRetryCount:      int(rt.chunkRetryCount.Load()),
		BudgetWaitNs:         rt.budgetWaitNs.Load(),
		BudgetWaitCount:      rt.budgetWaitCount.Load(),
		SafetyProjected:      rt.safetyProjected.Load(),
		ExecutionChunkMin:    positiveInt64ToInt(rt.executionMin.Load()),
		ExecutionChunkMax:    positiveInt64ToInt(rt.executionMax.Load()),
		WriterScaleDeferrals: int(rt.writerDeferrals.Load()),
		TotalQueryNs:         rt.totalQueryNs.Load(),
		TotalScanNs:          rt.totalScanNs.Load(),
		TotalWriteNs:         rt.totalWriteNs.Load(),
		TotalTransferRows:    rt.totalTransferRows.Load(),
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
