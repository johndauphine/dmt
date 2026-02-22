package transfer

import (
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
	ActiveJobs int // Number of concurrently executing transfer jobs
	QueueDepth int // Aggregate read-ahead queue depth across all active jobs
	ErrorCount int // Cumulative count of failed tables
}

// RuntimeTuner provides thread-safe access to runtime migration parameters.
type RuntimeTuner interface {
	Snapshot() RuntimeSnapshot
	Update(RuntimeUpdate) error
	ReportActiveJobs(delta int)  // Atomically adjust active job count (+1 on start, -1 on end)
	ReportQueueDepth(delta int)  // Atomically adjust aggregate queue depth (use deltas per job)
	ReportError()                // Atomically increment error count
	Metrics() RuntimeMetrics     // Read current operational metrics
}

// runtimeTuner is the concrete implementation of RuntimeTuner.
type runtimeTuner struct {
	mu         sync.RWMutex
	snapshot   RuntimeSnapshot
	activeJobs atomic.Int32
	queueDepth atomic.Int32
	errorCount atomic.Int32
}

// NewRuntimeTuner creates a RuntimeTuner with the given initial values.
func NewRuntimeTuner(initial RuntimeSnapshot) RuntimeTuner {
	return &runtimeTuner{snapshot: initial}
}

// Snapshot returns a copy of the current runtime parameters.
func (rt *runtimeTuner) Snapshot() RuntimeSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.snapshot
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

// ReportError atomically increments the error count.
func (rt *runtimeTuner) ReportError() {
	rt.errorCount.Add(1)
}

// Metrics returns the current operational metrics.
func (rt *runtimeTuner) Metrics() RuntimeMetrics {
	qd := int(rt.queueDepth.Load())
	if qd < 0 {
		qd = 0 // Guard against slight negative from concurrent delta reporting
	}
	return RuntimeMetrics{
		ActiveJobs: int(rt.activeJobs.Load()),
		QueueDepth: qd,
		ErrorCount: int(rt.errorCount.Load()),
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
