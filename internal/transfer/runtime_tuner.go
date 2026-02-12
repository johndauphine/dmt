package transfer

import "sync"

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

// RuntimeTuner provides thread-safe access to runtime migration parameters.
type RuntimeTuner interface {
	Snapshot() RuntimeSnapshot
	Update(RuntimeUpdate) error
}

// runtimeTuner is the concrete implementation of RuntimeTuner.
type runtimeTuner struct {
	mu       sync.RWMutex
	snapshot RuntimeSnapshot
}

// NewRuntimeTuner creates a RuntimeTuner with the given initial values.
func NewRuntimeTuner(initial RuntimeSnapshot) *runtimeTuner {
	return &runtimeTuner{snapshot: initial}
}

// Snapshot returns a copy of the current runtime parameters.
func (rt *runtimeTuner) Snapshot() RuntimeSnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.snapshot
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
