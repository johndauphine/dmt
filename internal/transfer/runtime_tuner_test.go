package transfer

import (
	"sync"
	"testing"
)

func TestRuntimeTunerSnapshotReturnsInitialValues(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		ChunkSize:            5000,
		ReadAheadBuffers:     4,
		ParallelReaders:      2,
		WriteAheadWriters:    3,
		CheckpointFrequency:  10,
		UpsertMergeChunkSize: 1000,
	})

	snap := tuner.Snapshot()
	if snap.ChunkSize != 5000 {
		t.Errorf("ChunkSize = %d, want 5000", snap.ChunkSize)
	}
	if snap.WriteAheadWriters != 3 {
		t.Errorf("WriteAheadWriters = %d, want 3", snap.WriteAheadWriters)
	}
	if snap.CheckpointFrequency != 10 {
		t.Errorf("CheckpointFrequency = %d, want 10", snap.CheckpointFrequency)
	}
}

func TestRuntimeTunerPartialUpdate(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		ChunkSize:         5000,
		WriteAheadWriters: 3,
		ParallelReaders:   2,
	})

	newChunk := 10000
	if err := tuner.Update(RuntimeUpdate{ChunkSize: &newChunk}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snap := tuner.Snapshot()
	if snap.ChunkSize != 10000 {
		t.Errorf("ChunkSize = %d, want 10000", snap.ChunkSize)
	}
	// Unchanged fields stay the same
	if snap.WriteAheadWriters != 3 {
		t.Errorf("WriteAheadWriters = %d, want 3 (unchanged)", snap.WriteAheadWriters)
	}
	if snap.ParallelReaders != 2 {
		t.Errorf("ParallelReaders = %d, want 2 (unchanged)", snap.ParallelReaders)
	}
}

func TestRuntimeTunerMultiFieldUpdate(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		ChunkSize:            1000,
		WriteAheadWriters:    1,
		CheckpointFrequency:  5,
		UpsertMergeChunkSize: 500,
	})

	newChunk := 2000
	newWriters := 4
	newFreq := 20
	if err := tuner.Update(RuntimeUpdate{
		ChunkSize:           &newChunk,
		WriteAheadWriters:   &newWriters,
		CheckpointFrequency: &newFreq,
	}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	snap := tuner.Snapshot()
	if snap.ChunkSize != 2000 {
		t.Errorf("ChunkSize = %d, want 2000", snap.ChunkSize)
	}
	if snap.WriteAheadWriters != 4 {
		t.Errorf("WriteAheadWriters = %d, want 4", snap.WriteAheadWriters)
	}
	if snap.CheckpointFrequency != 20 {
		t.Errorf("CheckpointFrequency = %d, want 20", snap.CheckpointFrequency)
	}
	// Unchanged
	if snap.UpsertMergeChunkSize != 500 {
		t.Errorf("UpsertMergeChunkSize = %d, want 500 (unchanged)", snap.UpsertMergeChunkSize)
	}
}

func TestRuntimeTunerConcurrentAccess(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000, WriteAheadWriters: 2})

	var wg sync.WaitGroup
	// Concurrent writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			tuner.Update(RuntimeUpdate{ChunkSize: &v})
		}(i * 100)
	}
	// Concurrent readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap := tuner.Snapshot()
			// ChunkSize should always be a valid value (not corrupted)
			if snap.ChunkSize < 0 {
				t.Errorf("got negative ChunkSize: %d", snap.ChunkSize)
			}
		}()
	}
	wg.Wait()
}
