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

func TestRuntimeTunerMetricsStartAtZero(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})
	m := tuner.Metrics()

	if m.ActiveJobs != 0 {
		t.Errorf("ActiveJobs = %d, want 0", m.ActiveJobs)
	}
	if m.QueueDepth != 0 {
		t.Errorf("QueueDepth = %d, want 0", m.QueueDepth)
	}
	if m.ErrorCount != 0 {
		t.Errorf("ErrorCount = %d, want 0", m.ErrorCount)
	}
	if m.TotalQueryNs != 0 || m.TotalScanNs != 0 || m.TotalWriteNs != 0 || m.TotalTransferRows != 0 {
		t.Error("expected all transfer time fields to start at 0")
	}
}

func TestRuntimeTunerReportActiveJobs(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})

	tuner.ReportActiveJobs(1)
	tuner.ReportActiveJobs(1)
	tuner.ReportActiveJobs(1)
	if got := tuner.Metrics().ActiveJobs; got != 3 {
		t.Errorf("ActiveJobs = %d, want 3", got)
	}

	tuner.ReportActiveJobs(-1)
	if got := tuner.Metrics().ActiveJobs; got != 2 {
		t.Errorf("ActiveJobs = %d, want 2 after -1", got)
	}
}

func TestRuntimeTunerReportQueueDepth(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})

	tuner.ReportQueueDepth(10)
	tuner.ReportQueueDepth(5)
	if got := tuner.Metrics().QueueDepth; got != 15 {
		t.Errorf("QueueDepth = %d, want 15", got)
	}

	// Simulate job cleanup reporting negative delta
	tuner.ReportQueueDepth(-15)
	if got := tuner.Metrics().QueueDepth; got != 0 {
		t.Errorf("QueueDepth = %d, want 0 after cleanup", got)
	}

	// Slight negative due to concurrency should be clamped to 0
	tuner.ReportQueueDepth(-1)
	if got := tuner.Metrics().QueueDepth; got != 0 {
		t.Errorf("QueueDepth = %d, want 0 (clamped)", got)
	}
}

func TestRuntimeTunerReportError(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})

	tuner.ReportError()
	tuner.ReportError()
	tuner.ReportError()

	if got := tuner.Metrics().ErrorCount; got != 3 {
		t.Errorf("ErrorCount = %d, want 3", got)
	}
}

func TestRuntimeTunerReportTransferTime(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})

	tuner.ReportTransferTime(100, 200, 300, 1000)
	tuner.ReportTransferTime(150, 250, 350, 2000)

	m := tuner.Metrics()
	if m.TotalQueryNs != 250 {
		t.Errorf("TotalQueryNs = %d, want 250", m.TotalQueryNs)
	}
	if m.TotalScanNs != 450 {
		t.Errorf("TotalScanNs = %d, want 450", m.TotalScanNs)
	}
	if m.TotalWriteNs != 650 {
		t.Errorf("TotalWriteNs = %d, want 650", m.TotalWriteNs)
	}
	if m.TotalTransferRows != 3000 {
		t.Errorf("TotalTransferRows = %d, want 3000", m.TotalTransferRows)
	}
}

func TestRuntimeTunerConcurrentMetrics(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tuner.ReportActiveJobs(1)
			tuner.ReportQueueDepth(5)
			tuner.ReportError()
			tuner.ReportTransferTime(10, 20, 30, 100)
		}()
	}
	wg.Wait()

	m := tuner.Metrics()
	if m.ActiveJobs != 100 {
		t.Errorf("ActiveJobs = %d, want 100", m.ActiveJobs)
	}
	if m.QueueDepth != 500 {
		t.Errorf("QueueDepth = %d, want 500", m.QueueDepth)
	}
	if m.ErrorCount != 100 {
		t.Errorf("ErrorCount = %d, want 100", m.ErrorCount)
	}
	if m.TotalQueryNs != 1000 {
		t.Errorf("TotalQueryNs = %d, want 1000", m.TotalQueryNs)
	}
	if m.TotalTransferRows != 10000 {
		t.Errorf("TotalTransferRows = %d, want 10000", m.TotalTransferRows)
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
