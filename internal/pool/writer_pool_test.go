package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestScaleDownRequeuesInFlightJobs verifies that when workers are scaled down,
// any job already pulled from the channel by a cancelled worker is re-queued
// and processed by a surviving worker (not silently dropped).
func TestScaleDownRequeuesInFlightJobs(t *testing.T) {
	const totalJobs = 100
	const rowsPerJob = 10

	var written int64

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters: 4,
		BufferSize: 1,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			atomic.AddInt64(&written, int64(len(rows)))
			return nil
		},
	})
	wp.Start()

	// Submit half the jobs
	for i := 0; i < totalJobs/2; i++ {
		rows := make([][]any, rowsPerJob)
		for r := range rows {
			rows[r] = []any{i*rowsPerJob + r}
		}
		wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
	}

	// Scale down from 4 to 1 worker while jobs are in flight
	if err := wp.ScaleWorkers(1); err != nil {
		t.Fatalf("ScaleWorkers: %v", err)
	}

	// Submit remaining jobs — only the surviving worker should process these
	for i := totalJobs / 2; i < totalJobs; i++ {
		rows := make([][]any, rowsPerJob)
		for r := range rows {
			rows[r] = []any{i*rowsPerJob + r}
		}
		wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
	}

	wp.Wait()

	expected := int64(totalJobs * rowsPerJob)
	got := atomic.LoadInt64(&written)
	if got != expected {
		t.Errorf("expected %d rows written, got %d (lost %d)", expected, got, expected-got)
	}
}

// TestScaleDownMultipleWorkersSimultaneously verifies that scaling down multiple
// workers at once doesn't lose any jobs.
func TestScaleDownMultipleWorkersSimultaneously(t *testing.T) {
	const totalJobs = 200
	const rowsPerJob = 5

	var written int64

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters: 8,
		BufferSize: 1,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			// Simulate some write latency to increase chance of in-flight jobs
			time.Sleep(time.Millisecond)
			atomic.AddInt64(&written, int64(len(rows)))
			return nil
		},
	})
	wp.Start()

	// Submit jobs concurrently with scale-down
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < totalJobs; i++ {
			rows := make([][]any, rowsPerJob)
			for r := range rows {
				rows[r] = []any{i*rowsPerJob + r}
			}
			wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
		}
	}()

	// Scale down aggressively while jobs are being submitted
	time.Sleep(5 * time.Millisecond)
	if err := wp.ScaleWorkers(2); err != nil {
		t.Fatalf("ScaleWorkers(2): %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := wp.ScaleWorkers(1); err != nil {
		t.Fatalf("ScaleWorkers(1): %v", err)
	}

	wg.Wait()
	wp.Wait()

	expected := int64(totalJobs * rowsPerJob)
	got := atomic.LoadInt64(&written)
	if got != expected {
		t.Errorf("expected %d rows written, got %d (lost %d)", expected, got, expected-got)
	}
}

// TestScaleUpAndDown verifies that scaling up then down preserves all jobs.
func TestScaleUpAndDown(t *testing.T) {
	const totalJobs = 150
	const rowsPerJob = 8

	var written int64

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters: 2,
		BufferSize: 1,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			atomic.AddInt64(&written, int64(len(rows)))
			return nil
		},
	})
	wp.Start()

	for i := 0; i < totalJobs/3; i++ {
		rows := make([][]any, rowsPerJob)
		for r := range rows {
			rows[r] = []any{i*rowsPerJob + r}
		}
		wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
	}

	// Scale up
	if err := wp.ScaleWorkers(6); err != nil {
		t.Fatalf("ScaleWorkers(6): %v", err)
	}

	for i := totalJobs / 3; i < 2*totalJobs/3; i++ {
		rows := make([][]any, rowsPerJob)
		for r := range rows {
			rows[r] = []any{i*rowsPerJob + r}
		}
		wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
	}

	// Scale back down
	if err := wp.ScaleWorkers(2); err != nil {
		t.Fatalf("ScaleWorkers(2): %v", err)
	}

	for i := 2 * totalJobs / 3; i < totalJobs; i++ {
		rows := make([][]any, rowsPerJob)
		for r := range rows {
			rows[r] = []any{i*rowsPerJob + r}
		}
		wp.Submit(WriteJob{Rows: rows, Seq: int64(i)})
	}

	wp.Wait()

	expected := int64(totalJobs * rowsPerJob)
	got := atomic.LoadInt64(&written)
	if got != expected {
		t.Errorf("expected %d rows written, got %d (lost %d)", expected, got, expected-got)
	}
}

func TestCalculateJobBufferSize(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PipelineBufferConfig
		wantMin int // result must be >= this
		wantMax int // result must be <= this (0 = no upper check)
	}{
		{
			name: "narrow rows get large buffer",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   2048,
				ChunkSize:        8000,
				RowBytes:         37, // Votes table
				NumWriters:       6,
				ReadAheadBuffers: 3,
			},
			// 2GB / (8000*37) = ~7282 total slots, minus 6+3 active = ~7273
			wantMin: 5000,
		},
		{
			name: "wide rows get small buffer",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   2048,
				ChunkSize:        8000,
				RowBytes:         2290, // Posts table
				NumWriters:       6,
				ReadAheadBuffers: 3,
			},
			// 2GB / (8000*2290) = ~117 total slots, minus 9 active = ~108
			wantMin: 7,   // at least numWriters+1
			wantMax: 150,
		},
		{
			name: "zero memory returns safe minimum",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   0,
				ChunkSize:        50000,
				RowBytes:         573,
				NumWriters:       6,
				ReadAheadBuffers: 3,
			},
			wantMin: 7, // numWriters+1
			wantMax: 7, // exactly the minimum
		},
		{
			name: "zero row bytes returns safe minimum",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   1024,
				ChunkSize:        10000,
				RowBytes:         0,
				NumWriters:       4,
				ReadAheadBuffers: 2,
			},
			wantMin: 5, // numWriters+1
			wantMax: 5,
		},
		{
			name: "tiny budget clamps to minimum",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   1, // 1MB
				ChunkSize:        100000,
				RowBytes:         10000,
				NumWriters:       8,
				ReadAheadBuffers: 4,
			},
			wantMin: 9, // numWriters+1
		},
		{
			name: "active slots subtracted from total",
			cfg: PipelineBufferConfig{
				MemoryBudgetMB:   100,
				ChunkSize:        1000,
				RowBytes:         100,
				NumWriters:       4,
				ReadAheadBuffers: 3,
			},
			// 100MB / (1000*100) = 1048 total slots, minus 4+3 = 1041
			wantMin: 1000,
			wantMax: 1100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateJobBufferSize(tt.cfg)
			if got < tt.wantMin {
				t.Errorf("CalculateJobBufferSize() = %d, want >= %d", got, tt.wantMin)
			}
			if tt.wantMax > 0 && got > tt.wantMax {
				t.Errorf("CalculateJobBufferSize() = %d, want <= %d", got, tt.wantMax)
			}
			if got < tt.cfg.NumWriters+1 {
				t.Errorf("CalculateJobBufferSize() = %d, must be >= numWriters+1 (%d)", got, tt.cfg.NumWriters+1)
			}
		})
	}
}
