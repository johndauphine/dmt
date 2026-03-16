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
		name              string
		pipelineMemoryMB  int
		chunkSize         int
		estimatedRowBytes int64
		numWriters        int
		wantMin           int // result must be >= this
		wantMax           int // result must be <= this (0 = no upper check)
	}{
		{
			name:              "narrow rows get large buffer",
			pipelineMemoryMB:  2048,
			chunkSize:         8000,
			estimatedRowBytes: 37, // Votes table
			numWriters:        6,
			wantMin:           1000, // 2GB / (8000*37) = ~7200
		},
		{
			name:              "wide rows get small buffer",
			pipelineMemoryMB:  2048,
			chunkSize:         8000,
			estimatedRowBytes: 2290, // Posts table
			numWriters:        6,
			wantMin:           7,   // at least numWriters+1
			wantMax:           200, // 2GB / (8000*2290) = ~117
		},
		{
			name:              "zero memory uses default 2GB",
			pipelineMemoryMB:  0,
			chunkSize:         50000,
			estimatedRowBytes: 573,
			numWriters:        6,
			wantMin:           7, // at least numWriters+1
		},
		{
			name:              "zero row bytes uses 1KB default",
			pipelineMemoryMB:  1024,
			chunkSize:         10000,
			estimatedRowBytes: 0,
			numWriters:        4,
			wantMin:           5, // at least numWriters+1
		},
		{
			name:              "minimum is numWriters+1",
			pipelineMemoryMB:  1, // tiny budget
			chunkSize:         100000,
			estimatedRowBytes: 10000, // huge rows
			numWriters:        8,
			wantMin:           9, // numWriters+1
		},
		{
			name:              "zero chunk uses default 50000",
			pipelineMemoryMB:  2048,
			chunkSize:         0,
			estimatedRowBytes: 100,
			numWriters:        4,
			wantMin:           5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateJobBufferSize(tt.pipelineMemoryMB, tt.chunkSize, tt.estimatedRowBytes, tt.numWriters)
			if got < tt.wantMin {
				t.Errorf("CalculateJobBufferSize() = %d, want >= %d", got, tt.wantMin)
			}
			if tt.wantMax > 0 && got > tt.wantMax {
				t.Errorf("CalculateJobBufferSize() = %d, want <= %d", got, tt.wantMax)
			}
			if got < tt.numWriters+1 {
				t.Errorf("CalculateJobBufferSize() = %d, must be >= numWriters+1 (%d)", got, tt.numWriters+1)
			}
		})
	}
}
