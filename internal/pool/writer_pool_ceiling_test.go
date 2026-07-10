package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForLiveWorkers blocks until the pool reports the expected live goroutine
// count or the deadline elapses. Retired idle workers exit asynchronously, so
// tests synchronize on the observable live count instead of sleeping.
func waitForLiveWorkers(t *testing.T, wp *WriterPool, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wp.GetLiveWorkerCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("live worker count = %d, want %d after downscale", wp.GetLiveWorkerCount(), want)
}

// concurrencyMeter records the peak number of WriteFunc bodies executing at
// once so a test can assert the post-downscale concurrency ceiling directly.
type concurrencyMeter struct {
	cur int64
	max int64
}

func (m *concurrencyMeter) enter() {
	n := atomic.AddInt64(&m.cur, 1)
	for {
		peak := atomic.LoadInt64(&m.max)
		if n <= peak || atomic.CompareAndSwapInt64(&m.max, peak, n) {
			break
		}
	}
}

func (m *concurrencyMeter) leave() { atomic.AddInt64(&m.cur, -1) }

func (m *concurrencyMeter) peak() int64 { return atomic.LoadInt64(&m.max) }

func (m *concurrencyMeter) reset() { atomic.StoreInt64(&m.max, atomic.LoadInt64(&m.cur)) }

// waitForCounter blocks until an atomic counter reaches at least want.
func waitForCounter(t *testing.T, counter *int64, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(counter) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (counter=%d, want>=%d)", what, atomic.LoadInt64(counter), want)
}

// TestScaleDownIdleWorkersDoNotConsumeNewJobs is the #642 regression: four idle
// workers scaled to one must retire the three surplus workers before they can
// dequeue a job, so newly submitted work runs at most one-at-a-time on the
// single survivor. Before the fix the retired workers each pulled one more job
// and entered WriteFunc, allowing four concurrent writes after a downscale to
// one.
func TestScaleDownIdleWorkersDoNotConsumeNewJobs(t *testing.T) {
	const initialWorkers = 4
	const survivorWorkers = 1
	const newJobs = 4

	var meter concurrencyMeter
	var entered, completed int64

	// gate is the release channel the current WriteFunc invocation blocks on.
	// Swapping it lets the warm-up round drain independently of the measured
	// round without unblocking the latter. Each invocation captures the gate
	// pointer *before* announcing itself so a swap can never redirect an
	// already-counted warm-up job onto the measured gate.
	var gate atomic.Pointer[chan struct{}]
	warmupGate := make(chan struct{})
	measuredGate := make(chan struct{})
	gate.Store(&warmupGate)

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters:    initialWorkers,
		BufferSize:    1,
		JobBufferSize: newJobs + 1, // hold every job so blocking is on WriteFunc, not Submit
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			g := gate.Load()
			meter.enter()
			defer meter.leave()
			atomic.AddInt64(&entered, 1)
			<-*g
			atomic.AddInt64(&completed, 1)
			return nil
		},
	})
	wp.Start()

	// Warm-up: get all four workers to run a job and then re-park idle on the
	// receive. Without this the downscale can cancel a worker before it ever
	// blocks on the job channel, and the loop-top retirement check would mask
	// the bug this test targets: a worker parked on the receive stealing the
	// next job. The measured round must find the workers already parked.
	for i := 0; i < initialWorkers; i++ {
		if ok := wp.Submit(WriteJob{Rows: [][]any{{-1 - i}}, Seq: int64(-1 - i)}); !ok {
			t.Fatalf("warm-up Submit returned false")
		}
	}
	waitForCounter(t, &entered, initialWorkers, "warm-up jobs to start")
	gate.Store(&measuredGate) // future jobs block on the measured gate
	close(warmupGate)         // let the four warm-up jobs finish and re-park
	waitForCounter(t, &completed, initialWorkers, "warm-up jobs to finish")
	waitForLiveWorkers(t, wp, initialWorkers) // all four survived the warm-up
	time.Sleep(20 * time.Millisecond)         // let the finished workers re-park on the receive
	atomic.StoreInt64(&entered, 0)
	meter.reset()

	// Retire three of the four idle workers. With the fix they wake on their
	// canceled context and exit; the buggy version leaves them parked on the
	// receive, so live never falls to one and this wait fails.
	if err := wp.ScaleWorkers(survivorWorkers); err != nil {
		t.Fatalf("ScaleWorkers(%d): %v", survivorWorkers, err)
	}
	waitForLiveWorkers(t, wp, survivorWorkers)

	for i := 0; i < newJobs; i++ {
		if ok := wp.Submit(WriteJob{Rows: [][]any{{i}}, Seq: int64(i)}); !ok {
			close(measuredGate)
			t.Fatalf("Submit(%d) returned false", i)
		}
	}

	// Only the single survivor may be inside WriteFunc; the retired workers must
	// not have taken any of the just-submitted jobs.
	waitForCounter(t, &entered, 1, "a worker to pick up a job after downscale")
	time.Sleep(20 * time.Millisecond) // give any errant retired worker a chance to also enter
	if peak := meter.peak(); peak > int64(survivorWorkers) {
		close(measuredGate)
		t.Fatalf("concurrent writers after 4->1 downscale = %d, want <= %d", peak, survivorWorkers)
	}

	close(measuredGate)
	wp.Wait()
	if peak := meter.peak(); peak > int64(survivorWorkers) {
		t.Fatalf("peak concurrent writers = %d, want <= %d", peak, survivorWorkers)
	}
}

// TestRapidIdleScaleDownUpRespectsLiveCeiling covers the ceiling criterion for
// the idle path: 4->1->4 over idle workers must never leave more than four live
// writers, because the three retired idle workers exit without work before the
// upscale spawns their replacements.
func TestRapidIdleScaleDownUpRespectsLiveCeiling(t *testing.T) {
	const peakWorkers = 4

	var meter concurrencyMeter
	release := make(chan struct{})

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters:    peakWorkers,
		BufferSize:    1,
		JobBufferSize: 2 * peakWorkers,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			meter.enter()
			defer meter.leave()
			<-release
			return nil
		},
	})
	wp.Start()

	if err := wp.ScaleWorkers(1); err != nil {
		t.Fatalf("ScaleWorkers(1): %v", err)
	}
	waitForLiveWorkers(t, wp, 1)
	if err := wp.ScaleWorkers(peakWorkers); err != nil {
		t.Fatalf("ScaleWorkers(%d): %v", peakWorkers, err)
	}
	waitForLiveWorkers(t, wp, peakWorkers)

	if live := wp.GetLiveWorkerCount(); live != peakWorkers {
		close(release)
		t.Fatalf("live workers after 4->1->4 = %d, want %d", live, peakWorkers)
	}

	// Saturate the restored pool and confirm concurrency never exceeds four.
	var submitted sync.WaitGroup
	for i := 0; i < 4*peakWorkers; i++ {
		submitted.Add(1)
		go func(seq int) {
			defer submitted.Done()
			wp.Submit(WriteJob{Rows: [][]any{{seq}}, Seq: int64(seq)})
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	if peak := meter.peak(); peak > int64(peakWorkers) {
		close(release)
		submitted.Wait()
		t.Fatalf("concurrent writers = %d, want <= %d", peak, peakWorkers)
	}

	close(release)
	submitted.Wait()
	wp.Wait()
}

// TestGetLiveWorkerCountTracksStartAndScale documents the count accessors:
// GetLiveWorkerCount equals the started worker count, and returns to the
// survivor count once idle retired workers exit.
func TestGetLiveWorkerCountTracksStartAndScale(t *testing.T) {
	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters: 3,
		BufferSize: 1,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			return nil
		},
	})
	wp.Start()

	waitForLiveWorkers(t, wp, 3)
	if got := wp.NumWriters(); got != 3 {
		t.Fatalf("NumWriters() = %d, want 3", got)
	}
	if got := wp.GetWorkerCount(); got != 3 {
		t.Fatalf("GetWorkerCount() = %d, want 3", got)
	}

	if err := wp.ScaleWorkers(1); err != nil {
		t.Fatalf("ScaleWorkers(1): %v", err)
	}
	waitForLiveWorkers(t, wp, 1)
	if got := wp.NumWriters(); got != 1 {
		t.Fatalf("NumWriters() after downscale = %d, want 1", got)
	}
	if got := wp.GetWorkerCount(); got != 1 {
		t.Fatalf("GetWorkerCount() after downscale = %d, want 1", got)
	}

	wp.Wait()
}
