package transfer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
)

func TestNewMemoryGuard_NilWhenNoLimit(t *testing.T) {
	mg := NewMemoryGuard(0)
	if mg != nil {
		t.Error("expected nil guard when limit is 0")
	}

	mg = NewMemoryGuard(-1)
	if mg != nil {
		t.Error("expected nil guard when limit is negative")
	}
}

func TestNewMemoryGuard_ThresholdAt80Percent(t *testing.T) {
	mg := NewMemoryGuard(1000) // 1000 MB
	if mg == nil {
		t.Fatal("expected non-nil guard")
	}

	expectedLimit := uint64(1000) * 1024 * 1024
	if mg.limitBytes != expectedLimit {
		t.Errorf("limitBytes = %d, want %d", mg.limitBytes, expectedLimit)
	}

	expectedThreshold := expectedLimit * 80 / 100
	if mg.threshold != expectedThreshold {
		t.Errorf("threshold = %d, want %d", mg.threshold, expectedThreshold)
	}
}

func TestMemoryGuard_NilIsNoOp(t *testing.T) {
	var mg *MemoryGuard
	ctx := context.Background()
	if !mg.waitIfNeeded(ctx) {
		t.Error("nil guard should always return true")
	}
}

func TestMemoryGuard_BelowThresholdPasses(t *testing.T) {
	// Set threshold extremely high so current heap is always below
	mg := NewMemoryGuard(1024 * 1024) // 1 TB
	ctx := context.Background()
	if !mg.waitIfNeeded(ctx) {
		t.Error("should return true when heap is well below threshold")
	}
}

func TestMemoryGuard_ContextCancellation(t *testing.T) {
	// Set threshold at 1 MB — almost certainly exceeded by test runtime
	mg := NewMemoryGuard(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately so waitIfNeeded doesn't block long
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result := mg.waitIfNeeded(ctx)
	if result {
		// If heap happens to be under 0.8 MB, the guard won't block.
		// That's fine — this test mainly verifies no deadlock on cancel.
		t.Log("heap was below 0.8MB threshold, guard did not block (ok)")
	}
}

func TestMemoryGuardSharedLeaderRunsFreeOSMemoryOnce(t *testing.T) {
	const waiters = 8
	guard := NewMemoryGuard(1) // test heap is reliably above this threshold
	if guard == nil {
		t.Fatal("NewMemoryGuard returned nil")
	}

	originalFreeOSMemory := freeOSMemory
	firstLeader := make(chan struct{})
	releaseLeader := make(chan struct{})
	var freeCalls atomic.Int32
	freeOSMemory = func() {
		if freeCalls.Add(1) == 1 {
			close(firstLeader)
		}
		<-releaseLeader
	}
	t.Cleanup(func() { freeOSMemory = originalFreeOSMemory })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(waiters)
	start := make(chan struct{})
	for range waiters {
		go func() {
			defer wg.Done()
			<-start
			guard.waitIfNeeded(ctx)
		}()
	}
	close(start)
	select {
	case <-firstLeader:
	case <-time.After(time.Second):
		t.Fatal("shared guard never elected a FreeOSMemory leader")
	}

	// Keep the elected leader inside the hook while every concurrent reader
	// contends on the same gcActive bit. If the guard were per pipeline, each
	// waiter would call this hook immediately instead.
	time.Sleep(25 * time.Millisecond)
	if got := freeCalls.Load(); got != 1 {
		t.Fatalf("FreeOSMemory calls while one pressure episode is active = %d, want 1", got)
	}
	cancel()
	close(releaseLeader)
	wg.Wait()
	if got := freeCalls.Load(); got != 1 {
		t.Fatalf("FreeOSMemory calls = %d, want one shared leader", got)
	}
}

func TestMemoryGuardForJobUsesSharedGuardAndNilFallback(t *testing.T) {
	shared := NewMemoryGuard(64)
	if got := memoryGuardForJob(&config.Config{}, Job{MemGuard: shared}); got != shared {
		t.Fatalf("memoryGuardForJob shared guard = %p, want %p", got, shared)
	}
	if got := memoryGuardForJob(&config.Config{}, Job{}); got != nil {
		t.Fatalf("memoryGuardForJob without a configured limit = %p, want nil fallback", got)
	}
}
