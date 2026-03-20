package transfer

import (
	"context"
	"testing"
	"time"
)

func TestNewMemoryGuard_NilWhenNoLimit(t *testing.T) {
	mg := newMemoryGuard(0)
	if mg != nil {
		t.Error("expected nil guard when limit is 0")
	}

	mg = newMemoryGuard(-1)
	if mg != nil {
		t.Error("expected nil guard when limit is negative")
	}
}

func TestNewMemoryGuard_ThresholdAt80Percent(t *testing.T) {
	mg := newMemoryGuard(1000) // 1000 MB
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
	var mg *memoryGuard
	ctx := context.Background()
	if !mg.waitIfNeeded(ctx) {
		t.Error("nil guard should always return true")
	}
}

func TestMemoryGuard_BelowThresholdPasses(t *testing.T) {
	// Set threshold extremely high so current heap is always below
	mg := newMemoryGuard(1024 * 1024) // 1 TB
	ctx := context.Background()
	if !mg.waitIfNeeded(ctx) {
		t.Error("should return true when heap is well below threshold")
	}
}

func TestMemoryGuard_ContextCancellation(t *testing.T) {
	// Set threshold at 1 MB — almost certainly exceeded by test runtime
	mg := newMemoryGuard(1)
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
