package transfer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls a condition until it returns true or timeout expires.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestStallDetector_NoStallWhenProgressing(t *testing.T) {
	var rows atomic.Int64
	rows.Store(100)

	d := NewStallDetector(func() int64 { return rows.Load() }, 50*time.Millisecond, 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	go d.Run(ctx)

	// Simulate steady progress
	for i := 0; i < 5; i++ {
		time.Sleep(60 * time.Millisecond)
		rows.Add(100)
	}

	cancel()

	if d.IsStalled() {
		t.Error("should not be stalled when progress is being made")
	}
}

func TestStallDetector_DetectsStall(t *testing.T) {
	var rows atomic.Int64
	rows.Store(100)

	d := NewStallDetector(func() int64 { return rows.Load() }, 50*time.Millisecond, 150*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.Run(ctx)

	// Don't make progress — poll until stall detected
	waitFor(t, 2*time.Second, func() bool { return d.IsStalled() }, "stall detection")
}

func TestStallDetector_ResolvesAfterProgress(t *testing.T) {
	var rows atomic.Int64
	rows.Store(100)

	d := NewStallDetector(func() int64 { return rows.Load() }, 50*time.Millisecond, 150*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.Run(ctx)

	// Wait for stall
	waitFor(t, 2*time.Second, func() bool { return d.IsStalled() }, "stall detection")

	// Resume progress
	rows.Add(500)

	// Wait for resolution
	waitFor(t, 2*time.Second, func() bool { return !d.IsStalled() }, "stall resolution")
}
