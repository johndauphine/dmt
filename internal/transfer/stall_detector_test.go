package transfer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestStallDetector_NoStallWhenProgressing(t *testing.T) {
	var rows atomic.Int64
	rows.Store(100)

	d := NewStallDetector(func() int64 { return rows.Load() }, 50*time.Millisecond, 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	go d.Run(ctx)

	// Simulate progress
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

	// Don't make any progress — wait for stall detection
	time.Sleep(300 * time.Millisecond)

	if !d.IsStalled() {
		t.Error("should detect stall when no progress is made")
	}
}

func TestStallDetector_ResolvesAfterProgress(t *testing.T) {
	var rows atomic.Int64
	rows.Store(100)

	d := NewStallDetector(func() int64 { return rows.Load() }, 50*time.Millisecond, 150*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.Run(ctx)

	// Wait for stall
	time.Sleep(300 * time.Millisecond)
	if !d.IsStalled() {
		t.Fatal("should be stalled")
	}

	// Resume progress
	rows.Add(500)
	time.Sleep(100 * time.Millisecond)

	if d.IsStalled() {
		t.Error("should resolve after progress resumes")
	}
}
