package transfer

import (
	"context"
	"testing"
	"time"
)

func TestMemBudgetNilIsNoOp(t *testing.T) {
	var b *MemBudget
	got, ok := b.acquire(context.Background(), 1000)
	if !ok || got != 0 {
		t.Fatalf("nil budget acquire = (%d,%v), want (0,true)", got, ok)
	}
	b.release(1000) // must not panic
	if !b.fullyReleased() {
		t.Fatal("nil budget fullyReleased = false")
	}
}

func TestNewMemBudgetDisabledOnNonPositive(t *testing.T) {
	if b := NewMemBudget(0); b != nil {
		t.Fatal("NewMemBudget(0) should return nil (disabled)")
	}
	if b := NewMemBudget(-5); b != nil {
		t.Fatal("NewMemBudget(-5) should return nil (disabled)")
	}
}

func TestMemBudgetAcquireRelease(t *testing.T) {
	b := NewMemBudget(1000)
	got, ok := b.acquire(context.Background(), 600)
	if !ok || got != 600 {
		t.Fatalf("acquire(600) = (%d,%v), want (600,true)", got, ok)
	}
	if b.fullyReleased() {
		t.Fatal("fullyReleased() = true while 600 is held")
	}
	b.release(got)
	if !b.fullyReleased() {
		t.Fatal("fullyReleased() = false after releasing everything")
	}
}

func TestMemBudgetClampOversizedChunk(t *testing.T) {
	b := NewMemBudget(1000)
	// A single chunk larger than the whole budget reserves exactly the
	// budget (runs alone) rather than deadlocking.
	got, ok := b.acquire(context.Background(), 5000)
	if !ok || got != 1000 {
		t.Fatalf("acquire(5000) on budget 1000 = (%d,%v), want (1000,true)", got, ok)
	}
	b.release(got)

	// Zero/negative requests floor to 1 so acquire/release stay paired.
	got, ok = b.acquire(context.Background(), 0)
	if !ok || got != 1 {
		t.Fatalf("acquire(0) = (%d,%v), want (1,true)", got, ok)
	}
	b.release(got)
}

func TestMemBudgetBlocksUntilReleased(t *testing.T) {
	b := NewMemBudget(1000)
	first, _ := b.acquire(context.Background(), 800)

	acquired := make(chan int64, 1)
	go func() {
		got, ok := b.acquire(context.Background(), 800) // only 200 free → blocks
		if ok {
			acquired <- got
		}
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire succeeded while 800 of 1000 held")
	case <-time.After(50 * time.Millisecond):
		// still blocked, as expected
	}

	b.release(first) // frees 800 → now 1000 free → second proceeds
	select {
	case got := <-acquired:
		if got != 800 {
			t.Fatalf("second acquire got %d, want 800", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second acquire did not proceed after release")
	}
}

func TestMemBudgetAcquireCancelledReservesNothing(t *testing.T) {
	b := NewMemBudget(1000)
	held, _ := b.acquire(context.Background(), 1000) // exhaust

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	got, ok := b.acquire(ctx, 500)
	if ok || got != 0 {
		t.Fatalf("acquire on cancelled ctx = (%d,%v), want (0,false)", got, ok)
	}

	b.release(held)
	if !b.fullyReleased() {
		t.Fatal("budget not fully released — a cancelled acquire must reserve nothing")
	}
}
