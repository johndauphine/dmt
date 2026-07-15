package transfer

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// MemBudget is a shared, byte-weighted admission control for scanned chunks
// entering the pipeline (#617). Readers acquire a chunk's measured scanned
// size before enqueueing it and release the reservation once it is written or
// abandoned. This bounds admitted row payload and redistributes capacity across
// active tables. It is not a hard bound on total process memory: each reader
// scans one chunk before admission, and driver encoding can allocate another
// copy. MemoryGuard and GOMEMLIMIT remain the process-level backstops.
//
// One MemBudget is created per migration and shared across every concurrent
// table pipeline, so a table running alone can use the whole budget while
// several running together divide it by contention — no static per-table
// split. A nil *MemBudget is a valid no-op (accounting disabled), which is
// how the pipeline behaves when no budget is configured.
type MemBudget struct {
	total int64
	sem   *semaphore.Weighted
}

// NewMemBudget returns a budget of totalBytes, or nil when totalBytes <= 0
// (disabled — the caller then runs with channel-depth limiting only).
func NewMemBudget(totalBytes int64) *MemBudget {
	if totalBytes <= 0 {
		return nil
	}
	return &MemBudget{total: totalBytes, sem: semaphore.NewWeighted(totalBytes)}
}

// clamp bounds a request to [1, total]. A single chunk larger than the whole
// budget still proceeds by reserving the entire budget (running alone) — the
// oversized-chunk guard that prevents a wide row from deadlocking the
// pipeline. Zero-byte requests are floored to 1 so acquire/release stay
// paired.
func (b *MemBudget) clamp(n int64) int64 {
	if n < 1 {
		return 1
	}
	if n > b.total {
		return b.total
	}
	return n
}

// acquire reserves clamp(n) bytes, blocking until they are available or ctx
// is cancelled. It returns the number of bytes actually reserved (to release
// later) and whether the reservation succeeded. On a nil budget it is a
// no-op success reserving 0. On cancellation it reserves nothing.
func (b *MemBudget) acquire(ctx context.Context, n int64) (int64, bool) {
	if b == nil {
		return 0, true
	}
	want := b.clamp(n)
	if err := b.sem.Acquire(ctx, want); err != nil {
		return 0, false
	}
	return want, true
}

// release returns n previously-acquired bytes to the budget. Releasing 0 or
// operating on a nil budget is a no-op.
func (b *MemBudget) release(n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.sem.Release(n)
}

// fullyReleased reports whether the entire budget is currently free, i.e.
// nothing is still reserved. It momentarily reserves and releases the whole
// budget, so it must only be called when no acquirer is active (e.g. after a
// transfer returns). Used by tests to assert the pipeline leaked no
// reservation on success, error, and cancel paths (#617).
func (b *MemBudget) fullyReleased() bool {
	if b == nil {
		return true
	}
	if b.sem.TryAcquire(b.total) {
		b.sem.Release(b.total)
		return true
	}
	return false
}
