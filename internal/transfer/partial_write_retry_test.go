package transfer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
)

// partialWriteFakePool records the rows passed to each WriteBatch call and, on
// the first call, returns a configurable error. It embeds pool.TargetPool so
// only the two methods executeWrite touches (WriteBatch, DBType) need bodies.
type partialWriteFakePool struct {
	pool.TargetPool
	mu       sync.Mutex
	calls    [][]int64 // ids seen per WriteBatch call
	firstErr error     // returned by the first WriteBatch call
	failed   bool
}

func (p *partialWriteFakePool) DBType() string { return "mysql" }

func (p *partialWriteFakePool) WriteBatch(_ context.Context, opts driver.WriteBatchOptions) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]int64, len(opts.Rows))
	for i, r := range opts.Rows {
		ids[i] = r[0].(int64)
	}
	p.calls = append(p.calls, ids)
	if !p.failed {
		p.failed = true
		return p.firstErr
	}
	return nil
}

// retryRows returns the flattened ids written by every call after the first
// (i.e. the retry), in order.
func (p *partialWriteFakePool) retryRows() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int64
	for _, c := range p.calls[1:] {
		out = append(out, c...)
	}
	return out
}

// fixedAdjuster always recommends the same (smaller) chunk size, as the
// rule-based adjuster does for a recognized chunk-size write error.
type fixedAdjuster int

func (f fixedAdjuster) EvaluateWriteError(context.Context, WriteErrorContext) int { return int(f) }

type lifecycleAdjuster struct {
	tuner                       RuntimeTuner
	applyingCalls, appliedCalls int
	applyingSawOverride         bool
	appliedSawChunk             int
	appliedSawBatch             int
}

func (*lifecycleAdjuster) EvaluateWriteError(context.Context, WriteErrorContext) int { return 3 }

func (a *lifecycleAdjuster) WriteErrorAdjustmentApplying() {
	a.applyingCalls++
	_, chunkSet := a.tuner.TableChunkSize("t")
	_, batchSet := a.tuner.TableBatchSize("t")
	a.applyingSawOverride = chunkSet || batchSet
}

func (a *lifecycleAdjuster) WriteErrorAdjustmentApplied() {
	a.appliedCalls++
	a.appliedSawChunk, _ = a.tuner.TableChunkSize("t")
	a.appliedSawBatch, _ = a.tuner.TableBatchSize("t")
}

func makeRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i + 1)}
	}
	return rows
}

func newRetryTestPool(fake pool.TargetPool) *writerPool {
	return &writerPool{
		tgtPool:            fake,
		targetSchema:       "s",
		targetTable:        "t",
		targetCols:         []string{"id"},
		colTypes:           []string{"int"},
		batchSizeFn:        func() int { return 1000 },
		writeErrorAdjuster: fixedAdjuster(3),
		tableName:          "t",
	}
}

func TestWriteErrorAdjustmentLifecycleBracketsActualTunerSetters(t *testing.T) {
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 1000})
	adjuster := &lifecycleAdjuster{tuner: tuner}
	wp := newRetryTestPool(&partialWriteFakePool{})
	wp.tuner = tuner
	wp.writeErrorAdjuster = adjuster

	if got := wp.evaluateAndAdjust(context.Background(), makeRows(10), errors.New("structural limit")); got != 3 {
		t.Fatalf("adjustment = %d, want 3", got)
	}
	if adjuster.applyingCalls != 1 || adjuster.appliedCalls != 1 || adjuster.applyingSawOverride ||
		adjuster.appliedSawChunk != 3 || adjuster.appliedSawBatch != 3 {
		t.Fatalf("lifecycle ordering = %+v", adjuster)
	}
}

// TestExecuteWriteResumesAfterCommittedPrefix pins #541: on a non-transactional
// target the failed attempt reports how many rows committed via
// driver.PartialWriteError, and the retry must resume AFTER that prefix so the
// committed rows are not re-inserted (duplicated — no PK exists on the target
// during drop_recreate transfer).
func TestExecuteWriteResumesAfterCommittedPrefix(t *testing.T) {
	fake := &partialWriteFakePool{
		firstErr: &driver.PartialWriteError{
			Committed: 6,
			Err:       errors.New("max_allowed_packet exceeded"),
		},
	}
	wp := newRetryTestPool(fake)

	rows := makeRows(10)
	if err := wp.executeWrite(context.Background(), 0, rows); err != nil {
		t.Fatalf("executeWrite: %v", err)
	}

	// The retry must write ONLY rows 7..10 (the uncommitted suffix).
	got := fake.retryRows()
	want := []int64{7, 8, 9, 10}
	if len(got) != len(want) {
		t.Fatalf("retry wrote %v, want %v (committed prefix 1..6 must not be re-written)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("retry wrote %v, want %v", got, want)
		}
	}
}

// TestExecuteWriteIdempotentPathIgnoresCommittedOffset pins review nit N1: for
// the idempotent-on-dup path (ROW_NUMBER resume) the PartialWriteError offset is
// measured within an inner merge sub-chunk, not against the full rows, and
// re-writing a committed prefix is a safe no-op anyway. executeWrite must NOT
// apply the offset there — it retries the whole chunk.
func TestExecuteWriteIdempotentPathIgnoresCommittedOffset(t *testing.T) {
	fake := &partialWriteFakePool{
		firstErr: &driver.PartialWriteError{
			Committed: 6,
			Err:       errors.New("max_allowed_packet exceeded"),
		},
	}
	wp := newRetryTestPool(fake)
	wp.idempotentOnDup = true
	wp.targetPKCols = []string{"id"}

	rows := makeRows(10)
	if err := wp.executeWrite(context.Background(), 0, rows); err != nil {
		t.Fatalf("executeWrite: %v", err)
	}
	got := fake.retryRows()
	if len(got) != 10 {
		t.Fatalf("idempotent retry must re-write all 10 rows (offset must be ignored), wrote %v", got)
	}
}

// TestExecuteWriteRetriesFromZeroOnPlainError verifies the transactional path is
// unchanged: a plain error (target rolled the whole chunk back, nothing
// committed) still retries from row 0.
func TestExecuteWriteRetriesFromZeroOnPlainError(t *testing.T) {
	fake := &partialWriteFakePool{firstErr: errors.New("max_allowed_packet exceeded")}
	wp := newRetryTestPool(fake)

	rows := makeRows(10)
	if err := wp.executeWrite(context.Background(), 0, rows); err != nil {
		t.Fatalf("executeWrite: %v", err)
	}

	got := fake.retryRows()
	if len(got) != 10 {
		t.Fatalf("plain-error retry must re-write all 10 rows (rollback semantics), wrote %v", got)
	}
	for i, id := range got {
		if id != int64(i+1) {
			t.Fatalf("retry wrote %v, want 1..10", got)
		}
	}
}
