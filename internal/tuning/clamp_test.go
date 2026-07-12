package tuning

import (
	"strings"
	"testing"
)

// TestEstimatedMemMB verifies the working-set formula matches the
// historical ai_smartconfig math (was ComputeEstimatedMemMB).
func TestEstimatedMemMB(t *testing.T) {
	// 16 workers × (4+2 buffers) × 50000 chunk × 248 B / MiB ≈ 1135 MB
	got := EstimatedMemMB(16, 4, 2, 50000, 248)
	if got < 1100 || got > 1200 {
		t.Errorf("EstimatedMemMB(16, 4, 2, 50000, 248) = %d, want ~1135 MB", got)
	}
}

// TestSafeChunkSize is the mirror image of EstimatedMemMB — round-tripping
// the result back through the formula must fit inside the budget.
func TestSafeChunkSize(t *testing.T) {
	cases := []struct {
		name        string
		budgetMB    int64
		workers     int
		raw, waw    int
		avgRowBytes int64
		want        int64
	}{
		{"so2010-realistic", 28000, 16, 4, 2, 248, 1233204},
		{"capped-rows", 28000, 16, 4, 2, 2000, 152917},
		{"zero-workers", 28000, 0, 4, 2, 500, 0},
		{"zero-buffers", 28000, 16, 0, 0, 500, 0},
		{"zero-budget", 0, 16, 4, 2, 500, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := safeChunkSize(tc.budgetMB, tc.workers, tc.raw, tc.waw, tc.avgRowBytes)
			if got != tc.want {
				t.Errorf("safeChunkSize = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestApplyMemoryClamp_NoOpWhenWithinBudget verifies the clamp leaves
// chunk_size alone when it already fits, and populates EstimatedMemMB.
func TestApplyMemoryClamp_NoOpWhenWithinBudget(t *testing.T) {
	out := Output{
		Workers:           4,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ChunkSize:         10000,
	}
	in := Input{AvgRowBytes: 500, MemoryBudgetMB: 8000}
	applyMemoryClamp(&out, in)
	if out.ChunkSize != 10000 {
		t.Errorf("ChunkSize should be unchanged at 10000, got %d", out.ChunkSize)
	}
	if out.EstimatedMemMB == 0 {
		t.Errorf("EstimatedMemMB should be populated; got 0")
	}
}

// TestApplyMemoryClamp_ShrinksWhenOverBudget verifies the clamp shrinks
// chunk_size to the safe ceiling when the input would blow the budget.
func TestApplyMemoryClamp_ShrinksWhenOverBudget(t *testing.T) {
	// 16 workers × (4+2) × 1_000_000 × 500 B = 48 GB; budget 1024 MB
	out := Output{
		Workers:           16,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ChunkSize:         1_000_000,
		Reasoning:         "regression-selected WAW=2, chunk_size=1000000",
	}
	in := Input{AvgRowBytes: 500, MemoryBudgetMB: 1024}
	applyMemoryClamp(&out, in)
	if out.ChunkSize >= 1_000_000 {
		t.Errorf("ChunkSize should be clamped down from 1_000_000; got %d", out.ChunkSize)
	}
	// Verify it's at-or-under the budget after clamp
	if out.EstimatedMemMB > 1024 {
		t.Errorf("EstimatedMemMB should fit within 1024 MB budget; got %d MB", out.EstimatedMemMB)
	}
	// Reasoning chain must record the clamp so the persisted text matches
	// the final out.ChunkSize (Copilot review on PR #203).
	if !strings.Contains(out.Reasoning, "memory clamp") {
		t.Errorf("Reasoning should record the clamp; got %q", out.Reasoning)
	}
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Reasoning should preserve the upstream selector note; got %q", out.Reasoning)
	}
}

// TestApplyMemoryClamp_NoReasoningWhenNoClamp verifies the clamp note is
// only appended when ChunkSize actually shrinks. A within-budget run
// must not get a misleading "memory clamp:" entry on its Reasoning.
func TestApplyMemoryClamp_NoReasoningWhenNoClamp(t *testing.T) {
	out := Output{
		Workers:           4,
		ReadAheadBuffers:  2,
		WriteAheadWriters: 1,
		ChunkSize:         10_000,
		Reasoning:         "smoothed-bins kept WAW=1",
	}
	in := Input{AvgRowBytes: 500, MemoryBudgetMB: 8_000}
	applyMemoryClamp(&out, in)
	if strings.Contains(out.Reasoning, "memory clamp") {
		t.Errorf("Reasoning must not mention clamp on within-budget run; got %q", out.Reasoning)
	}
}

// TestApplyMemoryClamp_ZeroBudgetIsNonbinding pins the #708 boundary:
// callers must resolve the envelope. An absent budget does not recreate a
// hidden fallback policy inside tuning, but estimates remain available for
// diagnostics and persistence.
func TestApplyMemoryClamp_ZeroBudgetIsNonbinding(t *testing.T) {
	out := Output{
		Workers:           16,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ChunkSize:         1_000_000,
		Reasoning:         "baseline",
	}

	applyMemoryClamp(&out, Input{AvgRowBytes: 500})

	if out.ChunkSize != 1_000_000 {
		t.Errorf("zero budget changed ChunkSize to %d, want 1000000", out.ChunkSize)
	}
	if out.EstimatedMemMB == 0 {
		t.Error("zero budget must still populate EstimatedMemMB")
	}
	if strings.Contains(out.Reasoning, "memory clamp") {
		t.Errorf("zero budget emitted clamp reasoning: %q", out.Reasoning)
	}
}

// TestApplyMemoryClamp_RefusesZeroChunk: when the safe ceiling is 0
// (zero workers / zero buffers), the clamp leaves chunk_size alone
// rather than blocking the migration with chunk_size=0.
func TestApplyMemoryClamp_RefusesZeroChunk(t *testing.T) {
	out := Output{
		Workers:           0, // pathological
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ChunkSize:         10000,
	}
	in := Input{AvgRowBytes: 500, MemoryBudgetMB: 1024}
	applyMemoryClamp(&out, in)
	if out.ChunkSize == 0 {
		t.Errorf("clamp should refuse to clamp chunk_size to 0; got 0")
	}
}
