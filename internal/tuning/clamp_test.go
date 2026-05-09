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

// TestMemoryBudgetMB locks in the budget-source selection rules
// (#158 fallback + #161 floor against the non-monotone cliff).
func TestMemoryBudgetMB(t *testing.T) {
	cases := []struct {
		name       string
		in         Input
		wantBudget int64
		wantSource string
	}{
		{
			"both unset → fallback default",
			Input{},
			fallbackBudgetMB,
			"fallback default",
		},
		{
			"cap only",
			Input{MaxMemoryMB: 2000},
			2000,
			"user max_memory_mb=2000",
		},
		{
			"available only — headroom",
			Input{AvailableMemoryMB: 30000},
			27952,
			"available_memory_mb=30000 - 2048 MB headroom",
		},
		{
			"available below 2GB+fallback → floored",
			Input{AvailableMemoryMB: 1000},
			fallbackBudgetMB,
			"floored at 1024",
		},
		{
			"both — cap is tighter",
			Input{MaxMemoryMB: 8000, AvailableMemoryMB: 30000},
			8000,
			"user max_memory_mb=8000",
		},
		{
			"both — headroom is tighter",
			Input{MaxMemoryMB: 50000, AvailableMemoryMB: 30000},
			27952,
			"available_memory_mb=30000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotBudget, gotSource := memoryBudgetMB(tc.in)
			if gotBudget != tc.wantBudget {
				t.Errorf("budget: got %d, want %d", gotBudget, tc.wantBudget)
			}
			if !strings.Contains(gotSource, tc.wantSource) {
				t.Errorf("source: got %q, want substring %q", gotSource, tc.wantSource)
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
	in := Input{AvgRowBytes: 500, MaxMemoryMB: 8000}
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
	}
	in := Input{AvgRowBytes: 500, MaxMemoryMB: 1024}
	applyMemoryClamp(&out, in)
	if out.ChunkSize >= 1_000_000 {
		t.Errorf("ChunkSize should be clamped down from 1_000_000; got %d", out.ChunkSize)
	}
	// Verify it's at-or-under the budget after clamp
	if out.EstimatedMemMB > 1024 {
		t.Errorf("EstimatedMemMB should fit within 1024 MB budget; got %d MB", out.EstimatedMemMB)
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
	in := Input{AvgRowBytes: 500, MaxMemoryMB: 1024}
	applyMemoryClamp(&out, in)
	if out.ChunkSize == 0 {
		t.Errorf("clamp should refuse to clamp chunk_size to 0; got 0")
	}
}
