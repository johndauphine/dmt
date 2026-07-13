package tuning

import (
	"math"
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

func TestEstimatedMemMB_RoundsUpAndSaturatesOverflow(t *testing.T) {
	if got := EstimatedMemMB(1, 1, 0, 1, 1); got != 1 {
		t.Errorf("one modeled byte = %d MiB, want rounded-up 1", got)
	}

	maxInt := int(^uint(0) >> 1)
	got := EstimatedMemMB(maxInt, maxInt, maxInt, maxInt, math.MaxInt64)
	if got <= 0 {
		t.Errorf("overflowing estimate = %d, want positive saturated value", got)
	}
}

func TestMemoryEstimateExceedsBudgetRetainsOverflowInformation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if !MemoryEstimateExceedsBudget(math.MaxInt64, maxInt, maxInt, maxInt, maxInt, math.MaxInt64) {
		t.Fatal("overflowing modeled product appeared to fit a near-MaxInt64 MiB budget")
	}
	if MemoryEstimateExceedsBudget(math.MaxInt64, 1, 1, 0, 1, 1) {
		t.Fatal("one modeled byte should fit a near-MaxInt64 MiB budget")
	}
	budgetBetweenSaturatedAndExactSumMB := int64(3_000)
	if int64(maxInt) > math.MaxInt32 {
		budgetBetweenSaturatedAndExactSumMB = 10_000_000_000_000
	}
	if !MemoryEstimateExceedsBudget(budgetBetweenSaturatedAndExactSumMB, 1, maxInt, maxInt, 1, 1) {
		t.Fatal("overflowing buffer sum lost information and appeared to fit the intermediate budget")
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
		{"negative-budget", -1, 16, 4, 2, 500, 0},
		{"negative-buffer", 28000, 16, -1, 2, 500, 0},
		{"zero-row-width", 28000, 16, 4, 2, 0, 0},
		{"minimum-progress-over-budget", 1, 1, 1, 0, 2 * 1024 * 1024, 1},
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

func TestSafeChunkSize_OverflowReturnsConservativeMinimum(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	got, minimumExceeds := safeChunkSizeDetail(1, maxInt, maxInt, maxInt, math.MaxInt64)
	if got != 1 || !minimumExceeds {
		t.Fatalf("overflow safe chunk = (%d, %v), want (1, true)", got, minimumExceeds)
	}
}

func TestSafeChunkSize_LargeBudgetKeepsExactOverflowingBufferSum(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	budgetMB := int64(6_000)
	if int64(maxInt) > math.MaxInt32 {
		budgetMB = 18_000_000_000_000
	}
	rows, minimumExceeds := safeChunkSizeDetail(budgetMB, 1, maxInt, maxInt, 1)
	if rows != 1 || minimumExceeds {
		t.Fatalf("exact overflowing-buffer inverse = (%d,%v), want (1,false): one row fits while two do not", rows, minimumExceeds)
	}

	out := Output{Workers: 1, ReadAheadBuffers: maxInt, WriteAheadWriters: maxInt, ChunkSize: 2}
	applyMemoryClamp(&out, Input{MemoryBudgetMB: budgetMB, SafetyRowBytes: 1, SafetyRowBytesKnown: true})
	if out.ChunkSize != 1 || out.MemoryEstimateOverBudget || strings.Contains(out.Reasoning, "still exceeds") {
		t.Fatalf("exact clamp surfaced false minimum-over-budget state: %+v", out)
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
	in := Input{AvgRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true, MemoryBudgetMB: 8000}
	applyMemoryClamp(&out, in)
	if out.ChunkSize != 10000 {
		t.Errorf("ChunkSize should be unchanged at 10000, got %d", out.ChunkSize)
	}
	if out.EstimatedMemMB == 0 {
		t.Errorf("EstimatedMemMB should be populated; got 0")
	}
	if out.MemoryEstimateOverBudget {
		t.Error("within-budget estimate marked over budget")
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
	in := Input{AvgRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true, MemoryBudgetMB: 1024}
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
	if out.MemoryEstimateOverBudget {
		t.Errorf("clamped output still marked over budget: %+v", out)
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
	in := Input{AvgRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true, MemoryBudgetMB: 8_000}
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

	applyMemoryClamp(&out, Input{AvgRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true})

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
	in := Input{AvgRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true, MemoryBudgetMB: 1024}
	applyMemoryClamp(&out, in)
	if out.ChunkSize == 0 {
		t.Errorf("clamp should refuse to clamp chunk_size to 0; got 0")
	}
}

func TestApplyMemoryClamp_UsesSafetyWidthNotOtherAverages(t *testing.T) {
	out := Output{
		Workers:           1,
		ReadAheadBuffers:  1,
		WriteAheadWriters: 0,
		ChunkSize:         1_000,
	}
	in := Input{
		MemoryBudgetMB:         1,
		AvgRowBytes:            100,
		RepresentativeRowBytes: 200,
		SafetyRowBytes:         2_000,
		SafetyRowBytesKnown:    true,
	}

	applyMemoryClamp(&out, in)
	if out.ChunkSize != 524 {
		t.Fatalf("ChunkSize = %d, want 524 from 2000-byte safety width", out.ChunkSize)
	}
	if !strings.Contains(out.Reasoning, "safety width 2000 B, widest observed table-average model; not a per-row bound") {
		t.Errorf("reasoning does not identify observed safety width: %q", out.Reasoning)
	}
}

func TestApplyMemoryClampUsesCardinalityAwareProfile(t *testing.T) {
	out := Output{
		Workers:           1,
		ReadAheadBuffers:  1,
		WriteAheadWriters: 0,
		ChunkSize:         20_000,
		Reasoning:         "baseline",
	}
	in := Input{
		MemoryBudgetMB:      1,
		SafetyRowBytes:      36_864,
		SafetyRowBytesKnown: true,
		MemoryProfile: NewMemoryProfile([]TableMemoryStat{
			{Name: "tiny_lookup", RowCount: 2, AvgRowBytes: 36_864},
			{Name: "large_table", RowCount: 1_000_000, AvgRowBytes: 100},
		}),
	}

	applyMemoryClamp(&out, in)
	if out.ChunkSize != 10_485 {
		t.Fatalf("ChunkSize = %d, want table-aware cap 10485", out.ChunkSize)
	}
	if out.MemoryEstimateOverBudget || out.EstimatedMemMB != 1 {
		t.Fatalf("clamped profile estimate = %d MiB/over=%v, want 1/false", out.EstimatedMemMB, out.MemoryEstimateOverBudget)
	}
	if !strings.Contains(out.Reasoning, "cardinality-aware 2-table model") {
		t.Fatalf("reasoning omitted active memory model: %q", out.Reasoning)
	}
}

func TestApplyMemoryClamp_UnknownSafetyWidthUsesLabeledFallback(t *testing.T) {
	out := Output{Workers: 1, ReadAheadBuffers: 1, ChunkSize: 3_000}
	in := Input{
		MemoryBudgetMB:         1,
		AvgRowBytes:            8_000,
		RepresentativeRowBytes: 9_000,
	}

	applyMemoryClamp(&out, in)
	if out.ChunkSize != 2_097 {
		t.Fatalf("ChunkSize = %d, want 2097 from independent 500-byte fallback", out.ChunkSize)
	}
	if !strings.Contains(out.Reasoning, "safety width 500 B, unobserved fallback estimate") {
		t.Errorf("reasoning does not label fallback safety width: %q", out.Reasoning)
	}
}

func TestApplyMemoryClamp_UnknownSafetyWidthLabeledWithoutClamp(t *testing.T) {
	out := Output{Workers: 1, ReadAheadBuffers: 1, ChunkSize: 10, Reasoning: "baseline"}
	in := Input{MemoryBudgetMB: 1}

	applyMemoryClamp(&out, in)
	if out.ChunkSize != 10 {
		t.Fatalf("within-budget ChunkSize changed to %d", out.ChunkSize)
	}
	if out.MemoryEstimateOverBudget {
		t.Fatal("within-budget fallback estimate marked over budget")
	}
	if !strings.Contains(out.Reasoning, "unobserved fallback estimate (no positive schema width)") {
		t.Errorf("unknown width was not labeled without a clamp: %q", out.Reasoning)
	}
}

func TestApplyMemoryClamp_MinimumProgressCanRemainOverBudget(t *testing.T) {
	out := Output{Workers: 1, ReadAheadBuffers: 1, ChunkSize: 10, Reasoning: "baseline"}
	in := Input{
		MemoryBudgetMB:      1,
		SafetyRowBytes:      2 * 1024 * 1024,
		SafetyRowBytesKnown: true,
	}

	applyMemoryClamp(&out, in)
	if out.ChunkSize != 1 {
		t.Fatalf("ChunkSize = %d, want one-row minimum progress", out.ChunkSize)
	}
	if !out.MemoryEstimateOverBudget {
		t.Fatal("one-row fallback that exceeds budget was not surfaced")
	}
	if out.EstimatedMemMB != 2 {
		t.Errorf("EstimatedMemMB = %d, want 2", out.EstimatedMemMB)
	}
	if !strings.Contains(out.Reasoning, "still exceeds budget") {
		t.Errorf("reasoning falsely implies the minimum fits: %q", out.Reasoning)
	}
}
