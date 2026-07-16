package tuning

import (
	"reflect"
	"strings"
	"testing"
)

// TestDefaultOutput_KnobsByCPU locks in the cpu-cores → workers mapping and
// the dependent connection-pool / max-partitions formulas.
func TestDefaultOutput_KnobsByCPU(t *testing.T) {
	cases := []struct {
		name             string
		cpuCores         int
		wantWorkers      int
		wantMaxPartition int
		wantSrcConns     int
		wantTgtConns     int
	}{
		{"one core", 1, 2, 2, 8, 8},
		{"two cores", 2, 2, 2, 8, 8},
		{"four cores", 4, 2, 2, 8, 8},
		{"six cores", 6, 4, 4, 12, 12},
		{"eight cores", 8, 6, 6, 16, 16},
		{"eighteen cores", 18, 16, 16, 36, 68},
		{"thirty-two cores", 32, 30, 30, 64, 244},
		{"sixty-four cores", 64, 62, 62, 128, 996},
		{"one-hundred-twenty-eight cores", 128, 126, 126, 256, 4036},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Input{CPUCores: tc.cpuCores, AvgRowBytes: 500, Platform: "linux"}
			profile := DriverProfile{Name: "postgres", BaselineWAW: 2, ScaleWritersWithCores: true, OptimumBulkChunkBytes: 25_000_000}
			out := DefaultOutput(in, profile)
			if out.Workers != tc.wantWorkers {
				t.Errorf("Workers: got %d, want %d", out.Workers, tc.wantWorkers)
			}
			if out.MaxPartitions != tc.wantMaxPartition {
				t.Errorf("MaxPartitions: got %d, want %d", out.MaxPartitions, tc.wantMaxPartition)
			}
			if out.MaxSourceConnections != tc.wantSrcConns {
				t.Errorf("MaxSourceConnections: got %d, want %d", out.MaxSourceConnections, tc.wantSrcConns)
			}
			if out.MaxTargetConnections != tc.wantTgtConns {
				t.Errorf("MaxTargetConnections: got %d, want %d", out.MaxTargetConnections, tc.wantTgtConns)
			}
		})
	}
}

// TestDefaultOutput_ChunkSizeFloors locks in chunkRowsFromProfile's two
// outputs that prevent pathological zero / over-protocol values:
//   - Floor at 1 row when avg_row_bytes > byte budget (would otherwise
//     return 0 and block the migration).
//   - Cap at HardChunkLimit when set (the per-target protocol cap from
//     #166 — currently 0 for all drivers, but the enforcement lands now).
func TestDefaultOutput_ChunkSizeFloors(t *testing.T) {
	// Floor: 10 MB fallback budget at 50 MB/row → 0 rows mathematically;
	// floored to 1 so WriteBatch doesn't get a zero-size chunk.
	in := Input{CPUCores: 8, AvgRowBytes: 2_000, RepresentativeRowBytes: 50_000_000, Platform: "linux"}
	profile := DriverProfile{Name: "test", BaselineWAW: 2}
	out := DefaultOutput(in, profile)
	if out.ChunkSize != 1 {
		t.Errorf("ChunkSize for 50MB rows: got %d, want 1 (floor)", out.ChunkSize)
	}

	// Hard limit cap: 25 MB optimum at 500 B = 50000 rows mathematically;
	// HardChunkLimit=1000 should cap to 1000.
	in = Input{
		CPUCores:               8,
		AvgRowBytes:            500,
		RepresentativeRowBytes: 500,
		SafetyRowBytes:         500,
		SafetyRowBytesKnown:    true,
		MemoryBudgetMB:         64_000,
		Platform:               "linux",
	}
	profile = DriverProfile{
		Name:                  "test",
		BaselineWAW:           2,
		OptimumBulkChunkBytes: 25_000_000,
		HardChunkLimit:        1000,
	}
	out = DefaultOutput(in, profile)
	if out.ChunkSize != 1000 {
		t.Errorf("ChunkSize with HardChunkLimit=1000: got %d, want 1000", out.ChunkSize)
	}
	if out.ChunkSize < 1 || out.EstimatedMemMB > in.MemoryBudgetMB || out.MemoryEstimateOverBudget {
		t.Fatalf("hard-limited output is not safely estimated: %+v", out)
	}

	// Hard limit not binding: 25 MB optimum at 500 B = 50000 rows;
	// HardChunkLimit=100000 leaves the optimum alone.
	profile.HardChunkLimit = 100000
	out = DefaultOutput(in, profile)
	if out.ChunkSize != 50000 {
		t.Errorf("ChunkSize with HardChunkLimit=100000 (non-binding): got %d, want 50000", out.ChunkSize)
	}
	if out.ChunkSize < 1 || out.EstimatedMemMB > in.MemoryBudgetMB || out.MemoryEstimateOverBudget {
		t.Fatalf("non-binding hard-limit output is not safely estimated: %+v", out)
	}
}

// TestDefaultOutput_ChunkSizeFromProfile verifies the per-target byte-shaped
// anchor (#166) drives chunk_size, and the 10 MB conservative fallback
// fires for unmeasured targets.
func TestDefaultOutput_ChunkSizeFromProfile(t *testing.T) {
	cases := []struct {
		name        string
		optimumBy   int64
		avgRowBytes int64
		wantRows    int
	}{
		{"PG measured 25MB at 500B avg", 25_000_000, 500, 50_000},
		{"PG measured 25MB at 248B avg (narrow)", 25_000_000, 248, 100_806},
		{"PG measured 25MB at 5000B avg (wide)", 25_000_000, 5000, 5_000},
		{"unmeasured fallback (10MB) at 500B", 0, 500, 20_000},
		{"unmeasured fallback (10MB) at 248B", 0, 248, 40_322},
		{"avg=0 → defaults to 500", 25_000_000, 0, 50_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: tc.optimumBy}
			in := Input{CPUCores: 8, AvgRowBytes: tc.avgRowBytes, RepresentativeRowBytes: tc.avgRowBytes, Platform: "linux"}
			out := DefaultOutput(in, profile)
			if out.ChunkSize != tc.wantRows {
				t.Errorf("ChunkSize: got %d, want %d", out.ChunkSize, tc.wantRows)
			}
		})
	}
}

// TestDefaultOutput_UsesRepresentativeWidth pins the #703 semantic split: the
// legacy capped model feature must not drive byte-target-to-row conversion.
func TestDefaultOutput_UsesRepresentativeWidth(t *testing.T) {
	in := Input{
		CPUCores:               8,
		Platform:               "linux",
		AvgRowBytes:            2_000,
		RepresentativeRowBytes: 8_000,
	}
	profile := DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 16_000_000}

	out := DefaultOutput(in, profile)
	if out.ChunkSize != 2_000 {
		t.Fatalf("ChunkSize = %d, want 2000 from representative width; legacy AvgRowBytes would produce 8000", out.ChunkSize)
	}
}

func TestDefaultOutput_PathologicalCPUCoresDoNotOverflow(t *testing.T) {
	minInt := -int(^uint(0)>>1) - 1
	maxInt := int(^uint(0) >> 1)
	low := DefaultOutput(Input{CPUCores: minInt, RepresentativeRowBytes: 500}, DriverProfile{BaselineWAW: 1})
	if low.Workers != 2 || low.MaxSourceConnections != 8 || low.MaxTargetConnections != 6 {
		t.Fatalf("CPUCores=MinInt produced invalid minimum baseline: %+v", low)
	}
	high := DefaultOutput(Input{CPUCores: maxInt, RepresentativeRowBytes: 500}, DriverProfile{BaselineWAW: 1})
	if high.Workers != maxInt-2 || high.MaxSourceConnections != maxInt || high.MaxTargetConnections != maxInt {
		t.Fatalf("CPUCores=MaxInt produced overflowed baseline: %+v", high)
	}
}

// TestDefaultOutput_PlatformWAW locks in the platform-aware WAW cap: virtualized
// network transports (WSL2, Docker Desktop on macOS / Windows) cap at 1;
// native Linux uses the driver's declared baseline.
func TestDefaultOutput_PlatformWAW(t *testing.T) {
	cases := []struct {
		platform    string
		baselineWAW int
		wantWAW     int
	}{
		{"linux", 2, 2},
		{"linux", 4, 4},
		{"linux", 100, 100},
		{"wsl2", 2, 1},
		{"darwin", 2, 1},
		{"windows", 2, 1},
		{"linux", 0, 1}, // floor at 1 even on linux
		{"darwin", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.platform, func(t *testing.T) {
			in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: tc.platform}
			profile := DriverProfile{Name: "test", BaselineWAW: tc.baselineWAW, OptimumBulkChunkBytes: 25_000_000}
			out := DefaultOutput(in, profile)
			if out.WriteAheadWriters != tc.wantWAW {
				t.Errorf("platform=%s baselineWAW=%d: WriteAheadWriters got %d, want %d",
					tc.platform, tc.baselineWAW, out.WriteAheadWriters, tc.wantWAW)
			}
			wantSource, wantTarget := ConnectionPoolSizes(out.Workers, out.ParallelReaders, out.WriteAheadWriters)
			if out.MaxSourceConnections != wantSource || out.MaxTargetConnections != wantTarget {
				t.Errorf("platform=%s baselineWAW=%d: pools got (%d,%d), want (%d,%d)",
					tc.platform, tc.baselineWAW,
					out.MaxSourceConnections, out.MaxTargetConnections, wantSource, wantTarget)
			}
		})
	}
}

// TestDefaultOutput_ScalesWAWWithCores locks in the driver scaling policy.
// behavior: when ScaleWritersWithCores is set on the driver (PG, MySQL),
// the baseline WAW is max(declared, cores/4) so the tuner doesn't silently
// regress the cores-scaled value config would otherwise have set when no
// history is available. MSSQL leaves the flag false and gets the declared
// baseline. Platform cap fires last.
func TestDefaultOutput_ScalesWAWWithCores(t *testing.T) {
	cases := []struct {
		name        string
		platform    string
		cores       int
		scale       bool
		baselineWAW int
		wantWAW     int
	}{
		{"PG-style scaled, large host", "linux", 16, true, 2, 4}, // cores/4=4 > 2
		{"PG-style scaled, small host", "linux", 4, true, 2, 2},  // cores/4=1 < 2 → baseline wins
		{"PG-style scaled, very large", "linux", 32, true, 2, 8}, // cores/4=8 > 2
		{"PG-style scaled, pre-epic uncapped", "linux", 128, true, 2, 32},
		{"MSSQL-style not scaled", "linux", 32, false, 2, 2}, // scaling off → declared baseline
		{"PG-style scaled, virtualized capped", "wsl2", 32, true, 2, 1},
		{"PG-style scaled, darwin capped", "darwin", 32, true, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Input{CPUCores: tc.cores, AvgRowBytes: 500, Platform: tc.platform}
			profile := DriverProfile{
				Name:                  "test",
				BaselineWAW:           tc.baselineWAW,
				ScaleWritersWithCores: tc.scale,
				OptimumBulkChunkBytes: 25_000_000,
			}
			out := DefaultOutput(in, profile)
			if out.WriteAheadWriters != tc.wantWAW {
				t.Errorf("WriteAheadWriters: got %d, want %d", out.WriteAheadWriters, tc.wantWAW)
			}
		})
	}
}

// TestDefaultOutput_FixedKnobs locks the constants the deterministic baseline
// hard-codes (read_ahead_buffers, parallel_readers, etc.) so future drift
// from the documented defaults shows up as a test diff.
func TestDefaultOutput_FixedKnobs(t *testing.T) {
	in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: "linux"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := DefaultOutput(in, profile)

	if out.ReadAheadBuffers != 4 {
		t.Errorf("ReadAheadBuffers: got %d, want 4", out.ReadAheadBuffers)
	}
	if out.ParallelReaders != 2 {
		t.Errorf("ParallelReaders: got %d, want 2", out.ParallelReaders)
	}
	if out.LargeTableThreshold != 1_000_000 {
		t.Errorf("LargeTableThreshold: got %d, want 1_000_000", out.LargeTableThreshold)
	}
	if out.UpsertMergeChunkSize != 5000 {
		t.Errorf("UpsertMergeChunkSize: got %d, want 5000", out.UpsertMergeChunkSize)
	}
	if out.CheckpointFrequency != 20 {
		t.Errorf("CheckpointFrequency: got %d, want 20", out.CheckpointFrequency)
	}
	if out.MaxRetries != 3 {
		t.Errorf("MaxRetries: got %d, want 3", out.MaxRetries)
	}
}

func TestDefaultOutput_GlobalMemoryClampUsesRepresentativeWidth(t *testing.T) {
	profile := DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	base := Input{
		CPUCores:               8,
		Platform:               "linux",
		MemoryBudgetMB:         16,
		RepresentativeRowBytes: 500,
		SafetyRowBytes:         500,
		SafetyRowBytesKnown:    true,
	}

	narrow := DefaultOutput(base, profile)
	if narrow.ChunkSize != 932 {
		t.Fatalf("500-byte representative ChunkSize = %d, want 932", narrow.ChunkSize)
	}
	if narrow.EstimatedMemMB > base.MemoryBudgetMB || narrow.MemoryEstimateOverBudget {
		t.Fatalf("narrow default not clamped to budget: %+v", narrow)
	}

	wideInput := base
	wideInput.SafetyRowBytes = 4_000
	wide := DefaultOutput(wideInput, profile)
	if wide.ChunkSize != narrow.ChunkSize {
		t.Fatalf("widest-table width changed global ChunkSize: narrow=%d wide=%d", narrow.ChunkSize, wide.ChunkSize)
	}
	if wide.EstimatedMemMB > wideInput.MemoryBudgetMB || wide.MemoryEstimateOverBudget {
		t.Fatalf("wide default not clamped to budget: %+v", wide)
	}
}

func TestDefaultOutput_UnknownSafetyWidthUsesFallback(t *testing.T) {
	in := Input{
		CPUCores:               8,
		Platform:               "linux",
		MemoryBudgetMB:         16,
		RepresentativeRowBytes: 500,
	}
	out := DefaultOutput(in, DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000})
	if out.ChunkSize != 932 {
		t.Fatalf("fallback-width ChunkSize = %d, want 932", out.ChunkSize)
	}
	if !strings.Contains(out.Reasoning, "representative width 500 B") {
		t.Fatalf("Reasoning does not preserve fallback-width provenance: %q", out.Reasoning)
	}
}

func TestDefaultOutput_OneRowMinimumCanRemainOverBudget(t *testing.T) {
	in := Input{
		CPUCores:               128,
		Platform:               "linux",
		MemoryBudgetMB:         1,
		RepresentativeRowBytes: 50_000_000,
		SafetyRowBytes:         1_000_000,
		SafetyRowBytesKnown:    true,
	}
	out := DefaultOutputWithOverrides(in, DriverProfile{BaselineWAW: 2}, DefaultOverrides{
		Workers:           12,
		ReadAheadBuffers:  16,
		WriteAheadWriters: 8,
	})
	if out.ChunkSize != 1 {
		t.Fatalf("ChunkSize = %d, want one-row minimum", out.ChunkSize)
	}
	if !out.MemoryEstimateOverBudget || out.EstimatedMemMB <= in.MemoryBudgetMB {
		t.Fatalf("one-row over-budget truth lost: %+v", out)
	}
	if !strings.Contains(out.Reasoning, "1-row minimum-progress fallback still exceeds budget") {
		t.Fatalf("Reasoning does not report unsafe one-row minimum: %q", out.Reasoning)
	}
}

func TestDefaultOutputWithOverrides_ClampsForEffectiveConcurrency(t *testing.T) {
	in := Input{
		CPUCores:               8,
		Platform:               "linux",
		MemoryBudgetMB:         64,
		RepresentativeRowBytes: 500,
		SafetyRowBytes:         500,
		SafetyRowBytesKnown:    true,
	}
	overrides := DefaultOverrides{
		Workers:           12,
		ReadAheadBuffers:  8,
		WriteAheadWriters: 12,
		ParallelReaders:   9,
	}
	out := DefaultOutputWithOverrides(in, DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}, overrides)

	if out.Workers != 12 || out.MaxPartitions != 12 || out.ReadAheadBuffers != 8 || out.WriteAheadWriters != 12 || out.ParallelReaders != 9 {
		t.Fatalf("effective overrides not applied: %+v", out)
	}
	if out.ChunkSize != 559 {
		t.Fatalf("ChunkSize = %d, want 559 for effective override tuple", out.ChunkSize)
	}
	if out.EstimatedMemMB > in.MemoryBudgetMB || out.MemoryEstimateOverBudget {
		t.Fatalf("override tuple not clamped to budget: %+v", out)
	}
	if out.MaxSourceConnections != 112 || out.MaxTargetConnections != 148 {
		t.Fatalf("override pools = (%d,%d), want (112,148)", out.MaxSourceConnections, out.MaxTargetConnections)
	}
}

func TestDefaultOutputWithOverrides_IgnoresNonPositiveValues(t *testing.T) {
	in := Input{CPUCores: 8, Platform: "linux", RepresentativeRowBytes: 500, SafetyRowBytes: 500, SafetyRowBytesKnown: true}
	profile := DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	want := DefaultOutput(in, profile)
	got := DefaultOutputWithOverrides(in, profile, DefaultOverrides{
		Workers:           -1,
		ReadAheadBuffers:  0,
		WriteAheadWriters: -2,
		ParallelReaders:   0,
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-positive overrides changed default:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestDefaultOutputMatchesTuneWithoutHistory(t *testing.T) {
	in := Input{
		CPUCores:               64,
		MemoryGB:               32,
		MemoryBudgetMB:         512,
		Platform:               "linux",
		SourceDBType:           "mssql",
		TargetDBType:           "postgres",
		TargetMode:             "drop_recreate",
		RepresentativeRowBytes: 800,
		SafetyRowBytes:         1_200,
		SafetyRowBytesKnown:    true,
	}
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           2,
		ScaleWritersWithCores: true,
		OptimumBulkChunkBytes: 25_000_000,
	}
	got := DefaultOutput(in, profile)
	want := Tune(in, profile, nil, DBTuning{})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultOutput diverged from Tune(nil):\n got: %+v\nwant: %+v", got, want)
	}
}
