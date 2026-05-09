package tuning

import "testing"

// TestBaseline_KnobsByCPU locks in the cpu-cores → workers mapping and
// the dependent connection-pool / max-partitions formulas.
func TestBaseline_KnobsByCPU(t *testing.T) {
	cases := []struct {
		name             string
		cpuCores         int
		wantWorkers      int
		wantMaxPartition int
		wantSrcConns     int
		wantTgtConns     int
	}{
		{"low — floor at 2", 1, 2, 2, 6, 8},
		{"low — exact floor", 4, 2, 2, 6, 8},
		{"medium", 8, 6, 6, 10, 16},
		{"high", 18, 16, 16, 20, 36},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Input{CPUCores: tc.cpuCores, AvgRowBytes: 500, Platform: "linux"}
			profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
			out := baseline(in, profile)
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

// TestBaseline_ChunkSizeFromProfile verifies the per-target byte-shaped
// anchor (#166) drives chunk_size, and the 10 MB conservative fallback
// fires for unmeasured targets.
func TestBaseline_ChunkSizeFromProfile(t *testing.T) {
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
			in := Input{CPUCores: 8, AvgRowBytes: tc.avgRowBytes, Platform: "linux"}
			out := baseline(in, profile)
			if out.ChunkSize != tc.wantRows {
				t.Errorf("ChunkSize: got %d, want %d", out.ChunkSize, tc.wantRows)
			}
		})
	}
}

// TestBaseline_PlatformWAW locks in the platform-aware WAW cap: virtualized
// network transports (WSL2, Docker Desktop on macOS / Windows) cap at 1;
// native Linux uses the driver's declared baseline.
func TestBaseline_PlatformWAW(t *testing.T) {
	cases := []struct {
		platform    string
		baselineWAW int
		wantWAW     int
	}{
		{"linux", 2, 2},
		{"linux", 4, 4},
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
			out := baseline(in, profile)
			if out.WriteAheadWriters != tc.wantWAW {
				t.Errorf("platform=%s baselineWAW=%d: WriteAheadWriters got %d, want %d",
					tc.platform, tc.baselineWAW, out.WriteAheadWriters, tc.wantWAW)
			}
		})
	}
}

// TestBaseline_ScalesWAWWithCores locks in the parity-with-config-applyDefaults
// behavior: when ScaleWritersWithCores is set on the driver (PG, MySQL),
// the baseline WAW is max(declared, cores/4) so the tuner doesn't silently
// regress the cores-scaled value config would otherwise have set when no
// history is available. MSSQL leaves the flag false and gets the declared
// baseline. Platform cap fires last.
func TestBaseline_ScalesWAWWithCores(t *testing.T) {
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
		{"MSSQL-style not scaled", "linux", 32, false, 2, 2},     // scaling off → declared baseline
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
			out := baseline(in, profile)
			if out.WriteAheadWriters != tc.wantWAW {
				t.Errorf("WriteAheadWriters: got %d, want %d", out.WriteAheadWriters, tc.wantWAW)
			}
		})
	}
}

// TestBaseline_FixedKnobs locks the constants the deterministic baseline
// hard-codes (read_ahead_buffers, parallel_readers, etc.) so future drift
// from the documented defaults shows up as a test diff.
func TestBaseline_FixedKnobs(t *testing.T) {
	in := Input{CPUCores: 8, AvgRowBytes: 500, Platform: "linux"}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := baseline(in, profile)

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
