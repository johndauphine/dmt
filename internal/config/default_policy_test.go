package config

import (
	"strconv"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/systemmemory"
	"github.com/johndauphine/dmt/internal/tuning"
)

func newDefaultPolicyTestConfig(target string, cores int, platform string, snapshot systemmemory.Snapshot) *Config {
	return &Config{
		Source: SourceConfig{Type: "postgres"},
		Target: TargetConfig{Type: target},
		Migration: MigrationConfig{
			TargetMode: "upsert",
		},
		autoConfig: AutoConfig{
			CPUCores: cores,
			Platform: platform,
		},
		memoryReader: &fakeMemoryReader{snapshot: snapshot},
	}
}

func largePolicyTestSnapshot() systemmemory.Snapshot {
	return systemmemory.Snapshot{
		CapacityMB:  65_536,
		AvailableMB: 65_536,
		Source:      "host",
	}
}

func assertConfigPolicyMatchesOutput(t *testing.T, cfg *Config, out tuning.Output) {
	t.Helper()

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"workers", int64(cfg.Migration.Workers), int64(out.Workers)},
		{"chunk_size", int64(cfg.Migration.ChunkSize), int64(out.ChunkSize)},
		{"read_ahead_buffers", int64(cfg.Migration.ReadAheadBuffers), int64(out.ReadAheadBuffers)},
		{"write_ahead_writers", int64(cfg.Migration.WriteAheadWriters), int64(out.WriteAheadWriters)},
		{"parallel_readers", int64(cfg.Migration.ParallelReaders), int64(out.ParallelReaders)},
		{"max_partitions", int64(cfg.Migration.MaxPartitions), int64(out.MaxPartitions)},
		{"large_table_threshold", cfg.Migration.LargeTableThreshold, out.LargeTableThreshold},
		{"upsert_merge_chunk_size", int64(cfg.Migration.UpsertMergeChunkSize), int64(out.UpsertMergeChunkSize)},
		{"checkpoint_frequency", int64(cfg.Migration.CheckpointFrequency), int64(out.CheckpointFrequency)},
		{"max_retries", int64(cfg.Migration.MaxRetries), int64(out.MaxRetries)},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %d, want DefaultOutput value %d", check.name, check.got, check.want)
		}
	}

	wantSource, wantTarget := tuning.ConnectionPoolSizes(
		cfg.Migration.Workers,
		cfg.Migration.ParallelReaders,
		cfg.Migration.WriteAheadWriters,
	)
	if cfg.Migration.MaxSourceConnections != wantSource || cfg.Migration.MaxTargetConnections != wantTarget {
		t.Errorf("connection pools = %d/%d, want effective-tuple formula %d/%d",
			cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections, wantSource, wantTarget)
	}
	if cfg.Source.ChunkSize != cfg.Migration.ChunkSize || cfg.Target.ChunkSize != cfg.Migration.ChunkSize {
		t.Errorf("endpoint chunks = %d/%d, want migration chunk %d",
			cfg.Source.ChunkSize, cfg.Target.ChunkSize, cfg.Migration.ChunkSize)
	}
}

func TestApplyDefaultsMatchesCanonicalDefaultOutput(t *testing.T) {
	withEmptySecretsFile(t)

	tests := []struct {
		name           string
		target         string
		cores          int
		platform       string
		wantWorkers    int
		wantChunk      int
		wantWAW        int
		wantSourcePool int
		wantTargetPool int
		wantEstimate   int64
	}{
		{"postgres linux", "postgres", 8, "linux", 6, 50_000, 2, 16, 16, 859},
		{"mssql linux", "mssql", 8, "linux", 6, 3_000, 2, 16, 16, 52},
		{"sqlite linux", "sqlite", 8, "linux", 6, 20_000, 1, 16, 10, 287},
		{"postgres darwin", "postgres", 32, "darwin", 12, 50_000, 1, 28, 16, 1431},
		{"unknown fallback", "not-registered", 8, "linux", 6, 20_000, 2, 16, 16, 344},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newDefaultPolicyTestConfig(tc.target, tc.cores, tc.platform, largePolicyTestSnapshot())
			if err := cfg.applyDefaults(); err != nil {
				t.Fatalf("applyDefaults: %v", err)
			}

			in := cfg.defaultTuningInput()
			profile := driver.BuildTuningProfile(tc.target, in.SafetyRowBytes, driver.TargetProbe{})
			out := tuning.DefaultOutput(in, profile)
			assertConfigPolicyMatchesOutput(t, cfg, out)

			if cfg.Migration.Workers != tc.wantWorkers || cfg.Migration.ChunkSize != tc.wantChunk ||
				cfg.Migration.ReadAheadBuffers != 4 || cfg.Migration.WriteAheadWriters != tc.wantWAW ||
				cfg.Migration.ParallelReaders != 2 || cfg.Migration.MaxPartitions != tc.wantWorkers {
				t.Errorf("exact defaults = W%d CS%d RAB%d WAW%d PR%d MP%d, want W%d CS%d RAB4 WAW%d PR2 MP%d",
					cfg.Migration.Workers, cfg.Migration.ChunkSize, cfg.Migration.ReadAheadBuffers,
					cfg.Migration.WriteAheadWriters, cfg.Migration.ParallelReaders, cfg.Migration.MaxPartitions,
					tc.wantWorkers, tc.wantChunk, tc.wantWAW, tc.wantWorkers)
			}
			if cfg.Migration.LargeTableThreshold != 1_000_000 || cfg.Migration.UpsertMergeChunkSize != 5_000 ||
				cfg.Migration.CheckpointFrequency != 20 || cfg.Migration.MaxRetries != 3 {
				t.Errorf("fixed policy fields = threshold:%d upsert:%d checkpoint:%d retries:%d",
					cfg.Migration.LargeTableThreshold, cfg.Migration.UpsertMergeChunkSize,
					cfg.Migration.CheckpointFrequency, cfg.Migration.MaxRetries)
			}
			if cfg.Migration.MaxSourceConnections != tc.wantSourcePool || cfg.Migration.MaxTargetConnections != tc.wantTargetPool {
				t.Errorf("exact pools = %d/%d, want %d/%d",
					cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections,
					tc.wantSourcePool, tc.wantTargetPool)
			}
			if got := tuning.EstimatedMemMB(
				cfg.Migration.Workers,
				cfg.Migration.ReadAheadBuffers,
				cfg.Migration.WriteAheadWriters,
				cfg.Migration.ChunkSize,
				loadTimeFallbackRowBytes,
			); got != tc.wantEstimate {
				t.Errorf("estimated memory = %d MB, want %d MB", got, tc.wantEstimate)
			}
		})
	}
}

func TestApplyDefaultsWorkerAndHighCoreBoundaries(t *testing.T) {
	withEmptySecretsFile(t)

	tests := []struct {
		cores          int
		wantWorkers    int
		wantWAW        int
		wantSourcePool int
		wantTargetPool int
	}{
		{1, 4, 2, 12, 12},
		{2, 4, 2, 12, 12},
		{4, 4, 2, 12, 12},
		{6, 4, 2, 12, 12},
		{8, 6, 2, 16, 16},
		{32, 12, 8, 28, 100},
		{64, 12, 8, 28, 100},
		{128, 12, 8, 28, 100},
	}

	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.cores), func(t *testing.T) {
			cfg := newDefaultPolicyTestConfig("postgres", tc.cores, "linux", largePolicyTestSnapshot())
			if err := cfg.applyDefaults(); err != nil {
				t.Fatalf("applyDefaults: %v", err)
			}
			if cfg.Migration.Workers != tc.wantWorkers || cfg.Migration.WriteAheadWriters != tc.wantWAW {
				t.Errorf("cores=%d defaults W/WAW=%d/%d, want %d/%d",
					tc.cores, cfg.Migration.Workers, cfg.Migration.WriteAheadWriters, tc.wantWorkers, tc.wantWAW)
			}
			if cfg.Migration.MaxSourceConnections != tc.wantSourcePool || cfg.Migration.MaxTargetConnections != tc.wantTargetPool {
				t.Errorf("cores=%d pools=%d/%d, want %d/%d", tc.cores,
					cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections,
					tc.wantSourcePool, tc.wantTargetPool)
			}
		})
	}
}

func TestApplyDefaultsVirtualizedPlatformsKeepSingleWriterBaseline(t *testing.T) {
	withEmptySecretsFile(t)

	for _, platform := range []string{"darwin", "windows", "wsl2"} {
		t.Run(platform, func(t *testing.T) {
			cfg := newDefaultPolicyTestConfig("postgres", 128, platform, largePolicyTestSnapshot())
			if err := cfg.applyDefaults(); err != nil {
				t.Fatalf("applyDefaults: %v", err)
			}
			if cfg.Migration.Workers != 12 || cfg.Migration.WriteAheadWriters != 1 {
				t.Fatalf("%s defaults W/WAW=%d/%d, want 12/1",
					platform, cfg.Migration.Workers, cfg.Migration.WriteAheadWriters)
			}
			if cfg.Migration.MaxSourceConnections != 28 || cfg.Migration.MaxTargetConnections != 16 {
				t.Fatalf("%s pools=%d/%d, want 28/16", platform,
					cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections)
			}
		})
	}
}

func TestApplyDefaultsUpsertMergeChunkUsesCanonicalPolicyInBothModes(t *testing.T) {
	withEmptySecretsFile(t)

	for _, mode := range []string{"drop_recreate", "upsert"} {
		t.Run(mode, func(t *testing.T) {
			cfg := newDefaultPolicyTestConfig("postgres", 8, "linux", largePolicyTestSnapshot())
			cfg.Migration.TargetMode = mode
			if err := cfg.applyDefaults(); err != nil {
				t.Fatalf("applyDefaults: %v", err)
			}
			if cfg.Migration.UpsertMergeChunkSize != 5_000 {
				t.Fatalf("%s upsert_merge_chunk_size = %d, want canonical 5000",
					mode, cfg.Migration.UpsertMergeChunkSize)
			}
		})
	}
}

func TestDefaultTuningInputUsesResolvedSnapshotAndUnobservedWidth(t *testing.T) {
	cfg := &Config{
		Source:    SourceConfig{Type: "mssql"},
		Target:    TargetConfig{Type: "postgres"},
		Migration: MigrationConfig{TargetMode: "upsert"},
		autoConfig: AutoConfig{
			CPUCores: 17,
			Platform: "wsl2",
			MemoryEnvelope: MemoryEnvelope{
				CapacityMB:  1537,
				AvailableMB: 512,
				BudgetMB:    358,
				Source:      "cgroup-v2",
			},
		},
	}

	in := cfg.defaultTuningInput()
	if in.CPUCores != 17 || in.Platform != "wsl2" || in.MemoryGB != 2 || in.MemoryBudgetMB != 358 {
		t.Fatalf("resource input = cores:%d platform:%q memoryGB:%d budget:%d, want 17/wsl2/2/358",
			in.CPUCores, in.Platform, in.MemoryGB, in.MemoryBudgetMB)
	}
	if in.SourceDBType != "mssql" || in.TargetDBType != "postgres" || in.TargetMode != "upsert" {
		t.Fatalf("database input = %s/%s/%s", in.SourceDBType, in.TargetDBType, in.TargetMode)
	}
	if in.AvgRowBytes != 500 || in.RepresentativeRowBytes != 500 || in.SafetyRowBytes != 500 ||
		in.UncappedAvgRowBytes != 500 || in.SafetyRowBytesKnown {
		t.Fatalf("load-time widths = legacy:%d representative:%d safety:%d uncapped:%d known:%v",
			in.AvgRowBytes, in.RepresentativeRowBytes, in.SafetyRowBytes,
			in.UncappedAvgRowBytes, in.SafetyRowBytesKnown)
	}
}

func TestApplyDefaultsMemoryClampUsesResolvedEnvelope(t *testing.T) {
	withEmptySecretsFile(t)

	tests := []struct {
		name       string
		snapshot   systemmemory.Snapshot
		maxMemory  int64
		wantBudget int64
		wantChunk  int
	}{
		{
			name: "small container",
			snapshot: systemmemory.Snapshot{
				CapacityMB: 512, AvailableMB: 512, Source: "cgroup-v2",
			},
			wantBudget: 358,
			wantChunk:  5213,
		},
		{
			name: "explicit cap",
			snapshot: systemmemory.Snapshot{
				CapacityMB: 8192, AvailableMB: 8192, Source: "host",
			},
			maxMemory:  64,
			wantBudget: 64,
			wantChunk:  932,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeMemoryReader{snapshot: tc.snapshot}
			cfg := newDefaultPolicyTestConfig("postgres", 64, "linux", tc.snapshot)
			cfg.memoryReader = reader
			cfg.Migration.MaxMemoryMB = tc.maxMemory
			if err := cfg.applyDefaults(); err != nil {
				t.Fatalf("applyDefaults: %v", err)
			}
			if reader.reads != 1 {
				t.Fatalf("memory snapshot reads = %d, want 1", reader.reads)
			}
			if cfg.autoConfig.MemoryEnvelope.BudgetMB != tc.wantBudget || cfg.Migration.ChunkSize != tc.wantChunk {
				t.Fatalf("budget/chunk = %d/%d, want %d/%d",
					cfg.autoConfig.MemoryEnvelope.BudgetMB, cfg.Migration.ChunkSize, tc.wantBudget, tc.wantChunk)
			}
			estimated := tuning.EstimatedMemMB(
				cfg.Migration.Workers,
				cfg.Migration.ReadAheadBuffers,
				cfg.Migration.WriteAheadWriters,
				cfg.Migration.ChunkSize,
				loadTimeFallbackRowBytes,
			)
			if estimated > tc.wantBudget || cfg.Migration.ChunkSize < 1 {
				t.Fatalf("final estimate/chunk = %dMB/%d, budget=%d", estimated, cfg.Migration.ChunkSize, tc.wantBudget)
			}
			if !strings.Contains(cfg.autoConfig.DefaultPolicyReasoning, "memory clamp") ||
				!strings.Contains(cfg.autoConfig.DefaultPolicyReasoning, "unobserved fallback") {
				t.Errorf("policy reasoning does not record fallback clamp: %q", cfg.autoConfig.DefaultPolicyReasoning)
			}
		})
	}
}

func TestApplyDefaultsPreservesAllUserPins(t *testing.T) {
	withEmptySecretsFile(t)

	cfg := newDefaultPolicyTestConfig("postgres", 128, "darwin", largePolicyTestSnapshot())
	cfg.Source.ChunkSize = 333
	cfg.Target.ChunkSize = 444
	cfg.Migration.Workers = 5
	cfg.Migration.ChunkSize = 1234
	cfg.Migration.ReadAheadBuffers = 7
	cfg.Migration.WriteAheadWriters = 6
	cfg.Migration.ParallelReaders = 3
	cfg.Migration.MaxPartitions = 9
	cfg.Migration.LargeTableThreshold = 777
	cfg.Migration.UpsertMergeChunkSize = 888
	cfg.Migration.CheckpointFrequency = 11
	cfg.Migration.MaxRetries = 4
	cfg.Migration.MaxSourceConnections = 31
	cfg.Migration.MaxTargetConnections = 32

	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}

	checks := []struct {
		name       string
		got, want  int64
		provenance string
	}{
		{"workers", int64(cfg.Migration.Workers), 5, provenanceMigrationWorkers},
		{"chunk", int64(cfg.Migration.ChunkSize), 1234, provenanceMigrationChunkSize},
		{"RAB", int64(cfg.Migration.ReadAheadBuffers), 7, provenanceMigrationReadAheadBuffers},
		{"WAW", int64(cfg.Migration.WriteAheadWriters), 6, provenanceMigrationWriteAheadWriters},
		{"PR", int64(cfg.Migration.ParallelReaders), 3, provenanceMigrationParallelReaders},
		{"partitions", int64(cfg.Migration.MaxPartitions), 9, provenanceMigrationMaxPartitions},
		{"threshold", cfg.Migration.LargeTableThreshold, 777, provenanceMigrationLargeTableThreshold},
		{"upsert", int64(cfg.Migration.UpsertMergeChunkSize), 888, provenanceMigrationUpsertMergeChunkSize},
		{"checkpoint", int64(cfg.Migration.CheckpointFrequency), 11, provenanceMigrationCheckpointFrequency},
		{"retries", int64(cfg.Migration.MaxRetries), 4, provenanceMigrationMaxRetries},
		{"source pool", int64(cfg.Migration.MaxSourceConnections), 31, provenanceMigrationMaxSourceConns},
		{"target pool", int64(cfg.Migration.MaxTargetConnections), 32, provenanceMigrationMaxTargetConns},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("pinned %s = %d, want %d", check.name, check.got, check.want)
		}
		if got := cfg.tunableProvenance(check.provenance); got != ProvenanceUserConfig {
			t.Errorf("%s provenance = %q, want user config", check.name, got)
		}
	}
	if cfg.Source.ChunkSize != 333 || cfg.Target.ChunkSize != 444 {
		t.Errorf("pinned endpoint chunks = %d/%d, want 333/444", cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
	if !cfg.autoConfig.DefaultPolicyChunkPinned {
		t.Error("explicit chunk was not recorded as pinned for diagnostics")
	}
}

func TestApplyDefaultsPreservesSecretsPins(t *testing.T) {
	withSecretsFile(t, `
migration_defaults:
  workers: 5
  read_ahead_buffers: 7
  write_ahead_writers: 6
  parallel_readers: 3
  max_source_connections: 31
  max_target_connections: 32
  checkpoint_frequency: 11
  max_retries: 4
`)

	cfg := newDefaultPolicyTestConfig("postgres", 64, "linux", largePolicyTestSnapshot())
	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}

	checks := []struct {
		name       string
		got, want  int
		provenance string
	}{
		{"workers", cfg.Migration.Workers, 5, provenanceMigrationWorkers},
		{"RAB", cfg.Migration.ReadAheadBuffers, 7, provenanceMigrationReadAheadBuffers},
		{"WAW", cfg.Migration.WriteAheadWriters, 6, provenanceMigrationWriteAheadWriters},
		{"PR", cfg.Migration.ParallelReaders, 3, provenanceMigrationParallelReaders},
		{"source pool", cfg.Migration.MaxSourceConnections, 31, provenanceMigrationMaxSourceConns},
		{"target pool", cfg.Migration.MaxTargetConnections, 32, provenanceMigrationMaxTargetConns},
		{"checkpoint", cfg.Migration.CheckpointFrequency, 11, provenanceMigrationCheckpointFrequency},
		{"retries", cfg.Migration.MaxRetries, 4, provenanceMigrationMaxRetries},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("secrets-pinned %s = %d, want %d", check.name, check.got, check.want)
		}
		if got := cfg.tunableProvenance(check.provenance); got != ProvenanceSecretsDefault {
			t.Errorf("%s provenance = %q, want secrets default", check.name, got)
		}
	}
	if cfg.Migration.ChunkSize != 50_000 || cfg.Migration.MaxPartitions != 5 ||
		cfg.Migration.LargeTableThreshold != 1_000_000 || cfg.Migration.UpsertMergeChunkSize != 5_000 {
		t.Errorf("generated fields beside secrets pins = chunk:%d partitions:%d threshold:%d upsert:%d",
			cfg.Migration.ChunkSize, cfg.Migration.MaxPartitions,
			cfg.Migration.LargeTableThreshold, cfg.Migration.UpsertMergeChunkSize)
	}
}

func TestGeneratedSecretsTemplateLeavesCanonicalDefaultsUnpinned(t *testing.T) {
	withSecretsFile(t, secrets.GenerateTemplate())

	cfg, err := LoadBytes(minConfigYAML("  target_mode: drop_recreate\n"))
	if err != nil {
		t.Fatalf("LoadBytes with generated secrets template: %v", err)
	}
	checks := []struct {
		name       string
		got, want  int
		provenance string
	}{
		{name: "read_ahead_buffers", got: cfg.Migration.ReadAheadBuffers, want: 4, provenance: provenanceMigrationReadAheadBuffers},
		{name: "checkpoint_frequency", got: cfg.Migration.CheckpointFrequency, want: 20, provenance: provenanceMigrationCheckpointFrequency},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %d, want canonical generated default %d", check.name, check.got, check.want)
		}
		if got := cfg.tunableProvenance(check.provenance); got != ProvenanceAutoDefault {
			t.Errorf("%s provenance = %q, want %q; generated secrets template must not pin it", check.name, got, ProvenanceAutoDefault)
		}
	}
}

func TestApplyDefaultsAutoChunkUsesPinnedConcurrency(t *testing.T) {
	withEmptySecretsFile(t)

	cfg := newDefaultPolicyTestConfig("postgres", 8, "linux", systemmemory.Snapshot{
		CapacityMB: 8192, AvailableMB: 8192, Source: "host",
	})
	cfg.Migration.MaxMemoryMB = 64
	cfg.Migration.Workers = 12
	cfg.Migration.ReadAheadBuffers = 4
	cfg.Migration.WriteAheadWriters = 8
	cfg.Migration.ParallelReaders = 2

	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}
	if cfg.Migration.ChunkSize != 932 {
		t.Fatalf("auto chunk = %d, want 932 clamped for pinned W12/RAB4/WAW8", cfg.Migration.ChunkSize)
	}
	if cfg.Migration.MaxPartitions != 12 {
		t.Errorf("auto max_partitions = %d, want pinned workers 12", cfg.Migration.MaxPartitions)
	}
	if cfg.Migration.MaxSourceConnections != 28 || cfg.Migration.MaxTargetConnections != 100 {
		t.Errorf("effective-tuple pools = %d/%d, want 28/100",
			cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections)
	}
	if got := tuning.EstimatedMemMB(12, 4, 8, cfg.Migration.ChunkSize, loadTimeFallbackRowBytes); got != 64 {
		t.Errorf("final estimate = %d MB, want 64 MB", got)
	}
	if cfg.autoConfig.DefaultPolicyChunkPinned {
		t.Error("generated chunk was recorded as pinned")
	}
}

func TestDebugDumpReportsCanonicalClampAndPinnedOverBudgetTruth(t *testing.T) {
	withEmptySecretsFile(t)

	newConfig := func(chunk int) *Config {
		cfg := newDefaultPolicyTestConfig("postgres", 8, "linux", systemmemory.Snapshot{
			CapacityMB: 8192, AvailableMB: 8192, Source: "host",
		})
		cfg.Migration.MaxMemoryMB = 64
		cfg.Migration.Workers = 12
		cfg.Migration.ReadAheadBuffers = 4
		cfg.Migration.WriteAheadWriters = 8
		cfg.Migration.ParallelReaders = 2
		cfg.Migration.ChunkSize = chunk
		if err := cfg.applyDefaults(); err != nil {
			t.Fatalf("applyDefaults: %v", err)
		}
		return cfg
	}

	auto := newConfig(0)
	autoDump := auto.DebugDump()
	for _, want := range []string{
		"Platform: linux",
		"ChunkSize: 932 (auto: canonical no-history tuning policy; source: auto default)",
		"Modeled Working Set: ~64 MB",
		"Budget Status: within 64 MB budget",
		"Load-time Policy Reasoning:",
		"memory clamp",
		"unobserved fallback",
	} {
		if !strings.Contains(autoDump, want) {
			t.Errorf("automatic DebugDump missing %q:\n%s", want, autoDump)
		}
	}

	pinned := newConfig(50_000)
	if pinned.Migration.ChunkSize != 50_000 {
		t.Fatalf("explicit chunk changed to %d, want 50000", pinned.Migration.ChunkSize)
	}
	pinnedDump := pinned.DebugDump()
	for _, want := range []string{
		"ChunkSize: 50000 (source: config)",
		"Modeled Working Set: ~3434 MB",
		"Budget Status: exceeds 64 MB budget; explicit chunk_size preserved",
		"Width Source: unobserved 500-byte fallback estimate",
	} {
		if !strings.Contains(pinnedDump, want) {
			t.Errorf("pinned DebugDump missing %q:\n%s", want, pinnedDump)
		}
	}

	for _, stale := range []string{
		"75K",
		"default 5M",
		"default 10;",
		"(cores-2)",
		"cores/4 clamped",
		"envelope-budget-scaled",
	} {
		if strings.Contains(autoDump, stale) || strings.Contains(pinnedDump, stale) {
			t.Errorf("DebugDump retained stale formula %q", stale)
		}
	}
}
