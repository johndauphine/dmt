package orchestrator

import (
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/tuning"
)

// TestGetSystemBasedSuggestions_NonDegenerate is the post-PR-#175 successor
// to TestBuildOfflineAutoTuneInput (issue #157). The old test pinned that
// the AI offline-input had non-zero AvailableMemoryMB / AvgRowBytes. After
// PR #175 the AI prompt is gone; #708 also makes a zero memory envelope mean
// "no clamp input" instead of inventing a fallback. End-to-end check:
// GetSystemBasedSuggestions must still produce a usable parameter set across
// reasonable configurations, including direct callers with no envelope.
func TestGetSystemBasedSuggestions_NonDegenerate(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.Config
	}{
		{
			"typical: PG→PG, no max_memory_mb",
			&config.Config{
				Source: config.SourceConfig{Type: "postgres"},
				Target: config.TargetConfig{Type: "postgres"},
			},
		},
		{
			"unknown target → conservative fallback (no panic)",
			&config.Config{
				Source: config.SourceConfig{Type: "mssql"},
				Target: config.TargetConfig{Type: "unknownDB"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := GetSystemBasedSuggestions(tc.cfg)
			if s == nil {
				t.Fatal("GetSystemBasedSuggestions returned nil")
			}
			if s.Workers < 4 {
				t.Errorf("Workers=%d, want >= 4 (canonical baseline floor)", s.Workers)
			}
			if s.ChunkSizeRecommendation <= 0 {
				t.Errorf("ChunkSizeRecommendation=%d, want > 0 (issue #157 successor)", s.ChunkSizeRecommendation)
			}
			if s.ReadAheadBuffers != 4 {
				t.Errorf("ReadAheadBuffers=%d, want 4 (baseline)", s.ReadAheadBuffers)
			}
			if s.WriteAheadWriters < 1 {
				t.Errorf("WriteAheadWriters=%d, want >= 1 (baseline floor)", s.WriteAheadWriters)
			}
			if s.EstimatedMemMB <= 0 {
				t.Errorf("EstimatedMemMB=%d, want > 0", s.EstimatedMemMB)
			}
			if s.AvgRowSizeBytes != 500 || s.RepresentativeRowBytes != 500 || s.SafetyRowBytes != 500 {
				t.Errorf("offline fallback widths = legacy %d representative %d safety %d, want 500/500/500",
					s.AvgRowSizeBytes, s.RepresentativeRowBytes, s.SafetyRowBytes)
			}
			if s.SafetyRowBytesKnown {
				t.Error("offline 500-byte safety fallback must remain explicitly unobserved")
			}
		})
	}
}

func TestGetSystemBasedSuggestions_PreservesForcedExplore(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{Type: "mssql"},
		Target: config.TargetConfig{Type: "postgres"},
		Migration: config.MigrationConfig{
			Explore:     true,
			ExploreMode: "balanced",
		},
	}

	got := GetSystemBasedSuggestions(cfg)
	if !strings.Contains(got.Reasoning, "exploration: planned grid") {
		t.Fatalf("offline --explore did not request a planned-grid probe; reasoning: %q", got.Reasoning)
	}
}

func TestGetSystemBasedSuggestions_UsesResolvedMemoryEnvelope(t *testing.T) {
	cfg := loadSmallEnvelopeConfig(t)
	budgetMB := cfg.AutoConfig().MemoryEnvelope.BudgetMB
	s := GetSystemBasedSuggestions(cfg)
	if s.EstimatedMemMB > budgetMB {
		t.Fatalf("EstimatedMemMB=%d exceeds resolved budget=%d", s.EstimatedMemMB, budgetMB)
	}
	if s.ChunkSizeRecommendation >= 50_000 {
		t.Errorf("small envelope left offline chunk_size=%d, want a memory clamp below 50000", s.ChunkSizeRecommendation)
	}
}

func TestGetSystemBasedSuggestions_MatchesSharedTargetProfile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		target    string
		wantChunk int
	}{
		{name: "registered postgres target", target: "postgres", wantChunk: 50_000},
		{name: "unknown target fallback", target: "offline-profile-missing", wantChunk: 20_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Source: config.SourceConfig{Type: "mssql"},
				Target: config.TargetConfig{Type: tc.target},
			}
			got := GetSystemBasedSuggestions(cfg)

			in := tuning.Input{
				CPUCores:               runtime.NumCPU(),
				Platform:               driver.DetectPlatform(),
				SourceDBType:           cfg.Source.Type,
				TargetDBType:           tc.target,
				AvgRowBytes:            500,
				UncappedAvgRowBytes:    500,
				RepresentativeRowBytes: 500,
				SafetyRowBytes:         500,
				SafetyRowBytesKnown:    false,
			}
			profile := driver.BuildTuningProfile(tc.target, in.SafetyRowBytes, driver.TargetProbe{})
			want := tuning.DefaultOutput(in, profile)

			if got.ChunkSizeRecommendation != tc.wantChunk {
				t.Fatalf("offline chunk size = %d, want exact target-profile result %d", got.ChunkSizeRecommendation, tc.wantChunk)
			}
			if got.Workers != want.Workers || got.ChunkSizeRecommendation != want.ChunkSize ||
				got.ReadAheadBuffers != want.ReadAheadBuffers || got.WriteAheadWriters != want.WriteAheadWriters ||
				got.ParallelReaders != want.ParallelReaders || got.MaxPartitions != want.MaxPartitions ||
				got.MaxSourceConnections != want.MaxSourceConnections || got.MaxTargetConnections != want.MaxTargetConnections ||
				got.EstimatedMemMB != want.EstimatedMemMB || got.MemoryEstimateOverBudget != want.MemoryEstimateOverBudget {
				t.Fatalf("offline suggestions did not match shared profile/default output:\n got=%+v\nwant=%+v", got, want)
			}
		})
	}
}

func TestDryRunEstimatedMemMBIncludesReadAndWriteBuffers(t *testing.T) {
	cfg := &config.Config{Migration: config.MigrationConfig{
		Workers:           4,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
		ChunkSize:         10_000,
	}}
	got := dryRunEstimatedMemMB(cfg)
	want := tuning.EstimatedMemMB(4, 4, 2, 10_000, 500)
	if got != want {
		t.Fatalf("dry-run estimate = %d MB, want %d MB from RAB+WAW model", got, want)
	}
	withoutWriters := tuning.EstimatedMemMB(4, 4, 0, 10_000, 500)
	if got <= withoutWriters {
		t.Fatalf("dry-run estimate %d MB did not include writers; RAB-only estimate is %d MB", got, withoutWriters)
	}
}

func TestAnalyzeConfig_TargetOnlyUsesEnvelopeAwareSuggestions(t *testing.T) {
	cfg := loadSmallEnvelopeConfig(t)
	orch := &Orchestrator{
		config:     cfg,
		targetPool: &deleteRuntimeTargetPool{},
	}
	s, err := orch.AnalyzeConfig(t.Context(), "")
	if err != nil {
		t.Fatalf("AnalyzeConfig: %v", err)
	}
	budgetMB := cfg.AutoConfig().MemoryEnvelope.BudgetMB
	if s.EstimatedMemMB > budgetMB {
		t.Fatalf("target-only EstimatedMemMB=%d exceeds resolved budget=%d", s.EstimatedMemMB, budgetMB)
	}
	if s.ChunkSizeRecommendation >= 50_000 {
		t.Errorf("target-only chunk_size=%d, want envelope-aware clamp below 50000", s.ChunkSizeRecommendation)
	}
}

func loadSmallEnvelopeConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadBytes([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  max_memory_mb: 64
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if got := cfg.AutoConfig().MemoryEnvelope.BudgetMB; got != 64 {
		t.Fatalf("test requires 64 MB envelope; got %d", got)
	}
	return cfg
}

func TestIsLocalDBHost(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"", true},
		{"localhost", true},
		{"LOCALHOST", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"0.0.0.0", true},
		{"db.example.com", false},
		{"10.0.0.5", false},
		{"some-cloud-rds.amazonaws.com", false},
	}
	for _, tc := range tests {
		t.Run(tc.host, func(t *testing.T) {
			if got := isLocalDBHost(tc.host); got != tc.want {
				t.Errorf("isLocalDBHost(%q) = %v, want %v", tc.host, got, tc.want)
			}
		})
	}
}
