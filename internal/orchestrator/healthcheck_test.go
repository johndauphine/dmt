package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
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
			if s.Workers < 2 {
				t.Errorf("Workers=%d, want >= 2 (baseline floor)", s.Workers)
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
		})
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
