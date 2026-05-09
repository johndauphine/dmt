package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

// TestGetSystemBasedSuggestions_NonDegenerate is the post-PR-#175 successor
// to TestBuildOfflineAutoTuneInput (issue #157). The old test pinned that
// the AI offline-input had non-zero AvailableMemoryMB / AvgRowBytes so the
// AI prompt's memory-budget block didn't render "0 MB" / "~0 rows". After
// PR #175 the AI prompt is gone; the same protection lives inside the
// deterministic tuner (memoryBudgetMB falls back to fallbackBudgetMB,
// baseline defaults avg to 500). End-to-end check: GetSystemBasedSuggestions
// must produce a usable parameter set across reasonable system sizes,
// including pathological zero inputs.
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
			"user cap honored",
			&config.Config{
				Source:    config.SourceConfig{Type: "mssql"},
				Target:    config.TargetConfig{Type: "postgres"},
				Migration: config.MigrationConfig{MaxMemoryMB: 2000},
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
