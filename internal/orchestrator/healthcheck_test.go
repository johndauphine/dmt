package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

// TestBuildOfflineAutoTuneInput pins issue #157: the offline AI smartconfig
// path must populate AvgRowBytes and AvailableMemoryMB on the AutoTuneInput,
// or the prompt's Memory budget block degenerates to "Memory budget: 0 MB"
// and the LLM under-provisions. Also asserts that the user's max_memory_mb
// flows through to the AI input.
func TestBuildOfflineAutoTuneInput(t *testing.T) {
	cases := []struct {
		name              string
		cfg               *config.Config
		cores             int
		memGB             int
		availableMemoryMB int64
		wantMaxMemMB      int64
	}{
		{
			name: "no max_memory_mb set",
			cfg: &config.Config{
				Source: config.SourceConfig{Type: "mssql"},
				Target: config.TargetConfig{Type: "postgres"},
			},
			cores:             16,
			memGB:             32,
			availableMemoryMB: 28000,
			wantMaxMemMB:      0,
		},
		{
			name: "max_memory_mb=2000 flows through",
			cfg: &config.Config{
				Source:    config.SourceConfig{Type: "postgres"},
				Target:    config.TargetConfig{Type: "postgres"},
				Migration: config.MigrationConfig{MaxMemoryMB: 2000},
			},
			cores:             8,
			memGB:             16,
			availableMemoryMB: 14000,
			wantMaxMemMB:      2000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildOfflineAutoTuneInput(tc.cfg, tc.cores, tc.memGB, tc.availableMemoryMB)

			if got.CPUCores != tc.cores {
				t.Errorf("CPUCores = %d, want %d", got.CPUCores, tc.cores)
			}
			if got.MemoryGB != tc.memGB {
				t.Errorf("MemoryGB = %d, want %d", got.MemoryGB, tc.memGB)
			}
			// Issue #157: these two fields, when zero, cause the prompt to
			// render "Memory budget: 0 MB" and "safe chunk_size ceiling is
			// ~0 rows". They MUST be populated on this path.
			if got.AvailableMemoryMB == 0 {
				t.Errorf("AvailableMemoryMB must not be 0 on offline AI path (issue #157); got: %+v", got)
			}
			if got.AvailableMemoryMB != tc.availableMemoryMB {
				t.Errorf("AvailableMemoryMB = %d, want %d", got.AvailableMemoryMB, tc.availableMemoryMB)
			}
			if got.AvgRowBytes == 0 {
				t.Errorf("AvgRowBytes must not be 0 on offline AI path (issue #157); got: %+v", got)
			}
			if got.AvgRowBytes != 500 {
				t.Errorf("AvgRowBytes = %d, want 500 (the documented offline default mirroring calculateAvgRowSize)", got.AvgRowBytes)
			}
			if got.MaxMemoryMB != tc.wantMaxMemMB {
				t.Errorf("MaxMemoryMB = %d, want %d (user cap must flow through to AI input)", got.MaxMemoryMB, tc.wantMaxMemMB)
			}
			if got.DatabaseType != tc.cfg.Source.Type {
				t.Errorf("DatabaseType = %q, want %q", got.DatabaseType, tc.cfg.Source.Type)
			}
			if got.TargetType != tc.cfg.Target.Type {
				t.Errorf("TargetType = %q, want %q", got.TargetType, tc.cfg.Target.Type)
			}
		})
	}
}
