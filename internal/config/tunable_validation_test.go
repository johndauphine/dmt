package config

import (
	"strings"
	"testing"
)

func TestValidateRejectsNegativeCandidateTunables(t *testing.T) {
	tests := []struct {
		name string
		set  func(*MigrationConfig)
		want string
	}{
		{name: "workers", set: func(m *MigrationConfig) { m.Workers = -1 }, want: "migration.workers"},
		{name: "chunk size", set: func(m *MigrationConfig) { m.ChunkSize = -1 }, want: "migration.chunk_size"},
		{name: "write ahead writers", set: func(m *MigrationConfig) { m.WriteAheadWriters = -1 }, want: "migration.write_ahead_writers"},
		{name: "parallel readers", set: func(m *MigrationConfig) { m.ParallelReaders = -1 }, want: "migration.parallel_readers"},
		{name: "read ahead buffers", set: func(m *MigrationConfig) { m.ReadAheadBuffers = -1 }, want: "migration.read_ahead_buffers"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Source:    SourceConfig{Type: "sqlite", Database: "source.db"},
				Target:    TargetConfig{Type: "sqlite", Database: "target.db"},
				Migration: MigrationConfig{TargetMode: "drop_recreate"},
			}
			tc.set(&cfg.Migration)
			err := cfg.validate()
			if err == nil || !strings.Contains(err.Error(), tc.want+" must not be negative") {
				t.Fatalf("validate() error = %v, want %q", err, tc.want+" must not be negative")
			}
		})
	}
}
