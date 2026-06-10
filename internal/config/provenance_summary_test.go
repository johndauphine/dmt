package config

import (
	"strings"
	"testing"
)

// TestTuningProvenanceSummary_GroupsByOwnership verifies the #461 run-start
// line: user-pinned knobs are called out as not-tunable, tuner-derived
// values are attributed, and everything else reads as a default.
func TestTuningProvenanceSummary_GroupsByOwnership(t *testing.T) {
	c := &Config{}
	c.Migration.Workers = 8
	c.Migration.ChunkSize = 200_000
	c.Migration.WriteAheadWriters = 4
	c.Migration.MaxMemoryMB = 4096
	c.setTunableProvenance(provenanceMigrationWorkers, ProvenanceUserConfig)
	c.setTunableProvenance(provenanceMigrationChunkSize, ProvenanceSmartConfig)

	got := c.TuningProvenanceSummary()

	if !strings.Contains(got, "max_memory_mb=4096") || !strings.Contains(got, "pinned") {
		t.Errorf("user memory cap should appear pinned; got %q", got)
	}
	if !strings.Contains(got, "workers=8") {
		t.Errorf("missing pinned workers; got %q", got)
	}
	if !strings.Contains(got, "tuner-derived: chunk_size=200000") {
		t.Errorf("missing derived chunk_size; got %q", got)
	}
	if !strings.Contains(got, "defaults: ") || !strings.Contains(got, "write_ahead_writers=4") {
		t.Errorf("missing defaults group; got %q", got)
	}
}
