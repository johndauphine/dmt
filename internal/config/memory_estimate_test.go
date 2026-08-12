package config

import (
	"math"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/tuning"
)

// TestApplyRuntimeMemoryLimit_SetsGOMEMLIMIT verifies the #462 wiring: the
// envelope budget becomes the runtime's soft memory limit so GC pacing and
// pipeline buffer sizing work from the same number.
func TestApplyRuntimeMemoryLimit_SetsGOMEMLIMIT(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "") // isolate from an inherited operator env var
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.autoConfig.MemoryEnvelope.BudgetMB = 512
	// Compatibility fields are projections only; the envelope is authoritative.
	c.autoConfig.EffectiveMaxMemoryMB = 999
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != int64(512)<<20 {
		t.Fatalf("memory limit = %d, want %d", got, int64(512)<<20)
	}
}

func TestApplyRuntimeMemoryLimit_NoBudgetIsNoOp(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != prev {
		t.Fatalf("memory limit changed to %d with no budget configured; want unchanged %d", got, prev)
	}
}

// TestApplyRuntimeMemoryLimit_EnvWins verifies an operator-supplied
// GOMEMLIMIT is left untouched — the runtime already honors it and the
// derived budget must not override an explicit choice.
func TestApplyRuntimeMemoryLimit_EnvWins(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	c := &Config{}
	c.autoConfig.MemoryEnvelope.BudgetMB = 512
	c.ApplyRuntimeMemoryLimit()

	if got := debug.SetMemoryLimit(-1); got != prev {
		t.Fatalf("memory limit = %d, want untouched %d when GOMEMLIMIT env is set", got, prev)
	}
}

func TestEstimateMemoryUsageSaturatesOverflow(t *testing.T) {
	cfg := &Config{Migration: MigrationConfig{
		Workers:          int(^uint(0) >> 1),
		ReadAheadBuffers: int(^uint(0) >> 1),
		ChunkSize:        int(^uint(0) >> 1),
	}}
	if got := cfg.EstimateMemoryUsage(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("overflowing memory estimate = %d, want MaxInt64 saturation", got)
	}
	formatted := cfg.FormatMemoryEstimate(0)
	if strings.Contains(formatted, "-") || !strings.Contains(formatted, "500 bytes/row") {
		t.Fatalf("overflow/fallback memory display is misleading: %q", formatted)
	}

	_, summary := cfg.RefineSettingsForRowSizes([]TableRowSize{
		{Name: "huge", RowCount: math.MaxInt64, EstimatedRowSize: math.MaxInt64},
	})
	if !strings.Contains(summary, "max") || strings.Contains(summary, "-") || strings.Contains(summary, "weighted avg 1 bytes") {
		t.Fatalf("overflowing row-size summary became invalid: %q", summary)
	}
}

func TestDefaultPolicyReadAheadBuffersUsesCanonicalValue(t *testing.T) {
	out := tuning.DefaultOutput(
		tuning.Input{CPUCores: 64, Platform: "linux", RepresentativeRowBytes: 500},
		tuning.DriverProfile{BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000},
	)
	if out.ReadAheadBuffers != 4 {
		t.Fatalf("canonical default RAB = %d, want 4", out.ReadAheadBuffers)
	}
}
