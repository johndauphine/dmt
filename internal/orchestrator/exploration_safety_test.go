package orchestrator

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/tuning"
)

func TestRuntimeControllerOptionsUseSafetyOnlyForExploration(t *testing.T) {
	tests := []struct {
		name string
		tier string
		want bool
	}{
		{name: "exploration", tier: tuning.TierExploration, want: true},
		{name: "regression", tier: tuning.TierRegression},
		{name: "smoothed bins", tier: tuning.TierSmoothedBins},
		{name: "baseline", tier: tuning.TierBaseline},
		{name: "manual or failed analysis", tier: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &TransferRunner{
				config:     &config.Config{},
				tuningTier: tc.tier,
			}
			if got := runner.runtimeControllerOptions("run-1", nil).SafetyOnly; got != tc.want {
				t.Fatalf("SafetyOnly = %v, want %v for tier %q", got, tc.want, tc.tier)
			}
		})
	}
}

func TestApplyTuningManualResetsRunScopedState(t *testing.T) {
	o := &Orchestrator{
		config:          &config.Config{Migration: config.MigrationConfig{Tuning: "manual"}},
		lastTuningRowID: 42,
		lastTuningTier:  tuning.TierExploration,
	}

	o.applyTuning(context.Background())

	if o.lastTuningRowID != 0 || o.lastTuningTier != "" {
		t.Fatalf("run-scoped tuning state = (row=%d, tier=%q), want zero values",
			o.lastTuningRowID, o.lastTuningTier)
	}
}
