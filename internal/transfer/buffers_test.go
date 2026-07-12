package transfer

import (
	"math"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

func TestSubtractConnectionOverheadMB(t *testing.T) {
	tests := []struct {
		name              string
		budgetMB          int64
		connectionCount   int64
		wantPipelineMemMB int64
		wantExhausted     bool
	}{
		{name: "normal subtraction", budgetMB: 4_096, connectionCount: 20, wantPipelineMemMB: 3_896},
		{name: "overhead exhausts budget", budgetMB: 100, connectionCount: 16, wantPipelineMemMB: 1, wantExhausted: true},
		{name: "overflowing overhead exhausts budget", budgetMB: 100, connectionCount: math.MaxInt64, wantPipelineMemMB: 1, wantExhausted: true},
		{name: "zero budget", budgetMB: 0, connectionCount: 16, wantPipelineMemMB: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, exhausted := subtractConnectionOverheadMB(tc.budgetMB, tc.connectionCount)
			if got != tc.wantPipelineMemMB || exhausted != tc.wantExhausted {
				t.Errorf("subtractConnectionOverheadMB(%d, %d) = (%d, %v), want (%d, %v)", tc.budgetMB, tc.connectionCount, got, exhausted, tc.wantPipelineMemMB, tc.wantExhausted)
			}
		})
	}
}

func TestPipelineBudgetSaturatedConnectionCountsExhaustReserve(t *testing.T) {
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
	maxInt := int(^uint(0) >> 1)
	cfg.Migration.MaxSourceConnections = maxInt
	cfg.Migration.MaxTargetConnections = maxInt

	if got := PipelineMemBudgetBytes(cfg); got != 1024*1024 {
		t.Fatalf("PipelineMemBudgetBytes with saturated connection pools = %d, want 1 MiB exhausted reserve", got)
	}
}

func TestPipelineBudgetBytesSaturatesOnOverflow(t *testing.T) {
	if got := pipelineBudgetBytes(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("pipelineBudgetBytes(MaxInt64) = %d, want MaxInt64 saturation", got)
	}
	if got := pipelineBudgetBytes(64); got != 64*1024*1024 {
		t.Fatalf("pipelineBudgetBytes(64) = %d, want 64 MiB", got)
	}
}

func TestTransferBudgetsUseResolvedEnvelopeNotMutatedLegacyCap(t *testing.T) {
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
	envelopeBudgetMB := cfg.AutoConfig().MemoryEnvelope.BudgetMB
	if envelopeBudgetMB != 64 {
		t.Fatalf("test requires the 64 MB user ceiling to bind; resolved budget = %d", envelopeBudgetMB)
	}

	// Simulate a stale downstream field. Once the envelope is resolved,
	// consumers must not reapply Migration.MaxMemoryMB.
	cfg.Migration.MaxMemoryMB = 1
	if got := MemoryGuardLimitMB(cfg); got != envelopeBudgetMB {
		t.Errorf("MemoryGuardLimitMB = %d, want resolved envelope %d", got, envelopeBudgetMB)
	}
	connCount := int64(cfg.Migration.MaxSourceConnections + cfg.Migration.MaxTargetConnections)
	wantPipelineMB, _ := subtractConnectionOverheadMB(envelopeBudgetMB, connCount)
	wantPipelineBytes := wantPipelineMB * 1024 * 1024
	if got := PipelineMemBudgetBytes(cfg); got != wantPipelineBytes {
		t.Errorf("PipelineMemBudgetBytes = %d, want %d from envelope after one overhead subtraction", got, wantPipelineBytes)
	}
}
