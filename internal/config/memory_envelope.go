package config

import (
	"fmt"
	"math"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/systemmemory"
)

const automaticMemoryBudgetPercent int64 = 70
const maxMemoryEnvelopeMB int64 = math.MaxInt64 >> 20

// resolveMemoryEnvelope turns one host/cgroup-aware snapshot into the single
// immutable budget used by config defaults and downstream compatibility
// consumers. maxMemoryMB is a ceiling only; it never substitutes for an
// invalid or unavailable system snapshot.
func resolveMemoryEnvelope(snapshot systemmemory.Snapshot, maxMemoryMB int64) (MemoryEnvelope, error) {
	if snapshot.CapacityMB <= 0 {
		return MemoryEnvelope{}, fmt.Errorf("memory snapshot has invalid effective capacity %d MB", snapshot.CapacityMB)
	}
	if snapshot.CapacityMB > maxMemoryEnvelopeMB {
		return MemoryEnvelope{}, fmt.Errorf("memory snapshot capacity %d MB exceeds safe byte-conversion maximum %d MB", snapshot.CapacityMB, maxMemoryEnvelopeMB)
	}
	if snapshot.AvailableMB <= 0 {
		return MemoryEnvelope{}, fmt.Errorf("memory snapshot has invalid effective availability %d MB", snapshot.AvailableMB)
	}
	if snapshot.AvailableMB > maxMemoryEnvelopeMB {
		return MemoryEnvelope{}, fmt.Errorf("memory snapshot availability %d MB exceeds safe byte-conversion maximum %d MB", snapshot.AvailableMB, maxMemoryEnvelopeMB)
	}
	if snapshot.AvailableMB > snapshot.CapacityMB {
		return MemoryEnvelope{}, fmt.Errorf("memory snapshot availability %d MB exceeds effective capacity %d MB", snapshot.AvailableMB, snapshot.CapacityMB)
	}
	if maxMemoryMB < 0 {
		return MemoryEnvelope{}, fmt.Errorf("migration.max_memory_mb must not be negative: %d", maxMemoryMB)
	}

	// Floor AvailableMB*70/100 without overflowing the multiplication for
	// pathological injected values. Real memory snapshots are far smaller,
	// but keeping the resolver total makes its validation contract explicit.
	autoBudgetMB := (snapshot.AvailableMB/100)*automaticMemoryBudgetPercent +
		(snapshot.AvailableMB%100)*automaticMemoryBudgetPercent/100
	if autoBudgetMB <= 0 {
		return MemoryEnvelope{}, fmt.Errorf("automatic memory budget is not positive: %d MB from %d MB available", autoBudgetMB, snapshot.AvailableMB)
	}

	budgetMB := autoBudgetMB
	if maxMemoryMB > 0 && maxMemoryMB < budgetMB {
		budgetMB = maxMemoryMB
	}
	if budgetMB <= 0 {
		return MemoryEnvelope{}, fmt.Errorf("final memory budget is not positive: %d MB", budgetMB)
	}
	if budgetMB > maxMemoryEnvelopeMB {
		return MemoryEnvelope{}, fmt.Errorf("final memory budget %d MB exceeds safe byte-conversion maximum %d MB", budgetMB, maxMemoryEnvelopeMB)
	}

	return MemoryEnvelope{
		CapacityMB:  snapshot.CapacityMB,
		AvailableMB: snapshot.AvailableMB,
		BudgetMB:    budgetMB,
		Source:      snapshot.Source,
	}, nil
}

// resolveMemoryEnvelope reads system memory exactly once during defaults,
// then publishes compatibility projections for consumers that have not yet
// migrated to MemoryEnvelope.
func (c *Config) resolveMemoryEnvelope() error {
	reader := c.memoryReader
	if reader == nil {
		reader = systemmemory.NewReader()
	}
	snapshot, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading effective system memory: %w", err)
	}

	envelope, err := resolveMemoryEnvelope(snapshot, c.Migration.MaxMemoryMB)
	if err != nil {
		return err
	}
	c.autoConfig.MemoryEnvelope = envelope
	c.autoConfig.AvailableMemoryMB = envelope.AvailableMB
	c.autoConfig.EffectiveMaxMemoryMB = envelope.BudgetMB

	logging.Debug("Memory envelope resolved: capacity=%d MB available=%d MB budget=%d MB source=%s",
		envelope.CapacityMB, envelope.AvailableMB, envelope.BudgetMB, envelope.Source)
	return nil
}
