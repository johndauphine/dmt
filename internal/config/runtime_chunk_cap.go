package config

import (
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
)

// RuntimeChunkSizeCapFor derives the current row-count ceiling for an
// arbitrary effective runtime tuple. The immutable #708 memory envelope is the
// only budget source. The global ceiling uses the workload's row-count-weighted
// representative width so one wide outlier does not throttle every table;
// steady transfer uses shared measured-byte admission and MemoryGuard, while
// each pipeline evaluates its own complete inventory around writer-count
// transitions. TargetHardChunkLimit remains independently binding for protocol
// safety. A zero result means neither source can provide a usable cap; it never
// means that runtime growth is unbounded.
func (c *Config) RuntimeChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if c == nil {
		return 0
	}
	memoryCap := c.runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters)
	return minNonZeroInt(memoryCap, positiveInt(c.Migration.TargetHardChunkLimit))
}

// RuntimeChunkGrowthCapFor is the fail-closed recomputation callback for
// resource growth. Unlike RuntimeChunkSizeCapFor, it returns zero when the
// tuple lacks observed-width evidence or when even the one-row minimum-progress
// footprint exceeds the immutable envelope. The controller must suppress a
// prospective WAW increase on zero rather than treating cap=1 as fitting.
func (c *Config) RuntimeChunkGrowthCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if c == nil || !c.Migration.RuntimeSafetyRowBytesKnown || c.Migration.RuntimeSafetyRowBytes <= 0 ||
		c.Migration.RuntimeRepresentativeRowBytes <= 0 {
		return 0
	}
	if c.runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters) <= 0 {
		return 0
	}
	// Keep the existing fail-closed widest-width feasibility gate. Runtime
	// growth may use the representative global ceiling only when at least one
	// modeled row from the widest observed table fits the immutable envelope;
	// each pipeline then checks its complete inventory before writer growth is
	// authorized.
	if tuning.MemoryEstimateExceedsBudget(
		c.autoConfig.MemoryEnvelope.BudgetMB,
		workers,
		readAheadBuffers,
		writeAheadWriters,
		1,
		c.Migration.RuntimeSafetyRowBytes,
	) {
		return 0
	}
	return c.RuntimeChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters)
}

// ResetRuntimeChunkSafety clears current-run schema evidence and derived growth
// authorization before a fresh tuning attempt. TargetHardChunkLimit is left
// intact: the orchestrator owns probe lifecycle and may immediately finalize a
// protocol-only degraded path after this reset.
func (c *Config) ResetRuntimeChunkSafety() {
	if c == nil {
		return
	}
	c.Migration.RuntimeChunkSizeCap = 0
	c.Migration.RuntimeRepresentativeRowBytes = 0
	c.Migration.RuntimeSafetyRowBytes = 0
	c.Migration.RuntimeSafetyRowBytesKnown = false
	c.Migration.RuntimeChunkGrowthAllowed = false
}

func (c *Config) runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if c.Migration.RuntimeRepresentativeRowBytes <= 0 {
		return 0
	}
	rows := tuning.SafeChunkSize(
		c.autoConfig.MemoryEnvelope.BudgetMB,
		workers,
		readAheadBuffers,
		writeAheadWriters,
		c.Migration.RuntimeRepresentativeRowBytes,
	)
	return positiveInt64ToInt(rows)
}

// FinalizeRuntimeChunkSizeCap derives the cap using the current effective
// tuple. ApplyTunerSuggestions calls it after provenance-aware knob application;
// the orchestrator may call it again after installing a probed
// TargetHardChunkLimit. That second use also covers manual tuning or failed
// analysis: no width is invented, but a protocol-only cap remains available.
//
// This method deliberately does not mutate chunk_size. TransferRunner owns the
// pre-controller atomic clamp so it can persist the original-to-cap runtime
// adjustment and mark the run adjusted (#709).
func (c *Config) FinalizeRuntimeChunkSizeCap() {
	if c == nil {
		return
	}
	m := &c.Migration
	memoryCap := c.runtimeMemoryChunkSizeCapFor(m.Workers, m.ReadAheadBuffers, m.WriteAheadWriters)
	protocolCap := positiveInt(m.TargetHardChunkLimit)
	m.RuntimeChunkSizeCap = minNonZeroInt(memoryCap, protocolCap)

	oneRowOverBudget := false
	if memoryCap > 0 && m.RuntimeSafetyRowBytesKnown && m.RuntimeSafetyRowBytes > 0 {
		oneRowOverBudget = tuning.MemoryEstimateExceedsBudget(
			c.autoConfig.MemoryEnvelope.BudgetMB,
			m.Workers,
			m.ReadAheadBuffers,
			m.WriteAheadWriters,
			1,
			m.RuntimeSafetyRowBytes,
		)
	}
	m.RuntimeChunkGrowthAllowed = c.RuntimeChunkGrowthCapFor(m.Workers, m.ReadAheadBuffers, m.WriteAheadWriters) > 0

	switch {
	case oneRowOverBudget:
		logging.Warn("Runtime growth disabled: one modeled row at the widest observed table-average exceeds the memory envelope (budget=%d MB safety_width=%d B workers=%d read_ahead=%d write_ahead=%d); global cap=%d uses representative width %d B; measured-byte admission/MemoryGuard enforce steady transfer",
			c.autoConfig.MemoryEnvelope.BudgetMB, m.RuntimeSafetyRowBytes, m.Workers, m.ReadAheadBuffers, m.WriteAheadWriters, m.RuntimeChunkSizeCap, m.RuntimeRepresentativeRowBytes)
	case memoryCap > 0 && protocolCap > 0 && m.RuntimeChunkGrowthAllowed:
		logging.Debug("Runtime global chunk cap derived: cap=%d rows (representative memory=%d, protocol=%d, representative width=%d B); per-table complete-inventory checks gate runtime writer growth; resource growth enabled",
			m.RuntimeChunkSizeCap, memoryCap, protocolCap, m.RuntimeRepresentativeRowBytes)
	case memoryCap > 0 && protocolCap > 0:
		logging.Debug("Runtime global chunk cap derived: cap=%d rows (representative memory=%d, protocol=%d, representative width=%d B); per-table complete-inventory checks gate runtime writer growth; resource growth disabled because widest-width evidence is unavailable",
			m.RuntimeChunkSizeCap, memoryCap, protocolCap, m.RuntimeRepresentativeRowBytes)
	case memoryCap > 0 && m.RuntimeChunkGrowthAllowed:
		logging.Debug("Runtime global chunk cap derived: cap=%d rows (representative memory cap, representative width=%d B); per-table complete-inventory checks gate runtime writer growth; resource growth enabled",
			m.RuntimeChunkSizeCap, m.RuntimeRepresentativeRowBytes)
	case memoryCap > 0:
		logging.Debug("Runtime global chunk cap derived: cap=%d rows (representative memory cap, representative width=%d B); per-table complete-inventory checks gate runtime writer growth; resource growth disabled because widest-width evidence is unavailable",
			m.RuntimeChunkSizeCap, m.RuntimeRepresentativeRowBytes)
	case protocolCap > 0:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (protocol-only; representative width unavailable); resource growth disabled",
			m.RuntimeChunkSizeCap)
	default:
		logging.Debug("Runtime global chunk cap unavailable (representative width unavailable and no protocol cap); resource growth disabled")
	}
}

func minNonZeroInt(values ...int) int {
	minimum := 0
	for _, value := range values {
		if value > 0 && (minimum == 0 || value < minimum) {
			minimum = value
		}
	}
	return minimum
}

func positiveInt(value int) int {
	if value > 0 {
		return value
	}
	return 0
}

func positiveInt64ToInt(value int64) int {
	if value <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if value > int64(maxInt) {
		return maxInt
	}
	return int(value)
}
