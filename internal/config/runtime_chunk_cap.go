package config

import (
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
)

// RuntimeChunkSizeCapFor derives the current row-count ceiling for an
// arbitrary effective runtime tuple. The immutable #708 memory envelope is the
// only budget source. A complete runtime table profile uses cardinality-aware
// sizing; otherwise #703's observed safety width supplies the conservative
// scalar cap. TargetHardChunkLimit remains independently binding for protocol safety.
// A zero result means neither source can provide a usable cap; it never means
// that runtime growth is unbounded.
func (c *Config) RuntimeChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if c == nil {
		return 0
	}
	memoryCap := c.runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters)
	return minNonZeroInt(memoryCap, positiveInt(c.Migration.TargetHardChunkLimit))
}

// RuntimeChunkGrowthCapFor is the fail-closed recomputation callback for
// resource growth. Unlike RuntimeChunkSizeCapFor, it returns zero when the
// tuple lacks observed-width evidence, the cardinality profile is incomplete,
// or even the one-row minimum-progress footprint exceeds the immutable
// envelope. The controller must suppress a prospective WAW increase on zero
// rather than treating cap=1 as fitting.
func (c *Config) RuntimeChunkGrowthCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if c == nil || !c.Migration.RuntimeSafetyRowBytesKnown || c.Migration.RuntimeSafetyRowBytes <= 0 ||
		!c.Migration.RuntimeMemoryProfile.Complete() {
		return 0
	}
	if c.runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters) <= 0 {
		return 0
	}
	if c.runtimeMemoryModel().ExceedsBudget(
		c.autoConfig.MemoryEnvelope.BudgetMB,
		workers,
		readAheadBuffers,
		writeAheadWriters,
		1,
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
	c.Migration.RuntimeSafetyRowBytes = 0
	c.Migration.RuntimeSafetyRowBytesKnown = false
	c.Migration.RuntimeMemoryProfile = tuning.MemoryProfile{}
	c.Migration.RuntimeChunkGrowthAllowed = false
}

func (c *Config) runtimeMemoryChunkSizeCapFor(workers, readAheadBuffers, writeAheadWriters int) int {
	if !c.Migration.RuntimeSafetyRowBytesKnown || c.Migration.RuntimeSafetyRowBytes <= 0 {
		return 0
	}
	rows := c.runtimeMemoryModel().SafeChunkSize(
		c.autoConfig.MemoryEnvelope.BudgetMB,
		workers,
		readAheadBuffers,
		writeAheadWriters,
	)
	return positiveInt64ToInt(rows)
}

func (c *Config) runtimeMemoryModel() tuning.MemoryModel {
	return tuning.NewMemoryModel(c.Migration.RuntimeMemoryProfile, c.Migration.RuntimeSafetyRowBytes)
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
	if memoryCap > 0 {
		oneRowOverBudget = c.runtimeMemoryModel().ExceedsBudget(
			c.autoConfig.MemoryEnvelope.BudgetMB,
			m.Workers,
			m.ReadAheadBuffers,
			m.WriteAheadWriters,
			1,
		)
	}
	m.RuntimeChunkGrowthAllowed = c.RuntimeChunkGrowthCapFor(m.Workers, m.ReadAheadBuffers, m.WriteAheadWriters) > 0

	switch {
	case oneRowOverBudget:
		logging.Warn("Runtime chunk cap is the 1-row minimum-progress fallback, but one modeled row still exceeds the memory envelope (budget=%d MB safety_width=%d B workers=%d read_ahead=%d write_ahead=%d); resource growth disabled",
			c.autoConfig.MemoryEnvelope.BudgetMB, m.RuntimeSafetyRowBytes, m.Workers, m.ReadAheadBuffers, m.WriteAheadWriters)
	case memoryCap > 0 && protocolCap > 0 && m.RuntimeChunkGrowthAllowed:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (memory=%d, protocol=%d, cardinality-aware tables=%d, observed safety width=%d B); resource growth enabled",
			m.RuntimeChunkSizeCap, memoryCap, protocolCap, m.RuntimeMemoryProfile.Len(), m.RuntimeSafetyRowBytes)
	case memoryCap > 0 && protocolCap > 0:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (memory=%d conservative scalar fallback, protocol=%d, observed safety width=%d B, incomplete cardinality profile); resource growth disabled",
			m.RuntimeChunkSizeCap, memoryCap, protocolCap, m.RuntimeSafetyRowBytes)
	case memoryCap > 0 && m.RuntimeChunkGrowthAllowed:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (memory cap, cardinality-aware tables=%d, observed safety width=%d B); resource growth enabled",
			m.RuntimeChunkSizeCap, m.RuntimeMemoryProfile.Len(), m.RuntimeSafetyRowBytes)
	case memoryCap > 0:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (conservative scalar fallback, observed safety width=%d B, incomplete cardinality profile); resource growth disabled",
			m.RuntimeChunkSizeCap, m.RuntimeSafetyRowBytes)
	case protocolCap > 0:
		logging.Debug("Runtime chunk cap derived: cap=%d rows (protocol-only; safety width unobserved); resource growth disabled",
			m.RuntimeChunkSizeCap)
	default:
		logging.Debug("Runtime chunk cap unavailable (safety width unobserved and no protocol cap); resource growth disabled")
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
