package config

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/johndauphine/dmt/internal/logging"
)

// ApplyRuntimeMemoryLimit sets the Go runtime's soft memory limit
// (GOMEMLIMIT) from the resolved envelope budget so the GC paces itself against
// the same number the pipeline buffers are sized from (#462). Without
// this, the only memory enforcement is the transfer memory guard, which
// fires at 80% with no pacing before it; with it, the guard becomes the
// backstop for the case GC cannot solve (live data exceeding budget).
//
// A user-supplied GOMEMLIMIT environment variable wins — the runtime
// already honors it at startup and overriding an explicit operator
// choice here would be rude.
func (c *Config) ApplyRuntimeMemoryLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		logging.Debug("GOMEMLIMIT set in environment — leaving runtime memory limit untouched")
		return
	}
	mb := c.autoConfig.MemoryEnvelope.BudgetMB
	if mb <= 0 {
		return
	}
	debug.SetMemoryLimit(mb << 20)
	logging.Debug("Runtime memory limit (GOMEMLIMIT) set to %d MB from memory envelope", mb)
}

// TableRowSize holds row size info for a table
type TableRowSize struct {
	Name             string
	RowCount         int64
	EstimatedRowSize int64
}

// RefineSettingsForRowSizes reports actual table row sizes for informational purposes.
// Previously this function would reduce chunk_size/workers based on memory estimates,
// but this caused performance regressions. Memory pressure is handled by the Go GC
// soft limit — set from the envelope budget by ApplyRuntimeMemoryLimit (#462) —
// with the transfer pipeline's memory guard as the backstop.
// Returns false (no adjustments made) and a description of row sizes.
func (c *Config) RefineSettingsForRowSizes(tables []TableRowSize) (adjusted bool, changes string) {
	if len(tables) == 0 {
		return false, ""
	}

	// Calculate weighted average row size based on row counts (for informational purposes)
	var totalRows int64
	var weightedSum int64
	var maxRowSize int64
	var maxRowSizeTable string

	for _, t := range tables {
		if t.EstimatedRowSize > 0 {
			totalRows += t.RowCount
			weightedSum += t.RowCount * t.EstimatedRowSize
			if t.EstimatedRowSize > maxRowSize {
				maxRowSize = t.EstimatedRowSize
				maxRowSizeTable = t.Name
			}
		}
	}

	if totalRows == 0 || weightedSum == 0 {
		return false, ""
	}

	weightedAvgRowSize := weightedSum / totalRows

	// No adjustments made - just report row sizes for visibility
	return false, fmt.Sprintf("Row sizes: weighted avg %s, max %s in %s",
		FormatMemorySize(weightedAvgRowSize), FormatMemorySize(maxRowSize), maxRowSizeTable)
}

// formatAutoValue formats a value with auto-tuning explanation if applicable
func formatAutoValue(current, original int, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatAutoValue64 formats an int64 value with auto-tuning explanation if applicable
func formatAutoValue64(current, original int64, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatMemorySize formats bytes as a human-readable size
func formatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// EstimateMemoryUsage calculates expected memory usage given actual row sizes.
// avgRowSize should be the weighted average row size across all tables.
// Returns estimated bytes.
func (c *Config) EstimateMemoryUsage(avgRowSize int64) int64 {
	if avgRowSize <= 0 {
		avgRowSize = 500
	}
	// Formula: workers * (readers * buffers + writers * buffers) * chunk_size * avg_row_size
	// Simplified: workers * total_buffers * chunk_size * avg_row_size
	// Each worker has read-ahead buffers + pending writes
	totalBuffers := int64(c.Migration.ReadAheadBuffers) * 2 // read + write queues
	return int64(c.Migration.Workers) * totalBuffers * int64(c.Migration.ChunkSize) * avgRowSize
}

// FormatMemoryEstimate returns a human-readable memory estimate string.
func (c *Config) FormatMemoryEstimate(avgRowSize int64) string {
	mem := c.EstimateMemoryUsage(avgRowSize)
	return fmt.Sprintf("~%s (%d workers * %d buffers * %d chunk * %d bytes/row)",
		formatMemorySize(mem),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers*2,
		c.Migration.ChunkSize,
		avgRowSize)
}

// FormatMemorySize exports the formatMemorySize function for use by other packages.
func FormatMemorySize(bytes int64) string {
	return formatMemorySize(bytes)
}

// AutoConfig returns the auto-configuration metadata.
func (c *Config) AutoConfig() AutoConfig {
	return c.autoConfig
}
