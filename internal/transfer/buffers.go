package transfer

import (
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
)

// No buffer sizing constants — all pipeline buffer depths are derived from
// the memory budget via pool.CalculatePipelineBuffers.
const (
	defaultConnectionCount = int64(16)
	connectionOverheadMB   = int64(10)
)

// pipelineBudgetMB returns the one resolved envelope budget after subtracting
// connection overhead exactly once. The same helper feeds per-table buffer
// sizing and the shared in-flight MemBudget (#708).
func pipelineBudgetMB(cfg *config.Config) (budgetMB int64, reserveExhausted bool) {
	if cfg == nil {
		return 0, false
	}
	budgetMB = cfg.AutoConfig().MemoryEnvelope.BudgetMB
	if budgetMB <= 0 {
		return 0, false
	}

	connCount := int64(cfg.Migration.MaxSourceConnections + cfg.Migration.MaxTargetConnections)
	if connCount <= 0 {
		connCount = defaultConnectionCount
	}
	return subtractConnectionOverheadMB(budgetMB, connCount)
}

func subtractConnectionOverheadMB(budgetMB, connCount int64) (pipelineMB int64, reserveExhausted bool) {
	if budgetMB <= 0 {
		return 0, false
	}
	pipelineMB = budgetMB - connCount*connectionOverheadMB
	if pipelineMB <= 0 {
		// The connection model already consumes the envelope. Preserve only a
		// 1 MiB positive coordination budget; MemoryGuard remains the hard
		// backstop, and the caller surfaces the exhausted reserve.
		return 1, true
	}
	return pipelineMB, false
}

// calculatePipelineBuffers derives both chunkChan and jobChan buffer depths
// for a specific table from the system's memory budget and the table's actual
// Go heap cost per row. No magic numbers — all values come from system detection,
// user config, or per-table column metadata.
func calculatePipelineBuffers(cfg *config.Config, job Job, tableName string, tuner RuntimeTuner, numWriters int, numReaders int, readAheadBuffers int) pool.PipelineBufferSizes {
	// Resolve chunk size: per-table override → global tuner → config default
	chunkSize := cfg.Migration.ChunkSize
	if tuner != nil {
		if cs, ok := tuner.TableChunkSize(tableName); ok && cs > 0 {
			chunkSize = cs
		} else if cs := tuner.Snapshot().ChunkSize; cs > 0 {
			chunkSize = cs
		}
	}

	tableBudgetMB, _ := pipelineBudgetMB(cfg)

	// Divide budget among concurrent table pipelines. Workers controls how many
	// tables transfer simultaneously, and each gets its own channels and buffers.
	// Without this division, each table independently claims the full budget,
	// causing total memory to be Workers × budget.
	concurrentTables := int64(cfg.Migration.Workers)
	if concurrentTables > 1 {
		tableBudgetMB = tableBudgetMB / concurrentTables
		if tableBudgetMB < 1 {
			tableBudgetMB = 1
		}
	}

	return pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		MemoryBudgetMB:   tableBudgetMB,
		ChunkSize:        chunkSize,
		RowBytes:         job.Table.EstimatedRowSize,
		NumWriters:       numWriters,
		NumReaders:       numReaders,
		ReadAheadBuffers: readAheadBuffers,
	})
}

// PipelineMemBudgetBytes computes the total in-flight byte budget shared
// across all concurrent table pipelines (#617). It reuses the same
// effective-memory-minus-connection-overhead figure as the per-table buffer
// sizing above, but deliberately does NOT divide by Workers: the shared
// MemBudget lets whichever tables are actually running split it by
// contention, so a table running alone gets the whole budget. Returns 0
// when no positive limit is configured (budget disabled).
func PipelineMemBudgetBytes(cfg *config.Config) int64 {
	budgetMB, reserveExhausted := pipelineBudgetMB(cfg)
	if reserveExhausted {
		envelopeMB := cfg.AutoConfig().MemoryEnvelope.BudgetMB
		connections := cfg.Migration.MaxSourceConnections + cfg.Migration.MaxTargetConnections
		if connections <= 0 {
			connections = int(defaultConnectionCount)
		}
		logging.Warn("transfer memory envelope exhausted by connection-overhead estimate (budget=%d MB, connections=%d at %d MB each); limiting shared pipeline budget to %d MB",
			envelopeMB, connections, connectionOverheadMB, budgetMB)
	}
	return budgetMB * 1024 * 1024
}

// MemoryGuardLimitMB returns the migration-wide heap limit used by the
// memory-pressure backstop. It is the resolved envelope budget itself; transfer
// sub-budgets subtract connection overhead downstream without redefining the
// guardrail.
func MemoryGuardLimitMB(cfg *config.Config) int64 {
	if cfg == nil {
		return 0
	}
	return cfg.AutoConfig().MemoryEnvelope.BudgetMB
}
