package transfer

import (
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/pool"
)

// No buffer sizing constants — all pipeline buffer depths are derived from
// the memory budget via pool.CalculatePipelineBuffers.
//
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

	// Derive memory budget from system-detected or user-configured limits.
	// Use user cap if set, otherwise auto-detected effective limit.
	effectiveMemMB := cfg.AutoConfig().EffectiveMaxMemoryMB
	if cfg.Migration.MaxMemoryMB > 0 && cfg.Migration.MaxMemoryMB < effectiveMemMB {
		effectiveMemMB = cfg.Migration.MaxMemoryMB
	}

	// Subtract estimated connection pool overhead from pipeline budget.
	// Each database connection holds driver buffers and prepared statement caches.
	connCount := int64(cfg.Migration.MaxSourceConnections + cfg.Migration.MaxTargetConnections)
	if connCount == 0 {
		connCount = int64(numWriters * 4) // estimate if not configured
	}
	connOverheadMB := connCount * 10 // ~10MB per Go database/sql connection
	pipelineBudgetMB := effectiveMemMB - connOverheadMB
	if pipelineBudgetMB <= 0 {
		pipelineBudgetMB = effectiveMemMB / 2 // fallback: half of effective memory
	}

	// Divide budget among concurrent table pipelines. Workers controls how many
	// tables transfer simultaneously, and each gets its own channels and buffers.
	// Without this division, each table independently claims the full budget,
	// causing total memory to be Workers × budget.
	concurrentTables := int64(cfg.Migration.Workers)
	if concurrentTables > 1 {
		pipelineBudgetMB = pipelineBudgetMB / concurrentTables
	}

	return pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		MemoryBudgetMB:   pipelineBudgetMB,
		ChunkSize:        chunkSize,
		RowBytes:         job.Table.EstimatedRowSize,
		NumWriters:       numWriters,
		NumReaders:       numReaders,
		ReadAheadBuffers: readAheadBuffers,
	})
}
