package transfer

import (
	"math"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/pool"
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

	connCount := configuredConnectionCount(cfg)
	return subtractConnectionOverheadMB(budgetMB, connCount)
}

func configuredConnectionCount(cfg *config.Config) int64 {
	var total int64
	for _, count := range []int{cfg.Migration.MaxSourceConnections, cfg.Migration.MaxTargetConnections} {
		if count <= 0 {
			continue
		}
		count64 := int64(count)
		if total > math.MaxInt64-count64 {
			return math.MaxInt64
		}
		total += count64
	}
	if total == 0 {
		return defaultConnectionCount
	}
	return total
}

func subtractConnectionOverheadMB(budgetMB, connCount int64) (pipelineMB int64, reserveExhausted bool) {
	if budgetMB <= 0 {
		return 0, false
	}
	if connCount <= 0 {
		connCount = defaultConnectionCount
	}
	if connCount > (budgetMB-1)/connectionOverheadMB {
		// The connection model already consumes the envelope. Preserve only a
		// 1 MiB positive coordination budget; MemoryGuard remains the hard
		// backstop, and the caller surfaces the exhausted reserve.
		return 1, true
	}
	pipelineMB = budgetMB - connCount*connectionOverheadMB
	return pipelineMB, false
}

// runtimeSizingTuple returns the effective live concurrency values used by
// the memory model. Runtime updates are read atomically from the tuner; direct
// callers without a tuner fall back to config.
type runtimeSizingTuple struct {
	workers           int
	readAheadBuffers  int
	writeAheadWriters int
}

func effectiveRuntimeSizingTuple(cfg *config.Config, tuner RuntimeTuner) runtimeSizingTuple {
	var tuple runtimeSizingTuple
	if cfg != nil {
		tuple.workers = cfg.Migration.Workers
		tuple.readAheadBuffers = cfg.Migration.ReadAheadBuffers
		tuple.writeAheadWriters = cfg.Migration.WriteAheadWriters
	}
	if tuner != nil {
		snapshot := tuner.Snapshot()
		if snapshot.Workers > 0 {
			tuple.workers = snapshot.Workers
		}
		if snapshot.ReadAheadBuffers > 0 {
			tuple.readAheadBuffers = snapshot.ReadAheadBuffers
		}
		if snapshot.WriteAheadWriters > 0 {
			tuple.writeAheadWriters = snapshot.WriteAheadWriters
		}
	}
	if tuple.workers < 1 {
		tuple.workers = 1
	}
	if tuple.readAheadBuffers < 0 {
		tuple.readAheadBuffers = 0
	}
	if tuple.writeAheadWriters < 1 {
		// runPipeline has the same minimum: even a sparse direct-call config
		// constructs one writer, so the safety model must count it.
		tuple.writeAheadWriters = 1
	}
	return tuple
}

// tableSizingRowBytes returns this table's own observed/estimated width. A
// missing table width falls back to the widest observed safety width from the
// current analysis; no unobserved width is invented for a runtime writer-growth
// transition check.
func tableSizingRowBytes(cfg *config.Config, tableRowBytes int64) (int64, bool) {
	if tableRowBytes > 0 {
		return tableRowBytes, true
	}
	if cfg != nil && cfg.Migration.RuntimeSafetyRowBytesKnown && cfg.Migration.RuntimeSafetyRowBytes > 0 {
		return cfg.Migration.RuntimeSafetyRowBytes, true
	}
	return 0, false
}

// perTablePipelineBudgetBytes divides the resolved pipeline budget by the
// maximum number of concurrent table jobs without first truncating it to whole
// MiB. It is a buffer-depth sizing target, not steady-state admission: required
// minimum depths can exceed the share, while the shared measured-byte budget
// redistributes actual in-flight capacity dynamically.
func perTablePipelineBudgetBytes(cfg *config.Config, workers int) int64 {
	budgetMB, _ := pipelineBudgetMB(cfg)
	if budgetMB <= 0 {
		return 0
	}
	budgetBytes := pipelineBudgetBytes(budgetMB)
	if workers <= 1 {
		return budgetBytes
	}
	return budgetBytes / int64(workers)
}

// runtimeTableChunkSizeCap returns the prospective writer-transition ceiling
// for one table under its complete live pipeline inventory. Unlike the global
// representative cap, this uses the table's own row width, the actual fixed
// channel depths, reader scan slack, writer encode copies, and the maximum
// number of concurrent table jobs. The ceiling is committed to a pipeline's
// ratchet only after an accepted writer-count transition. TargetHardChunkLimit
// remains independently binding. Zero means neither a memory-evidence cap nor
// a protocol cap is available.
func runtimeTableChunkSizeCap(
	cfg *config.Config,
	tableRowBytes int64,
	workers int,
	numReaders int,
	numWriters int,
	buffers pool.PipelineBufferSizes,
) int {
	cap, _ := runtimeTableChunkSizeCapDetail(cfg, tableRowBytes, workers, numReaders, numWriters, buffers)
	return cap
}

func runtimeTableChunkSizeCapDetail(
	cfg *config.Config,
	tableRowBytes int64,
	workers int,
	numReaders int,
	numWriters int,
	buffers pool.PipelineBufferSizes,
) (cap int, minimumExceedsBudget bool) {
	if cfg == nil {
		return 0, false
	}
	if workers < 1 {
		workers = 1
	}
	rowBytes, known := tableSizingRowBytes(cfg, tableRowBytes)
	memoryCap := 0
	if known {
		pipelineMB, _ := pipelineBudgetMB(cfg)
		rows, minimumExceeds := pool.SafePipelineChunkSizeDetail(
			pipelineBudgetBytes(pipelineMB),
			workers,
			pool.PipelineBufferConfig{
				RowBytes:   rowBytes,
				NumWriters: numWriters,
				NumReaders: numReaders,
			},
			buffers,
		)
		memoryCap = positiveInt64ToInt(rows)
		minimumExceedsBudget = minimumExceeds
	}
	return minPositiveInt(memoryCap, cfg.Migration.TargetHardChunkLimit), minimumExceedsBudget
}

func minPositiveInt(values ...int) int {
	minimum := 0
	for _, value := range values {
		if value > 0 && (minimum == 0 || value < minimum) {
			minimum = value
		}
	}
	return minimum
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

func capPositiveInt(value, cap int) int {
	if value > 0 && cap > 0 && value > cap {
		return cap
	}
	return value
}

func requestedReaderChunkSize(cfg *config.Config, tuner RuntimeTuner, tableName string) int {
	chunkSize := 0
	if cfg != nil {
		chunkSize = cfg.Migration.ChunkSize
	}
	if tuner != nil {
		if value, ok := tuner.TableChunkSize(tableName); ok && value > 0 {
			chunkSize = value
		} else if value := tuner.Snapshot().ChunkSize; value > 0 {
			chunkSize = value
		}
	}
	return chunkSize
}

func requestedWriterBatchSize(cfg *config.Config, tuner RuntimeTuner, tableName string) int {
	batchSize := 0
	if cfg != nil {
		batchSize = cfg.Migration.ChunkSize
	}
	if tuner != nil {
		if value, ok := tuner.TableBatchSize(tableName); ok && value > 0 {
			batchSize = value
		} else if value := tuner.Snapshot().ChunkSize; value > 0 {
			batchSize = value
		}
	}
	return batchSize
}

// calculatePipelineBuffers derives both chunkChan and jobChan buffer depths
// for a specific table from the system's memory budget and the table's actual
// Go heap cost per row. The requested chunk remains the steady execution
// policy, matching the pre-epic path. Shared measured-byte admission and the
// process memory guard provide steady backpressure; the complete-inventory
// model remains a fail-closed gate and nonrelaxing ratchet for runtime writer
// transitions. Feeding its static per-table projection into ordinary execution
// made the effective action WAW/PR-dependent and regressed transfer throughput.
func calculatePipelineBuffers(cfg *config.Config, job Job, tableName string, tuner RuntimeTuner, numWriters int, numReaders int, readAheadBuffers int) pool.PipelineBufferSizes {
	tuple := effectiveRuntimeSizingTuple(cfg, tuner)
	chunkSize := requestedReaderChunkSize(cfg, tuner, tableName)
	tableBudgetBytes := perTablePipelineBudgetBytes(cfg, tuple.workers)
	rowBytes, _ := tableSizingRowBytes(cfg, job.Table.EstimatedRowSize)

	return pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		MemoryBudgetBytes: tableBudgetBytes,
		ChunkSize:         chunkSize,
		RowBytes:          rowBytes,
		NumWriters:        numWriters,
		NumReaders:        numReaders,
		ReadAheadBuffers:  readAheadBuffers,
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
		connections := configuredConnectionCount(cfg)
		logging.Warn("transfer memory envelope exhausted by connection-overhead estimate (budget=%d MB, connections=%d at %d MB each); limiting shared pipeline budget to %d MB",
			envelopeMB, connections, connectionOverheadMB, budgetMB)
	}
	return pipelineBudgetBytes(budgetMB)
}

func pipelineBudgetBytes(budgetMB int64) int64 {
	const bytesPerMiB = int64(1024 * 1024)
	if budgetMB <= 0 {
		return 0
	}
	if budgetMB > math.MaxInt64/bytesPerMiB {
		return math.MaxInt64
	}
	return budgetMB * bytesPerMiB
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
