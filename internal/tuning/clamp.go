package tuning

import (
	"math"
	"math/big"
)

const bytesPerMiB int64 = 1024 * 1024

// saturatingMulPositive multiplies two positive values. It saturates at
// MaxInt64 and reports overflow instead of allowing the product to wrap and
// appear memory-safe. Callers validate malformed (non-positive) inputs before
// using it; returning zero for them keeps that contract explicit.
func saturatingMulPositive(a, b int64) (product int64, overflow bool) {
	if a <= 0 || b <= 0 {
		return 0, false
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64, true
	}
	return a * b, false
}

func saturatingAddNonNegative(a, b int64) (sum int64, overflow bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a > math.MaxInt64-b {
		return math.MaxInt64, true
	}
	return a + b, false
}

// bufferCount converts the two independent buffer knobs before adding them so
// their sum cannot overflow an int first. Negative values are malformed;
// a zero on one side is valid as long as the combined count is positive.
func bufferCount(readAheadBuffers, writeAheadWriters int) (int64, bool, bool) {
	if readAheadBuffers < 0 || writeAheadWriters < 0 {
		return 0, false, false
	}
	sum, overflow := saturatingAddNonNegative(int64(readAheadBuffers), int64(writeAheadWriters))
	if sum <= 0 {
		return 0, false, overflow
	}
	return sum, true, overflow
}

// modeledMemoryBytes evaluates workers × buffers × chunk × row width with
// saturating arithmetic. valid=false means the sizing tuple is malformed;
// overflow=true means the real result is larger than MaxInt64 and must be
// treated conservatively as over budget.
func modeledMemoryBytes(workers, readAheadBuffers, writeAheadWriters, chunkSize int, rowBytes int64) (bytes int64, valid, overflow bool) {
	if workers <= 0 || chunkSize <= 0 || rowBytes <= 0 {
		return 0, false, false
	}
	buffers, valid, overflow := bufferCount(readAheadBuffers, writeAheadWriters)
	if !valid {
		return 0, false, overflow
	}

	product := int64(workers)
	for _, factor := range []int64{buffers, int64(chunkSize), rowBytes} {
		var stepOverflow bool
		product, stepOverflow = saturatingMulPositive(product, factor)
		overflow = overflow || stepOverflow
		if overflow {
			return math.MaxInt64, true, true
		}
	}
	return product, true, false
}

// EstimatedMemMB is the approximate working-set footprint in MiB for the
// chosen knobs. workers × (read_ahead + write_ahead) × chunk_size × row width
// is the prevailing model. The estimate rounds up so any non-zero modeled
// allocation consumes at least 1 MiB, and overflowing products saturate rather
// than wrapping into an unsafe small value. Malformed inputs return 0.
func EstimatedMemMB(workers, readAheadBuffers, writeAheadWriters, chunkSize int, rowBytes int64) int64 {
	bytes, valid, _ := modeledMemoryBytes(workers, readAheadBuffers, writeAheadWriters, chunkSize, rowBytes)
	if !valid {
		return 0
	}
	mb := bytes / bytesPerMiB
	if bytes%bytesPerMiB != 0 {
		mb++
	}
	return mb
}

// MemoryEstimateExceedsBudget compares the full modeled byte product with an
// MiB budget without losing overflow information. big.Int is used only for
// this small, pre-run safety comparison; it prevents independently saturated
// values from reversing the result for pathological direct callers.
func MemoryEstimateExceedsBudget(budgetMB int64, workers, readAheadBuffers, writeAheadWriters, chunkSize int, rowBytes int64) bool {
	if budgetMB <= 0 || workers <= 0 || readAheadBuffers < 0 || writeAheadWriters < 0 || chunkSize <= 0 || rowBytes <= 0 {
		return false
	}
	buffers := new(big.Int).SetInt64(int64(readAheadBuffers))
	buffers.Add(buffers, big.NewInt(int64(writeAheadWriters)))
	if buffers.Sign() <= 0 {
		return false
	}

	estimate := new(big.Int).SetInt64(int64(workers))
	estimate.Mul(estimate, buffers)
	estimate.Mul(estimate, big.NewInt(int64(chunkSize)))
	estimate.Mul(estimate, big.NewInt(rowBytes))

	budget := new(big.Int).SetInt64(budgetMB)
	budget.Lsh(budget, 20)
	return estimate.Cmp(budget) > 0
}

// SafeChunkSize returns the largest chunk size in rows that fits inside
// budgetMB at the given workers / buffers / safety-row width. Malformed inputs
// return 0. Otherwise it returns at least 1 so migration can make progress;
// callers that need to distinguish a fitting row from an over-budget minimum
// use safeChunkSizeDetail.
func SafeChunkSize(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int, rowBytes int64) int64 {
	rows, _ := safeChunkSizeDetail(budgetMB, workers, readAheadBuffers, writeAheadWriters, rowBytes)
	return rows
}

// safeChunkSize preserves the package-private entry point used by existing
// tests and internal callers while the exported helper serves other packages.
func safeChunkSize(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int, rowBytes int64) int64 {
	return SafeChunkSize(budgetMB, workers, readAheadBuffers, writeAheadWriters, rowBytes)
}

func safeChunkSizeDetail(budgetMB int64, workers, readAheadBuffers, writeAheadWriters int, rowBytes int64) (rows int64, minimumExceedsBudget bool) {
	if budgetMB <= 0 || workers <= 0 || readAheadBuffers < 0 || writeAheadWriters < 0 || rowBytes <= 0 {
		return 0, false
	}
	buffers := new(big.Int).SetInt64(int64(readAheadBuffers))
	buffers.Add(buffers, big.NewInt(int64(writeAheadWriters)))
	if buffers.Sign() <= 0 {
		return 0, false
	}

	perRow := new(big.Int).SetInt64(int64(workers))
	perRow.Mul(perRow, buffers)
	perRow.Mul(perRow, big.NewInt(rowBytes))
	budget := new(big.Int).SetInt64(budgetMB)
	budget.Lsh(budget, 20)
	if perRow.Cmp(budget) > 0 {
		return 1, true
	}
	rowsBig := new(big.Int).Quo(budget, perRow)
	if rowsBig.Sign() <= 0 {
		return 1, true
	}
	if !rowsBig.IsInt64() {
		return math.MaxInt64, false
	}
	return rowsBig.Int64(), false
}

// applyMemoryClamp enforces the caller-resolved memory budget on out.ChunkSize
// using SafetyRowBytes, then recomputes EstimatedMemMB. Representative and
// legacy average widths intentionally do not participate in this hard-safety
// path. A non-positive budget remains nonbinding, though the estimate is still
// populated.
//
// If even one row exceeds the budget, ChunkSize is clamped to the one-row
// minimum-progress fallback and MemoryEstimateOverBudget remains true. The
// reasoning explicitly says that this is not a fitting configuration.
func applyMemoryClamp(out *Output, in Input) {
	rowBytes, known := in.safetyRowBytes()
	widthSource := "unobserved fallback estimate"
	if known {
		widthSource = "widest observed table-average model; not a per-row bound"
	} else {
		// Width provenance matters even when no clamp fires: a formula/load
		// fallback is an estimate, not evidence that schema inspection found a
		// 500-byte row. Keep that distinction in the persisted reasoning.
		out.Reasoning = appendReasoning(out.Reasoning,
			"memory estimate: safety width %d B, unobserved fallback estimate (no positive schema width)",
			rowBytes,
		)
	}

	budgetMB := in.MemoryBudgetMB
	overBudget := MemoryEstimateExceedsBudget(
		budgetMB,
		out.Workers,
		out.ReadAheadBuffers,
		out.WriteAheadWriters,
		out.ChunkSize,
		rowBytes,
	)

	if overBudget {
		safe, minimumExceeds := safeChunkSizeDetail(
			budgetMB,
			out.Workers,
			out.ReadAheadBuffers,
			out.WriteAheadWriters,
			rowBytes,
		)
		if safe > 0 && safe < int64(out.ChunkSize) {
			oldCS := out.ChunkSize
			out.ChunkSize = int(safe) // safe < current int, so the cast fits.
			if minimumExceeds {
				out.Reasoning = appendReasoning(out.Reasoning,
					"memory clamp: chunk_size %d → 1 row minimum-progress fallback; one modeled row still exceeds budget %d MB (safety width %d B, %s)",
					oldCS, budgetMB, rowBytes, widthSource,
				)
			} else {
				out.Reasoning = appendReasoning(out.Reasoning,
					"memory clamp: chunk_size %d → %d rows (budget %d MB, safety width %d B, %s)",
					oldCS, out.ChunkSize, budgetMB, rowBytes, widthSource,
				)
			}
		} else if minimumExceeds {
			// A pre-existing one-row recommendation still needs an explicit
			// warning; silence here would falsely suggest the estimate fits.
			out.Reasoning = appendReasoning(out.Reasoning,
				"memory clamp: 1-row minimum-progress fallback still exceeds budget %d MB (safety width %d B, %s)",
				budgetMB, rowBytes, widthSource,
			)
		}
	}

	out.EstimatedMemMB = EstimatedMemMB(
		out.Workers,
		out.ReadAheadBuffers,
		out.WriteAheadWriters,
		out.ChunkSize,
		rowBytes,
	)

	out.MemoryEstimateOverBudget = MemoryEstimateExceedsBudget(
		budgetMB,
		out.Workers,
		out.ReadAheadBuffers,
		out.WriteAheadWriters,
		out.ChunkSize,
		rowBytes,
	)
}
