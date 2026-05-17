package tuning

import "math"

// safeAvgRowBytes guards against historical rows that recorded
// AvgRowBytes=0 (older schema, manual inserts, or corrupted writes).
// Falls back to 500 — the same heuristic the analyzer's
// calculateAvgRowSize uses when no row stats are available (#157).
func safeAvgRowBytes(v int64) int64 {
	if v <= 0 {
		return 500
	}
	return v
}

// SafeAvgRowBytes is the exported entry point for callers outside the
// tuning package (e.g. the smartconfig adapter in internal/driver) that
// need the same fallback policy when converting rows/sec → bytes/sec for
// FinalThroughputBytes (#224). Keeps the fallback constant in one place.
func SafeAvgRowBytes(v int64) int64 { return safeAvgRowBytes(v) }

// throughputBytes returns the row's throughput in bytes/sec for the
// regression's y vector (#224). Prefers FinalThroughputBytes when the
// adapter populated it; falls back to FinalThroughput × safeAvgRowBytes
// for older fixtures and test rows that only set the rows/sec field.
// Returning float64 (not int64) keeps the OLS solve unchanged — bytes/sec
// from a multiply-then-cast in the adapter is exactly representable up
// past the petabytes-per-second range, well past any realistic value.
func throughputBytes(r HistoryRecord) float64 {
	if r.FinalThroughputBytes > 0 {
		return float64(r.FinalThroughputBytes)
	}
	return r.FinalThroughput * float64(safeAvgRowBytes(r.AvgRowBytes))
}

func standardize(x, mean, std float64) float64 {
	if std <= 0 {
		return 0
	}
	return (x - mean) / std
}

// stddevSafe returns sqrt(E[X²] - (E[X])²), or 1 when the variance is
// zero or the count is degenerate. Returning 1 keeps standardize() a
// no-op rather than producing NaN when a feature is constant.
func stddevSafe(sqSum, mean, n float64) float64 {
	if n <= 0 {
		return 1
	}
	variance := sqSum/n - mean*mean
	if variance <= 0 {
		return 1
	}
	return math.Sqrt(variance)
}
