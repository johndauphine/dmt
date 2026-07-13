package tuning

import "math"

const fallbackRowBytes int64 = 500

// safeAvgRowBytes guards against historical rows that recorded
// AvgRowBytes=0 (older schema, manual inserts, or corrupted writes).
// Falls back to 500 — the same heuristic the analyzer's
// calculateAvgRowSize uses when no row stats are available (#157).
func safeAvgRowBytes(v int64) int64 {
	if v <= 0 {
		return fallbackRowBytes
	}
	return v
}

// representativeRowBytes returns the performance-planning width. Deliberately
// do not fall back to AvgRowBytes: that field is the legacy capped model
// feature, while this field has row-count-weighted semantics.
func (in Input) representativeRowBytes() int64 {
	if in.RepresentativeRowBytes > 0 {
		return in.RepresentativeRowBytes
	}
	return fallbackRowBytes
}

// safetyRowBytes returns the widest/fallback width used when hard memory math
// cannot use a complete cardinality profile, plus its observed provenance. A
// non-positive width always clears known, even if a malformed caller set the
// flag, and uses the documented fallback.
func (in Input) safetyRowBytes() (width int64, known bool) {
	if in.SafetyRowBytes > 0 {
		return in.SafetyRowBytes, in.SafetyRowBytesKnown
	}
	return fallbackRowBytes, false
}

// rowsFromBytes converts a positive byte target to a minimum-progress row
// count without allowing an int conversion to wrap on extreme inputs.
func rowsFromBytes(byteCount, rowBytes int64) int {
	if rowBytes <= 0 {
		rowBytes = fallbackRowBytes
	}
	rows := byteCount / rowBytes
	if rows < 1 {
		return 1
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(rows) > uint64(maxInt) {
		return maxInt
	}
	return int(rows)
}

// scaledByteTarget applies the exploration/regression fraction without
// allowing float-to-int conversion at the MaxInt64 boundary to wrap negative.
func scaledByteTarget(byteCount int64, fraction float64) int64 {
	if byteCount <= 0 || !(fraction > 0) {
		return 0
	}
	scaled := float64(byteCount) * fraction
	if math.IsInf(scaled, 0) || scaled >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if scaled < 1 {
		return 0
	}
	return int64(scaled)
}

// scalePositiveIntRatio computes floor(value*numerator/denominator) with
// saturation. It is used for epsilon chunk perturbations so a pathological
// near-MaxInt input cannot wrap negative and appear to be a valid decrease.
func scalePositiveIntRatio(value, numerator, denominator int) int {
	if value <= 0 || numerator <= 0 || denominator <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	quotient, remainder := value/denominator, value%denominator
	if quotient > maxInt/numerator {
		return maxInt
	}
	base := quotient * numerator
	extra := remainder * numerator / denominator // remainder < denominator; current ratios are 3/2 and 67/100.
	if base > maxInt-extra {
		return maxInt
	}
	return base + extra
}

// chunkBytesForModel converts a persisted row-count chunk to the regression's
// legacy byte feature without allowing an overflowing product to wrap around
// and masquerade as a small candidate.
func chunkBytesForModel(chunkSize int, avgRowBytes int64) int64 {
	if chunkSize <= 0 || avgRowBytes <= 0 {
		return 0
	}
	bytes, overflow := saturatingMulPositive(int64(chunkSize), avgRowBytes)
	if overflow {
		return math.MaxInt64
	}
	return bytes
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
	if !(r.FinalThroughput > 0) || math.IsInf(r.FinalThroughput, 0) {
		return 0
	}
	bytes := r.FinalThroughput * float64(safeAvgRowBytes(r.AvgRowBytes))
	if math.IsInf(bytes, 0) || bytes >= float64(math.MaxInt64) {
		return float64(math.MaxInt64)
	}
	return bytes
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
