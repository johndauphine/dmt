package tuning

import "math"

// regressionObservedPredictionFloor is deliberately conservative: the
// selected point is the argmax over every allowed regression candidate, so
// even that best point landing two orders of magnitude below the median
// completed run is evidence that the fitted surface collapsed outside its
// useful support. In that case the measured smoothed-bin path is safer.
const regressionObservedPredictionFloor = 0.01

// validateRegressionPrediction decides whether a fitted point carries enough
// information to change live tuning. It runs before Output is mutated. The
// 95% interval must be computable, finite, ordered, bracket its point estimate,
// and stay wholly above the physically meaningful zero-throughput boundary.
//
// The last two checks catch finite-but-ill-conditioned fits: a selected argmax
// that is effectively zero relative to completed history, or uncertainty more
// than an order of magnitude beyond either the point estimate (the same rule
// used by formatPredictionInterval) or the median observed byte-throughput.
func validateRegressionPrediction(predicted, low, high float64, intervalOK bool, rows []HistoryRecord) (string, bool) {
	if !isFinite(predicted) {
		return "non-finite point estimate", false
	}
	if predicted <= 0 {
		return "nonpositive point estimate", false
	}
	if !intervalOK {
		return "95% prediction interval unavailable", false
	}
	if !isFinite(low) || !isFinite(high) {
		return "non-finite 95% prediction interval", false
	}
	if low > high || predicted < low || predicted > high {
		return "malformed 95% prediction interval", false
	}
	if low <= 0 {
		return "95% prediction interval includes nonpositive throughput", false
	}

	observed := medianObservedThroughputBytes(rows)
	if observed > 0 && predicted < regressionObservedPredictionFloor*observed {
		return "point estimate is effectively zero relative to completed history", false
	}

	uncertainty := math.Max(predicted-low, high-predicted)
	if predictionIntervalTooWide(predicted, high) ||
		(observed > 0 && uncertainty > piRelativeRatio*observed) {
		return "95% prediction interval is materially uninformative", false
	}
	return "", true
}

func medianObservedThroughputBytes(rows []HistoryRecord) float64 {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		value := throughputBytes(row)
		if value > 0 && isFinite(value) {
			values = append(values, value)
		}
	}
	return medianOfFloats(values)
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
