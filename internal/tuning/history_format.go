package tuning

import "fmt"

// formatPredictionInterval formats the regression's 95% PI for display
// in the reasoning string. Inputs are in BYTES per second (#224 —
// matches the regression's new dependent variable). Picks units (MB/s
// vs GB/s) via formatBytesPerSec so the line stays readable. Always
// emits both endpoints in the same unit for easy visual comparison.
//
// Negative low values (the PI can extend below zero when uncertainty is
// large) are clamped to 0 in the display — a "throughput could be
// negative" string would mislead users; the information that matters is
// "the lower bound is essentially zero, model has very low confidence."
//
// Width cap (#218): when the upper bound is physically absurd
// (>piAbsoluteCeiling bytes/s) or far beyond the predicted value
// (>piRelativeRatio × pred), the raw number is replaced with a
// "wide" marker. The math is technically correct in that case — the
// model is honestly saying "I have no idea at this x*" — but printing
// 9.6e23 erodes operator trust ("did dmt overflow?") and the lower
// bound is already clamped to 0, so the numeric range carries no
// usable information. The marker preserves the no-confidence signal
// without the misleading number. Lives near the rest of the
// reasoning-string format so future tweaks stay co-located.
func formatPredictionInterval(pred, low, high float64) string {
	if high > piAbsoluteCeiling || (pred > 0 && high > piRelativeRatio*pred) {
		return "wide — low model confidence at this point"
	}
	if low < 0 {
		low = 0
	}
	// Pick the unit from the upper bound so both endpoints render at
	// the same scale (Copilot review on PR #289). Without this a PI
	// of [500 MB/s, 2 GB/s] rendered as "500 MB/s–2.00 GB/s" — same
	// quantity in mixed units defeats the visual comparison the docs
	// above promised.
	unit := bytesPerSecUnit(high)
	return fmt.Sprintf("%s–%s", formatBytesPerSecAs(low, unit), formatBytesPerSecAs(high, unit))
}

// formatBytesPerSec renders a bytes/sec value with units the operator
// can scan at a glance (#224). MB/s for typical disk-bound workloads
// (5-2000 MB/s on modern NVMe + PG COPY), GB/s for the rare very-fast
// case (e.g. small-row in-memory targets). Sub-1MB/s renders in MB/s
// with two decimal places rather than collapsing to KB/s — operators
// care about the order of magnitude more than the last digit, and a
// uniform unit makes side-by-side comparison easier.
//
// Negative inputs are clamped to 0 (Copilot review on PR #289): the
// regression can predict a negative bytes/sec when poorly conditioned,
// and "-3 MB/s" in the operator's reasoning log erodes trust without
// communicating the underlying model-confidence problem any better
// than "0 MB/s" does (the PI's "wide" marker is the real signal).
func formatBytesPerSec(bytesPerSec float64) string {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	return formatBytesPerSecAs(bytesPerSec, bytesPerSecUnit(bytesPerSec))
}

// bytesPerSecUnit picks "GB/s" or "MB/s" by magnitude. Extracted so
// formatPredictionInterval can choose one unit for both endpoints.
func bytesPerSecUnit(bytesPerSec float64) string {
	if bytesPerSec >= 1_000_000_000 {
		return "GB/s"
	}
	return "MB/s"
}

// formatBytesPerSecAs formats with a caller-supplied unit. Used by
// formatPredictionInterval to force matching units across endpoints.
func formatBytesPerSecAs(bytesPerSec float64, unit string) string {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	if unit == "GB/s" {
		return fmt.Sprintf("%.2f GB/s", bytesPerSec/1_000_000_000)
	}
	return fmt.Sprintf("%.0f MB/s", bytesPerSec/1_000_000)
}

// piAbsoluteCeiling caps the displayed PI upper bound at a value well
// past any realistic throughput for the databases dmt targets, in
// BYTES per second (#224 — re-anchored from the old rows/sec ceiling).
// Fast NVMe + PG COPY tops out around 2-5 GB/s on dense workloads;
// 1 TB/s is two-plus orders of magnitude past that and catches the
// leverage-explosion case from #218 without false-positiving on
// aggressive but real predictions.
const piAbsoluteCeiling = 1_000_000_000_000.0 // 1 TB/s

// piRelativeRatio is the second-line cap: when high > k × pred, the
// CI is so wide it tells the operator nothing useful about whether
// the picked config is well-understood. 10× catches the "model has
// some signal but extrapolation blew the upper bound" case where the
// absolute ceiling wouldn't fire (e.g., pred=2 GB/s, high=50 GB/s).
const piRelativeRatio = 10.0

// appendReasoning concatenates a new structured reason onto the existing
// Output.Reasoning string, semicolon-separated.
func appendReasoning(existing, format string, args ...any) string {
	added := fmt.Sprintf(format, args...)
	if existing == "" {
		return added
	}
	return existing + "; " + added
}
