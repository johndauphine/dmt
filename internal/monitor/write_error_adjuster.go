// Deterministic per-write-error chunk-size adjuster — the rule-based
// replacement for the AI write-error adjuster removed in PR #195.
//
// Some write failures aren't transient — they signal a structural
// limit was exceeded. The most common are placeholder/parameter caps:
//
//	MySQL  : "Prepared statement contains too many placeholders" — the
//	         driver-side prepared statement has a 65535-parameter cap;
//	         a row × column-count product exceeding that limit fails
//	         every retry.
//	MSSQL  : "The incoming request has too many parameters" — SQL
//	         Server's stored procedure parameter cap is 2100; same
//	         row × column overflow pattern.
//	MySQL  : "max_allowed_packet" — the server's per-statement byte
//	         budget; large rows × large chunk overflows the network
//	         buffer.
//
// All three are addressed by the same response: halve the chunk size
// for this table and retry. The retry takes effect at the next
// transfer.Execute call (writerPool.evaluateAndAdjust calls
// SetTableChunkSize / SetTableBatchSize on the runtime tuner).
//
// Returns 0 when the error message doesn't match any known limit
// pattern — the caller takes that as "not a known structural error,
// don't adjust." Other failures (network errors, deadlocks, missing
// tables) bubble up through the normal retry / abort logic.

package monitor

import (
	"context"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/transfer"
)

// RuleWriteErrorAdjuster implements transfer.WriteErrorAdjuster with
// a small set of known-error patterns. Stateless; safe to share
// across goroutines.
type RuleWriteErrorAdjuster struct{}

// NewRuleWriteErrorAdjuster returns the deterministic adjuster.
// Constructor exists for symmetry with other monitor types and to
// give the wiring layer a stable factory.
func NewRuleWriteErrorAdjuster() *RuleWriteErrorAdjuster {
	return &RuleWriteErrorAdjuster{}
}

// EvaluateWriteError implements transfer.WriteErrorAdjuster. Returns
// the suggested next chunk size (in rows), or 0 when the error isn't
// one we recognize as a structural-limit failure.
//
// The "halve" response is conservative — for a 65k-placeholder cap on
// a 50-column table, the breakpoint is around 1300 rows; halving from
// (say) 5000 to 2500 may not be enough to clear it on the next try,
// but the retry loop will halve again. Two or three halvings get us
// well under any practical cap. Going more aggressively (×0.25 or
// ×0.1) would converge faster but risks shrinking past the
// throughput-optimal range when the original failure was a one-off.
func (a *RuleWriteErrorAdjuster) EvaluateWriteError(_ context.Context, errCtx transfer.WriteErrorContext) int {
	if !isStructuralLimitError(errCtx.ErrorMessage) {
		return 0
	}
	next := errCtx.RowCount / 2
	if next < 1 {
		next = 1
	}
	return next
}

// isStructuralLimitError checks the error message for known patterns
// that indicate a per-statement structural limit was exceeded.
// Case-insensitive — the underlying drivers use different casing
// (lowercase from MySQL's protocol, mixed from go-mssqldb).
func isStructuralLimitError(msg string) bool {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "too many placeholders"):
		// MySQL: prepared statement parameter cap (65535 placeholders).
		return true
	case strings.Contains(lower, "too many parameters"):
		// MSSQL: stored procedure parameter cap (2100).
		return true
	case strings.Contains(lower, "max_allowed_packet"):
		// MySQL: per-statement byte budget exceeded.
		return true
	case strings.Contains(lower, "request size") && strings.Contains(lower, "exceed"):
		// MSSQL: occasional alternate phrasing for oversized batches.
		return true
	}
	return false
}
