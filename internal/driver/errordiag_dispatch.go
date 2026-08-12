package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmt/v5/internal/driver/errordiag"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/observability"
)

// LookupDeterministicDiagnosis returns a diagnosis if the target driver's
// catalog has a matching pattern. driverName may be a canonical name or
// alias — it's normalized via Canonicalize. Returns nil if no pattern
// matches. The second return value is the pattern name that matched
// (for future #176 telemetry); empty when nothing matched.
func LookupDeterministicDiagnosis(driverName, errMsg string) (*ErrorDiagnosis, string) {
	m, ok := errordiag.Lookup(Canonicalize(driverName), errMsg)
	if !ok {
		return nil, ""
	}
	return &ErrorDiagnosis{
		Cause:       m.Diagnosis.Cause,
		Suggestions: m.Diagnosis.Suggestions,
		Confidence:  m.Diagnosis.Confidence,
		Category:    m.Diagnosis.Category,
	}, m.PatternName
}

// DiagnoseError is the unified entry point for error diagnosis.
// It consults the deterministic catalog (#173); on a miss it returns
// a generic "no diagnosis available" stub so the user still sees a
// useful hint next to the raw error.
//
// dmt previously had an AI fallback here. It was removed because the
// long-tail value (rare/vendor-specific errors) did not justify
// sending error-message content — which routinely contains row data
// like email addresses, SSNs, and other PII — to a third-party LLM.
// Catalog growth happens via the unmatched-error log signal below
// (suitable for telemetry under #176), not via opportunistic LLM calls.
//
// Returns nil only when ctx is canceled or errCtx is nil — callers
// should otherwise expect a non-nil result they can emit.
func DiagnoseError(ctx context.Context, errCtx *ErrorContext) *ErrorDiagnosis {
	if errCtx == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return nil
	}

	if diag, pattern := LookupDeterministicDiagnosis(errCtx.TargetDBType, errCtx.ErrorMessage); diag != nil {
		logging.Debug("error diagnosis: deterministic match pattern=%q", pattern)
		// Deterministic match — no fallback fired, nothing to record.
		// The errordiag fallback counter only fires on the unmatched
		// path below (#176 — surface is a catalog-growth signal).
		return diag
	}

	// Catalog growth signal. We log a SHA-256 hash (stable identifier
	// for "this exact error showed up N times") plus a scrubbed prefix
	// (everything up to the first quote / colon-data / newline, since
	// those are where real DB drivers put row content like emails, IDs,
	// and DETAIL clauses). This keeps the catalog-growth signal alive
	// without re-introducing the PII-egress problem that motivated
	// removing the AI path in this PR.
	prefix, hash := errorFingerprint(errCtx.ErrorMessage)
	logging.Debug("error diagnosis: no deterministic pattern for driver=%q prefix=%q hash=%s",
		Canonicalize(errCtx.TargetDBType), prefix, hash)
	// Persist only the normalized (driver, scrubbed prefix) pair —
	// the SHA-256 hash is row-message-specific and a bulk load that
	// fails on 10K rows would produce one fingerprint per row,
	// exploding fallback_events. The prefix is already scrubbed and
	// rune-truncated upstream, so it groups the same catalog gap
	// across distinct row values. The hash stays in the debug log
	// for "we saw this exact message N times" forensics where the
	// row content is acceptable to retain (codex review on #176).
	observability.RecordFallback(observability.SurfaceErrordiag,
		fmt.Sprintf("%s|%s", Canonicalize(errCtx.TargetDBType), prefix))

	return noDiagnosisAvailable()
}

// DiagnoseSchemaError diagnoses a DDL/schema error and emits the
// diagnosis through the registered handler (or logging fallback).
// Convenience wrapper used by the orchestrator's target-mode strategies
// for CREATE TABLE / PK / INDEX / FK / CHECK failures.
func DiagnoseSchemaError(ctx context.Context, tableName, tableSchema, sourceDBType, targetDBType, operation string, err error) {
	if err == nil {
		return
	}
	errCtx := &ErrorContext{
		ErrorMessage: fmt.Sprintf("%s: %v", operation, err),
		TableName:    tableName,
		TableSchema:  tableSchema,
		SourceDBType: sourceDBType,
		TargetDBType: targetDBType,
	}
	if diag := DiagnoseError(ctx, errCtx); diag != nil {
		EmitDiagnosis(diag)
	}
}

// noDiagnosisAvailable is the sentinel returned when the deterministic
// catalog does not match. Returning *something* is more useful than
// silence — the operator sees the raw error plus a hint that no canned
// suggestion exists for this pattern and a pointer to where to add one.
func noDiagnosisAvailable() *ErrorDiagnosis {
	return &ErrorDiagnosis{
		Cause: "No automatic diagnosis available for this error pattern.",
		Suggestions: []string{
			"Review the raw error message above for clues.",
			"Consult the target driver's documentation for the relevant error code.",
			"If this error class is common, consider adding a pattern to internal/driver/errordiag (#173).",
		},
		Confidence: "low",
		Category:   "other",
	}
}

// errorFingerprint returns a (prefix, hash) pair suitable for logging
// an unmatched error without leaking row content. The prefix is the
// leading structural phrase (everything before the first quote, colon,
// or newline — the load-bearing parts DB drivers use to put row
// values, DETAIL clauses, and quoted identifiers) rune-truncated to a
// short bound. The hash is a SHA-256 hex prefix over the full message,
// stable across runs so a maintainer can count "we saw this exact
// error N times this week" without preserving the message text.
const errorPrefixMaxRunes = 60

func errorFingerprint(msg string) (prefix string, hash string) {
	sum := sha256.Sum256([]byte(msg))
	hash = hex.EncodeToString(sum[:8])

	// First line only — DB drivers put DETAIL on subsequent lines.
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	// Strip from the first quote (single or double) onward — that's
	// where constraint names, table identifiers, and row values live.
	if i := strings.IndexAny(msg, "\"'`"); i >= 0 {
		msg = msg[:i]
	}
	// Strip from "value: <X>" / "data type X" colons onward where the
	// value would otherwise follow.
	if i := strings.IndexByte(msg, ':'); i >= 0 {
		// Keep prefixes like "pq:" / "Error 1062:" / "mssql:" by
		// permitting an early colon, but drop later ones.
		if i > 12 {
			msg = msg[:i]
		}
	}
	prefix = truncateRunes(strings.TrimSpace(msg), errorPrefixMaxRunes)
	return prefix, hash
}

// truncateRunes returns s if it fits in max runes; otherwise the
// rune-aligned prefix followed by an ellipsis. Safe against splitting
// a multi-byte UTF-8 codepoint.
func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max {
			return s[:i] + "…"
		}
		n++
	}
	return s
}
