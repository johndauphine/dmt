package driver

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver/errordiag"
	"github.com/johndauphine/dmt/internal/logging"
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
		// TODO(#176): record observability event {surface: "errordiag", source: "deterministic", pattern}.
		return diag
	}

	// Catalog growth signal: log the first 80 chars of any unmatched error
	// so a maintainer reviewing the logs (or, after #176, scanning the
	// telemetry counter) can decide which patterns to add to the catalog.
	logging.Debug("error diagnosis: no deterministic pattern for driver=%q msg=%q",
		Canonicalize(errCtx.TargetDBType), truncateForFingerprint(errCtx.ErrorMessage, 80))
	// TODO(#176): record observability event {surface: "errordiag", source: "unmatched", fingerprint}.

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

// truncateForFingerprint returns a length-bounded version of s suitable
// for inclusion in a log fingerprint. Short and stable; the goal is a
// human-readable hint, not a cryptographic identifier.
func truncateForFingerprint(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
