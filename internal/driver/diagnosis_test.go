package driver

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestErrorDiagnosis_Format(t *testing.T) {
	tests := []struct {
		name         string
		diag         *ErrorDiagnosis
		wantContains []string
	}{
		{
			name: "basic diagnosis",
			diag: &ErrorDiagnosis{
				Cause:       "Data type mismatch",
				Suggestions: []string{"Fix 1", "Fix 2"},
				Confidence:  "high",
				Category:    "type_mismatch",
			},
			wantContains: []string{
				"Error Diagnosis",
				"Cause: Data type mismatch",
				"Suggestions:",
				"1. Fix 1",
				"2. Fix 2",
				"Confidence: high",
				"Category: type_mismatch",
			},
		},
		{
			name: "single suggestion",
			diag: &ErrorDiagnosis{
				Cause:       "Connection timeout",
				Suggestions: []string{"Retry the operation"},
				Confidence:  "medium",
				Category:    "connection",
			},
			wantContains: []string{
				"Cause: Connection timeout",
				"1. Retry the operation",
				"Confidence: medium",
				"Category: connection",
			},
		},
		{
			name: "empty suggestions",
			diag: &ErrorDiagnosis{
				Cause:       "Unknown error",
				Suggestions: []string{},
				Confidence:  "low",
				Category:    "other",
			},
			wantContains: []string{
				"Cause: Unknown error",
				"Suggestions:",
				"Confidence: low",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.diag.Format()
			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("Format() missing expected content %q\nGot:\n%s", want, got)
				}
			}
		})
	}
}

func TestErrorDiagnosis_FormatBox(t *testing.T) {
	diag := &ErrorDiagnosis{
		Cause:       "Data type mismatch",
		Suggestions: []string{"Fix 1", "Fix 2"},
		Confidence:  "high",
		Category:    "type_mismatch",
	}

	got := diag.FormatBox()

	// Box borders present.
	for _, ch := range []string{"┌", "┐", "└", "┘", "│"} {
		if !strings.Contains(got, ch) {
			t.Errorf("FormatBox() should contain border character %q", ch)
		}
	}

	// Content present.
	for _, want := range []string{"Error Diagnosis", "Data type mismatch", "Fix 1", "Fix 2"} {
		if !strings.Contains(got, want) {
			t.Errorf("FormatBox() should contain %q", want)
		}
	}
}

func TestEmitDiagnosis_HandlerReceives(t *testing.T) {
	var received *ErrorDiagnosis

	SetDiagnosisHandler(func(d *ErrorDiagnosis) {
		received = d
	})
	defer SetDiagnosisHandler(nil)

	diag := &ErrorDiagnosis{
		Cause:       "Test cause",
		Suggestions: []string{"Fix it"},
		Confidence:  "high",
		Category:    "other",
	}
	EmitDiagnosis(diag)

	if received == nil {
		t.Fatal("handler should have received the diagnosis")
	}
	if received.Cause != "Test cause" {
		t.Errorf("Cause mismatch: got %q, want %q", received.Cause, "Test cause")
	}
}

func TestEmitDiagnosis_NilIsNoOp(t *testing.T) {
	called := false
	SetDiagnosisHandler(func(_ *ErrorDiagnosis) { called = true })
	defer SetDiagnosisHandler(nil)

	EmitDiagnosis(nil)
	if called {
		t.Error("handler should not be invoked on nil diagnosis")
	}
}

func TestDiagnoseSchemaError_NilErrorIsNoOp(t *testing.T) {
	called := false
	SetDiagnosisHandler(func(_ *ErrorDiagnosis) { called = true })
	defer SetDiagnosisHandler(nil)

	DiagnoseSchemaError(context.Background(), "tbl", "schema", "postgres", "postgres", "CREATE TABLE", nil)
	if called {
		t.Error("handler should not be invoked when err is nil")
	}
}

func TestDiagnoseError_DeterministicMatchEmits(t *testing.T) {
	var received *ErrorDiagnosis
	SetDiagnosisHandler(func(d *ErrorDiagnosis) { received = d })
	defer SetDiagnosisHandler(nil)

	errCtx := &ErrorContext{
		ErrorMessage: `pq: duplicate key value violates unique constraint "users_pkey"`,
		TargetDBType: "postgres",
	}
	diag := DiagnoseError(context.Background(), errCtx)
	if diag == nil {
		t.Fatal("expected a diagnosis from a known PG pattern")
	}
	EmitDiagnosis(diag)
	if received == nil {
		t.Fatal("handler should have received diagnosis")
	}
	if !strings.Contains(received.Cause, "users_pkey") {
		t.Errorf("Cause should mention the constraint name; got %q", received.Cause)
	}
	if received.Category != "constraint" {
		t.Errorf("expected category=constraint, got %q", received.Category)
	}
}

func TestDiagnoseError_NoMatchReturnsStub(t *testing.T) {
	errCtx := &ErrorContext{
		ErrorMessage: "some completely unhandled gibberish error xyzzy",
		TargetDBType: "postgres",
	}
	diag := DiagnoseError(context.Background(), errCtx)
	if diag == nil {
		t.Fatal("expected the no-diagnosis-available stub on a miss")
	}
	if !strings.Contains(diag.Cause, "No automatic diagnosis") {
		t.Errorf("expected stub Cause to contain 'No automatic diagnosis'; got %q", diag.Cause)
	}
	if diag.Category != "other" || diag.Confidence != "low" {
		t.Errorf("stub diagnosis has unexpected metadata: category=%q confidence=%q",
			diag.Category, diag.Confidence)
	}
}

func TestDiagnoseError_NilErrCtx(t *testing.T) {
	if d := DiagnoseError(context.Background(), nil); d != nil {
		t.Errorf("DiagnoseError(nil errCtx) should return nil, got %+v", d)
	}
}

func TestLookupDeterministicDiagnosis_KnownPattern(t *testing.T) {
	diag, pattern := LookupDeterministicDiagnosis("postgres", `pq: deadlock detected`)
	if diag == nil {
		t.Fatal("expected a deterministic match for the deadlock pattern")
	}
	if pattern == "" {
		t.Error("PatternName should be populated when there's a match")
	}
}

func TestLookupDeterministicDiagnosis_UnknownPattern(t *testing.T) {
	diag, pattern := LookupDeterministicDiagnosis("postgres", "no such pattern fits this string")
	if diag != nil || pattern != "" {
		t.Errorf("expected nil diagnosis and empty pattern on miss; got diag=%+v pattern=%q", diag, pattern)
	}
}

func TestLookupDeterministicDiagnosis_AliasNormalization(t *testing.T) {
	// "pg" is a registered alias of postgres; Canonicalize should map it.
	diag, _ := LookupDeterministicDiagnosis("pg", `pq: deadlock detected`)
	if diag == nil {
		t.Fatal("alias 'pg' should resolve to postgres catalog")
	}
}

// TestErrorFingerprint_StripsRowContent guards the PII-leak fix
// (Copilot review on PR #237). The unmatched-error log must NOT
// surface quoted identifiers, single-quoted values, or DETAIL clauses
// — those are exactly where real DB drivers put row content.
func TestErrorFingerprint_StripsRowContent(t *testing.T) {
	tests := []struct {
		name      string
		msg       string
		mustNot   string // substring that must be absent from prefix
		mustStart string // substring the prefix should still begin with
	}{
		{
			name:      "pg duplicate key with email in detail",
			msg:       "pq: duplicate key value violates unique constraint \"users_email_key\"\nDETAIL: Key (email)=(alice@hospital.com) already exists.",
			mustNot:   "alice@hospital.com",
			mustStart: "pq: duplicate key value",
		},
		{
			name:      "pg invalid input with quoted value",
			msg:       `pq: invalid input syntax for type integer: "SSN-123-45-6789"`,
			mustNot:   "SSN-123-45-6789",
			mustStart: "pq: invalid input syntax for type integer",
		},
		{
			name:      "mysql duplicate entry with row value",
			msg:       "Error 1062 (23000): Duplicate entry 'alice@hospital.com' for key 'users.email_unique'",
			mustNot:   "alice@hospital.com",
			mustStart: "Error 1062",
		},
		{
			name:      "mssql null violation with table name",
			msg:       "mssql: Cannot insert the value NULL into column 'email', table 'prod.dbo.users'; column does not allow nulls",
			mustNot:   "prod.dbo.users",
			mustStart: "mssql: Cannot insert the value NULL into column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, hash := errorFingerprint(tt.msg)
			if strings.Contains(prefix, tt.mustNot) {
				t.Errorf("prefix should not contain %q (PII leak); got %q", tt.mustNot, prefix)
			}
			if tt.mustStart != "" && !strings.HasPrefix(prefix, tt.mustStart) {
				t.Errorf("prefix should start with %q; got %q", tt.mustStart, prefix)
			}
			if len(hash) != 16 {
				t.Errorf("hash should be 16 hex chars (8 bytes); got %d (%q)", len(hash), hash)
			}
		})
	}
}

// TestErrorFingerprint_StableHash asserts the hash is deterministic
// across calls — required so a maintainer can correlate "we saw this
// exact error N times" across runs.
func TestErrorFingerprint_StableHash(t *testing.T) {
	const msg = "pq: deadlock detected"
	_, h1 := errorFingerprint(msg)
	_, h2 := errorFingerprint(msg)
	if h1 != h2 {
		t.Errorf("hash should be deterministic; got %q vs %q", h1, h2)
	}
	if h1 == "" {
		t.Error("hash should be non-empty")
	}
}

// TestErrorFingerprint_RuneSafeTruncation guards against splitting a
// multi-byte UTF-8 codepoint when truncating. Pre-fix the function
// sliced by byte; that produced invalid UTF-8 in the log.
func TestErrorFingerprint_RuneSafeTruncation(t *testing.T) {
	// Build a message of all multi-byte runes longer than the rune cap.
	// Each "あ" is 3 bytes in UTF-8.
	long := strings.Repeat("あ", errorPrefixMaxRunes+10)
	prefix, _ := errorFingerprint(long)
	if !utf8.ValidString(prefix) {
		t.Errorf("prefix is not valid UTF-8 — byte-slice split a rune; got %q", prefix)
	}
}
