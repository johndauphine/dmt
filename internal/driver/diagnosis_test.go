package driver

import (
	"context"
	"strings"
	"testing"
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
