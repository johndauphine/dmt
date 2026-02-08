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
				"AI Error Diagnosis",
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

func TestErrorDiagnosis_Format_Structure(t *testing.T) {
	diag := &ErrorDiagnosis{
		Cause:       "Test cause",
		Suggestions: []string{"Suggestion A", "Suggestion B", "Suggestion C"},
		Confidence:  "high",
		Category:    "constraint",
	}

	got := diag.Format()
	lines := strings.Split(got, "\n")

	// Verify basic structure
	if len(lines) < 5 {
		t.Errorf("Format() output too short, got %d lines", len(lines))
	}

	// First line should be the title
	if !strings.Contains(lines[0], "AI Error Diagnosis") {
		t.Errorf("First line should contain title, got: %s", lines[0])
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

	// Should contain box characters
	if !strings.Contains(got, "┌") || !strings.Contains(got, "┐") {
		t.Error("FormatBox() should contain top border characters")
	}
	if !strings.Contains(got, "└") || !strings.Contains(got, "┘") {
		t.Error("FormatBox() should contain bottom border characters")
	}
	if !strings.Contains(got, "│") {
		t.Error("FormatBox() should contain vertical border characters")
	}

	// Should contain content
	if !strings.Contains(got, "AI Error Diagnosis") {
		t.Error("FormatBox() should contain title")
	}
	if !strings.Contains(got, "Data type mismatch") {
		t.Error("FormatBox() should contain cause")
	}
	if !strings.Contains(got, "Fix 1") || !strings.Contains(got, "Fix 2") {
		t.Error("FormatBox() should contain suggestions")
	}
}

func TestDiagnoseSchemaError_NoPanic(t *testing.T) {
	ctx := context.Background()
	// Calling with nil error should not panic
	// DiagnoseSchemaError now emits via handler (or logs as fallback)
	DiagnoseSchemaError(ctx, "test_table", "public", "postgres", "mssql", "CREATE TABLE", nil)
	// Just verify no panic
}

func TestSetDiagnosisHandler(t *testing.T) {
	var received *ErrorDiagnosis

	// Register a handler
	SetDiagnosisHandler(func(d *ErrorDiagnosis) {
		received = d
	})
	defer SetDiagnosisHandler(nil)

	// Emit a diagnosis
	diag := &ErrorDiagnosis{
		Cause:       "Test cause",
		Suggestions: []string{"Fix it"},
		Confidence:  "high",
		Category:    "other",
	}
	EmitDiagnosis(diag)

	// Verify handler received it
	if received == nil {
		t.Error("Handler should have received diagnosis")
	}
	if received.Cause != "Test cause" {
		t.Errorf("Expected cause 'Test cause', got '%s'", received.Cause)
	}
}
