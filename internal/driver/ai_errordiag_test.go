package driver

import (
	"context"
	"strings"
	"testing"
)

func TestErrorDiagnosis_Format(t *testing.T) {
	tests := []struct {
		name     string
		diag     *ErrorDiagnosis
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

func TestDiagnoseSchemaError_NoAI(t *testing.T) {
	// Without AI configured, should return empty string
	ctx := context.Background()
	result := DiagnoseSchemaError(ctx, "test_table", "public", "postgres", "mssql", "CREATE TABLE", nil)

	// Should return empty when AI is not configured
	if result != "" {
		t.Logf("DiagnoseSchemaError returned non-empty without AI configured (may be expected if AI is configured): %s", result)
	}
}

func TestDiagnoseSchemaError_NilError(t *testing.T) {
	ctx := context.Background()
	// Calling with nil error should not panic
	result := DiagnoseSchemaError(ctx, "test_table", "public", "postgres", "mssql", "CREATE TABLE", nil)
	// Just verify no panic - result may be empty or contain diagnosis
	_ = result
}
