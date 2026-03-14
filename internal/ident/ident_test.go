package ident

import "testing"

func TestSanitizePG(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", "col_"},
		{"simple lowercase", "userid", "userid"},
		{"uppercase", "PackedByPersonID", "packedbypersonid"},
		{"with underscore", "last_edited_by", "last_edited_by"},
		{"special chars", "User-Id", "user_id"},
		{"starts with digit", "1column", "col_1column"},
		{"accented chars", "Ñoño", "ñoño"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizePG(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizePG(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
