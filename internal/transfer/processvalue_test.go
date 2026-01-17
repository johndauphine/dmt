package transfer

import (
	"testing"

	"github.com/godror/godror"
)

func TestProcessValueGodrorNumber(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		colType  string
		expected any
	}{
		{
			name:     "godror.Number positive integer",
			val:      godror.Number("12345"),
			colType:  "NUMBER",
			expected: "12345",
		},
		{
			name:     "godror.Number negative integer",
			val:      godror.Number("-9876"),
			colType:  "NUMBER",
			expected: "-9876",
		},
		{
			name:     "godror.Number decimal",
			val:      godror.Number("123.456789"),
			colType:  "NUMBER(10,6)",
			expected: "123.456789",
		},
		{
			name:     "godror.Number large number preserves precision",
			val:      godror.Number("99999999999999999999"),
			colType:  "NUMBER(38)",
			expected: "99999999999999999999",
		},
		{
			name:     "godror.Number scientific notation",
			val:      godror.Number("1.23E10"),
			colType:  "NUMBER",
			expected: "1.23E10",
		},
		{
			name:     "godror.Number zero",
			val:      godror.Number("0"),
			colType:  "NUMBER",
			expected: "0",
		},
		{
			name:     "nil value unchanged",
			val:      nil,
			colType:  "NUMBER",
			expected: nil,
		},
		{
			name:     "string unchanged",
			val:      "test string",
			colType:  "VARCHAR2",
			expected: "test string",
		},
		{
			name:     "int64 unchanged",
			val:      int64(12345),
			colType:  "NUMBER",
			expected: int64(12345),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processValue(tt.val, tt.colType)
			if result != tt.expected {
				t.Errorf("processValue(%v, %q) = %v (%T), want %v (%T)",
					tt.val, tt.colType, result, result, tt.expected, tt.expected)
			}
		})
	}
}
