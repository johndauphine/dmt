package pipeline

import (
	"testing"
)

func TestSplitPKRange(t *testing.T) {
	tests := []struct {
		name     string
		minPK    any
		maxPK    any
		n        int
		expected int // number of ranges
	}{
		{
			name:     "single range",
			minPK:    int64(1),
			maxPK:    int64(100),
			n:        1,
			expected: 1,
		},
		{
			name:     "two ranges",
			minPK:    int64(1),
			maxPK:    int64(100),
			n:        2,
			expected: 2,
		},
		{
			name:     "four ranges",
			minPK:    int64(1),
			maxPK:    int64(100),
			n:        4,
			expected: 4,
		},
		{
			name:     "non-integer pk",
			minPK:    "abc",
			maxPK:    "xyz",
			n:        4,
			expected: 1, // falls back to single range
		},
		{
			name:     "small range",
			minPK:    int64(1),
			maxPK:    int64(3),
			n:        10,
			expected: 2, // reduced to fit range
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := splitPKRange(tt.minPK, tt.maxPK, tt.n)
			if len(ranges) != tt.expected {
				t.Errorf("splitPKRange() returned %d ranges, want %d", len(ranges), tt.expected)
			}
		})
	}
}

func TestParseNumericPK(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int64
		ok       bool
	}{
		{"nil", nil, 0, false},
		{"int", int(42), 42, true},
		{"int32", int32(42), 42, true},
		{"int64", int64(42), 42, true},
		{"float64", float64(42), 42, true},
		{"string valid", "42", 42, true},
		{"string invalid", "abc", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := parseNumericPK(tt.input)
			if ok != tt.ok {
				t.Errorf("parseNumericPK() ok = %v, want %v", ok, tt.ok)
			}
			if ok && result != tt.expected {
				t.Errorf("parseNumericPK() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDecrementPK(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"int64", int64(10), int64(9)},
		{"int32", int32(10), int32(9)},
		{"int", int(10), int(9)},
		{"string", "abc", "abc"}, // unchanged
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decrementPK(tt.input)
			if result != tt.expected {
				t.Errorf("decrementPK() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsIntegerType(t *testing.T) {
	tests := []struct {
		dataType string
		expected bool
	}{
		{"int", true},
		{"INT", true},
		{"int4", true},
		{"integer", true},
		{"bigint", true},
		{"int8", true},
		{"smallint", true},
		{"int2", true},
		{"tinyint", true},
		{"varchar", false},
		{"text", false},
		{"uuid", false},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			result := isIntegerType(tt.dataType)
			if result != tt.expected {
				t.Errorf("isIntegerType(%q) = %v, want %v", tt.dataType, result, tt.expected)
			}
		})
	}
}
