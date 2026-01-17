package mysql

import (
	"testing"
)

func TestConvertRowValues(t *testing.T) {
	tests := []struct {
		name     string
		input    []any
		expected []any
	}{
		{
			name:     "nil values",
			input:    []any{nil, nil},
			expected: []any{nil, nil},
		},
		{
			name:     "bool true converts to 1",
			input:    []any{true},
			expected: []any{1},
		},
		{
			name:     "bool false converts to 0",
			input:    []any{false},
			expected: []any{0},
		},
		{
			name:     "byte slice unchanged",
			input:    []any{[]byte("hello")},
			expected: []any{[]byte("hello")},
		},
		{
			name:     "string unchanged",
			input:    []any{"test string"},
			expected: []any{"test string"},
		},
		{
			name:     "int64 unchanged",
			input:    []any{int64(12345)},
			expected: []any{int64(12345)},
		},
		{
			name:     "float64 unchanged",
			input:    []any{float64(123.45)},
			expected: []any{float64(123.45)},
		},
		{
			name:     "mixed types",
			input:    []any{int64(1), "hello", true, nil},
			expected: []any{int64(1), "hello", 1, nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertRowValues(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d, want %d", len(result), len(tt.expected))
			}
			for i := range result {
				// Handle []byte comparison
				if b1, ok := result[i].([]byte); ok {
					if b2, ok := tt.expected[i].([]byte); ok {
						if string(b1) != string(b2) {
							t.Errorf("index %d: got %v, want %v", i, result[i], tt.expected[i])
						}
						continue
					}
				}
				if result[i] != tt.expected[i] {
					t.Errorf("index %d: got %v (%T), want %v (%T)", i, result[i], result[i], tt.expected[i], tt.expected[i])
				}
			}
		})
	}
}
