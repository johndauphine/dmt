package oracle

import (
	"testing"

	"github.com/godror/godror"
)

func TestNormalizeOracleValues(t *testing.T) {
	tests := []struct {
		name     string
		input    [][]any
		expected [][]any
	}{
		{
			name:     "nil values unchanged",
			input:    [][]any{{nil, nil}},
			expected: [][]any{{nil, nil}},
		},
		{
			name:     "godror.Number positive integer",
			input:    [][]any{{godror.Number("12345")}},
			expected: [][]any{{"12345"}},
		},
		{
			name:     "godror.Number negative integer",
			input:    [][]any{{godror.Number("-9876")}},
			expected: [][]any{{"-9876"}},
		},
		{
			name:     "godror.Number decimal",
			input:    [][]any{{godror.Number("123.456789")}},
			expected: [][]any{{"123.456789"}},
		},
		{
			name:     "godror.Number large number preserves precision",
			input:    [][]any{{godror.Number("99999999999999999999")}},
			expected: [][]any{{"99999999999999999999"}},
		},
		{
			name:     "godror.Number scientific notation",
			input:    [][]any{{godror.Number("1.23E10")}},
			expected: [][]any{{"1.23E10"}},
		},
		{
			name:     "string unchanged",
			input:    [][]any{{"test string"}},
			expected: [][]any{{"test string"}},
		},
		{
			name:     "int64 unchanged",
			input:    [][]any{{int64(12345)}},
			expected: [][]any{{int64(12345)}},
		},
		{
			name:     "float64 unchanged",
			input:    [][]any{{float64(123.45)}},
			expected: [][]any{{float64(123.45)}},
		},
		{
			name:     "byte slice unchanged",
			input:    [][]any{{[]byte("hello")}},
			expected: [][]any{{[]byte("hello")}},
		},
		{
			name:     "bool unchanged",
			input:    [][]any{{true, false}},
			expected: [][]any{{true, false}},
		},
		{
			name: "mixed types with godror.Number",
			input: [][]any{
				{int64(1), "hello", godror.Number("42.5"), nil},
			},
			expected: [][]any{
				{int64(1), "hello", "42.5", nil},
			},
		},
		{
			name: "multiple rows",
			input: [][]any{
				{godror.Number("100"), "row1"},
				{godror.Number("200"), "row2"},
				{godror.Number("300"), "row3"},
			},
			expected: [][]any{
				{"100", "row1"},
				{"200", "row2"},
				{"300", "row3"},
			},
		},
		{
			name:     "empty rows",
			input:    [][]any{},
			expected: [][]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying test data
			input := make([][]any, len(tt.input))
			for i := range tt.input {
				input[i] = make([]any, len(tt.input[i]))
				copy(input[i], tt.input[i])
			}

			normalizeOracleValues(input)

			if len(input) != len(tt.expected) {
				t.Fatalf("row count mismatch: got %d, want %d", len(input), len(tt.expected))
			}

			for i := range input {
				if len(input[i]) != len(tt.expected[i]) {
					t.Fatalf("row %d column count mismatch: got %d, want %d", i, len(input[i]), len(tt.expected[i]))
				}

				for j := range input[i] {
					// Handle []byte comparison
					if b1, ok := input[i][j].([]byte); ok {
						if b2, ok := tt.expected[i][j].([]byte); ok {
							if string(b1) != string(b2) {
								t.Errorf("row %d, col %d: got %v, want %v", i, j, input[i][j], tt.expected[i][j])
							}
							continue
						}
					}
					if input[i][j] != tt.expected[i][j] {
						t.Errorf("row %d, col %d: got %v (%T), want %v (%T)",
							i, j, input[i][j], input[i][j], tt.expected[i][j], tt.expected[i][j])
					}
				}
			}
		})
	}
}
