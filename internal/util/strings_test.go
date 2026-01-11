package util

import (
	"reflect"
	"testing"
)

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single value",
			input:    "foo",
			expected: []string{"foo"},
		},
		{
			name:     "multiple values",
			input:    "foo,bar,baz",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "with whitespace",
			input:    " foo , bar , baz ",
			expected: []string{"foo", "bar", "baz"},
		},
		{
			name:     "trailing comma",
			input:    "foo,bar,",
			expected: []string{"foo", "bar"},
		},
		{
			name:     "leading comma",
			input:    ",foo,bar",
			expected: []string{"foo", "bar"},
		},
		{
			name:     "multiple commas",
			input:    "foo,,bar",
			expected: []string{"foo", "bar"},
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: nil,
		},
		{
			name:     "only whitespace",
			input:    "   ",
			expected: nil,
		},
		{
			name:     "whitespace between commas",
			input:    " , , ",
			expected: nil,
		},
		{
			name:     "column names with spaces",
			input:    "Column A, Column B, Column C",
			expected: []string{"Column A", "Column B", "Column C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitCSV(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("SplitCSV(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
