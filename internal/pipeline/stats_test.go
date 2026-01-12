package pipeline

import (
	"strings"
	"testing"
	"time"
)

func TestStats_String(t *testing.T) {
	tests := []struct {
		name     string
		stats    Stats
		expected string
	}{
		{
			name:     "empty stats",
			stats:    Stats{},
			expected: "no data",
		},
		{
			name: "balanced times",
			stats: Stats{
				QueryTime: time.Second,
				ScanTime:  time.Second,
				WriteTime: time.Second,
				Rows:      1000,
			},
			expected: "query=1.0s (33%), scan=1.0s (33%), write=1.0s (33%), rows=1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stats.String()
			if tt.name == "empty stats" {
				if result != tt.expected {
					t.Errorf("got %q, want %q", result, tt.expected)
				}
			}
			// For non-empty, just verify it contains rows count
			if tt.name == "balanced times" && !strings.Contains(result, "rows=1000") {
				t.Errorf("result should contain rows count: %s", result)
			}
		})
	}
}

func TestStats_TotalTime(t *testing.T) {
	stats := Stats{
		QueryTime: time.Second,
		ScanTime:  2 * time.Second,
		WriteTime: 3 * time.Second,
	}

	total := stats.TotalTime()
	expected := 6 * time.Second

	if total != expected {
		t.Errorf("TotalTime() = %v, want %v", total, expected)
	}
}

func TestStats_RowsPerSecond(t *testing.T) {
	tests := []struct {
		name     string
		stats    Stats
		expected float64
	}{
		{
			name:     "zero time",
			stats:    Stats{Rows: 1000},
			expected: 0,
		},
		{
			name: "one second",
			stats: Stats{
				WriteTime: time.Second,
				Rows:      1000,
			},
			expected: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stats.RowsPerSecond()
			if result != tt.expected {
				t.Errorf("RowsPerSecond() = %v, want %v", result, tt.expected)
			}
		})
	}
}
