package orchestrator

import (
	"strings"
	"testing"
	"time"
)

func TestFormatMigrationSummary(t *testing.T) {
	result := &MigrationResult{
		RunID:           "run-123",
		Status:          "partial",
		StartedAt:       time.Date(2026, 5, 19, 10, 0, 0, 0, time.UTC),
		CompletedAt:     time.Date(2026, 5, 19, 10, 2, 3, 0, time.UTC),
		DurationSeconds: 123,
		TablesTotal:     3,
		TablesSuccess:   2,
		TablesFailed:    1,
		RowsTransferred: 1234567,
		RowsPerSecond:   10037,
		TableStats: []TableResult{
			{Name: "dbo.users", Rows: 1234567, Status: "success"},
			{Name: "dbo.orders", Rows: 0, Status: "failed", Error: "duplicate key"},
		},
		FailedTables: []string{"dbo.orders"},
		Error:        "1 tables failed",
	}

	output := FormatMigrationSummary(result)

	for _, want := range []string{
		"DMT run summary",
		"Run ID          : run-123",
		"Status          : partial",
		"Started         : 2026-05-19 10:00:00 UTC",
		"Completed       : 2026-05-19 10:02:03 UTC",
		"Duration        : 2m3s",
		"Tables          : 3 total, 2 succeeded, 1 failed",
		"Rows            : 1,234,567 transferred (10,037 rows/sec)",
		"dbo.users                         1,234,567 rows  success",
		"dbo.orders                                0 rows  failed: duplicate key",
		"Failed tables:\n  - dbo.orders",
		"Error           : 1 tables failed",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("summary missing %q:\n%s", want, output)
		}
	}
}

func TestFormatMigrationSummaryNil(t *testing.T) {
	if got := FormatMigrationSummary(nil); got != "" {
		t.Fatalf("FormatMigrationSummary(nil) = %q, want empty string", got)
	}
}

func TestFormatCount(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{1234567890, "1,234,567,890"},
		{-9876543, "-9,876,543"},
	}

	for _, tt := range tests {
		if got := FormatCount(tt.n); got != tt.want {
			t.Errorf("FormatCount(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
