package calibration

import (
	"testing"
	"time"
)

func TestQuickConfigs(t *testing.T) {
	configs := QuickConfigs()

	if len(configs) != 5 {
		t.Errorf("QuickConfigs() returned %d configs, expected 5", len(configs))
	}

	// Verify all configs have required fields
	for i, cfg := range configs {
		if cfg.Name == "" {
			t.Errorf("Config %d has empty name", i)
		}
		if cfg.ChunkSize <= 0 {
			t.Errorf("Config %s has invalid ChunkSize: %d", cfg.Name, cfg.ChunkSize)
		}
		if cfg.Workers <= 0 {
			t.Errorf("Config %s has invalid Workers: %d", cfg.Name, cfg.Workers)
		}
		if cfg.ReadAheadBuffers <= 0 {
			t.Errorf("Config %s has invalid ReadAheadBuffers: %d", cfg.Name, cfg.ReadAheadBuffers)
		}
	}

	// Verify expected configs exist
	expectedNames := map[string]bool{
		"small_conservative": true,
		"medium_balanced":    true,
		"large_aggressive":   true,
		"chunk_focus":        true,
		"parallel_focus":     true,
	}
	for _, cfg := range configs {
		if !expectedNames[cfg.Name] {
			t.Errorf("Unexpected config name: %s", cfg.Name)
		}
	}
}

func TestGetConfigs(t *testing.T) {
	tests := []struct {
		depth    CalibrationDepth
		expected int
	}{
		{DepthQuick, 5},
		{DepthStandard, 5}, // Currently returns QuickConfigs
		{DepthThorough, 5}, // Currently returns QuickConfigs
		{"invalid", 5},     // Default to QuickConfigs
	}

	for _, tt := range tests {
		configs := GetConfigs(tt.depth)
		if len(configs) != tt.expected {
			t.Errorf("GetConfigs(%s) returned %d configs, expected %d", tt.depth, len(configs), tt.expected)
		}
	}
}

func TestCategorizeError(t *testing.T) {
	tests := []struct {
		errMsg   string
		expected ErrorCategory
	}{
		{"out of memory", ErrorCategoryOOM},
		{"OOM killed", ErrorCategoryOOM},
		{"memory allocation failed", ErrorCategoryOOM},
		{"context deadline exceeded", ErrorCategoryTimeout},
		{"timeout after 30s", ErrorCategoryTimeout},
		{"context canceled", ErrorCategoryTimeout},
		{"permission denied", ErrorCategoryPermission},
		{"access denied", ErrorCategoryPermission},
		{"deadlock detected", ErrorCategoryDeadlock},
		{"random error", ErrorCategoryOther},
	}

	for _, tt := range tests {
		err := &testError{msg: tt.errMsg}
		got := CategorizeError(err)
		if got != tt.expected {
			t.Errorf("CategorizeError(%q) = %s, expected %s", tt.errMsg, got, tt.expected)
		}
	}

	// Test nil error
	if got := CategorizeError(nil); got != ErrorCategoryNone {
		t.Errorf("CategorizeError(nil) = %s, expected %s", got, ErrorCategoryNone)
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestCalibrationResultBestSuccessfulRun(t *testing.T) {
	result := &CalibrationResult{
		Runs: []RunMetrics{
			{ConfigName: "config1", Status: StatusSuccess, RowsPerSecond: 100},
			{ConfigName: "config2", Status: StatusFailed, RowsPerSecond: 0},
			{ConfigName: "config3", Status: StatusSuccess, RowsPerSecond: 200},
			{ConfigName: "config4", Status: StatusSuccess, RowsPerSecond: 150},
		},
	}

	best := result.BestSuccessfulRun()
	if best == nil {
		t.Fatal("BestSuccessfulRun() returned nil")
	}
	if best.ConfigName != "config3" {
		t.Errorf("BestSuccessfulRun() returned %s, expected config3", best.ConfigName)
	}

	// Test with all failed runs
	allFailed := &CalibrationResult{
		Runs: []RunMetrics{
			{ConfigName: "config1", Status: StatusFailed},
			{ConfigName: "config2", Status: StatusFailed},
		},
	}
	if allFailed.BestSuccessfulRun() != nil {
		t.Error("BestSuccessfulRun() should return nil when all runs failed")
	}

	// Test with empty runs
	empty := &CalibrationResult{}
	if empty.BestSuccessfulRun() != nil {
		t.Error("BestSuccessfulRun() should return nil for empty result")
	}
}

func TestCalibrationResultSuccessCount(t *testing.T) {
	result := &CalibrationResult{
		Runs: []RunMetrics{
			{Status: StatusSuccess},
			{Status: StatusFailed},
			{Status: StatusSuccess},
			{Status: StatusSuccess},
		},
	}

	if got := result.SuccessCount(); got != 3 {
		t.Errorf("SuccessCount() = %d, expected 3", got)
	}
}

func TestCalibrationResultFormatResultsTable(t *testing.T) {
	result := &CalibrationResult{
		Runs: []RunMetrics{
			{
				ConfigName:       "test_config",
				Status:           StatusSuccess,
				RowsPerSecond:    50000,
				QueryTimePercent: 30,
				ScanTimePercent:  20,
				WriteTimePercent: 50,
			},
			{
				ConfigName:    "failed_config",
				Status:        StatusFailed,
				ErrorCategory: ErrorCategoryOOM,
			},
		},
	}

	table := result.FormatResultsTable()
	if table == "" {
		t.Error("FormatResultsTable() returned empty string")
	}
	if !contains(table, "test_config") {
		t.Error("FormatResultsTable() missing config name")
	}
	if !contains(table, "OK") {
		t.Error("FormatResultsTable() missing success status")
	}
	if !contains(table, "FAILED") {
		t.Error("FormatResultsTable() missing failed status")
	}
}

func TestCalibrationResultFormatYAML(t *testing.T) {
	result := &CalibrationResult{
		Recommendation: &RecommendedConfig{
			ChunkSize:           100000,
			Workers:             8,
			ReadAheadBuffers:    12,
			EstimatedRowsPerSec: 150000,
		},
	}

	yaml := result.FormatYAML()
	if yaml == "" {
		t.Error("FormatYAML() returned empty string")
	}
	if !contains(yaml, "chunk_size: 100000") {
		t.Error("FormatYAML() missing chunk_size")
	}
	if !contains(yaml, "workers: 8") {
		t.Error("FormatYAML() missing workers")
	}

	// Test with nil recommendation
	noRec := &CalibrationResult{}
	if noRec.FormatYAML() != "" {
		t.Error("FormatYAML() should return empty string with nil recommendation")
	}
}

func TestRunMetrics(t *testing.T) {
	cfg := CalibrationConfig{
		Name:             "test",
		ChunkSize:        100000,
		Workers:          8,
		ReadAheadBuffers: 12,
		ParallelReaders:  3,
	}

	metrics := RunMetrics{
		ConfigName:       cfg.Name,
		Config:           cfg,
		Status:           StatusSuccess,
		Duration:         5 * time.Second,
		RowsTransferred:  500000,
		RowsPerSecond:    100000,
		QueryTimePercent: 30,
		WriteTimePercent: 50,
	}

	if metrics.Status != StatusSuccess {
		t.Errorf("Expected success status, got %s", metrics.Status)
	}
	if metrics.RowsPerSecond != 100000 {
		t.Errorf("Expected 100000 rows/sec, got %f", metrics.RowsPerSecond)
	}
}

func TestFallbackRecommendation(t *testing.T) {
	result := &CalibrationResult{
		Runs: []RunMetrics{
			{
				ConfigName: "config1",
				Config: CalibrationConfig{
					ChunkSize:        50000,
					Workers:          4,
					ReadAheadBuffers: 8,
					ParallelReaders:  2,
				},
				Status:        StatusSuccess,
				RowsPerSecond: 100000,
			},
			{
				ConfigName: "config2",
				Config: CalibrationConfig{
					ChunkSize:        100000,
					Workers:          8,
					ReadAheadBuffers: 12,
					ParallelReaders:  3,
				},
				Status:        StatusSuccess,
				RowsPerSecond: 150000,
			},
		},
	}

	rec := FallbackRecommendation(result)
	if rec == nil {
		t.Fatal("FallbackRecommendation() returned nil")
	}
	if rec.ChunkSize != 100000 {
		t.Errorf("Expected ChunkSize 100000, got %d", rec.ChunkSize)
	}
	if rec.Workers != 8 {
		t.Errorf("Expected Workers 8, got %d", rec.Workers)
	}
	if rec.Confidence != "low" {
		t.Errorf("Expected low confidence, got %s", rec.Confidence)
	}

	// Test with all failed runs
	allFailed := &CalibrationResult{
		Runs: []RunMetrics{
			{Status: StatusFailed},
		},
	}
	if FallbackRecommendation(allFailed) != nil {
		t.Error("FallbackRecommendation() should return nil when all runs failed")
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr)))
}
