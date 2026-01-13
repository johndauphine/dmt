package orchestrator

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/data-transfer-tool/internal/config"
)

func TestComputeConfigHash(t *testing.T) {
	t.Run("same config produces same hash", func(t *testing.T) {
		cfg1 := &config.Config{
			Source: config.SourceConfig{
				Type:     "mssql",
				Host:     "localhost",
				Port:     1433,
				Database: "testdb",
				User:     "sa",
				Password: "secret123",
			},
			Target: config.TargetConfig{
				Type:     "postgres",
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "postgres",
				Password: "secret456",
			},
		}

		cfg2 := &config.Config{
			Source: config.SourceConfig{
				Type:     "mssql",
				Host:     "localhost",
				Port:     1433,
				Database: "testdb",
				User:     "sa",
				Password: "secret123",
			},
			Target: config.TargetConfig{
				Type:     "postgres",
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "postgres",
				Password: "secret456",
			},
		}

		hash1 := computeConfigHash(cfg1)
		hash2 := computeConfigHash(cfg2)

		if hash1 != hash2 {
			t.Errorf("same config produced different hashes: %s != %s", hash1, hash2)
		}
	})

	t.Run("different config produces different hash", func(t *testing.T) {
		cfg1 := &config.Config{
			Source: config.SourceConfig{
				Host: "host1",
				Port: 1433,
			},
		}

		cfg2 := &config.Config{
			Source: config.SourceConfig{
				Host: "host2",
				Port: 1433,
			},
		}

		hash1 := computeConfigHash(cfg1)
		hash2 := computeConfigHash(cfg2)

		if hash1 == hash2 {
			t.Errorf("different configs produced same hash: %s", hash1)
		}
	})

	t.Run("hash is hex string of expected length", func(t *testing.T) {
		cfg := &config.Config{
			Source: config.SourceConfig{Host: "localhost"},
		}

		hash := computeConfigHash(cfg)

		// Should be 16 hex chars (8 bytes)
		if len(hash) != 16 {
			t.Errorf("hash length = %d, want 16", len(hash))
		}

		// Should be valid hex
		for _, c := range hash {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				t.Errorf("hash contains non-hex character: %c", c)
			}
		}
	})

	t.Run("password changes do not affect hash due to sanitization", func(t *testing.T) {
		cfg1 := &config.Config{
			Source: config.SourceConfig{
				Host:     "localhost",
				Password: "password1",
			},
		}

		cfg2 := &config.Config{
			Source: config.SourceConfig{
				Host:     "localhost",
				Password: "password2",
			},
		}

		hash1 := computeConfigHash(cfg1)
		hash2 := computeConfigHash(cfg2)

		// Passwords are sanitized to [REDACTED], so hashes should be equal
		if hash1 != hash2 {
			t.Errorf("password change affected hash: %s != %s", hash1, hash2)
		}
	})
}

func TestMigrationResultJSON(t *testing.T) {
	t.Run("marshal complete result", func(t *testing.T) {
		result := MigrationResult{
			RunID:           "test-run-123",
			Status:          "success",
			StartedAt:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			CompletedAt:     time.Date(2025, 1, 15, 10, 5, 0, 0, time.UTC),
			DurationSeconds: 300.5,
			TablesTotal:     5,
			TablesSuccess:   4,
			TablesFailed:    1,
			RowsTransferred: 100000,
			RowsPerSecond:   333,
			FailedTables:    []string{"failed_table"},
			TableStats: []TableResult{
				{Name: "users", Rows: 50000, Status: "success"},
				{Name: "posts", Rows: 50000, Status: "success"},
			},
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		// Parse back and verify
		var parsed MigrationResult
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if parsed.RunID != "test-run-123" {
			t.Errorf("RunID = %q, want %q", parsed.RunID, "test-run-123")
		}
		if parsed.TablesTotal != 5 {
			t.Errorf("TablesTotal = %d, want %d", parsed.TablesTotal, 5)
		}
		if len(parsed.TableStats) != 2 {
			t.Errorf("len(TableStats) = %d, want %d", len(parsed.TableStats), 2)
		}
	})

	t.Run("marshal result with error", func(t *testing.T) {
		result := MigrationResult{
			RunID:  "failed-run",
			Status: "failed",
			Error:  "connection refused",
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if parsed["error"] != "connection refused" {
			t.Errorf("error = %q, want %q", parsed["error"], "connection refused")
		}
	})

	t.Run("empty error omitted from JSON", func(t *testing.T) {
		result := MigrationResult{
			RunID:  "success-run",
			Status: "success",
			Error:  "", // empty
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if _, ok := parsed["error"]; ok {
			t.Error("expected 'error' field to be omitted when empty")
		}
	})
}

func TestIsRetryableError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "connection reset", err: errors.New("connection reset by peer"), want: true},
		{name: "deadlock", err: errors.New("DEADLOCK detected"), want: true},
		{name: "context deadline", err: errors.New("context deadline exceeded"), want: true},
		{name: "retry hint", err: errors.New("please retry later"), want: true},
		{name: "non-retryable", err: errors.New("permission denied"), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableError(tc.err); got != tc.want {
				t.Fatalf("isRetryableError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStatusResultJSON(t *testing.T) {
	t.Run("marshal status result", func(t *testing.T) {
		result := StatusResult{
			RunID:           "status-run-123",
			Status:          "running",
			Phase:           "transferring",
			StartedAt:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			TablesTotal:     10,
			TablesComplete:  5,
			TablesRunning:   2,
			TablesPending:   2,
			TablesFailed:    1,
			RowsTransferred: 50000,
			ProgressPercent: 55.5,
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed StatusResult
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if parsed.RunID != "status-run-123" {
			t.Errorf("RunID = %q, want %q", parsed.RunID, "status-run-123")
		}
		if parsed.Phase != "transferring" {
			t.Errorf("Phase = %q, want %q", parsed.Phase, "transferring")
		}
		if parsed.ProgressPercent != 55.5 {
			t.Errorf("ProgressPercent = %f, want %f", parsed.ProgressPercent, 55.5)
		}
	})
}

func TestTableResultJSON(t *testing.T) {
	t.Run("marshal table result with error", func(t *testing.T) {
		result := TableResult{
			Name:   "failed_table",
			Rows:   0,
			Status: "failed",
			Error:  "primary key not found",
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if parsed["error"] != "primary key not found" {
			t.Errorf("error = %q, want %q", parsed["error"], "primary key not found")
		}
	})

	t.Run("error omitted when empty", func(t *testing.T) {
		result := TableResult{
			Name:   "success_table",
			Rows:   1000,
			Status: "success",
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if _, ok := parsed["error"]; ok {
			t.Error("expected 'error' field to be omitted when empty")
		}
	})
}

func TestOptionsDefaults(t *testing.T) {
	opts := Options{}

	if opts.StateFile != "" {
		t.Errorf("default StateFile = %q, want empty", opts.StateFile)
	}
	if opts.RunID != "" {
		t.Errorf("default RunID = %q, want empty", opts.RunID)
	}
	if opts.ForceResume {
		t.Error("default ForceResume = true, want false")
	}
}

func TestOptionsWithValues(t *testing.T) {
	opts := Options{
		StateFile:   "/tmp/state.yaml",
		RunID:       "custom-run-id",
		ForceResume: true,
	}

	if opts.StateFile != "/tmp/state.yaml" {
		t.Errorf("StateFile = %q, want %q", opts.StateFile, "/tmp/state.yaml")
	}
	if opts.RunID != "custom-run-id" {
		t.Errorf("RunID = %q, want %q", opts.RunID, "custom-run-id")
	}
	if !opts.ForceResume {
		t.Error("ForceResume = false, want true")
	}
}
