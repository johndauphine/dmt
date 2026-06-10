package orchestrator

import (
	"encoding/json"
	"errors"
	"github.com/johndauphine/dmt/internal/orchestrator/schemaevolution"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/audit"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/source"
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
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
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

	t.Run("marshal schema contract decisions", func(t *testing.T) {
		result := MigrationResult{
			RunID:  "schema-run",
			Status: "failed",
			SchemaContractDecisions: []SchemaContractDecision{{
				Entity:   schemaevolution.SchemaContractEntityDataType,
				Mode:     string(config.SchemaContractEvolve),
				Drift:    string(drift.TypeNarrowed),
				Schema:   "dbo",
				Table:    "Users",
				Object:   "name",
				Previous: "varchar(255)",
				Current:  "varchar(50)",
				Action:   schemaevolution.SchemaContractActionBlocked,
				Reason:   "data_type=evolve blocks unsafe narrowed, lossy, or tightening changes before transfer",
			}},
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed MigrationResult
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}
		if len(parsed.SchemaContractDecisions) != 1 {
			t.Fatalf("decision count = %d, want 1", len(parsed.SchemaContractDecisions))
		}
		if parsed.SchemaContractDecisions[0].Action != schemaevolution.SchemaContractActionBlocked {
			t.Fatalf("decision = %#v, want blocked action", parsed.SchemaContractDecisions[0])
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

	t.Run("marshal schema contract decisions", func(t *testing.T) {
		result := StatusResult{
			RunID:  "status-run-123",
			Status: "running",
			SchemaContractDecisions: []SchemaContractDecision{{
				Entity: schemaevolution.SchemaContractEntityColumns,
				Mode:   string(config.SchemaContractDiscardRow),
				Drift:  string(drift.AddedColumn),
				Schema: "dbo",
				Table:  "Users",
				Object: "email",
				Action: schemaevolution.SchemaContractActionDiscardedRow,
				Reason: "columns=discard_row skips tables with newly added source columns",
			}},
		}

		data, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("json.Marshal() error: %v", err)
		}

		var parsed StatusResult
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}
		if len(parsed.SchemaContractDecisions) != 1 {
			t.Fatalf("decision count = %d, want 1", len(parsed.SchemaContractDecisions))
		}
		if parsed.SchemaContractDecisions[0].Action != schemaevolution.SchemaContractActionDiscardedRow {
			t.Fatalf("decision = %#v, want discarded_row action", parsed.SchemaContractDecisions[0])
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

// TestSanitizeForLog pins the log-injection guard added in PR #151 review:
// AI-supplied strings (ObservedRetryRates / Reasoning) are logged verbatim
// at INFO and could otherwise contain newlines that break log parsers.
func TestSanitizeForLog(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"single line", "single line"},
		{"multi\nline\nreasoning", "multi line reasoning"},
		{"tabs\ttoo", "tabs too"},
		{"carriage\r\nreturn", "carriage return"},
		{"  leading and trailing  ", "leading and trailing"},
		{"runs   of    spaces", "runs of spaces"},
		{"", ""},
		{"\n\n\n", ""},
	}
	for _, tc := range cases {
		got := sanitizeForLog(tc.in)
		if got != tc.want {
			t.Errorf("sanitizeForLog(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParsePGSize covers the conversion from PG SHOW values to MB.
func TestParsePGSize(t *testing.T) {
	cases := []struct {
		raw    string
		wantMB int64
		wantOK bool
	}{
		{"8GB", 8 * 1024, true},
		{"16384MB", 16384, true},
		{"1024kB", 1, true}, // 1024 KB = 1 MB
		{"512kB", 0, true},  // sub-MB rounds down to 0
		{"2TB", 2 * 1024 * 1024, true},
		{"  8GB  ", 8 * 1024, true}, // trim
		{"GARBAGE", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := parsePGSize(tc.raw)
		if ok != tc.wantOK || got != tc.wantMB {
			t.Errorf("parsePGSize(%q) = (%d, %v), want (%d, %v)",
				tc.raw, got, ok, tc.wantMB, tc.wantOK)
		}
	}
}

func TestPartialMigrationError(t *testing.T) {
	err := &PartialMigrationError{
		Failed: []TableFailure{
			{TableName: "dbo.Users"},
			{TableName: "dbo.Orders"},
		},
	}

	msg := err.Error()
	if !strings.Contains(msg, "2 failed table(s)") {
		t.Errorf("Error() = %q, want substring %q", msg, "2 failed table(s)")
	}
	if !strings.Contains(msg, "dbo.Users") || !strings.Contains(msg, "dbo.Orders") {
		t.Errorf("Error() = %q, want both table names listed", msg)
	}

	if code := err.ExitCode(); code != exitcodes.TransferError {
		t.Errorf("ExitCode() = %d, want %d (TransferError)", code, exitcodes.TransferError)
	}
}

func TestClearResumeProgressFailsOnStaleCheckpointCleanupErrors(t *testing.T) {
	tests := []struct {
		name               string
		clearErr           error
		partitionClearErr  error
		wantErr            string
		wantClear          int
		wantPartitionClear int
	}{
		{
			name:               "table clear failure is fatal",
			clearErr:           errors.New("state db locked"),
			wantErr:            "clearing transfer progress for votes",
			wantClear:          1,
			wantPartitionClear: 1,
		},
		{
			name:               "partition clear failure is fatal",
			partitionClearErr:  errors.New("partition delete failed"),
			wantErr:            "clearing partition progress for votes",
			wantClear:          0,
			wantPartitionClear: 1,
		},
		{
			name:               "all clears succeed",
			wantClear:          1,
			wantPartitionClear: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &resumeProgressClearState{
				clearErr:          tt.clearErr,
				partitionClearErr: tt.partitionClearErr,
			}
			o := &Orchestrator{state: state}

			err := o.clearResumeProgress("run-266", "transfer:public.votes", 42, "votes")
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("clearResumeProgress() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("clearResumeProgress() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("clearResumeProgress() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
			}
			if state.clearCalls != tt.wantClear {
				t.Fatalf("ClearTransferProgress calls = %d, want %d", state.clearCalls, tt.wantClear)
			}
			if state.partitionClearCalls != tt.wantPartitionClear {
				t.Fatalf("ClearPartitionTransferProgress calls = %d, want %d",
					state.partitionClearCalls, tt.wantPartitionClear)
			}
		})
	}
}

func TestExpectedResumeRowsUsesPartitionProgress(t *testing.T) {
	tests := []struct {
		name            string
		isPartitioned   bool
		tableLastPK     any
		tableRowsDone   int64
		summary         checkpoint.PartitionProgressSummary
		summaryErr      error
		wantRows        int64
		wantHasProgress bool
		wantSummaryCall bool
		wantErr         string
	}{
		{
			name:            "non partitioned uses table progress only",
			tableLastPK:     int64(10),
			tableRowsDone:   100,
			wantRows:        100,
			wantHasProgress: true,
		},
		{
			name:          "partition progress works without table progress",
			isPartitioned: true,
			summary: checkpoint.PartitionProgressSummary{
				RowsDone:               750,
				PartitionsWithProgress: 3,
			},
			wantRows:        750,
			wantHasProgress: true,
			wantSummaryCall: true,
		},
		{
			name:          "partition progress wins when it is larger",
			isPartitioned: true,
			tableLastPK:   int64(10),
			tableRowsDone: 100,
			summary: checkpoint.PartitionProgressSummary{
				RowsDone:               750,
				PartitionsWithProgress: 3,
			},
			wantRows:        750,
			wantHasProgress: true,
			wantSummaryCall: true,
		},
		{
			name:            "falls back to table progress when no partition progress exists",
			isPartitioned:   true,
			tableLastPK:     int64(10),
			tableRowsDone:   100,
			wantRows:        100,
			wantHasProgress: true,
			wantSummaryCall: true,
		},
		{
			name:            "partition summary errors are fatal",
			isPartitioned:   true,
			summaryErr:      errors.New("state db locked"),
			wantSummaryCall: true,
			wantErr:         "getting partition progress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &resumeProgressSummaryState{
				summary: tt.summary,
				err:     tt.summaryErr,
			}
			o := &Orchestrator{state: state}

			gotRows, gotHasProgress, err := o.expectedResumeRows(
				"run-267", "transfer:public.votes",
				tt.isPartitioned, tt.tableLastPK, tt.tableRowsDone,
			)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expectedResumeRows() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("expectedResumeRows() error = nil, want %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expectedResumeRows() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
			}
			if gotRows != tt.wantRows {
				t.Errorf("rows = %d, want %d", gotRows, tt.wantRows)
			}
			if gotHasProgress != tt.wantHasProgress {
				t.Errorf("hasProgress = %t, want %t", gotHasProgress, tt.wantHasProgress)
			}
			if (state.calls > 0) != tt.wantSummaryCall {
				t.Errorf("summary called = %t, want %t", state.calls > 0, tt.wantSummaryCall)
			}
		})
	}
}

func TestRecordSuccessfulTuningResult(t *testing.T) {
	state := &tuningResultState{}
	o := &Orchestrator{state: state, lastChunkRetryCount: 3}

	o.recordSuccessfulTuningResult(1000, 2*time.Second)

	if state.calls != 1 {
		t.Fatalf("UpdateTuningResult calls = %d, want 1", state.calls)
	}
	if state.throughput != 500 {
		t.Errorf("throughput = %f, want 500", state.throughput)
	}
	if state.durationSecs != 2 {
		t.Errorf("durationSecs = %f, want 2", state.durationSecs)
	}
	if state.chunkRetryCount != 3 {
		t.Errorf("chunkRetryCount = %d, want 3", state.chunkRetryCount)
	}
}

func TestRecordSuccessfulTuningResultSkipsInvalidDuration(t *testing.T) {
	state := &tuningResultState{}
	o := &Orchestrator{state: state}

	o.recordSuccessfulTuningResult(1000, 0)
	o.recordSuccessfulTuningResult(1000, -time.Second)

	if state.calls != 0 {
		t.Fatalf("UpdateTuningResult calls = %d, want 0", state.calls)
	}
}

func TestValidateResumeHeartbeat(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		run       *checkpoint.Run
		force     bool
		wantError string
	}{
		{
			name: "fresh heartbeat",
			run: &checkpoint.Run{
				ID:            "fresh-run",
				StartedAt:     now.Add(-10 * time.Minute),
				LastHeartbeat: now.Add(-30 * time.Second),
			},
		},
		{
			name: "stale heartbeat",
			run: &checkpoint.Run{
				ID:            "stale-run",
				StartedAt:     now.Add(-2 * time.Hour),
				LastHeartbeat: now.Add(-20 * time.Minute),
			},
			wantError: "stale-run",
		},
		{
			name: "force allows stale heartbeat",
			run: &checkpoint.Run{
				ID:            "force-run",
				StartedAt:     now.Add(-2 * time.Hour),
				LastHeartbeat: now.Add(-20 * time.Minute),
			},
			force: true,
		},
		{
			name: "legacy run falls back to started_at",
			run: &checkpoint.Run{
				ID:        "legacy-run",
				StartedAt: now.Add(-20 * time.Minute),
			},
			wantError: "legacy-run",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Orchestrator{opts: Options{
				ForceResume:     tt.force,
				RunHeartbeatTTL: time.Minute,
			}}
			err := o.validateResumeHeartbeat(tt.run, now)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateResumeHeartbeat() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateResumeHeartbeat() error = nil, want %q", tt.wantError)
			}
			for _, want := range []string{tt.wantError, "--force-resume", "last heartbeat"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("validateResumeHeartbeat() error = %q, want substring %q", err.Error(), want)
				}
			}
		})
	}
}

type resumeProgressClearState struct {
	checkpoint.StateBackend
	clearErr            error
	partitionClearErr   error
	clearCalls          int
	partitionClearCalls int
}

func (s *resumeProgressClearState) ClearTransferProgress(int64) error {
	s.clearCalls++
	return s.clearErr
}

func (s *resumeProgressClearState) ClearPartitionTransferProgress(string, string) error {
	s.partitionClearCalls++
	return s.partitionClearErr
}

type resumeProgressSummaryState struct {
	checkpoint.StateBackend
	summary checkpoint.PartitionProgressSummary
	err     error
	calls   int
}

func (s *resumeProgressSummaryState) GetPartitionTransferProgressSummary(
	string,
	string,
) (checkpoint.PartitionProgressSummary, error) {
	s.calls++
	return s.summary, s.err
}

type tuningResultState struct {
	checkpoint.StateBackend
	calls           int
	throughput      float64
	durationSecs    float64
	chunkRetryCount int
}

func (s *tuningResultState) UpdateTuningResult(throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error {
	s.calls++
	s.throughput = throughput
	s.durationSecs = durationSecs
	s.chunkRetryCount = chunkRetryCount
	return nil
}

// TestComputeConfigHash_AllowPartialInvariant guards against the
// regression Codex caught on the #248 PR: adding the new
// MigrationConfig.AllowPartial field must not change the resume
// config hash, because that would invalidate every in-progress
// resume across the #248 upgrade. Hash continuity is enforced via
// `json:"-"` on the field.
func TestComputeConfigHash_AllowPartialInvariant(t *testing.T) {
	base := &config.Config{
		Source: config.SourceConfig{Type: "mssql", Host: "h", Port: 1433, Database: "d", User: "u", Password: "p"},
		Target: config.TargetConfig{Type: "postgres", Host: "h", Port: 5432, Database: "d", User: "u", Password: "p"},
	}
	other := *base
	other.Migration.AllowPartial = true

	baseHash := computeConfigHash(base)
	otherHash := computeConfigHash(&other)
	if baseHash != otherHash {
		t.Errorf("flipping MigrationConfig.AllowPartial changed the resume config hash; "+
			"json:\"-\" tag on the field is missing or has regressed (base=%s, other=%s)",
			baseHash, otherHash)
	}
}

func TestComputeConfigHash_FailOnSchemaDriftInvariant(t *testing.T) {
	base := &config.Config{
		Source: config.SourceConfig{Type: "mssql", Host: "h", Port: 1433, Database: "d", User: "u", Password: "p"},
		Target: config.TargetConfig{Type: "postgres", Host: "h", Port: 5432, Database: "d", User: "u", Password: "p"},
	}
	other := *base
	other.Migration.FailOnSchemaDrift = true

	baseHash := computeConfigHash(base)
	otherHash := computeConfigHash(&other)
	if baseHash != otherHash {
		t.Errorf("flipping MigrationConfig.FailOnSchemaDrift changed the resume config hash; "+
			"json:\"-\" tag on the field is missing or has regressed (base=%s, other=%s)",
			baseHash, otherHash)
	}
}

func TestReportSchemaDrift_FailOnSchemaDriftReturnsError(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("New state: %v", err)
	}
	defer state.Close()

	previous := source.Table{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false, OrdinalPos: 1},
		},
		PrimaryKey: []string{"id"},
	}
	snapshotJSON, err := drift.MarshalTableSnapshot(drift.BuildTableSnapshot(previous))
	if err != nil {
		t.Fatalf("MarshalTableSnapshot: %v", err)
	}
	if err := state.SaveSchemaSnapshot("run-1", "mssql|source_db|dbo", "Users", snapshotJSON); err != nil {
		t.Fatalf("SaveSchemaSnapshot: %v", err)
	}

	current := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false, OrdinalPos: 1},
			{Name: "email", DataType: "varchar", MaxLength: 255, IsNullable: true, OrdinalPos: 2},
		},
		PrimaryKey: []string{"id"},
	}}
	o := &Orchestrator{
		config: &config.Config{
			Source:    config.SourceConfig{Type: "mssql", Database: "source_db", Schema: "dbo"},
			Migration: config.MigrationConfig{FailOnSchemaDrift: true},
		},
		state:   state,
		auditor: audit.Disabled(),
	}

	_, err = o.reportSchemaDrift(current, true)
	var driftErr *SchemaDriftError
	if !errors.As(err, &driftErr) {
		t.Fatalf("reportSchemaDrift error = %T (%v), want *SchemaDriftError", err, err)
	}
	if driftErr.ExitCode() != exitcodes.TransferError {
		t.Fatalf("ExitCode = %d, want TransferError", driftErr.ExitCode())
	}
	if len(driftErr.Report.Changes) != 1 || driftErr.Report.Changes[0].Kind != drift.AddedColumn {
		t.Fatalf("changes = %+v, want one added_column", driftErr.Report.Changes)
	}
}

func TestReportSchemaDrift_IgnoresSnapshotsExcludedByCurrentFilters(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("New state: %v", err)
	}
	defer state.Close()

	for _, tableName := range []string{"Users", "Archive"} {
		table := source.Table{
			Schema: "dbo",
			Name:   tableName,
			Columns: []source.Column{
				{Name: "id", DataType: "int", IsNullable: false, OrdinalPos: 1},
			},
			PrimaryKey: []string{"id"},
		}
		snapshotJSON, err := drift.MarshalTableSnapshot(drift.BuildTableSnapshot(table))
		if err != nil {
			t.Fatalf("MarshalTableSnapshot(%s): %v", tableName, err)
		}
		if err := state.SaveSchemaSnapshot("run-1", "mssql|source_db|dbo", tableName, snapshotJSON); err != nil {
			t.Fatalf("SaveSchemaSnapshot(%s): %v", tableName, err)
		}
	}

	o := &Orchestrator{
		config: &config.Config{
			Source: config.SourceConfig{Type: "mssql", Database: "source_db", Schema: "dbo"},
			Migration: config.MigrationConfig{
				ExcludeTables:     []string{"Archive"},
				FailOnSchemaDrift: true,
			},
		},
		state:   state,
		auditor: audit.Disabled(),
	}

	current := []source.Table{{
		Schema: "dbo",
		Name:   "Users",
		Columns: []source.Column{
			{Name: "id", DataType: "int", IsNullable: false, OrdinalPos: 1},
		},
		PrimaryKey: []string{"id"},
	}}
	if _, err := o.reportSchemaDrift(current, true); err != nil {
		t.Fatalf("reportSchemaDrift returned excluded-table drift: %v", err)
	}
}

func TestSchemaSnapshotNamespaceIncludesSourceDatabase(t *testing.T) {
	first := &Orchestrator{config: &config.Config{
		Source: config.SourceConfig{Type: "mysql", Database: "tenant_a"},
	}}
	second := &Orchestrator{config: &config.Config{
		Source: config.SourceConfig{Type: "mysql", Database: "tenant_b"},
	}}

	if first.schemaSnapshotNamespace() == second.schemaSnapshotNamespace() {
		t.Fatalf("schema-less sources with different databases share namespace %q", first.schemaSnapshotNamespace())
	}
}

// TestTableNamesForTuning covers the small helper that bridges
// filterTables and SmartConfigAnalyzer.SetTableNameFilter (#241).
// The helper must (a) return nil for an empty input — that hands the
// analyzer back to its pre-#241 "no filter" path — and (b) preserve
// names verbatim so the analyzer's case-insensitive match still works.
func TestTableNamesForTuning(t *testing.T) {
	t.Run("nil for empty input keeps analyze CLI path unchanged", func(t *testing.T) {
		if got := tableNamesForTuning(nil); got != nil {
			t.Errorf("nil input: got %v, want nil", got)
		}
		if got := tableNamesForTuning([]source.Table{}); got != nil {
			t.Errorf("empty slice: got %v, want nil", got)
		}
	})

	t.Run("returns names in input order", func(t *testing.T) {
		tables := []source.Table{
			{Name: "orders"},
			{Name: "Customers"},
			{Name: "audit_log"},
		}
		got := tableNamesForTuning(tables)
		want := []string{"orders", "Customers", "audit_log"}
		if len(got) != len(want) {
			t.Fatalf("got %d names, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("got[%d]=%q, want %q", i, got[i], want[i])
			}
		}
	})
}
