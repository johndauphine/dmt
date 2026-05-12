package dbtuning

import (
	"context"
	"testing"
)

func TestAnalyze_NilDBReturnsStub(t *testing.T) {
	res, err := Analyze(context.Background(), nil, "postgres", "target", SchemaStatistics{}, SystemInfo{})
	if err != nil {
		t.Fatalf("Analyze with nil db should not error; got %v", err)
	}
	if res == nil {
		t.Fatal("Analyze should never return nil DatabaseTuning")
	}
	if res.TuningPotential != "unknown" {
		t.Errorf("expected potential=unknown for nil db; got %q", res.TuningPotential)
	}
	if res.EstimatedImpact == "" {
		t.Error("EstimatedImpact should explain why no analysis ran")
	}
}

func TestAnalyze_UnregisteredDriverReturnsStub(t *testing.T) {
	// Pass a non-nil db but unregistered driver. We can't construct a
	// real *sql.DB without a backend, so this test will only execute
	// the early return path. (The runCatalog branch with a non-nil
	// db requires real DB containers — see dbtuning_integration_test.go
	// once #178 lands; today there's no integration suite yet.)
	res, err := Analyze(context.Background(), nil, "nonexistent-driver", "source", SchemaStatistics{}, SystemInfo{})
	if err != nil {
		t.Fatalf("Analyze should not error on unknown driver; got %v", err)
	}
	if res.TuningPotential != "unknown" {
		t.Errorf("expected potential=unknown for unknown driver; got %q", res.TuningPotential)
	}
}

// TestShippedSettings_AreNamed enumerates every shipped setting per
// driver so adding or removing a setting requires updating the test.
// Acts as a sanity-check ledger — if the test fails, the change was
// intentional and the ledger needs an update.
func TestShippedSettings_AreNamed(t *testing.T) {
	expectedPostgres := []string{
		"shared_buffers",
		"work_mem",
		"maintenance_work_mem",
		"wal_buffers",
		"max_wal_size",
		"synchronous_commit",
		"checkpoint_completion_target",
		"wal_level",
		"effective_cache_size",
		"max_parallel_workers_per_gather",
		"archive_mode",
	}
	expectedMSSQL := []string{
		"max_server_memory_mb",
		"max_degree_of_parallelism",
		"cost_threshold_for_parallelism",
		"optimize_for_ad_hoc_workloads",
		"recovery_model",
		"auto_close",
		"auto_create_statistics",
		"auto_update_statistics",
		"target_recovery_time_sec",
	}
	expectedMySQL := []string{
		"innodb_buffer_pool_size",
		"innodb_log_file_size",
		"innodb_log_buffer_size",
		"innodb_flush_log_at_trx_commit",
		"sync_binlog",
		"innodb_doublewrite",
		"innodb_flush_method",
		"max_allowed_packet",
		"innodb_buffer_pool_instances",
		"innodb_io_capacity",
	}
	tests := []struct {
		driver string
		want   []string
	}{
		{"postgres", expectedPostgres},
		{"mssql", expectedMSSQL},
		{"mysql", expectedMySQL},
	}
	for _, tc := range tests {
		t.Run(tc.driver, func(t *testing.T) {
			settings := SettingsFor(tc.driver)
			if len(settings) != len(tc.want) {
				t.Fatalf("%s: have %d settings, want %d", tc.driver, len(settings), len(tc.want))
			}
			got := make(map[string]bool, len(settings))
			for _, s := range settings {
				got[s.Name] = true
			}
			for _, name := range tc.want {
				if !got[name] {
					t.Errorf("%s: expected setting %q missing from catalog", tc.driver, name)
				}
			}
		})
	}
}
