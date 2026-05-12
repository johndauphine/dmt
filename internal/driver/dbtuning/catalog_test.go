package dbtuning

import (
	"context"
	"database/sql"
	"testing"
)

// TestRegister_AppendsAndIsCaseInsensitive verifies that catalogs
// concatenate across calls and that lookup is case-insensitive.
func TestRegister_AppendsAndIsCaseInsensitive(t *testing.T) {
	const driverName = "test_catalog_appends"

	Register(driverName, []Setting{{Name: "a", Evaluate: stubEval("a", 1)}})
	Register(driverName, []Setting{{Name: "b", Evaluate: stubEval("b", 2)}})

	if got := SettingCount(driverName); got != 2 {
		t.Errorf("expected 2 settings after two Register calls, got %d", got)
	}
	if got := SettingCount("TEST_CATALOG_APPENDS"); got != 2 {
		t.Errorf("case-insensitive lookup failed; got %d", got)
	}
}

// TestSettingsFor_ReturnsCopy ensures callers can't mutate the
// shared catalog slice by accident.
func TestSettingsFor_ReturnsCopy(t *testing.T) {
	const driverName = "test_settings_copy"
	Register(driverName, []Setting{{Name: "x"}, {Name: "y"}})

	a := SettingsFor(driverName)
	a[0].Name = "MUTATED"

	b := SettingsFor(driverName)
	if b[0].Name == "MUTATED" {
		t.Errorf("SettingsFor returned a mutable reference to the shared slice; mutation leaked")
	}
}

// TestSettingsFor_UnknownDriver returns nil rather than panicking.
func TestSettingsFor_UnknownDriver(t *testing.T) {
	if got := SettingsFor("nonexistent-driver-xyz"); got != nil {
		t.Errorf("expected nil for unknown driver, got %v", got)
	}
}

// TestShippedCatalogs_HaveMinimumCoverage asserts each driver ships
// the documented number of settings. Drops if a setting is silently
// removed without intent.
func TestShippedCatalogs_HaveMinimumCoverage(t *testing.T) {
	tests := []struct {
		driver  string
		minimum int
	}{
		{"postgres", 10},
		{"mssql", 8},
		{"mysql", 9},
	}
	for _, tc := range tests {
		t.Run(tc.driver, func(t *testing.T) {
			if got := SettingCount(tc.driver); got < tc.minimum {
				t.Errorf("%s catalog has %d settings; minimum coverage is %d", tc.driver, got, tc.minimum)
			}
		})
	}
}

// TestRunCatalog_CollectsCurrentsAndRecs validates the dispatcher's
// contract: it walks each Setting, collects non-nil current values
// into the map, and appends recommendations.
func TestRunCatalog_CollectsCurrentsAndRecs(t *testing.T) {
	const driverName = "test_runcatalog"
	Register(driverName, []Setting{
		{Name: "a", Evaluate: func(_ context.Context, _ *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
			return "value-a", nil, nil // current value only, no rec
		}},
		{Name: "b", Evaluate: func(_ context.Context, _ *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
			return "value-b", &TuningRecommendation{Parameter: "b", Impact: "high", Priority: 1}, nil
		}},
		{Name: "skipped", Evaluate: func(_ context.Context, _ *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
			return nil, nil, nil // soft skip — no value, no rec, no error
		}},
		{Name: "errored", Evaluate: func(_ context.Context, _ *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
			return nil, nil, sql.ErrNoRows
		}},
	})

	current, recs := runCatalog(context.Background(), nil, driverName, RoleTarget, SystemInfo{}, SchemaStatistics{})

	if v, ok := current["a"]; !ok || v != "value-a" {
		t.Errorf("expected current['a']='value-a'; got %v ok=%v", v, ok)
	}
	if v, ok := current["b"]; !ok || v != "value-b" {
		t.Errorf("expected current['b']='value-b'; got %v ok=%v", v, ok)
	}
	if _, ok := current["skipped"]; ok {
		t.Errorf("skipped setting (nil current) should not appear in current map")
	}
	if _, ok := current["errored"]; ok {
		t.Errorf("errored setting should not appear in current map")
	}
	if len(recs) != 1 {
		t.Errorf("expected exactly one recommendation, got %d", len(recs))
	} else if recs[0].Parameter != "b" {
		t.Errorf("expected rec for 'b', got %q", recs[0].Parameter)
	}
}

// stubEval is a tiny test helper that returns the given value with no
// recommendation and no error. The "priority" arg is unused today but
// reserved for tests that want to thread distinct settings through
// runCatalog.
func stubEval(value string, _ int) EvaluateFunc {
	return func(_ context.Context, _ *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
		return value, nil, nil
	}
}
