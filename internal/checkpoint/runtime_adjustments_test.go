package checkpoint

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeAdjustmentFreshSchemaDeclaresMeasurementState(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	rows, err := state.db.Query("PRAGMA table_info(ai_adjustments)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		if name != "effect_measured" {
			continue
		}
		found = true
		if columnType != "INTEGER" || notNull != 1 || fmt.Sprint(defaultValue) != "0" {
			t.Fatalf("effect_measured schema = type:%q notnull:%d default:%v, want INTEGER/1/0",
				columnType, notNull, defaultValue)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("fresh ai_adjustments schema is missing effect_measured")
	}
}

func TestRuntimeAdjustmentLegacyUpgradeIsIdempotentAndPreservesAfterValues(t *testing.T) {
	dataDir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE runs (
			id TEXT PRIMARY KEY,
			started_at TEXT NOT NULL,
			completed_at TEXT,
			status TEXT NOT NULL,
			source_schema TEXT NOT NULL,
			target_schema TEXT NOT NULL,
			config TEXT
		);
		CREATE TABLE ai_adjustments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			adjustment_number INTEGER NOT NULL,
			timestamp TEXT NOT NULL,
			action TEXT NOT NULL,
			adjustments TEXT NOT NULL,
			throughput_before REAL,
			throughput_after REAL,
			effect_percent REAL,
			cpu_before REAL,
			cpu_after REAL,
			memory_before REAL,
			memory_after REAL,
			reasoning TEXT,
			confidence TEXT,
			UNIQUE(run_id, adjustment_number)
		);
		INSERT INTO runs (id, started_at, status, source_schema, target_schema)
		VALUES ('legacy-run', '2025-01-01 00:00:00', 'success', 'dbo', 'public');
		INSERT INTO ai_adjustments (
			run_id, adjustment_number, timestamp, action, adjustments,
			throughput_before, throughput_after, effect_percent,
			cpu_before, cpu_after, memory_before, memory_after
		) VALUES (
			'legacy-run', 1, '2025-01-01 00:00:01', 'scale_up', '{}',
			100.5, 123.5, 4.5, 20.5, 7.5, 40.5, 55.5
		);
	`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	assertLegacyRow := func(state *State) {
		t.Helper()
		var count, measured int
		var throughputAfter, effectPercent, cpuAfter, memoryAfter float64
		if err := state.db.QueryRow(`
			SELECT COUNT(*), effect_measured, throughput_after, effect_percent, cpu_after, memory_after
			FROM ai_adjustments
		`).Scan(&count, &measured, &throughputAfter, &effectPercent, &cpuAfter, &memoryAfter); err != nil {
			t.Fatal(err)
		}
		if count != 1 || measured != 0 || throughputAfter != 123.5 || effectPercent != 4.5 || cpuAfter != 7.5 || memoryAfter != 55.5 {
			t.Fatalf("upgraded legacy row = count:%d measured:%d after:[%v %v %v %v]",
				count, measured, throughputAfter, effectPercent, cpuAfter, memoryAfter)
		}
	}

	state, err := New(dataDir)
	if err != nil {
		t.Fatalf("first legacy upgrade: %v", err)
	}
	assertLegacyRow(state)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	state, err = New(dataDir)
	if err != nil {
		t.Fatalf("second idempotent legacy upgrade: %v", err)
	}
	defer state.Close()
	assertLegacyRow(state)

	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].EffectMeasured {
		t.Fatalf("legacy row semantic state = %#v, want one unmeasured record", records)
	}
}

func newRuntimeAdjustmentState(t *testing.T, runID string) *State {
	t.Helper()
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestSaveRuntimeAdjustmentUnmeasuredStoresNullAfterMetrics(t *testing.T) {
	state := newRuntimeAdjustmentState(t, "unmeasured-run")
	record := RuntimeAdjustmentRecord{
		AdjustmentNumber: 1,
		Timestamp:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Action:           "scale_up",
		Adjustments:      map[string]int{"workers": 4},
		ThroughputBefore: 100,
		ThroughputAfter:  999,
		EffectPercent:    888,
		CPUBefore:        20,
		CPUAfter:         777,
		MemoryBefore:     30,
		MemoryAfter:      666,
	}
	if err := state.SaveRuntimeAdjustment("unmeasured-run", record); err != nil {
		t.Fatal(err)
	}

	var measured int
	var throughputAfter, effectPercent, cpuAfter, memoryAfter sql.NullFloat64
	if err := state.db.QueryRow(`
		SELECT effect_measured, throughput_after, effect_percent, cpu_after, memory_after
		FROM ai_adjustments WHERE run_id = 'unmeasured-run' AND adjustment_number = 1
	`).Scan(&measured, &throughputAfter, &effectPercent, &cpuAfter, &memoryAfter); err != nil {
		t.Fatal(err)
	}
	if measured != 0 || throughputAfter.Valid || effectPercent.Valid || cpuAfter.Valid || memoryAfter.Valid {
		t.Fatalf("unmeasured storage = state:%d after:[%#v %#v %#v %#v], want 0 and four NULLs",
			measured, throughputAfter, effectPercent, cpuAfter, memoryAfter)
	}

	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].EffectMeasured || records[0].ThroughputAfter != 0 ||
		records[0].EffectPercent != 0 || records[0].CPUAfter != 0 || records[0].MemoryAfter != 0 {
		t.Fatalf("unmeasured round trip = %#v", records)
	}
}

func TestSaveRuntimeAdjustmentMeasuredZerosRemainPresent(t *testing.T) {
	state := newRuntimeAdjustmentState(t, "measured-zero-run")
	record := RuntimeAdjustmentRecord{
		AdjustmentNumber: 1,
		Timestamp:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Action:           "hold",
		EffectMeasured:   true,
	}
	if err := state.SaveRuntimeAdjustment("measured-zero-run", record); err != nil {
		t.Fatal(err)
	}

	var measured int
	after := make([]sql.NullFloat64, 4)
	if err := state.db.QueryRow(`
		SELECT effect_measured, throughput_after, effect_percent, cpu_after, memory_after
		FROM ai_adjustments WHERE run_id = 'measured-zero-run' AND adjustment_number = 1
	`).Scan(&measured, &after[0], &after[1], &after[2], &after[3]); err != nil {
		t.Fatal(err)
	}
	if measured != 1 {
		t.Fatalf("effect_measured = %d, want 1", measured)
	}
	for i, value := range after {
		if !value.Valid || value.Float64 != 0 {
			t.Fatalf("measured zero after field %d = %#v, want non-NULL zero", i, value)
		}
	}

	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !records[0].EffectMeasured || records[0].ThroughputAfter != 0 ||
		records[0].EffectPercent != 0 || records[0].CPUAfter != 0 || records[0].MemoryAfter != 0 {
		t.Fatalf("measured-zero round trip = %#v", records)
	}
}

func TestSaveRuntimeAdjustmentUpsertFillsMeasurementAtomically(t *testing.T) {
	state := newRuntimeAdjustmentState(t, "upsert-run")
	initial := RuntimeAdjustmentRecord{
		AdjustmentNumber: 2,
		Timestamp:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Action:           "scale_down",
		Adjustments:      map[string]int{"workers": 2},
		ThroughputBefore: 100,
		CPUBefore:        90,
		MemoryBefore:     80,
		Reasoning:        "initial decision",
		Confidence:       "deterministic",
	}
	if err := state.SaveRuntimeAdjustment("upsert-run", initial); err != nil {
		t.Fatal(err)
	}
	measured := initial
	measured.EffectMeasured = true
	measured.ThroughputAfter = 125
	measured.EffectPercent = 25
	measured.CPUAfter = 70
	measured.MemoryAfter = 75
	if err := state.SaveRuntimeAdjustment("upsert-run", measured); err != nil {
		t.Fatal(err)
	}

	records, err := state.GetRuntimeAdjustmentsByAction("scale_down", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("upsert row count = %d, want 1", len(records))
	}
	got := records[0]
	if !got.EffectMeasured || got.ThroughputAfter != 125 || got.EffectPercent != 25 || got.CPUAfter != 70 || got.MemoryAfter != 75 {
		t.Fatalf("upsert measurement = %#v", got)
	}
	if got.ThroughputBefore != 100 || got.CPUBefore != 90 || got.MemoryBefore != 80 || got.Reasoning != "initial decision" {
		t.Fatalf("upsert changed initial decision fields = %#v", got)
	}
}

func TestScanRuntimeAdjustmentCollapsesPartialMeasuredRow(t *testing.T) {
	state := newRuntimeAdjustmentState(t, "corrupt-run")
	_, err := state.db.Exec(`
		INSERT INTO ai_adjustments (
			run_id, adjustment_number, timestamp, action, adjustments,
			throughput_before, effect_measured, throughput_after, effect_percent,
			cpu_before, cpu_after, memory_before, memory_after
		) VALUES (
			'corrupt-run', 1, '2025-01-01 00:00:00', 'scale_up', '{}',
			100, 1, 110, 10, 20, NULL, 30, 40
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].EffectMeasured || records[0].ThroughputAfter != 0 ||
		records[0].EffectPercent != 0 || records[0].CPUAfter != 0 || records[0].MemoryAfter != 0 {
		t.Fatalf("partial measured row was not collapsed: %#v", records)
	}
}
