package checkpoint

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

func createLegacyAITimestampDB(t *testing.T, dataDir string, indexCollision bool) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE ai_adjustments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id TEXT NOT NULL,
			adjustment_number INTEGER NOT NULL,
			timestamp TEXT NOT NULL,
			action TEXT NOT NULL,
			adjustments TEXT NOT NULL,
			throughput_before REAL,
			effect_measured INTEGER NOT NULL DEFAULT 0,
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

		CREATE TABLE ai_tuning_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL,
			source_db_type TEXT NOT NULL,
			target_db_type TEXT,
			total_tables INTEGER NOT NULL,
			total_rows INTEGER NOT NULL,
			avg_row_size_bytes INTEGER,
			cpu_cores INTEGER,
			memory_gb INTEGER,
			workers INTEGER NOT NULL,
			chunk_size INTEGER NOT NULL,
			read_ahead_buffers INTEGER,
			write_ahead_writers INTEGER,
			parallel_readers INTEGER,
			max_partitions INTEGER,
			large_table_threshold INTEGER,
			max_source_connections INTEGER,
			max_target_connections INTEGER,
			estimated_memory_mb INTEGER,
			ai_reasoning TEXT,
			was_ai_used INTEGER NOT NULL DEFAULT 0
		);

		INSERT INTO ai_tuning_history (
			timestamp, source_db_type, target_db_type, total_tables, total_rows,
			workers, chunk_size
		) VALUES
			('2024-11-03 01:30:00', 'mssql', 'postgres', 1, 10, 1, 1000),
			('2024-03-10 02:30:00', 'mssql', 'postgres', 2, 20, 2, 2000),
			('not-a-timestamp', 'mssql', 'postgres', 3, 30, 3, 3000);

		INSERT INTO ai_adjustments (
			run_id, adjustment_number, timestamp, action, adjustments
		) VALUES
			('legacy-run', 1, '2024-11-03 01:30:00', 'scale_down', '{}'),
			('legacy-run', 2, 'malformed', 'scale_down', '{}');
	`)
	if err != nil {
		t.Fatal(err)
	}
	if indexCollision {
		if _, err := db.Exec(`CREATE TABLE idx_ai_adjustments_timestamp_unix_ms (value INTEGER)`); err != nil {
			t.Fatal(err)
		}
	}
}

func assertAITimestampSchema(t *testing.T, state *State) {
	t.Helper()
	for _, table := range []string{"ai_tuning_history", "ai_adjustments"} {
		var columnType string
		var notNull int
		query := `SELECT type, "notnull" FROM pragma_table_info(?) WHERE name = 'timestamp_unix_ms'`
		if err := state.db.QueryRow(query, table).Scan(&columnType, &notNull); err != nil {
			t.Fatalf("%s timestamp_unix_ms: %v", table, err)
		}
		if columnType != "INTEGER" || notNull != 0 {
			t.Fatalf("%s.timestamp_unix_ms = type %q notnull %d, want nullable INTEGER", table, columnType, notNull)
		}
	}
	for _, index := range []string{"idx_ai_tuning_timestamp_unix_ms", "idx_ai_adjustments_timestamp_unix_ms"} {
		var count int
		if err := state.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, index).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("index %s count = %d, want 1", index, count)
		}
	}
}

func TestAITimestampFreshSchemaHasNullableEpochColumnsAndIndexes(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	assertAITimestampSchema(t, state)
}

func TestAITimestampLegacyMigrationPreservesUnresolvedRowsAndIsIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	createLegacyAITimestampDB(t, dataDir, false)
	var logOutput bytes.Buffer
	logging.SetOutput(&logOutput)
	defer logging.SetOutput(os.Stdout)

	assertState := func(state *State) {
		t.Helper()
		assertAITimestampSchema(t, state)

		var tuningRows, resolvedTuning, adjustmentRows, resolvedAdjustments int
		if err := state.db.QueryRow(`SELECT COUNT(*), COUNT(timestamp_unix_ms) FROM ai_tuning_history`).Scan(&tuningRows, &resolvedTuning); err != nil {
			t.Fatal(err)
		}
		if err := state.db.QueryRow(`SELECT COUNT(*), COUNT(timestamp_unix_ms) FROM ai_adjustments`).Scan(&adjustmentRows, &resolvedAdjustments); err != nil {
			t.Fatal(err)
		}
		if tuningRows != 3 || resolvedTuning != 0 || adjustmentRows != 2 || resolvedAdjustments != 0 {
			t.Fatalf("legacy counts = tuning %d/%d adjustments %d/%d, want 3/0 and 2/0",
				tuningRows, resolvedTuning, adjustmentRows, resolvedAdjustments)
		}

		var legacyText string
		if err := state.db.QueryRow(`
			SELECT group_concat(timestamp, '|')
			FROM (SELECT timestamp FROM ai_tuning_history ORDER BY id)
		`).Scan(&legacyText); err != nil {
			t.Fatal(err)
		}
		wantText := "2024-11-03 01:30:00|2024-03-10 02:30:00|not-a-timestamp"
		if legacyText != wantText {
			t.Fatalf("legacy tuning text = %q, want %q", legacyText, wantText)
		}

		history, err := state.GetTuningHistory(0, "mssql", "postgres")
		if err != nil {
			t.Fatal(err)
		}
		if len(history) != 3 || history[0].ID != 3 || history[1].ID != 2 || history[2].ID != 1 {
			t.Fatalf("legacy history order = %#v, want IDs 3,2,1", history)
		}
		for _, row := range history {
			if !row.Timestamp.IsZero() {
				t.Fatalf("legacy tuning row %d fabricated timestamp %v", row.ID, row.Timestamp)
			}
		}

		adjustments, err := state.GetRuntimeAdjustments(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(adjustments) != 2 || adjustments[0].ID != 2 || adjustments[1].ID != 1 {
			t.Fatalf("legacy adjustment order = %#v, want IDs 2,1", adjustments)
		}
		for _, row := range adjustments {
			if !row.Timestamp.IsZero() {
				t.Fatalf("legacy adjustment row %d fabricated timestamp %v", row.ID, row.Timestamp)
			}
		}
	}

	state, err := New(dataDir)
	if err != nil {
		t.Fatalf("first migration: %v", err)
	}
	assertState(state)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	state, err = New(dataDir)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer state.Close()
	assertState(state)
	const warningText = "Legacy AI history timestamps remain unresolved"
	if count := strings.Count(logOutput.String(), warningText); count != 1 {
		t.Fatalf("legacy timestamp warning count = %d, want exactly 1 across migration and reopen; output: %s", count, logOutput.String())
	}
}

func TestAITimestampMigrationRollsBackBothTablesOnIndexFailure(t *testing.T) {
	dataDir := t.TempDir()
	createLegacyAITimestampDB(t, dataDir, true)

	if state, err := New(dataDir); err == nil {
		_ = state.Close()
		t.Fatal("migration unexpectedly succeeded with an index-name collision")
	} else if !strings.Contains(err.Error(), "idx_ai_adjustments_timestamp_unix_ms") {
		t.Fatalf("migration error = %v, want adjustment index collision", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"ai_tuning_history", "ai_adjustments"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = 'timestamp_unix_ms'`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s epoch column survived rolled-back migration", table)
		}
	}
	var tuningIndexCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_ai_tuning_timestamp_unix_ms'`).Scan(&tuningIndexCount); err != nil {
		t.Fatal(err)
	}
	if tuningIndexCount != 0 {
		t.Fatal("tuning epoch index survived rolled-back migration")
	}
}

func TestAITimestampPartialMigrationCompletesWithoutBackfill(t *testing.T) {
	dataDir := t.TempDir()
	createLegacyAITimestampDB(t, dataDir, false)
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		ALTER TABLE ai_tuning_history ADD COLUMN timestamp_unix_ms INTEGER;
		CREATE INDEX idx_ai_tuning_timestamp_unix_ms ON ai_tuning_history(timestamp_unix_ms);
	`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	assertAITimestampSchema(t, state)
	var resolvedTuning, resolvedAdjustments int
	if err := state.db.QueryRow(`SELECT COUNT(timestamp_unix_ms) FROM ai_tuning_history`).Scan(&resolvedTuning); err != nil {
		t.Fatal(err)
	}
	if err := state.db.QueryRow(`SELECT COUNT(timestamp_unix_ms) FROM ai_adjustments`).Scan(&resolvedAdjustments); err != nil {
		t.Fatal(err)
	}
	if resolvedTuning != 0 || resolvedAdjustments != 0 {
		t.Fatalf("partial migration backfilled tuning=%d adjustments=%d timestamps", resolvedTuning, resolvedAdjustments)
	}
}

func TestAITimestampExistingColumnsWithUnresolvedRowsWarnOncePerDatabase(t *testing.T) {
	dataDir := t.TempDir()
	createLegacyAITimestampDB(t, dataDir, false)
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "migrate.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		ALTER TABLE ai_tuning_history ADD COLUMN timestamp_unix_ms INTEGER;
		CREATE INDEX idx_ai_tuning_timestamp_unix_ms ON ai_tuning_history(timestamp_unix_ms);
		ALTER TABLE ai_adjustments ADD COLUMN timestamp_unix_ms INTEGER;
		CREATE INDEX idx_ai_adjustments_timestamp_unix_ms ON ai_adjustments(timestamp_unix_ms);
	`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var logOutput bytes.Buffer
	logging.SetOutput(&logOutput)
	defer logging.SetOutput(os.Stdout)
	for i := 0; i < 2; i++ {
		state, err := New(dataDir)
		if err != nil {
			t.Fatalf("open %d: %v", i+1, err)
		}
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	}
	const warningText = "Legacy AI history timestamps remain unresolved"
	if count := strings.Count(logOutput.String(), warningText); count != 1 {
		t.Fatalf("existing-column legacy warning count = %d, want 1; output: %s", count, logOutput.String())
	}
}

func TestAITimestampTuningDualWriteAndMixedOrdering(t *testing.T) {
	state, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	_, err = state.db.Exec(`
		INSERT INTO ai_tuning_history (
			timestamp, source_db_type, target_db_type, total_tables, total_rows,
			workers, chunk_size
		) VALUES
			('9999-12-31 23:59:59', 'mssql', 'postgres', 1, 10, 1, 1000),
			('not-a-timestamp', 'mssql', 'postgres', 2, 20, 2, 2000)
	`)
	if err != nil {
		t.Fatal(err)
	}

	older := time.Date(2024, 1, 2, 3, 4, 5, 123_000_000, time.FixedZone("west", -7*60*60))
	latest := time.Date(2025, 6, 7, 8, 9, 10, 987_000_000, time.FixedZone("east", 5*60*60+30*60))
	for i, ts := range []time.Time{older, latest, latest} {
		if _, err := state.SaveTuningRecord(TuningRecord{
			Timestamp:    ts,
			SourceDBType: "mssql",
			TargetDBType: "postgres",
			TotalTables:  i + 3,
			TotalRows:    int64(i + 30),
			Workers:      i + 3,
			ChunkSize:    (i + 3) * 1000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var legacyText string
	var epoch int64
	if err := state.db.QueryRow(`SELECT timestamp, timestamp_unix_ms FROM ai_tuning_history WHERE id = 4`).Scan(&legacyText, &epoch); err != nil {
		t.Fatal(err)
	}
	if legacyText != latest.UTC().Format(legacyAITimestampLayout) || epoch != latest.UTC().UnixMilli() {
		t.Fatalf("dual write = %q/%d, want %q/%d", legacyText, epoch,
			latest.UTC().Format(legacyAITimestampLayout), latest.UTC().UnixMilli())
	}

	history, err := state.GetTuningHistory(0, "mssql", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []int64{5, 4, 3, 2, 1}
	if len(history) != len(wantIDs) {
		t.Fatalf("history length = %d, want %d", len(history), len(wantIDs))
	}
	for i, wantID := range wantIDs {
		if history[i].ID != wantID {
			t.Fatalf("history[%d].ID = %d, want %d", i, history[i].ID, wantID)
		}
		if i < 3 && history[i].Timestamp.Location() != time.UTC {
			t.Fatalf("history[%d] timestamp location = %v, want UTC", i, history[i].Timestamp.Location())
		}
		if i >= 3 && !history[i].Timestamp.IsZero() {
			t.Fatalf("legacy history[%d] timestamp = %v, want unknown", i, history[i].Timestamp)
		}
	}
	limited, err := state.GetTuningHistory(3, "mssql", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 3 || limited[0].ID != 5 || limited[1].ID != 4 || limited[2].ID != 3 {
		t.Fatalf("limited mixed order = %#v", limited)
	}

	if _, err := state.SaveTuningRecord(TuningRecord{}); err == nil {
		t.Fatal("zero tuning timestamp should be rejected")
	}
	var rowCount int
	if err := state.db.QueryRow(`SELECT COUNT(*) FROM ai_tuning_history`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	if rowCount != 5 {
		t.Fatalf("zero timestamp write changed row count to %d", rowCount)
	}
}

func TestAITimestampRuntimeDualWriteOrderingAndUpsertPreservation(t *testing.T) {
	state := newRuntimeAdjustmentState(t, "epoch-run")
	initialTime := time.Date(2025, 2, 3, 4, 5, 6, 789_000_000, time.FixedZone("east", 2*60*60))
	initial := RuntimeAdjustmentRecord{
		AdjustmentNumber: 1,
		Timestamp:        initialTime,
		Action:           "scale_down",
		Adjustments:      map[string]int{"workers": 2},
	}
	if err := state.SaveRuntimeAdjustment("epoch-run", initial); err != nil {
		t.Fatal(err)
	}
	measured := initial
	measured.Timestamp = initialTime.Add(24 * time.Hour)
	measured.EffectMeasured = true
	if err := state.SaveRuntimeAdjustment("epoch-run", measured); err != nil {
		t.Fatal(err)
	}
	second := initial
	second.AdjustmentNumber = 2
	if err := state.SaveRuntimeAdjustment("epoch-run", second); err != nil {
		t.Fatal(err)
	}
	_, err := state.db.Exec(`
		INSERT INTO ai_adjustments (
			run_id, adjustment_number, timestamp, action, adjustments
		) VALUES
			('epoch-run', 3, '9999-12-31 23:59:59', 'scale_down', '{}'),
			('epoch-run', 4, 'malformed', 'scale_down', '{}')
	`)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	var legacyText string
	var epoch int64
	if err := state.db.QueryRow(`
		SELECT COUNT(*), timestamp, timestamp_unix_ms
		FROM ai_adjustments WHERE run_id = 'epoch-run' AND adjustment_number = 1
	`).Scan(&count, &legacyText, &epoch); err != nil {
		t.Fatal(err)
	}
	if count != 1 || legacyText != initialTime.UTC().Format(legacyAITimestampLayout) || epoch != initialTime.UTC().UnixMilli() {
		t.Fatalf("upsert timestamp = count %d, %q/%d; want initial %q/%d",
			count, legacyText, epoch, initialTime.UTC().Format(legacyAITimestampLayout), initialTime.UTC().UnixMilli())
	}

	adjustments, err := state.GetRuntimeAdjustmentsByAction("scale_down", 0)
	if err != nil {
		t.Fatal(err)
	}
	wantNumbers := []int{2, 1, 4, 3}
	if len(adjustments) != len(wantNumbers) {
		t.Fatalf("adjustment count = %d, want %d", len(adjustments), len(wantNumbers))
	}
	for i, wantNumber := range wantNumbers {
		if adjustments[i].AdjustmentNumber != wantNumber {
			t.Fatalf("adjustments[%d].AdjustmentNumber = %d, want %d", i, adjustments[i].AdjustmentNumber, wantNumber)
		}
		if i < 2 && adjustments[i].Timestamp.Location() != time.UTC {
			t.Fatalf("adjustments[%d] timestamp location = %v, want UTC", i, adjustments[i].Timestamp.Location())
		}
		if i >= 2 && !adjustments[i].Timestamp.IsZero() {
			t.Fatalf("legacy adjustment %d fabricated timestamp %v", wantNumber, adjustments[i].Timestamp)
		}
	}

	if err := state.SaveRuntimeAdjustment("epoch-run", RuntimeAdjustmentRecord{AdjustmentNumber: 5}); err == nil {
		t.Fatal("zero runtime-adjustment timestamp should be rejected")
	}
}

func TestProjectionContextFingerprintMigrationKeepsLegacyRowsIncompatible(t *testing.T) {
	dataDir := t.TempDir()
	createLegacyAITimestampDB(t, dataDir, false)

	state, err := New(dataDir)
	if err != nil {
		t.Fatalf("migrating legacy tuning history: %v", err)
	}
	defer state.Close()

	var columnType string
	var notNull int
	if err := state.db.QueryRow(`
		SELECT type, "notnull"
		FROM pragma_table_info('ai_tuning_history')
		WHERE name = 'projection_context_fingerprint'
	`).Scan(&columnType, &notNull); err != nil {
		t.Fatalf("projection-context migration column: %v", err)
	}
	if columnType != "TEXT" || notNull != 0 {
		t.Fatalf("projection_context_fingerprint = type %q notnull %d, want nullable TEXT", columnType, notNull)
	}

	history, err := state.GetTuningHistory(0, "mssql", "postgres")
	if err != nil {
		t.Fatalf("GetTuningHistory after projection-context migration: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("legacy history rows = %d, want 3", len(history))
	}
	for _, row := range history {
		if row.ProjectionContextFingerprint != "" {
			t.Fatalf("legacy row %d invented projection context %q", row.ID, row.ProjectionContextFingerprint)
		}
		if row.SafetyProjected {
			t.Fatalf("legacy row %d unexpectedly marked projected", row.ID)
		}
	}
}
