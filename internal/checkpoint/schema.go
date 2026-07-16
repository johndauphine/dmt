package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
)

var legacyAITimestampWarnings sync.Map

func (s *State) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS runs (
		id TEXT PRIMARY KEY,
		started_at TEXT NOT NULL,
		completed_at TEXT,
		last_heartbeat TEXT,
		status TEXT NOT NULL DEFAULT 'running',
		resumable INTEGER NOT NULL DEFAULT 0,
		resumability_reason TEXT,
		phase TEXT NOT NULL DEFAULT 'initializing',
		source_schema TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		config TEXT,
		profile_name TEXT,
		config_path TEXT,
		strict_consistency INTEGER NOT NULL DEFAULT 0,
		lease_target_key TEXT,
		lease_owner_token TEXT,
		lease_generation INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS migration_leases (
		target_key TEXT PRIMARY KEY,
		target_identity TEXT NOT NULL,
		owner_token TEXT NOT NULL,
		generation INTEGER NOT NULL,
		run_id TEXT,
		acquired_at TEXT NOT NULL,
		renewed_at TEXT NOT NULL,
		expires_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT REFERENCES runs(id),
		task_type TEXT NOT NULL,
		task_key TEXT NOT NULL,
		task_schema TEXT,
		task_table TEXT,
		task_partition_id INTEGER,
		status TEXT NOT NULL DEFAULT 'pending',
		started_at TEXT,
		completed_at TEXT,
		retry_count INTEGER DEFAULT 0,
		max_retries INTEGER DEFAULT 3,
		error_message TEXT,
		snapshot_row_count INTEGER,
		UNIQUE(run_id, task_key)
	);

	CREATE TABLE IF NOT EXISTS task_outputs (
		task_id INTEGER REFERENCES tasks(id),
		key TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (task_id, key)
	);

	CREATE TABLE IF NOT EXISTS transfer_progress (
		task_id INTEGER PRIMARY KEY REFERENCES tasks(id),
		table_name TEXT NOT NULL,
		partition_id INTEGER,
		last_pk TEXT,
		rows_done INTEGER DEFAULT 0,
		rows_total INTEGER,
		updated_at TEXT
	);

	CREATE TABLE IF NOT EXISTS profiles (
		name TEXT PRIMARY KEY,
		description TEXT,
		config_enc BLOB NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS table_sync_timestamps (
		source_schema TEXT NOT NULL,
		table_name TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		last_sync_timestamp TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (source_schema, table_name, target_schema)
	);

	-- Immutable incremental watermark fence H1, sampled once when a run starts
	-- and read back unchanged on resume. Reads replay updated_at > T0 without a
	-- fragile engine-specific SQL upper bound, while the persisted sync watermark
	-- never advances past H1 or unfinished work (#647).
	CREATE TABLE IF NOT EXISTS incremental_fences (
		run_id TEXT NOT NULL,
		source_schema TEXT NOT NULL,
		table_name TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		upper_fence TEXT NOT NULL,
		created_at TEXT NOT NULL,
		PRIMARY KEY (run_id, source_schema, table_name, target_schema)
	);

	CREATE TABLE IF NOT EXISTS delete_reconciliations (
		source_schema TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		last_run_id TEXT NOT NULL,
		last_success_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (source_schema, target_schema)
	);

	CREATE TABLE IF NOT EXISTS delete_reconciliation_tables (
		run_id TEXT NOT NULL,
		table_name TEXT NOT NULL,
		candidate_rows INTEGER NOT NULL DEFAULT 0,
		deleted_rows INTEGER NOT NULL DEFAULT 0,
		skipped INTEGER NOT NULL DEFAULT 0,
		skip_reason TEXT,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (run_id, table_name)
	);

	CREATE TABLE IF NOT EXISTS schema_snapshots (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		source_schema TEXT NOT NULL,
		table_name TEXT NOT NULL,
		run_id TEXT NOT NULL,
		captured_at TEXT NOT NULL,
		schema_json TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_schema_snapshots_source_table
		ON schema_snapshots(source_schema, table_name, id);

	CREATE INDEX IF NOT EXISTS idx_tasks_run_status ON tasks(run_id, status);
	CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type);

	CREATE TABLE IF NOT EXISTS ai_adjustments (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		adjustment_number INTEGER NOT NULL,
		timestamp TEXT NOT NULL,
		timestamp_unix_ms INTEGER,
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

	CREATE INDEX IF NOT EXISTS idx_ai_adjustments_run ON ai_adjustments(run_id);
	CREATE INDEX IF NOT EXISTS idx_ai_adjustments_action ON ai_adjustments(action);

	CREATE TABLE IF NOT EXISTS ai_tuning_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp TEXT NOT NULL,
		timestamp_unix_ms INTEGER,
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
		was_ai_used INTEGER NOT NULL DEFAULT 0,
		safety_projected INTEGER NOT NULL DEFAULT 0,
		execution_chunk_size_min INTEGER,
		execution_chunk_size_max INTEGER,
		projection_context_fingerprint TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_ai_tuning_timestamp ON ai_tuning_history(timestamp);
	CREATE INDEX IF NOT EXISTS idx_ai_tuning_source_type ON ai_tuning_history(source_db_type);

	-- #176: per-run AI fallback events. Cross-process readable so
	-- "dmt status" (which spawns a fresh process) sees the running
	-- migration counts. UPSERT on (run_id, surface, fingerprint) keeps
	-- one row per distinct fingerprint and accumulates the count, so a
	-- pathological run with 10K Raw columns produces O(distinct types)
	-- rows, not O(occurrences). Empty fingerprint still aggregates under
	-- one row per surface.
	CREATE TABLE IF NOT EXISTS fallback_events (
		run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		surface TEXT NOT NULL,
		fingerprint TEXT NOT NULL DEFAULT '',
		count INTEGER NOT NULL DEFAULT 0,
		first_seen TEXT NOT NULL,
		last_seen TEXT NOT NULL,
		PRIMARY KEY (run_id, surface, fingerprint)
	);

	CREATE INDEX IF NOT EXISTS idx_fallback_events_run ON fallback_events(run_id);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	if err := s.ensureRunColumns(); err != nil {
		return err
	}
	if err := s.ensureProfileColumns(); err != nil {
		return err
	}
	if err := s.ensureTransferProgressColumns(); err != nil {
		return err
	}
	if err := s.ensureStrictSnapshotColumns(); err != nil {
		return err
	}
	if err := s.ensureTaskIdentityColumns(); err != nil {
		return err
	}
	if err := s.ensureRuntimeAdjustmentColumns(); err != nil {
		return err
	}
	if err := s.ensureTuningResultColumns(); err != nil {
		return err
	}
	if err := s.ensureAITimestampColumns(); err != nil {
		return err
	}

	// One-time migration: sanitize any passwords stored in config column
	return s.sanitizeStoredConfigs()
}

// ensureAITimestampColumns transactionally adds the unambiguous UTC epoch
// representation used by AI tuning history and runtime adjustments. Existing
// text timestamps are deliberately retained and left unresolved: their source
// timezone was never stored, so assigning an instant would fabricate data.
func (s *State) ensureAITimestampColumns() (err error) {
	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("opening connection for AI timestamp migration: %w", err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("beginning AI timestamp migration: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	type timestampTable struct {
		name     string
		pragma   string
		alterDDL string
		indexDDL string
	}
	tables := []timestampTable{
		{
			name:   "ai_tuning_history",
			pragma: "PRAGMA table_info(ai_tuning_history)",
			alterDDL: "ALTER TABLE ai_tuning_history " +
				"ADD COLUMN timestamp_unix_ms INTEGER",
			indexDDL: `CREATE INDEX IF NOT EXISTS idx_ai_tuning_timestamp_unix_ms
				ON ai_tuning_history(timestamp_unix_ms)`,
		},
		{
			name:   "ai_adjustments",
			pragma: "PRAGMA table_info(ai_adjustments)",
			alterDDL: "ALTER TABLE ai_adjustments " +
				"ADD COLUMN timestamp_unix_ms INTEGER",
			indexDDL: `CREATE INDEX IF NOT EXISTS idx_ai_adjustments_timestamp_unix_ms
				ON ai_adjustments(timestamp_unix_ms)`,
		},
	}

	for _, table := range tables {
		rows, queryErr := conn.QueryContext(ctx, table.pragma)
		if queryErr != nil {
			return fmt.Errorf("reading %s columns for AI timestamp migration: %w", table.name, queryErr)
		}
		hasEpoch := false
		for rows.Next() {
			var cid int
			var name, columnType string
			var notNull int
			var defaultValue any
			var primaryKey int
			if scanErr := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); scanErr != nil {
				_ = rows.Close()
				return fmt.Errorf("scanning %s columns for AI timestamp migration: %w", table.name, scanErr)
			}
			if name == "timestamp_unix_ms" {
				hasEpoch = true
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return fmt.Errorf("reading %s columns for AI timestamp migration: %w", table.name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			return fmt.Errorf("closing %s columns for AI timestamp migration: %w", table.name, closeErr)
		}

		if !hasEpoch {
			if _, err = conn.ExecContext(ctx, table.alterDDL); err != nil {
				return fmt.Errorf("migrating %s.timestamp_unix_ms: %w", table.name, err)
			}
		}
		if _, err = conn.ExecContext(ctx, table.indexDDL); err != nil {
			return fmt.Errorf("indexing %s.timestamp_unix_ms: %w", table.name, err)
		}
	}

	var unresolved bool
	if err = conn.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM ai_tuning_history WHERE timestamp_unix_ms IS NULL)
		    OR EXISTS(SELECT 1 FROM ai_adjustments WHERE timestamp_unix_ms IS NULL)
	`).Scan(&unresolved); err != nil {
		return fmt.Errorf("checking unresolved legacy AI timestamps: %w", err)
	}

	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("committing AI timestamp migration: %w", err)
	}
	committed = true
	if unresolved {
		if _, alreadyWarned := legacyAITimestampWarnings.LoadOrStore(s.dbPath, struct{}{}); !alreadyWarned {
			logging.Warn("Legacy AI history timestamps remain unresolved because their timezone is unknown; time-based calculations exclude them and count-based reads use deterministic ID ordering")
		}
	}
	return nil
}

// ensureRuntimeAdjustmentColumns transactionally upgrades legacy
// ai_adjustments tables with an explicit observation-state bit. SQLite fills
// the NOT NULL DEFAULT 0 value for every existing row without rewriting any of
// its legacy after metrics; those values remain available for forensics but
// are semantically unmeasured. The PRAGMA check makes repeated startup
// migrations idempotent.
func (s *State) ensureRuntimeAdjustmentColumns() (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("beginning ai_adjustments migration: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.Query("PRAGMA table_info(ai_adjustments)")
	if err != nil {
		return fmt.Errorf("reading ai_adjustments columns: %w", err)
	}
	hasEffectMeasured := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if scanErr := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); scanErr != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning ai_adjustments columns: %w", scanErr)
		}
		if name == "effect_measured" {
			hasEffectMeasured = true
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return fmt.Errorf("reading ai_adjustments columns: %w", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return fmt.Errorf("closing ai_adjustments columns: %w", closeErr)
	}

	if !hasEffectMeasured {
		if _, err = tx.Exec(`
			ALTER TABLE ai_adjustments
			ADD COLUMN effect_measured INTEGER NOT NULL DEFAULT 0
		`); err != nil {
			return fmt.Errorf("migrating ai_adjustments.effect_measured: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing ai_adjustments migration: %w", err)
	}
	return nil
}

func (s *State) ensureTaskIdentityColumns() error {
	columns, err := s.tableColumns("tasks")
	if err != nil {
		return err
	}
	present := make(map[string]bool, len(columns))
	for _, column := range columns {
		present[column] = true
	}
	for _, column := range []struct {
		name     string
		typeName string
	}{
		{name: "task_schema", typeName: "TEXT"},
		{name: "task_table", typeName: "TEXT"},
		{name: "task_partition_id", typeName: "INTEGER"},
	} {
		if present[column.name] {
			continue
		}
		if _, err := s.db.Exec(fmt.Sprintf("ALTER TABLE tasks ADD COLUMN %s %s", column.name, column.typeName)); err != nil {
			return fmt.Errorf("adding tasks.%s: %w", column.name, err)
		}
	}
	_, err = s.db.Exec(`
		DROP INDEX IF EXISTS idx_tasks_structured_identity;
		CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_structured_table_identity
		ON tasks(run_id, task_type, task_schema, task_table)
		WHERE task_schema IS NOT NULL AND task_table IS NOT NULL
		  AND task_partition_id IS NULL;
		CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_structured_partition_identity
		ON tasks(run_id, task_type, task_schema, task_table, task_partition_id)
		WHERE task_schema IS NOT NULL AND task_table IS NOT NULL
		  AND task_partition_id IS NOT NULL;
	`)
	return err
}

func (s *State) ensureRunColumns() error {
	columns, err := s.tableColumns("runs")
	if err != nil {
		return err
	}

	needsProfile := true
	needsConfigPath := true
	needsError := true
	needsPhase := true
	needsConfigHash := true
	needsLastHeartbeat := true
	needsResumable := true
	needsResumabilityReason := true
	needsLeaseTargetKey := true
	needsLeaseOwnerToken := true
	needsLeaseGeneration := true
	for _, col := range columns {
		switch col {
		case "profile_name":
			needsProfile = false
		case "config_path":
			needsConfigPath = false
		case "error":
			needsError = false
		case "phase":
			needsPhase = false
		case "config_hash":
			needsConfigHash = false
		case "last_heartbeat":
			needsLastHeartbeat = false
		case "resumable":
			needsResumable = false
		case "resumability_reason":
			needsResumabilityReason = false
		case "lease_target_key":
			needsLeaseTargetKey = false
		case "lease_owner_token":
			needsLeaseOwnerToken = false
		case "lease_generation":
			needsLeaseGeneration = false
		}
	}

	if needsProfile {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN profile_name TEXT`); err != nil {
			return err
		}
	}
	if needsConfigPath {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN config_path TEXT`); err != nil {
			return err
		}
	}
	if needsError {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN error TEXT`); err != nil {
			return err
		}
	}
	if needsPhase {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN phase TEXT DEFAULT 'initializing'`); err != nil {
			return err
		}
	}
	if needsConfigHash {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN config_hash TEXT`); err != nil {
			return err
		}
	}
	if needsLastHeartbeat {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN last_heartbeat TEXT`); err != nil {
			return err
		}
	}
	if needsResumable {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN resumable INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if needsResumabilityReason {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN resumability_reason TEXT`); err != nil {
			return err
		}
	}
	if needsLeaseTargetKey {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN lease_target_key TEXT`); err != nil {
			return err
		}
	}
	if needsLeaseOwnerToken {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN lease_owner_token TEXT`); err != nil {
			return err
		}
	}
	if needsLeaseGeneration {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN lease_generation INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}

	// Legacy state had no independent recoverability marker. Preserve every
	// running or partial run as recoverable so an upgrade cannot orphan its
	// checkpoints. The NULL reason is also a crash-safe migration marker: if a
	// prior process added one column and stopped, the next startup finishes the
	// backfill deterministically.
	if _, err := s.db.Exec(`
		UPDATE runs
		SET resumable = CASE WHEN status IN ('running', 'partial') THEN 1 ELSE 0 END,
		    resumability_reason = CASE
		        WHEN status = 'partial' THEN 'legacy partial outcome is available to resume'
		        WHEN status = 'running' THEN 'run is in progress or was interrupted'
		        ELSE 'run has a terminal outcome'
		    END
		WHERE resumability_reason IS NULL
	`); err != nil {
		return fmt.Errorf("backfilling run resumability: %w", err)
	}
	if _, err := s.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_runs_resumable_started
		ON runs(resumable, started_at)
	`); err != nil {
		return fmt.Errorf("creating resumable-run index: %w", err)
	}

	return nil
}

func (s *State) ensureProfileColumns() error {
	columns, err := s.tableColumns("profiles")
	if err != nil {
		return err
	}

	hasDescription := false
	for _, col := range columns {
		if col == "description" {
			hasDescription = true
			break
		}
	}

	if !hasDescription {
		if _, err := s.db.Exec(`ALTER TABLE profiles ADD COLUMN description TEXT`); err != nil {
			return err
		}
	}
	return nil
}

// ensureTransferProgressColumns adds columns introduced after the base
// transfer_progress schema. #464: range_state holds the keyset
// coordinator's per-range watermarks as JSON; NULL on pre-migration rows
// and ROW_NUMBER tasks, which resume with the legacy single watermark.
func (s *State) ensureTransferProgressColumns() error {
	columns, err := s.tableColumns("transfer_progress")
	if err != nil {
		return err
	}
	have := make(map[string]bool, len(columns))
	for _, col := range columns {
		have[col] = true
	}
	if !have["range_state"] {
		if _, err := s.db.Exec("ALTER TABLE transfer_progress ADD COLUMN range_state TEXT"); err != nil {
			return fmt.Errorf("migrating transfer_progress.range_state: %w", err)
		}
	}
	return nil
}

// ensureStrictSnapshotColumns adds the evidence captured for strict source
// snapshots (#664). The count is stored on the unpartitioned transfer task,
// while the run-level marker prevents `dmt validate` from mistaking a stale
// count from an older strict run for a newer ordinary migration.
func (s *State) ensureStrictSnapshotColumns() error {
	runColumns, err := s.tableColumns("runs")
	if err != nil {
		return err
	}
	hasStrictRun := false
	for _, column := range runColumns {
		if column == "strict_consistency" {
			hasStrictRun = true
			break
		}
	}
	if !hasStrictRun {
		if _, err := s.db.Exec("ALTER TABLE runs ADD COLUMN strict_consistency INTEGER NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("migrating runs.strict_consistency: %w", err)
		}
	}

	taskColumns, err := s.tableColumns("tasks")
	if err != nil {
		return err
	}
	hasSnapshotCount := false
	for _, column := range taskColumns {
		if column == "snapshot_row_count" {
			hasSnapshotCount = true
			break
		}
	}
	if !hasSnapshotCount {
		if _, err := s.db.Exec("ALTER TABLE tasks ADD COLUMN snapshot_row_count INTEGER"); err != nil {
			return fmt.Errorf("migrating tasks.snapshot_row_count: %w", err)
		}
	}
	return nil
}

func (s *State) ensureTuningResultColumns() error {
	columns, err := s.tableColumns("ai_tuning_history")
	if err != nil {
		return err
	}

	have := make(map[string]bool, len(columns))
	for _, col := range columns {
		have[col] = true
	}

	// Each entry: (column name, ALTER TABLE DDL).
	// Older migrations added throughput, duration, and chunk_retry_count.
	// The "regime" columns below were added in #144 follow-up to capture
	// effective DB tuning per run so the smartconfig prompt can compare
	// trajectory rows against the current run's tuning, not just hardware.
	migrations := []struct {
		col, ddl string
	}{
		{"final_throughput", "ALTER TABLE ai_tuning_history ADD COLUMN final_throughput REAL"},
		{"final_duration_seconds", "ALTER TABLE ai_tuning_history ADD COLUMN final_duration_seconds REAL"},
		{"chunk_retry_count", "ALTER TABLE ai_tuning_history ADD COLUMN chunk_retry_count INTEGER DEFAULT 0"},
		// #144 regime tracking — effective DB tuning settings captured at run start.
		{"platform", "ALTER TABLE ai_tuning_history ADD COLUMN platform TEXT"},
		{"target_shared_buffers_mb", "ALTER TABLE ai_tuning_history ADD COLUMN target_shared_buffers_mb INTEGER"},
		{"target_synchronous_commit", "ALTER TABLE ai_tuning_history ADD COLUMN target_synchronous_commit TEXT"},
		{"target_fsync", "ALTER TABLE ai_tuning_history ADD COLUMN target_fsync TEXT"},
		{"target_full_page_writes", "ALTER TABLE ai_tuning_history ADD COLUMN target_full_page_writes TEXT"},
		{"target_max_wal_size_mb", "ALTER TABLE ai_tuning_history ADD COLUMN target_max_wal_size_mb INTEGER"},
		{"target_wal_level", "ALTER TABLE ai_tuning_history ADD COLUMN target_wal_level TEXT"},
		{"source_max_server_memory_mb", "ALTER TABLE ai_tuning_history ADD COLUMN source_max_server_memory_mb INTEGER"},
		// #215 workload-identity columns. Together they form the
		// (source endpoint, target endpoint) tuple used by the new
		// Tier 1 exact-identity classifier. Pre-#215 rows have NULL
		// for these columns and naturally fall through to Tier 2 /
		// baseline — correct behavior since their identity is
		// unrecoverable.
		{"source_host", "ALTER TABLE ai_tuning_history ADD COLUMN source_host TEXT"},
		{"source_port", "ALTER TABLE ai_tuning_history ADD COLUMN source_port INTEGER"},
		{"source_database", "ALTER TABLE ai_tuning_history ADD COLUMN source_database TEXT"},
		{"source_schema", "ALTER TABLE ai_tuning_history ADD COLUMN source_schema TEXT"},
		{"target_host", "ALTER TABLE ai_tuning_history ADD COLUMN target_host TEXT"},
		{"target_port", "ALTER TABLE ai_tuning_history ADD COLUMN target_port INTEGER"},
		{"target_database", "ALTER TABLE ai_tuning_history ADD COLUMN target_database TEXT"},
		{"target_schema", "ALTER TABLE ai_tuning_history ADD COLUMN target_schema TEXT"},
		// #451: set when the runtime controller (or the structural
		// write-error adjuster) changed parameters mid-run. Such rows
		// attribute blended throughput to the configured params, so the
		// deterministic tuner excludes them from its training cohorts.
		// DEFAULT 0 keeps pre-migration rows eligible (their runs
		// predate the flag and mostly predate runtime tuning).
		{"adjusted_at_runtime", "ALTER TABLE ai_tuning_history ADD COLUMN adjusted_at_runtime INTEGER DEFAULT 0"},
		// Persist protocol, conditional transition, and legacy static projection
		// disclosure separately from the requested policy. The selector may retain
		// compatible exact-identity evidence while keeping projected rows out of
		// cross-workload cohorts; runtime transitions are excluded independently.
		{"safety_projected", "ALTER TABLE ai_tuning_history ADD COLUMN safety_projected INTEGER DEFAULT 0"},
		{"execution_chunk_size_min", "ALTER TABLE ai_tuning_history ADD COLUMN execution_chunk_size_min INTEGER"},
		{"execution_chunk_size_max", "ALTER TABLE ai_tuning_history ADD COLUMN execution_chunk_size_max INTEGER"},
		{"projection_context_fingerprint", "ALTER TABLE ai_tuning_history ADD COLUMN projection_context_fingerprint TEXT"},
	}
	for _, m := range migrations {
		if have[m.col] {
			continue
		}
		if _, err := s.db.Exec(m.ddl); err != nil {
			return fmt.Errorf("migrating ai_tuning_history.%s: %w", m.col, err)
		}
	}
	// #215: composite btree index on the workload identity columns so
	// the Tier 1 exact-identity lookup is O(log N). CREATE INDEX IF NOT
	// EXISTS is idempotent — safe to run on every startup.
	const idxDDL = `CREATE INDEX IF NOT EXISTS idx_ai_tuning_workload_identity
		ON ai_tuning_history(
			source_host, source_port, source_database, source_schema,
			target_host, target_port, target_database, target_schema
		)`
	if _, err := s.db.Exec(idxDDL); err != nil {
		return fmt.Errorf("creating idx_ai_tuning_workload_identity: %w", err)
	}
	return nil
}

// validTableNames is a whitelist of allowed table names for schema queries.
// This prevents SQL injection via the table parameter in tableColumns().
var validTableNames = map[string]bool{
	"transfer_progress":            true,
	"runs":                         true,
	"tasks":                        true,
	"profiles":                     true,
	"table_sync_timestamps":        true,
	"delete_reconciliations":       true,
	"delete_reconciliation_tables": true,
	"schema_snapshots":             true,
	"ai_adjustments":               true,
	"ai_tuning_history":            true,
}

func (s *State) tableColumns(table string) ([]string, error) {
	// Validate table name against whitelist to prevent SQL injection
	// SQLite PRAGMA table_info doesn't support parameterized queries
	if !validTableNames[table] {
		return nil, fmt.Errorf("invalid table name: %s", table)
	}

	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dfltValue any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// sanitizeStoredConfigs removes any secrets accidentally stored in config JSON.
func (s *State) sanitizeStoredConfigs() error {
	rows, err := s.db.Query(`SELECT id, config FROM runs WHERE config IS NOT NULL AND config != ''`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type update struct {
		id     string
		config string
	}
	var updates []update

	for rows.Next() {
		var id, configStr string
		if err := rows.Scan(&id, &configStr); err != nil {
			continue
		}

		// Avoid parsing most already-clean rows without making the scrubber
		// password-only. Older rows may contain AI/Slack secrets even when DB
		// passwords were already redacted.
		if !strings.Contains(configStr, `"Password"`) &&
			!strings.Contains(configStr, `"APIKey"`) &&
			!strings.Contains(configStr, `"api_key"`) &&
			!strings.Contains(configStr, `"WebhookURL"`) &&
			!strings.Contains(configStr, `"webhook_url"`) {
			continue
		}

		// Parse and sanitize
		var configMap map[string]any
		if err := json.Unmarshal([]byte(configStr), &configMap); err != nil {
			continue
		}

		modified := false
		for _, section := range []string{"Source", "Target"} {
			if sec, ok := configMap[section].(map[string]any); ok {
				if pw, ok := sec["Password"].(string); ok && pw != "" && pw != "[REDACTED]" {
					sec["Password"] = "[REDACTED]"
					modified = true
				}
			}
		}
		if ai, ok := configMap["AI"].(map[string]any); ok {
			if key, ok := ai["APIKey"].(string); ok && key != "" && key != "[REDACTED]" {
				ai["APIKey"] = "[REDACTED]"
				modified = true
			}
			if key, ok := ai["api_key"].(string); ok && key != "" && key != "[REDACTED]" {
				ai["api_key"] = "[REDACTED]"
				modified = true
			}
		}
		// Also sanitize Slack webhook
		if slack, ok := configMap["Slack"].(map[string]any); ok {
			if wh, ok := slack["WebhookURL"].(string); ok && wh != "" && wh != "[REDACTED]" {
				slack["WebhookURL"] = "[REDACTED]"
				modified = true
			}
			if wh, ok := slack["webhook_url"].(string); ok && wh != "" && wh != "[REDACTED]" {
				slack["webhook_url"] = "[REDACTED]"
				modified = true
			}
		}

		if modified {
			newConfig, _ := json.Marshal(configMap)
			updates = append(updates, update{id: id, config: string(newConfig)})
		}
	}

	// Apply updates
	for _, u := range updates {
		if _, err := s.db.Exec(`UPDATE runs SET config = ? WHERE id = ?`, u.config, u.id); err != nil {
			return fmt.Errorf("sanitizing config for run %s: %w", u.id, err)
		}
	}

	return nil
}
