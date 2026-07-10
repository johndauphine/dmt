package checkpoint

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State manages migration state in SQLite
type State struct {
	db      *sql.DB
	leaseMu sync.RWMutex
	lease   *MigrationLease
}

// Capabilities reports the SQLite backend's supported behavior.
func (s *State) Capabilities() BackendCapabilities {
	return BackendCapabilities{
		RunLifecycle:         true,
		TaskLifecycle:        true,
		TransferProgress:     true,
		PartitionProgress:    true,
		SyncTimestamps:       true,
		DeleteReconciliation: true,
		SchemaSnapshots:      true,
		FallbackEvents:       true,

		RunHistory:               true,
		RunConfigSnapshots:       true,
		Profiles:                 true,
		RuntimeAdjustmentHistory: true,
		TuningHistory:            true,
	}
}

// Task represents a migration task
type Task struct {
	ID           int64
	RunID        string
	TaskType     string
	TaskKey      string
	Status       string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	RetryCount   int
	MaxRetries   int
	ErrorMessage string
}

// Run represents a migration run
type Run struct {
	ID            string
	StartedAt     time.Time
	CompletedAt   *time.Time
	LastHeartbeat time.Time
	Status        string
	// Resumable is independent of Status: a partial outcome may remain
	// recoverable, while an explicitly accepted or abandoned partial does not.
	Resumable          bool
	ResumabilityReason string
	Phase              string // Current phase: initializing, transferring, finalizing, validating, complete
	SourceSchema       string
	TargetSchema       string
	Config             string
	// ConfigHash is the hash of the migration config, used for change detection on resume.
	// Both SQLite and file-based backends persist this field for config validation.
	ConfigHash  string
	ProfileName string
	ConfigPath  string
	Error       string // Error message if status is "failed"
	// LeaseTargetKey and LeaseGeneration identify the fencing token that owns
	// this run. Zero generation denotes a pre-lease/legacy run.
	LeaseTargetKey  string
	LeaseOwnerToken string
	LeaseGeneration int64
}

// TransferProgress tracks chunk-level progress
type TransferProgress struct {
	TaskID      int64
	TableName   string
	PartitionID *int
	LastPK      string
	// RangeState is the keyset per-range watermark JSON (#464); empty on
	// pre-migration rows and ROW_NUMBER tasks.
	RangeState string
	RowsDone   int64
	RowsTotal  int64
	UpdatedAt  time.Time
}

// TaskWithProgress combines task info with transfer progress
type TaskWithProgress struct {
	ID           int64
	RunID        string
	TaskType     string
	TaskKey      string
	Status       string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	RetryCount   int
	ErrorMessage string
	RowsDone     int64
	RowsTotal    int64
}

// New creates a new state manager
func New(dataDir string) (*State, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	// Enforce permissions in case umask relaxed them.
	if err := os.Chmod(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("setting data dir permissions: %w", err)
	}

	dbPath := filepath.Join(dataDir, "migrate.db")
	// Ensure the DB file exists with restrictive permissions before sql.Open creates it.
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		if f, createErr := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL, 0600); createErr == nil {
			f.Close()
		} else {
			return nil, fmt.Errorf("creating db file: %w", createErr)
		}
	}
	// WAL mode for better concurrency, busy_timeout to retry on lock contention
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Configure connection pool for multi-process access:
	// - MaxIdleConns(0): Close connections after use to ensure fresh reads across processes
	// - MaxOpenConns(1): Single connection at a time to avoid lock contention
	// This ensures each query sees the latest committed data from other processes
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)

	s := &State{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
	}

	return s, nil
}

// Close closes the database connection
func (s *State) Close() error {
	return s.db.Close()
}

// GetLastSyncTimestamp returns the last successful sync timestamp for a table.
// Returns nil if no previous sync exists (first sync should do full load).
func (s *State) GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error) {
	var tsStr sql.NullString
	err := s.db.QueryRow(`
		SELECT last_sync_timestamp FROM table_sync_timestamps
		WHERE source_schema = ? AND table_name = ? AND target_schema = ?
	`, sourceSchema, tableName, targetSchema).Scan(&tsStr)

	// Check the scan error BEFORE inspecting tsStr: a failed Scan (e.g.
	// "database is locked" under multi-process contention) leaves tsStr
	// zero-valued, so testing !tsStr.Valid first would swallow the error and
	// report "never synced" — silently degrading incremental sync to a full
	// reload (#564).
	if err == sql.ErrNoRows {
		return nil, nil // No previous sync
	}
	if err != nil {
		return nil, err
	}
	if !tsStr.Valid {
		return nil, nil // Row exists but timestamp is NULL — treat as no sync
	}

	ts, err := time.Parse(time.RFC3339Nano, tsStr.String)
	if err != nil {
		return nil, nil // Invalid timestamp format, treat as no sync
	}
	return &ts, nil
}

// UpdateSyncTimestamp records the source-side high watermark for a table after
// a successful sync. Incremental reads use a strict "date > watermark" filter,
// so callers should only persist a watermark whose source rows have been
// covered by the completed run.
func (s *State) UpdateSyncTimestamp(sourceSchema, tableName, targetSchema string, ts time.Time) error {
	_, err := s.db.Exec(`
		INSERT INTO table_sync_timestamps (source_schema, table_name, target_schema, last_sync_timestamp, updated_at)
		VALUES (?, ?, ?, ?, datetime('now'))
		ON CONFLICT(source_schema, table_name, target_schema) DO UPDATE SET
			last_sync_timestamp = excluded.last_sync_timestamp,
			updated_at = excluded.updated_at
	`, sourceSchema, tableName, targetSchema, ts.UTC().Format(time.RFC3339Nano))
	return err
}

// SaveSchemaSnapshot records the source schema shape for one table after a
// successful migration run. Snapshots are append-only; GetLatestSchemaSnapshots
// chooses the newest row for each table so history cleanup does not erase the
// baseline for the next run.
func (s *State) SaveSchemaSnapshot(runID, sourceSchema, tableName, schemaJSON string) error {
	_, err := s.db.Exec(`
		INSERT INTO schema_snapshots (source_schema, table_name, run_id, captured_at, schema_json)
		VALUES (?, ?, ?, ?, ?)
	`, sourceSchema, tableName, runID, time.Now().UTC().Format(time.RFC3339Nano), schemaJSON)
	return err
}

// GetLatestSchemaSnapshots returns the newest saved snapshot per table for a
// source schema, ordered by table name for deterministic callers and tests.
func (s *State) GetLatestSchemaSnapshots(sourceSchema string) ([]SchemaSnapshotRecord, error) {
	rows, err := s.db.Query(`
		SELECT ss.run_id, ss.source_schema, ss.table_name, ss.captured_at, ss.schema_json
		FROM schema_snapshots ss
		JOIN (
			SELECT table_name, MAX(id) AS id
			FROM schema_snapshots
			WHERE source_schema = ?
			GROUP BY table_name
		) latest ON latest.id = ss.id
		ORDER BY ss.table_name
	`, sourceSchema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []SchemaSnapshotRecord
	for rows.Next() {
		var record SchemaSnapshotRecord
		var capturedAt string
		if err := rows.Scan(
			&record.RunID,
			&record.SourceSchema,
			&record.TableName,
			&capturedAt,
			&record.SchemaJSON,
		); err != nil {
			return nil, err
		}
		ts, err := time.Parse(time.RFC3339Nano, capturedAt)
		if err != nil {
			return nil, err
		}
		record.CapturedAt = ts
		records = append(records, record)
	}
	return records, rows.Err()
}

// SaveFallbackEvent records one AI fallback event for a run (#176).
// Bumps the count for (run_id, surface, fingerprint) under UPSERT
// semantics; first_seen is preserved across calls, last_seen advances.
// Surface is one of observability.Surface* values; fingerprint may be
// empty for call sites that don't carry one (the row still aggregates
// the count under the surface).
//
// Called from the observability package's FallbackSink registration
// (orchestrator wires this in Run/Resume). Cheap by design — one
// UPSERT per fallback; a pathological migration's distinct-fingerprint
// count is bounded by the source schema's vocabulary, not the row
// count, so this stays well under the IO budget of the migration
// itself.
func (s *State) SaveFallbackEvent(runID, surface, fingerprint string) error {
	if runID == "" || surface == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`
		INSERT INTO fallback_events (run_id, surface, fingerprint, count, first_seen, last_seen)
		VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(run_id, surface, fingerprint) DO UPDATE SET
			count = count + 1,
			last_seen = excluded.last_seen
	`, runID, surface, fingerprint, now, now)
	return err
}

// GetFallbackEventsByRun returns every fallback row for a run in
// surface-then-fingerprint order so the status renderer's per-surface
// totals are deterministic. Returns an empty slice (never nil) when
// the run has no events so callers don't need a nil guard.
func (s *State) GetFallbackEventsByRun(runID string) ([]FallbackEventRecord, error) {
	rows, err := s.db.Query(`
		SELECT run_id, surface, fingerprint, count, first_seen, last_seen
		FROM fallback_events
		WHERE run_id = ?
		ORDER BY surface ASC, fingerprint ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FallbackEventRecord, 0)
	for rows.Next() {
		var rec FallbackEventRecord
		var firstStr, lastStr string
		if err := rows.Scan(&rec.RunID, &rec.Surface, &rec.Fingerprint, &rec.Count, &firstStr, &lastStr); err != nil {
			return nil, err
		}
		rec.FirstSeen, _ = time.Parse(time.RFC3339Nano, firstStr)
		rec.LastSeen, _ = time.Parse(time.RFC3339Nano, lastStr)
		out = append(out, rec)
	}
	return out, rows.Err()
}
