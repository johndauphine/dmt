package checkpoint

import (
	"database/sql"
	"time"
)

// GetLastSyncTimestamp returns the last successful sync timestamp for a table.
// Returns nil if no previous sync exists (first sync should do full load).
func (s *State) GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error) {
	var tsStr sql.NullString
	err := s.db.QueryRow(`
		SELECT last_sync_timestamp FROM table_sync_timestamps
		WHERE source_schema = ? AND table_name = ? AND target_schema = ?
	`, sourceSchema, tableName, targetSchema).Scan(&tsStr)

	if err == sql.ErrNoRows || !tsStr.Valid {
		return nil, nil // No previous sync
	}
	if err != nil {
		return nil, err
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
