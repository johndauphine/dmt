package checkpoint

import (
	"time"
)

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
