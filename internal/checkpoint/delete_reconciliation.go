package checkpoint

import (
	"database/sql"
	"time"
)

// GetDeleteReconciliationState returns the latest successful reconciliation
// state for a source/target schema pair. Nil means reconciliation has never
// completed for that pair.
func (s *State) GetDeleteReconciliationState(
	sourceSchema,
	targetSchema string,
) (*DeleteReconciliationState, error) {
	var rec DeleteReconciliationState
	var lastSuccessAt, updatedAt string
	err := s.db.QueryRow(`
		SELECT source_schema, target_schema, last_run_id, last_success_at, updated_at
		FROM delete_reconciliations
		WHERE source_schema = ? AND target_schema = ?
	`, sourceSchema, targetSchema).Scan(
		&rec.SourceSchema,
		&rec.TargetSchema,
		&rec.LastRunID,
		&lastSuccessAt,
		&updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	last, err := time.Parse(time.RFC3339Nano, lastSuccessAt)
	if err != nil {
		return nil, err
	}
	updated, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return nil, err
	}
	rec.LastSuccessAt = last
	rec.UpdatedAt = updated
	return &rec, nil
}

// RecordDeleteReconciliationSuccess marks delete reconciliation successful for
// a source/target schema pair. Failed or interrupted reconciliations should not
// call this method, leaving the previous success time as the scheduling anchor.
func (s *State) RecordDeleteReconciliationSuccess(
	runID,
	sourceSchema,
	targetSchema string,
	completedAt time.Time,
) error {
	ts := completedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`
		INSERT INTO delete_reconciliations
			(source_schema, target_schema, last_run_id, last_success_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(source_schema, target_schema) DO UPDATE SET
			last_run_id = excluded.last_run_id,
			last_success_at = excluded.last_success_at,
			updated_at = excluded.updated_at
	`, sourceSchema, targetSchema, runID, ts, ts)
	return err
}

func (s *State) SaveDeleteReconciliationTable(
	runID string,
	record DeleteReconciliationTableRecord,
) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	skipReason := sql.NullString{
		String: record.SkipReason,
		Valid:  record.SkipReason != "",
	}
	_, err := s.db.Exec(`
		INSERT INTO delete_reconciliation_tables
			(run_id, table_name, candidate_rows, deleted_rows, skipped, skip_reason, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id, table_name) DO UPDATE SET
			candidate_rows = excluded.candidate_rows,
			deleted_rows = excluded.deleted_rows,
			skipped = excluded.skipped,
			skip_reason = excluded.skip_reason,
			updated_at = excluded.updated_at
	`, runID, record.TableName, record.CandidateRows, record.DeletedRows,
		boolToInt(record.Skipped), skipReason, now)
	return err
}

func (s *State) GetDeleteReconciliationTables(runID string) ([]DeleteReconciliationTableRecord, error) {
	rows, err := s.db.Query(`
		SELECT run_id, table_name, candidate_rows, deleted_rows, skipped, skip_reason, updated_at
		FROM delete_reconciliation_tables
		WHERE run_id = ?
		ORDER BY table_name
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []DeleteReconciliationTableRecord
	for rows.Next() {
		var record DeleteReconciliationTableRecord
		var skipped int
		var skipReason sql.NullString
		var updatedAt string
		if err := rows.Scan(
			&record.RunID,
			&record.TableName,
			&record.CandidateRows,
			&record.DeletedRows,
			&skipped,
			&skipReason,
			&updatedAt,
		); err != nil {
			return nil, err
		}
		record.Skipped = skipped != 0
		if skipReason.Valid {
			record.SkipReason = skipReason.String
		}
		ts, err := time.Parse(time.RFC3339Nano, updatedAt)
		if err != nil {
			return nil, err
		}
		record.UpdatedAt = ts
		records = append(records, record)
	}
	return records, rows.Err()
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
