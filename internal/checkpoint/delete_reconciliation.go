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
