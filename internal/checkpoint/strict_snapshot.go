package checkpoint

import (
	"database/sql"
	"fmt"
)

var _ StrictSnapshotState = (*State)(nil)

// SetRunStrictConsistency records the snapshot contract for this run after
// its lease is bound, so a stale owner cannot alter how a later validation
// interprets the task evidence.
func (s *State) SetRunStrictConsistency(runID string, strict bool) error {
	strictValue := 0
	if strict {
		strictValue = 1
	}
	return s.withRunLeaseTx(runID, "set strict consistency", func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE runs SET strict_consistency = ? WHERE id = ?`, strictValue, runID)
		if err != nil {
			return err
		}
		return requireOneRun(result, runID, "set strict consistency")
	})
}

// SaveStrictSnapshotRowCount stores the exact full-table row count observed
// inside a pinned strict source transaction. Replacing the value on retry is
// intentional: the retried transfer replaces target rows from its newer
// snapshot, so validation must use that newer expectation too.
func (s *State) SaveStrictSnapshotRowCount(taskID, rowCount int64) error {
	return s.withTaskLeaseTx(taskID, "save strict snapshot row count", func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE tasks SET snapshot_row_count = ? WHERE id = ?`, rowCount, taskID)
		if err != nil {
			return err
		}
		return requireOneTask(result, taskID, "save strict snapshot row count")
	})
}

// GetStrictSnapshotRowCount returns a completed table task's exact snapshot
// count. An empty runID selects the latest run for the same source/target
// schemas without skipping a newer ordinary run for an older strict one.
func (s *State) GetStrictSnapshotRowCount(runID, sourceSchema, targetSchema, tableName string) (*int64, error) {
	var (
		count sql.NullInt64
		err   error
	)
	if runID != "" {
		err = s.db.QueryRow(`
			SELECT t.snapshot_row_count
			FROM runs r
			JOIN tasks t ON t.run_id = r.id
			WHERE r.id = ?
			  AND r.source_schema = ? AND r.target_schema = ?
			  AND r.strict_consistency = 1
			  AND t.task_type = 'transfer'
			  AND t.task_table = ?
			  AND t.task_partition_id IS NULL
			  AND t.status = 'success'
			  AND t.snapshot_row_count IS NOT NULL
		`, runID, sourceSchema, targetSchema, tableName).Scan(&count)
	} else {
		err = s.db.QueryRow(`
			SELECT t.snapshot_row_count
			FROM runs r
			JOIN tasks t ON t.run_id = r.id
			WHERE r.id = (
				SELECT id FROM runs
				WHERE source_schema = ? AND target_schema = ?
				ORDER BY started_at DESC, rowid DESC
				LIMIT 1
			)
			  AND r.strict_consistency = 1
			  AND t.task_type = 'transfer'
			  AND t.task_table = ?
			  AND t.task_partition_id IS NULL
			  AND t.status = 'success'
			  AND t.snapshot_row_count IS NOT NULL
		`, sourceSchema, targetSchema, tableName).Scan(&count)
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading strict snapshot row count for %s.%s: %w", sourceSchema, tableName, err)
	}
	if !count.Valid {
		return nil, nil
	}
	value := count.Int64
	return &value, nil
}
