package checkpoint

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

// CreateTask creates a new task or returns existing task ID
func (s *State) CreateTask(runID, taskType, taskKey string) (int64, error) {
	// Try to insert new task
	result, err := s.db.Exec(`
		INSERT INTO tasks (run_id, task_type, task_key, status)
		VALUES (?, ?, ?, 'pending')
		ON CONFLICT(run_id, task_key) DO NOTHING
	`, runID, taskType, taskKey)
	if err != nil {
		return 0, err
	}

	// Check if we inserted a new row
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		return result.LastInsertId()
	}

	// Task already exists - get its ID
	var taskID int64
	err = s.db.QueryRow(`
		SELECT id FROM tasks WHERE run_id = ? AND task_key = ?
	`, runID, taskKey).Scan(&taskID)
	return taskID, err
}

// UpdateTaskStatus updates a task's status
func (s *State) UpdateTaskStatus(taskID int64, status string, errorMsg string) error {
	if status == "running" {
		_, err := s.db.Exec(`
			UPDATE tasks SET status = ?, started_at = datetime('now')
			WHERE id = ?
		`, status, taskID)
		return err
	}

	_, err := s.db.Exec(`
		UPDATE tasks SET status = ?, completed_at = datetime('now'), error_message = ?
		WHERE id = ?
	`, status, errorMsg, taskID)
	return err
}

// IncrementRetry increments retry count and resets to pending
func (s *State) IncrementRetry(taskID int64, errorMsg string) error {
	_, err := s.db.Exec(`
		UPDATE tasks SET status = 'pending', retry_count = retry_count + 1, error_message = ?
		WHERE id = ?
	`, errorMsg, taskID)
	return err
}

// GetPendingTasks returns all pending tasks for a run
func (s *State) GetPendingTasks(runID string) ([]Task, error) {
	rows, err := s.db.Query(`
		SELECT id, run_id, task_type, task_key, status, retry_count, max_retries
		FROM tasks WHERE run_id = ? AND status = 'pending'
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.RunID, &t.TaskType, &t.TaskKey, &t.Status, &t.RetryCount, &t.MaxRetries); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// AllTasksComplete returns true if all tasks of a type are complete
func (s *State) AllTasksComplete(runID, taskType string) (bool, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM tasks
		WHERE run_id = ? AND task_type = ? AND status != 'success'
	`, runID, taskType).Scan(&count)
	return count == 0, err
}

// SaveTransferProgress saves chunk-level progress for resume
func (s *State) SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error {
	lastPKJSON, _ := json.Marshal(lastPK)
	_, err := s.db.Exec(`
		INSERT INTO transfer_progress (task_id, table_name, partition_id, last_pk, rows_done, rows_total, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
		ON CONFLICT(task_id) DO UPDATE SET
			last_pk = excluded.last_pk,
			rows_done = excluded.rows_done,
			updated_at = excluded.updated_at
	`, taskID, tableName, partitionID, string(lastPKJSON), rowsDone, rowsTotal)
	return err
}

// GetTransferProgress returns progress for a task
func (s *State) GetTransferProgress(taskID int64) (*TransferProgress, error) {
	var p TransferProgress
	err := s.db.QueryRow(`
		SELECT task_id, table_name, partition_id, last_pk, rows_done, rows_total
		FROM transfer_progress WHERE task_id = ?
	`, taskID).Scan(&p.TaskID, &p.TableName, &p.PartitionID, &p.LastPK, &p.RowsDone, &p.RowsTotal)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &p, err
}

// ClearTransferProgress removes saved progress for a task (for fresh re-transfer)
func (s *State) ClearTransferProgress(taskID int64) error {
	_, err := s.db.Exec(`DELETE FROM transfer_progress WHERE task_id = ?`, taskID)
	return err
}

// GetPartitionTransferProgressSummary returns aggregate saved progress across
// all partition tasks for one table in a run.
func (s *State) GetPartitionTransferProgressSummary(runID, tableTaskKey string) (PartitionProgressSummary, error) {
	var summary PartitionProgressSummary
	err := s.db.QueryRow(`
		SELECT COALESCE(SUM(tp.rows_done), 0), COUNT(*)
		FROM transfer_progress tp
		JOIN tasks t ON t.id = tp.task_id
		WHERE t.run_id = ? AND t.task_key LIKE ? ESCAPE '\'
			AND tp.last_pk IS NOT NULL AND tp.last_pk != 'null'
	`, runID, partitionTaskLikePattern(tableTaskKey)).Scan(&summary.RowsDone, &summary.PartitionsWithProgress)
	return summary, err
}

// ClearPartitionTransferProgress removes all partition-level progress rows for a
// table within a run (#227). Partitioned tables use task keys of the form
// "transfer:schema.table:p<N>", and each partition saves its own
// transfer_progress row. Resume preflight calls this after truncating a table
// so partitioned ROW_NUMBER (or keyset) resumes don't restart partitions from
// a stale rowNum/PK against a freshly-truncated target — which would silently
// skip rows 0..lastRowNum.
func (s *State) ClearPartitionTransferProgress(runID, tableTaskKey string) error {
	_, err := s.db.Exec(`
		DELETE FROM transfer_progress
		WHERE task_id IN (
			SELECT id FROM tasks
			WHERE run_id = ? AND task_key LIKE ? ESCAPE '\'
		)
	`, runID, partitionTaskLikePattern(tableTaskKey))
	return err
}

// CountPartitionTasks returns the number of existing partition tasks for a table in a run.
// It counts tasks matching the pattern "transfer:schema.table:p*".
func (s *State) CountPartitionTasks(runID, taskKeyPrefix string) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM tasks
		WHERE run_id = ? AND task_key LIKE ? ESCAPE '\'
	`, runID, partitionTaskLikePattern(taskKeyPrefix)).Scan(&count)
	return count, err
}

func partitionTaskLikePattern(taskKeyPrefix string) string {
	// Escape LIKE wildcards in the prefix so underscores and percent signs
	// in table names (e.g., order_items) are treated literally.
	escaped := strings.ReplaceAll(taskKeyPrefix, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `_`, `\_`)
	escaped = strings.ReplaceAll(escaped, `%`, `\%`)
	return escaped + ":p%"
}

// GetRunStats returns summary stats for a run
func (s *State) GetRunStats(runID string) (total, pending, running, success, failed int, err error) {
	err = s.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0)
		FROM tasks WHERE run_id = ?
	`, runID).Scan(&total, &pending, &running, &success, &failed)
	return
}

// GetCompletedTables returns table names that completed successfully in a run
func (s *State) GetCompletedTables(runID string) (map[string]bool, error) {
	rows, err := s.db.Query(`
		SELECT task_key FROM tasks
		WHERE run_id = ? AND task_type = 'transfer' AND status = 'success'
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	completed := make(map[string]bool)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		completed[key] = true
	}
	return completed, nil
}

// MarkRunAsResumed resets running tasks to pending for resume
func (s *State) MarkRunAsResumed(runID string) error {
	_, err := s.db.Exec(`
		UPDATE tasks SET status = 'pending', started_at = NULL
		WHERE run_id = ? AND status = 'running'
	`, runID)
	return err
}

// MarkTaskComplete marks a task as complete by run_id and task_key
func (s *State) MarkTaskComplete(runID, taskKey string) error {
	_, err := s.db.Exec(`
		INSERT INTO tasks (run_id, task_type, task_key, status, completed_at)
		VALUES (?, 'transfer', ?, 'success', datetime('now'))
		ON CONFLICT(run_id, task_key) DO UPDATE SET
			status = 'success',
			completed_at = datetime('now')
	`, runID, taskKey)
	return err
}

// GetAllTasks returns all tasks for a run with their progress
func (s *State) GetAllTasks(runID string) ([]Task, error) {
	rows, err := s.db.Query(`
		SELECT t.id, t.run_id, t.task_type, t.task_key, t.status,
		       t.started_at, t.completed_at, t.retry_count, t.max_retries, t.error_message
		FROM tasks t
		WHERE t.run_id = ?
		ORDER BY t.task_type, t.task_key
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		var startedAt, completedAt, errorMsg sql.NullString
		if err := rows.Scan(&t.ID, &t.RunID, &t.TaskType, &t.TaskKey, &t.Status,
			&startedAt, &completedAt, &t.RetryCount, &t.MaxRetries, &errorMsg); err != nil {
			return nil, err
		}
		if startedAt.Valid {
			ts, _ := time.Parse("2006-01-02 15:04:05", startedAt.String)
			t.StartedAt = &ts
		}
		if completedAt.Valid {
			ts, _ := time.Parse("2006-01-02 15:04:05", completedAt.String)
			t.CompletedAt = &ts
		}
		if errorMsg.Valid {
			t.ErrorMessage = errorMsg.String
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// GetTasksWithProgress returns all tasks for a run with transfer progress info
func (s *State) GetTasksWithProgress(runID string) ([]TaskWithProgress, error) {
	rows, err := s.db.Query(`
		SELECT t.id, t.run_id, t.task_type, t.task_key, t.status,
		       t.started_at, t.completed_at, t.retry_count, t.error_message,
		       tp.rows_done, tp.rows_total
		FROM tasks t
		LEFT JOIN transfer_progress tp ON t.id = tp.task_id
		WHERE t.run_id = ?
		ORDER BY t.task_type, t.task_key
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskWithProgress
	for rows.Next() {
		var t TaskWithProgress
		var startedAt, completedAt, errorMsg sql.NullString
		var rowsDone, rowsTotal sql.NullInt64
		if err := rows.Scan(&t.ID, &t.RunID, &t.TaskType, &t.TaskKey, &t.Status,
			&startedAt, &completedAt, &t.RetryCount, &errorMsg,
			&rowsDone, &rowsTotal); err != nil {
			return nil, err
		}
		if startedAt.Valid {
			ts, _ := time.Parse("2006-01-02 15:04:05", startedAt.String)
			t.StartedAt = &ts
		}
		if completedAt.Valid {
			ts, _ := time.Parse("2006-01-02 15:04:05", completedAt.String)
			t.CompletedAt = &ts
		}
		if errorMsg.Valid {
			t.ErrorMessage = errorMsg.String
		}
		if rowsDone.Valid {
			t.RowsDone = rowsDone.Int64
		}
		if rowsTotal.Valid {
			t.RowsTotal = rowsTotal.Int64
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// ProgressSaver implements transfer.ProgressSaver interface
type ProgressSaver struct {
	state StateBackend
}

// NewProgressSaver creates a progress saver wrapping any state backend
func NewProgressSaver(s StateBackend) *ProgressSaver {
	return &ProgressSaver{state: s}
}

// SaveProgress saves chunk-level progress for resume
func (p *ProgressSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error {
	return p.state.SaveTransferProgress(taskID, tableName, partitionID, lastPK, rowsDone, rowsTotal)
}

// GetProgress retrieves saved progress for a task
func (p *ProgressSaver) GetProgress(taskID int64) (lastPK any, rowsDone int64, err error) {
	prog, err := p.state.GetTransferProgress(taskID)
	if err != nil {
		return nil, 0, err
	}
	if prog == nil {
		return nil, 0, nil
	}
	// Unmarshal lastPK from JSON (stored as string in TransferProgress)
	if prog.LastPK != "" {
		if jsonErr := json.Unmarshal([]byte(prog.LastPK), &lastPK); jsonErr != nil {
			return nil, prog.RowsDone, nil // Ignore unmarshal errors, just return rowsDone
		}
	}
	return lastPK, prog.RowsDone, nil
}
