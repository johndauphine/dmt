package checkpoint

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// State manages migration state in SQLite
type State struct {
	db *sql.DB
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
	ID           string
	StartedAt    time.Time
	CompletedAt  *time.Time
	Status       string
	SourceSchema string
	TargetSchema string
	Config       string
}

// TransferProgress tracks chunk-level progress
type TransferProgress struct {
	TaskID      int64
	TableName   string
	PartitionID *int
	LastPK      string
	RowsDone    int64
	RowsTotal   int64
	UpdatedAt   time.Time
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

	s := &State{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
	}

	return s, nil
}

func (s *State) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS runs (
		id TEXT PRIMARY KEY,
		started_at TEXT NOT NULL,
		completed_at TEXT,
		status TEXT NOT NULL DEFAULT 'running',
		source_schema TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		config TEXT
	);

	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT REFERENCES runs(id),
		task_type TEXT NOT NULL,
		task_key TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		started_at TEXT,
		completed_at TEXT,
		retry_count INTEGER DEFAULT 0,
		max_retries INTEGER DEFAULT 3,
		error_message TEXT,
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

	CREATE INDEX IF NOT EXISTS idx_tasks_run_status ON tasks(run_id, status);
	CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	// One-time migration: sanitize any passwords stored in config column
	return s.sanitizeStoredConfigs()
}

// sanitizeStoredConfigs removes any passwords accidentally stored in config JSON
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

		// Check if this config contains unredacted passwords
		if !strings.Contains(configStr, `"Password"`) {
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
		// Also sanitize Slack webhook
		if slack, ok := configMap["Slack"].(map[string]any); ok {
			if wh, ok := slack["WebhookURL"].(string); ok && wh != "" && wh != "[REDACTED]" {
				slack["WebhookURL"] = "[REDACTED]"
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

// Close closes the database connection
func (s *State) Close() error {
	return s.db.Close()
}

// CreateRun creates a new migration run
func (s *State) CreateRun(id, sourceSchema, targetSchema string, config any) error {
	configJSON, _ := json.Marshal(config)
	_, err := s.db.Exec(`
		INSERT INTO runs (id, started_at, status, source_schema, target_schema, config)
		VALUES (?, datetime('now'), 'running', ?, ?, ?)
	`, id, sourceSchema, targetSchema, string(configJSON))
	return err
}

// CompleteRun marks a run as complete
func (s *State) CompleteRun(id string, status string) error {
	_, err := s.db.Exec(`
		UPDATE runs SET status = ?, completed_at = datetime('now')
		WHERE id = ?
	`, status, id)
	return err
}

// GetLastIncompleteRun returns the most recent incomplete run
func (s *State) GetLastIncompleteRun() (*Run, error) {
	var r Run
	var startedAtStr string
	err := s.db.QueryRow(`
		SELECT id, started_at, status, source_schema, target_schema
		FROM runs WHERE status = 'running'
		ORDER BY started_at DESC LIMIT 1
	`).Scan(&r.ID, &startedAtStr, &r.Status, &r.SourceSchema, &r.TargetSchema)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Parse SQLite datetime string
	r.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAtStr)
	return &r, nil
}

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

// ProgressSaver implements transfer.ProgressSaver interface
type ProgressSaver struct {
	state *State
}

// NewProgressSaver creates a progress saver wrapping the state
func NewProgressSaver(s *State) *ProgressSaver {
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
	// Unmarshal lastPK from JSON
	if prog.LastPK != "" {
		if err := json.Unmarshal([]byte(prog.LastPK), &lastPK); err != nil {
			return nil, prog.RowsDone, nil // Ignore unmarshal errors, just return rowsDone
		}
	}
	return lastPK, prog.RowsDone, nil
}

// GetAllRuns returns all runs for history
func (s *State) GetAllRuns() ([]Run, error) {
	rows, err := s.db.Query(`
		SELECT id, started_at, completed_at, status, source_schema, target_schema, config
		FROM runs ORDER BY started_at DESC LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		var startedAtStr string
		var completedAtStr sql.NullString
		var configStr sql.NullString
		if err := rows.Scan(&r.ID, &startedAtStr, &completedAtStr, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr); err != nil {
			return nil, err
		}
		r.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAtStr)
		if completedAtStr.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", completedAtStr.String)
			r.CompletedAt = &t
		}
		if configStr.Valid {
			r.Config = configStr.String
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// GetRunByID returns a specific run by ID
func (s *State) GetRunByID(runID string) (*Run, error) {
	var r Run
	var startedAtStr string
	var completedAtStr sql.NullString
	var configStr sql.NullString

	err := s.db.QueryRow(`
		SELECT id, started_at, completed_at, status, source_schema, target_schema, config
		FROM runs WHERE id = ?
	`, runID).Scan(&r.ID, &startedAtStr, &completedAtStr, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	r.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAtStr)
	if completedAtStr.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", completedAtStr.String)
		r.CompletedAt = &t
	}
	if configStr.Valid {
		r.Config = configStr.String
	}
	return &r, nil
}
