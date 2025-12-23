package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// FileState implements StateBackend using a single YAML file.
// Designed for Airflow and headless environments where SQLite is impractical.
type FileState struct {
	path  string
	mu    sync.RWMutex
	state *fileStateData
}

// fileStateData is the YAML structure for the state file.
type fileStateData struct {
	RunID        string                `yaml:"run_id"`
	StartedAt    time.Time             `yaml:"started_at"`
	CompletedAt  *time.Time            `yaml:"completed_at,omitempty"`
	Status       string                `yaml:"status"` // running, success, failed
	Phase        string                `yaml:"phase"`  // initializing, transferring, finalizing, validating, complete
	Error        string                `yaml:"error,omitempty"`
	SourceSchema string                `yaml:"source_schema"`
	TargetSchema string                `yaml:"target_schema"`
	ConfigHash   string                `yaml:"config_hash,omitempty"`
	ProfileName  string                `yaml:"profile_name,omitempty"`
	ConfigPath   string                `yaml:"config_path,omitempty"`
	Tables       map[string]tableState `yaml:"tables"`
}

// tableState tracks per-table progress.
type tableState struct {
	Status    string `yaml:"status"` // pending, running, success, failed
	LastPK    any    `yaml:"last_pk,omitempty"`
	RowsDone  int64  `yaml:"rows_done,omitempty"`
	RowsTotal int64  `yaml:"rows_total,omitempty"`
	TaskID    int64  `yaml:"task_id,omitempty"` // Synthetic task ID for compatibility
	Error     string `yaml:"error,omitempty"`
}

// NewFileState creates a file-based state manager.
// If the file exists, it loads the existing state.
func NewFileState(path string) (*FileState, error) {
	fs := &FileState{
		path: path,
		state: &fileStateData{
			Tables: make(map[string]tableState),
		},
	}

	// Load existing state if file exists
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading state file: %w", err)
		}
		if err := yaml.Unmarshal(data, fs.state); err != nil {
			return nil, fmt.Errorf("parsing state file: %w", err)
		}
		if fs.state.Tables == nil {
			fs.state.Tables = make(map[string]tableState)
		}
	}

	return fs, nil
}

// save writes the current state to the YAML file.
func (fs *FileState) save() error {
	data, err := yaml.Marshal(fs.state)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}
	if err := os.WriteFile(fs.path, data, 0600); err != nil {
		return fmt.Errorf("writing state file: %w", err)
	}
	return nil
}

// CreateRun initializes a new migration run.
func (fs *FileState) CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Compute config hash for change detection
	configJSON, _ := json.Marshal(config)
	hash := sha256.Sum256(configJSON)

	fs.state = &fileStateData{
		RunID:        id,
		StartedAt:    time.Now(),
		Status:       "running",
		SourceSchema: sourceSchema,
		TargetSchema: targetSchema,
		ConfigHash:   hex.EncodeToString(hash[:8]), // First 8 bytes
		ProfileName:  profileName,
		ConfigPath:   configPath,
		Tables:       make(map[string]tableState),
	}

	return fs.save()
}

// CompleteRun marks the run as complete.
func (fs *FileState) CompleteRun(id string, status string, errorMsg string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state.RunID != id {
		return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, id)
	}

	now := time.Now()
	fs.state.Status = status
	fs.state.CompletedAt = &now
	fs.state.Error = errorMsg

	return fs.save()
}

// GetLastIncompleteRun returns the current run if it's incomplete.
func (fs *FileState) GetLastIncompleteRun() (*Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID == "" || fs.state.Status != "running" {
		return nil, nil
	}

	phase := fs.state.Phase
	if phase == "" {
		phase = "initializing"
	}
	return &Run{
		ID:           fs.state.RunID,
		StartedAt:    fs.state.StartedAt,
		Status:       fs.state.Status,
		Phase:        phase,
		SourceSchema: fs.state.SourceSchema,
		TargetSchema: fs.state.TargetSchema,
		ConfigHash:   fs.state.ConfigHash,
		ProfileName:  fs.state.ProfileName,
		ConfigPath:   fs.state.ConfigPath,
	}, nil
}

// HasSuccessfulRunAfter checks if there's a successful run that supersedes the given incomplete run.
// For file state, this always returns false - we only track one run at a time,
// so if there's an incomplete run, it's the only run we know about.
func (fs *FileState) HasSuccessfulRunAfter(run *Run) (bool, error) {
	// File state only tracks one run - if it's incomplete, there's no later successful run
	return false, nil
}

// MarkRunAsResumed resets running tasks to pending.
func (fs *FileState) MarkRunAsResumed(runID string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for key, ts := range fs.state.Tables {
		if ts.Status == "running" {
			ts.Status = "pending"
			fs.state.Tables[key] = ts
		}
	}

	return fs.save()
}

// UpdatePhase updates the current phase of a migration run.
func (fs *FileState) UpdatePhase(runID, phase string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state != nil && fs.state.RunID == runID {
		fs.state.Phase = phase
		return fs.save()
	}
	return nil
}

// taskIDCounter generates synthetic task IDs for file-based state.
var taskIDCounter int64 = 1000

// CreateTask creates or returns an existing task.
func (fs *FileState) CreateTask(runID, taskType, taskKey string) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check if task exists
	if ts, ok := fs.state.Tables[taskKey]; ok {
		return ts.TaskID, nil
	}

	// Create new task
	taskIDCounter++
	fs.state.Tables[taskKey] = tableState{
		Status: "pending",
		TaskID: taskIDCounter,
	}

	if err := fs.save(); err != nil {
		return 0, err
	}
	return taskIDCounter, nil
}

// UpdateTaskStatus updates a task's status.
func (fs *FileState) UpdateTaskStatus(taskID int64, status string, errorMsg string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for key, ts := range fs.state.Tables {
		if ts.TaskID == taskID {
			ts.Status = status
			ts.Error = errorMsg
			fs.state.Tables[key] = ts
			return fs.save()
		}
	}

	return fmt.Errorf("task not found: %d", taskID)
}

// MarkTaskComplete marks a task as complete by run_id and task_key.
func (fs *FileState) MarkTaskComplete(runID, taskKey string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if ts, ok := fs.state.Tables[taskKey]; ok {
		ts.Status = "success"
		fs.state.Tables[taskKey] = ts
	} else {
		// Create if not exists
		taskIDCounter++
		fs.state.Tables[taskKey] = tableState{
			Status: "success",
			TaskID: taskIDCounter,
		}
	}

	return fs.save()
}

// GetCompletedTables returns table names that completed successfully.
func (fs *FileState) GetCompletedTables(runID string) (map[string]bool, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	completed := make(map[string]bool)
	for key, ts := range fs.state.Tables {
		if ts.Status == "success" {
			completed[key] = true
		}
	}
	return completed, nil
}

// GetRunStats returns summary stats for the run.
func (fs *FileState) GetRunStats(runID string) (total, pending, running, success, failed int, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for _, ts := range fs.state.Tables {
		total++
		switch ts.Status {
		case "pending":
			pending++
		case "running":
			running++
		case "success":
			success++
		case "failed":
			failed++
		}
	}
	return
}

// SaveTransferProgress saves chunk-level progress.
func (fs *FileState) SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find task by ID and update progress
	for key, ts := range fs.state.Tables {
		if ts.TaskID == taskID {
			ts.LastPK = lastPK
			ts.RowsDone = rowsDone
			ts.RowsTotal = rowsTotal
			ts.Status = "running"
			fs.state.Tables[key] = ts
			return fs.save()
		}
	}

	return nil // Silently ignore if task not found
}

// GetTransferProgress returns progress for a task.
func (fs *FileState) GetTransferProgress(taskID int64) (*TransferProgress, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for tableName, ts := range fs.state.Tables {
		if ts.TaskID == taskID && ts.LastPK != nil {
			return &TransferProgress{
				TaskID:    taskID,
				TableName: tableName,
				LastPK:    fmt.Sprintf("%v", ts.LastPK), // Convert to string for compatibility
				RowsDone:  ts.RowsDone,
				RowsTotal: ts.RowsTotal,
			}, nil
		}
	}

	return nil, nil
}

// ClearTransferProgress removes saved progress for a task (for fresh re-transfer).
func (fs *FileState) ClearTransferProgress(taskID int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for key, ts := range fs.state.Tables {
		if ts.TaskID == taskID {
			ts.LastPK = nil
			ts.RowsDone = 0
			ts.RowsTotal = 0
			ts.Status = "pending"
			fs.state.Tables[key] = ts
			return fs.save()
		}
	}

	return nil
}

// GetAllRuns returns empty slice (file state doesn't track history).
func (fs *FileState) GetAllRuns() ([]Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Return current run only if it exists
	if fs.state.RunID != "" {
		return []Run{
			{
				ID:           fs.state.RunID,
				StartedAt:    fs.state.StartedAt,
				CompletedAt:  fs.state.CompletedAt,
				Status:       fs.state.Status,
				Error:        fs.state.Error,
				SourceSchema: fs.state.SourceSchema,
				TargetSchema: fs.state.TargetSchema,
				ProfileName:  fs.state.ProfileName,
				ConfigPath:   fs.state.ConfigPath,
			},
		}, nil
	}
	return nil, nil
}

// GetTasksWithProgress returns all tasks for a run with transfer progress info.
func (fs *FileState) GetTasksWithProgress(runID string) ([]TaskWithProgress, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID != runID {
		return nil, nil
	}

	var tasks []TaskWithProgress
	for key, ts := range fs.state.Tables {
		tasks = append(tasks, TaskWithProgress{
			ID:           ts.TaskID,
			RunID:        runID,
			TaskType:     "transfer",
			TaskKey:      key,
			Status:       ts.Status,
			ErrorMessage: ts.Error,
			RowsDone:     ts.RowsDone,
			RowsTotal:    ts.RowsTotal,
		})
	}
	return tasks, nil
}

// GetRunByID returns the run if it matches.
func (fs *FileState) GetRunByID(runID string) (*Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID == runID {
		return &Run{
			ID:           fs.state.RunID,
			StartedAt:    fs.state.StartedAt,
			CompletedAt:  fs.state.CompletedAt,
			Status:       fs.state.Status,
			Error:        fs.state.Error,
			SourceSchema: fs.state.SourceSchema,
			TargetSchema: fs.state.TargetSchema,
			ProfileName:  fs.state.ProfileName,
			ConfigPath:   fs.state.ConfigPath,
		}, nil
	}

	return nil, nil
}

// Close is a no-op for file state.
func (fs *FileState) Close() error {
	return nil
}

// Path returns the state file path.
func (fs *FileState) Path() string {
	return fs.path
}

// Ensure FileState implements StateBackend
var _ StateBackend = (*FileState)(nil)
