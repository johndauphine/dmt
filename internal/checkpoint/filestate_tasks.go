package checkpoint

import (
	"fmt"
	"strings"
)

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

// GetPartitionTransferProgressSummary returns aggregate saved progress across
// all partition task records for one table.
func (fs *FileState) GetPartitionTransferProgressSummary(_ string, tableTaskKey string) (PartitionProgressSummary, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	prefix := tableTaskKey + ":p"
	var summary PartitionProgressSummary
	for key, ts := range fs.state.Tables {
		if strings.HasPrefix(key, prefix) && ts.LastPK != nil {
			summary.RowsDone += ts.RowsDone
			summary.PartitionsWithProgress++
		}
	}
	return summary, nil
}

// ClearPartitionTransferProgress clears all partition-level progress rows for a
// table (#227). Task keys for partitions have the form "<tableTaskKey>:p<N>".
// runID is unused for the file backend because each FileState instance is
// scoped to a single run.
func (fs *FileState) ClearPartitionTransferProgress(_ string, tableTaskKey string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	prefix := tableTaskKey + ":p"
	changed := false
	for key, ts := range fs.state.Tables {
		if strings.HasPrefix(key, prefix) {
			ts.LastPK = nil
			ts.RowsDone = 0
			ts.RowsTotal = 0
			ts.Status = "pending"
			fs.state.Tables[key] = ts
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return fs.save()
}

// CountPartitionTasks counts partition tasks for a table by scanning stored task keys.
func (fs *FileState) CountPartitionTasks(runID, taskKeyPrefix string) (int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	prefix := taskKeyPrefix + ":p"
	count := 0
	for key := range fs.state.Tables {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count, nil
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

// GetLastSyncTimestamp returns the last successful sync timestamp
// recorded for (sourceSchema, tableName, targetSchema). Pre-#255
// this was a no-op that returned (nil, nil), silently degrading
// date-based incremental sync to a full sync every run when the
