package checkpoint

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CreateTask creates or returns an existing task.
func (fs *FileState) CreateTask(runID, taskType, taskKey string) (int64, error) {
	var taskID int64
	err := fs.withRunLeaseMutation("create task", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		if ts, ok := fs.state.Tables[taskKey]; ok {
			if ts.TaskID <= 0 {
				ts.TaskID = fs.nextTaskIDLocked()
				fs.state.Tables[taskKey] = ts
				fs.rememberTaskIDLocked(ts.TaskID, taskKey)
			}
			taskID = ts.TaskID
			return nil
		}
		taskID = fs.nextTaskIDLocked()
		fs.state.Tables[taskKey] = tableState{
			Status:   "pending",
			TaskType: taskType,
			TaskID:   taskID,
		}
		fs.rememberTaskIDLocked(taskID, taskKey)
		return nil
	})
	return taskID, err
}

func (fs *FileState) CreateTransferTask(runID string, identity TransferTaskIdentity) (int64, error) {
	key := identity.TaskKey()
	var taskID int64
	err := fs.withRunLeaseMutation("create transfer task", func() error {
		if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
			return err
		}
		if ts, ok := fs.state.Tables[key]; ok {
			taskID = ts.TaskID
			return nil
		}
		taskID = fs.nextTaskIDLocked()
		fs.state.Tables[key] = tableState{
			Status:          "pending",
			TaskType:        "transfer",
			TaskSchema:      identity.Schema,
			TaskTable:       identity.Table,
			TaskPartitionID: clonePartitionID(identity.PartitionID),
			TaskID:          taskID,
		}
		fs.rememberTaskIDLocked(taskID, key)
		return nil
	})
	return taskID, err
}

func (fs *FileState) ensureStructuredTransferTasksLocked(runID string) error {
	if fs.state.RunID != runID {
		return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
	}
	changed := false
	for key, ts := range fs.state.Tables {
		if fileStateTaskType(ts) != "transfer" {
			continue
		}
		identity, canonical := ParseTransferTaskKey(key)
		if !canonical {
			return legacyTaskIdentityError(runID)
		}
		if ts.TaskTable == "" {
			ts.TaskType = "transfer"
			ts.TaskSchema = identity.Schema
			ts.TaskTable = identity.Table
			ts.TaskPartitionID = clonePartitionID(identity.PartitionID)
			changed = true
		} else if ts.TaskSchema != identity.Schema || ts.TaskTable != identity.Table || !samePartitionID(ts.TaskPartitionID, identity.PartitionID) {
			return fmt.Errorf("structured transfer task %q has identity fields that do not match its canonical key", key)
		}
		if ts.TaskID <= 0 {
			ts.TaskID = fs.nextTaskIDLocked()
			changed = true
		}
		fs.state.Tables[key] = ts
		fs.rememberTaskIDLocked(ts.TaskID, key)
	}
	if changed {
		return fs.save()
	}
	return nil
}

func clonePartitionID(partitionID *int) *int {
	if partitionID == nil {
		return nil
	}
	value := *partitionID
	return &value
}

func samePartitionID(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (fs *FileState) CountTransferPartitionTasks(runID string, schema, table string) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
		return 0, err
	}
	count := 0
	for _, ts := range fs.state.Tables {
		if ts.TaskType == "transfer" && ts.TaskSchema == schema && ts.TaskTable == table && ts.TaskPartitionID != nil {
			count++
		}
	}
	return count, nil
}

func (fs *FileState) ClearTransferPartitionProgress(runID string, schema, table string) error {
	return fs.withRunLeaseMutation("clear transfer partition progress", func() error {
		if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
			return err
		}
		for key, ts := range fs.state.Tables {
			if ts.TaskType != "transfer" || ts.TaskSchema != schema || ts.TaskTable != table || ts.TaskPartitionID == nil {
				continue
			}
			ts.LastPK = nil
			ts.PartitionID = nil
			ts.RowsDone = 0
			ts.RowsTotal = 0
			ts.Status = "pending"
			fs.state.Tables[key] = ts
		}
		return nil
	})
}

func (fs *FileState) GetTransferPartitionProgressSummary(runID string, schema, table string) (PartitionProgressSummary, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var summary PartitionProgressSummary
	if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
		return summary, err
	}
	for _, ts := range fs.state.Tables {
		if ts.TaskType == "transfer" && ts.TaskSchema == schema && ts.TaskTable == table && ts.TaskPartitionID != nil && ts.LastPK != nil {
			summary.RowsDone += ts.RowsDone
			summary.PartitionsWithProgress++
		}
	}
	return summary, nil
}

func (fs *FileState) MarkTransferTaskComplete(runID string, identity TransferTaskIdentity) error {
	return fs.withRunLeaseMutation("mark transfer task complete", func() error {
		if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
			return err
		}
		key := identity.TaskKey()
		ts, ok := fs.state.Tables[key]
		if !ok {
			return fmt.Errorf("transfer task not found: %s", key)
		}
		ts.Status = "success"
		fs.state.Tables[key] = ts
		return nil
	})
}

func (fs *FileState) CompleteTransferTask(runID string, identity TransferTaskIdentity, targetSchema string, watermark *time.Time) error {
	return fs.withRunLeaseMutation("complete transfer task", func() error {
		if err := fs.ensureStructuredTransferTasksLocked(runID); err != nil {
			return err
		}
		key := identity.TaskKey()
		ts, ok := fs.state.Tables[key]
		if !ok {
			return fmt.Errorf("transfer task not found: %s", key)
		}
		ts.Status = "success"
		fs.state.Tables[key] = ts
		if watermark != nil {
			if fs.state.SyncTimestamps == nil {
				fs.state.SyncTimestamps = make(map[string]map[string]map[string]time.Time)
			}
			if fs.state.SyncTimestamps[identity.Schema] == nil {
				fs.state.SyncTimestamps[identity.Schema] = make(map[string]map[string]time.Time)
			}
			if fs.state.SyncTimestamps[identity.Schema][identity.Table] == nil {
				fs.state.SyncTimestamps[identity.Schema][identity.Table] = make(map[string]time.Time)
			}
			fs.state.SyncTimestamps[identity.Schema][identity.Table][targetSchema] = *watermark
		}
		return nil
	})
}

// UpdateTaskStatus updates a task's status.
func (fs *FileState) UpdateTaskStatus(taskID int64, status string, errorMsg string) error {
	return fs.withRunLeaseMutation("update task status", func() error {
		key, ok := fs.taskKeyForIDLocked(taskID)
		if !ok {
			return fmt.Errorf("task not found: %d", taskID)
		}
		ts := fs.state.Tables[key]
		ts.Status = status
		ts.Error = errorMsg
		fs.state.Tables[key] = ts
		return nil
	})
}

// MarkTaskComplete marks a task as complete by run_id and task_key.
func (fs *FileState) MarkTaskComplete(runID, taskKey string) error {
	return fs.withRunLeaseMutation("mark task complete", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		if ts, ok := fs.state.Tables[taskKey]; ok {
			if ts.TaskID <= 0 {
				ts.TaskID = fs.nextTaskIDLocked()
				fs.rememberTaskIDLocked(ts.TaskID, taskKey)
			}
			ts.Status = "success"
			fs.state.Tables[taskKey] = ts
		} else {
			taskID := fs.nextTaskIDLocked()
			fs.state.Tables[taskKey] = tableState{
				Status:   "success",
				TaskType: "transfer",
				TaskID:   taskID,
			}
			fs.rememberTaskIDLocked(taskID, taskKey)
		}
		return nil
	})
}

// GetCompletedTables returns table names that completed successfully.
func (fs *FileState) GetCompletedTables(runID string) (map[string]bool, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	completed := make(map[string]bool)
	for key, ts := range fs.state.Tables {
		if ts.Status == "success" && fileStateTaskType(ts) == "transfer" {
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
func (fs *FileState) SaveTransferProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	return fs.withRunLeaseMutation("save transfer progress", func() error {
		key, ok := fs.taskKeyForIDLocked(taskID)
		if !ok {
			return fmt.Errorf("task not found: %d", taskID)
		}
		ts := fs.state.Tables[key]
		var pid *int
		if partitionID != nil {
			v := *partitionID
			pid = &v
		}
		ts.TableName = tableName
		ts.PartitionID = pid
		ts.LastPK = lastPK
		ts.RangeState = rangeState
		ts.RowsDone = rowsDone
		ts.RowsTotal = rowsTotal
		ts.Status = "running"
		fs.state.Tables[key] = ts
		return nil
	})
}

// GetTransferProgress returns progress for a task.
func (fs *FileState) GetTransferProgress(taskID int64) (*TransferProgress, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	key, ok := fs.taskKeyForIDLocked(taskID)
	if ok {
		ts := fs.state.Tables[key]
		if ts.LastPK == nil {
			return nil, nil
		}
		lastPKJSON, err := json.Marshal(ts.LastPK)
		if err != nil {
			return nil, fmt.Errorf("marshal last pk: %w", err)
		}
		tableName := ts.TableName
		if tableName == "" {
			tableName = key
		}
		var pid *int
		if ts.PartitionID != nil {
			v := *ts.PartitionID
			pid = &v
		}
		return &TransferProgress{
			TaskID:      taskID,
			TableName:   tableName,
			PartitionID: pid,
			LastPK:      string(lastPKJSON),
			RangeState:  ts.RangeState,
			RowsDone:    ts.RowsDone,
			RowsTotal:   ts.RowsTotal,
		}, nil
	}

	return nil, nil
}

// ClearTransferProgress removes saved progress for a task (for fresh re-transfer).
func (fs *FileState) ClearTransferProgress(taskID int64) error {
	return fs.withRunLeaseMutation("clear transfer progress", func() error {
		key, ok := fs.taskKeyForIDLocked(taskID)
		if !ok {
			return fmt.Errorf("task not found: %d", taskID)
		}
		ts := fs.state.Tables[key]
		ts.LastPK = nil
		ts.PartitionID = nil
		ts.RowsDone = 0
		ts.RowsTotal = 0
		ts.Status = "pending"
		fs.state.Tables[key] = ts
		return nil
	})
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
func (fs *FileState) ClearPartitionTransferProgress(runID string, tableTaskKey string) error {
	return fs.withRunLeaseMutation("clear partition transfer progress", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		prefix := tableTaskKey + ":p"
		for key, ts := range fs.state.Tables {
			if strings.HasPrefix(key, prefix) {
				ts.LastPK = nil
				ts.PartitionID = nil
				ts.RowsDone = 0
				ts.RowsTotal = 0
				ts.Status = "pending"
				fs.state.Tables[key] = ts
			}
		}
		return nil
	})
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
		return []Run{runFromFileState(fs.state)}, nil
	}
	return nil, nil
}

// GetRunsPage applies the status filter and offset/limit to the single
// file-backed run (file state has no history). It returns at most one run.
func (fs *FileState) GetRunsPage(status string, limit, offset int) ([]Run, int, error) {
	runs, err := fs.GetAllRuns()
	if err != nil {
		return nil, 0, err
	}
	if status != "" {
		filtered := runs[:0]
		for _, r := range runs {
			if r.Status == status {
				filtered = append(filtered, r)
			}
		}
		runs = filtered
	}
	total := len(runs)
	if offset >= total {
		return nil, total, nil
	}
	runs = runs[offset:]
	if limit > 0 && limit < len(runs) {
		runs = runs[:limit]
	}
	return runs, total, nil
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
			TaskType:     fileStateTaskType(ts),
			TaskKey:      key,
			Status:       ts.Status,
			ErrorMessage: ts.Error,
			RowsDone:     ts.RowsDone,
			RowsTotal:    ts.RowsTotal,
		})
	}
	return tasks, nil
}

func fileStateTaskType(ts tableState) string {
	if ts.TaskType == "" {
		return "transfer"
	}
	return ts.TaskType
}

// GetRunByID returns the run if it matches.
func (fs *FileState) GetRunByID(runID string) (*Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID == runID {
		run := runFromFileState(fs.state)
		return &run, nil
	}

	return nil, nil
}

// GetLastSyncTimestamp returns the last successful sync timestamp
// recorded for (sourceSchema, tableName, targetSchema). Pre-#255
// this was a no-op that returned (nil, nil), silently degrading
// date-based incremental sync to a full sync every run when the
