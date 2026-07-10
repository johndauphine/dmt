package checkpoint

import "fmt"

var _ StrictSnapshotState = (*FileState)(nil)

func (fs *FileState) SetRunStrictConsistency(runID string, strict bool) error {
	return fs.withRunLeaseMutation("set strict consistency", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		fs.state.StrictConsistency = strict
		return nil
	})
}

func (fs *FileState) SaveStrictSnapshotRowCount(taskID, rowCount int64) error {
	return fs.withRunLeaseMutation("save strict snapshot row count", func() error {
		key, ok := fs.taskKeyForIDLocked(taskID)
		if !ok {
			return fmt.Errorf("task not found: %d", taskID)
		}
		ts := fs.state.Tables[key]
		value := rowCount
		ts.SnapshotRowCount = &value
		fs.state.Tables[key] = ts
		return nil
	})
}

// GetStrictSnapshotRowCount mirrors the SQLite latest-run behavior, including
// migration-scoped partition tasks that carry one shared full-table count.
// FileState keeps only one run, so an empty runID means its current run.
func (fs *FileState) GetStrictSnapshotRowCount(runID, sourceSchema, targetSchema, tableName string) (*int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.state == nil || !fs.state.StrictConsistency ||
		fs.state.SourceSchema != sourceSchema || fs.state.TargetSchema != targetSchema ||
		(runID != "" && fs.state.RunID != runID) {
		return nil, nil
	}
	for _, task := range fs.state.Tables {
		if fileStateTaskType(task) != "transfer" || task.TaskTable != tableName ||
			task.Status != "success" ||
			task.SnapshotRowCount == nil {
			continue
		}
		value := *task.SnapshotRowCount
		return &value, nil
	}
	return nil, nil
}
