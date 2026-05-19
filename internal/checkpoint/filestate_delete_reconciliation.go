package checkpoint

import (
	"fmt"
	"sort"
	"time"
)

func (fs *FileState) GetDeleteReconciliationState(
	sourceSchema,
	targetSchema string,
) (*DeleteReconciliationState, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state == nil || fs.state.DeleteReconciliations == nil {
		return nil, nil
	}
	targets := fs.state.DeleteReconciliations[sourceSchema]
	if len(targets) == 0 {
		return nil, nil
	}
	rec, ok := targets[targetSchema]
	if !ok {
		return nil, nil
	}
	return &DeleteReconciliationState{
		SourceSchema:  sourceSchema,
		TargetSchema:  targetSchema,
		LastRunID:     rec.LastRunID,
		LastSuccessAt: rec.LastSuccessAt,
		UpdatedAt:     rec.UpdatedAt,
	}, nil
}

func (fs *FileState) RecordDeleteReconciliationSuccess(
	runID,
	sourceSchema,
	targetSchema string,
	completedAt time.Time,
) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.DeleteReconciliations == nil {
		fs.state.DeleteReconciliations = make(map[string]map[string]deleteReconciliationState)
	}
	if fs.state.DeleteReconciliations[sourceSchema] == nil {
		fs.state.DeleteReconciliations[sourceSchema] = make(map[string]deleteReconciliationState)
	}
	ts := completedAt.UTC()
	fs.state.DeleteReconciliations[sourceSchema][targetSchema] = deleteReconciliationState{
		LastRunID:     runID,
		LastSuccessAt: ts,
		UpdatedAt:     ts,
	}
	return fs.save()
}

func (fs *FileState) SaveDeleteReconciliationTable(
	runID string,
	record DeleteReconciliationTableRecord,
) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.RunID != runID {
		return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
	}
	if fs.state.DeleteReconciliationTables == nil {
		fs.state.DeleteReconciliationTables = make(map[string]deleteReconciliationTableState)
	}
	fs.state.DeleteReconciliationTables[record.TableName] = deleteReconciliationTableState{
		CandidateRows: record.CandidateRows,
		DeletedRows:   record.DeletedRows,
		Skipped:       record.Skipped,
		SkipReason:    record.SkipReason,
		UpdatedAt:     time.Now().UTC(),
	}
	return fs.save()
}

func (fs *FileState) GetDeleteReconciliationTables(
	runID string,
) ([]DeleteReconciliationTableRecord, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state == nil || fs.state.RunID != runID || len(fs.state.DeleteReconciliationTables) == 0 {
		return nil, nil
	}

	tableNames := make([]string, 0, len(fs.state.DeleteReconciliationTables))
	for tableName := range fs.state.DeleteReconciliationTables {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	records := make([]DeleteReconciliationTableRecord, 0, len(tableNames))
	for _, tableName := range tableNames {
		state := fs.state.DeleteReconciliationTables[tableName]
		records = append(records, DeleteReconciliationTableRecord{
			RunID:         runID,
			TableName:     tableName,
			CandidateRows: state.CandidateRows,
			DeletedRows:   state.DeletedRows,
			Skipped:       state.Skipped,
			SkipReason:    state.SkipReason,
			UpdatedAt:     state.UpdatedAt,
		})
	}
	return records, nil
}
