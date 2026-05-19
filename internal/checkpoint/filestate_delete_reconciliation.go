package checkpoint

import "time"

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
