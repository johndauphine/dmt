package checkpoint

import (
	"sort"
	"time"
)

func (fs *FileState) SaveSchemaSnapshot(runID, sourceSchema, tableName, schemaJSON string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.SchemaSnapshots == nil {
		fs.state.SchemaSnapshots = make(map[string]map[string]schemaSnapshotState)
	}
	if fs.state.SchemaSnapshots[sourceSchema] == nil {
		fs.state.SchemaSnapshots[sourceSchema] = make(map[string]schemaSnapshotState)
	}
	fs.state.SchemaSnapshots[sourceSchema][tableName] = schemaSnapshotState{
		RunID:      runID,
		CapturedAt: time.Now().UTC(),
		SchemaJSON: schemaJSON,
	}
	return fs.save()
}

func (fs *FileState) GetLatestSchemaSnapshots(sourceSchema string) ([]SchemaSnapshotRecord, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state == nil || fs.state.SchemaSnapshots == nil {
		return nil, nil
	}
	tables := fs.state.SchemaSnapshots[sourceSchema]
	if len(tables) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(tables))
	for tableName := range tables {
		names = append(names, tableName)
	}
	sort.Strings(names)

	records := make([]SchemaSnapshotRecord, 0, len(names))
	for _, tableName := range names {
		snapshot := tables[tableName]
		records = append(records, SchemaSnapshotRecord{
			RunID:        snapshot.RunID,
			SourceSchema: sourceSchema,
			TableName:    tableName,
			CapturedAt:   snapshot.CapturedAt,
			SchemaJSON:   snapshot.SchemaJSON,
		})
	}
	return records, nil
}
