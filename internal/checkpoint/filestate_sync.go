package checkpoint

import "time"

func (fs *FileState) GetLastSyncTimestamp(sourceSchema, tableName, targetSchema string) (*time.Time, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.state == nil || fs.state.SyncTimestamps == nil {
		return nil, nil
	}
	tables, ok := fs.state.SyncTimestamps[sourceSchema]
	if !ok {
		return nil, nil
	}
	targets, ok := tables[tableName]
	if !ok {
		return nil, nil
	}
	ts, ok := targets[targetSchema]
	if !ok {
		return nil, nil
	}
	return &ts, nil
}

// UpdateSyncTimestamp persists the sync timestamp for the given
// (sourceSchema, tableName, targetSchema) triple. Uses the
// crash-safe atomic-write path established in #254, so a
// SIGKILL/eviction partway through the save can't tear the file.
func (fs *FileState) UpdateSyncTimestamp(sourceSchema, tableName, targetSchema string, ts time.Time) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.state == nil {
		fs.state = &fileStateData{Tables: make(map[string]tableState)}
	}
	if fs.state.SyncTimestamps == nil {
		fs.state.SyncTimestamps = make(map[string]map[string]map[string]time.Time)
	}
	if fs.state.SyncTimestamps[sourceSchema] == nil {
		fs.state.SyncTimestamps[sourceSchema] = make(map[string]map[string]time.Time)
	}
	if fs.state.SyncTimestamps[sourceSchema][tableName] == nil {
		fs.state.SyncTimestamps[sourceSchema][tableName] = make(map[string]time.Time)
	}
	fs.state.SyncTimestamps[sourceSchema][tableName][targetSchema] = ts
	return fs.save()
}
