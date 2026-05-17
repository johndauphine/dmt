package checkpoint

func (fs *FileState) SaveAIAdjustment(runID string, record AIAdjustmentRecord) error {
	return nil
}

// GetAIAdjustments returns empty slice for file state (doesn't persist AI history).
func (fs *FileState) GetAIAdjustments(limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

// GetAIAdjustmentsByAction returns empty slice for file state.
func (fs *FileState) GetAIAdjustmentsByAction(action string, limit int) ([]AIAdjustmentRecord, error) {
	return nil, nil
}

// SaveAITuning is a no-op for file state (doesn't persist tuning history).
func (fs *FileState) SaveAITuning(record AITuningRecord) error {
	return nil
}

// UpdateAITuningResult is a no-op for file state.
func (fs *FileState) UpdateAITuningResult(throughput float64, durationSecs float64, chunkRetryCount int) error {
	return nil
}

// GetAITuningHistory returns empty slice for file state (doesn't persist tuning history).
func (fs *FileState) GetAITuningHistory(limit int, sourceType, targetType string) ([]AITuningRecord, error) {
	return nil, nil
}

// GetAITuningAggregatesByWaw returns nil for file state - no history persisted.
// len(nil) == 0 so the smartconfig format helper correctly skips the section.
func (fs *FileState) GetAITuningAggregatesByWaw(sourceType, targetType string) ([]WawAggregateRecord, error) {
	return nil, nil
}

// GetAITuningAggregatesByChunkSize returns nil for file state - no history persisted.
func (fs *FileState) GetAITuningAggregatesByChunkSize(sourceType, targetType string) ([]ChunkSizeAggregateRecord, error) {
	return nil, nil
}

// SaveFallbackEvent persists an AI fallback occurrence (#176) so a
// separate "dmt status" poll can read counts even though the running
// migration's in-memory counters are inaccessible. UPSERTs into the
// nested run_id -> surface -> fingerprint map. Fingerprint may be ""
// (call sites with no fingerprint still accumulate under one record
// for the surface).
//
// The file backend is the Airflow path; cross-process visibility is
// the whole reason this method exists rather than being a no-op like
