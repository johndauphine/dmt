package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

func (fs *FileState) CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Compute config hash for change detection
	configJSON, _ := json.Marshal(config)
	hash := sha256.Sum256(configJSON)

	// Carry over sync_timestamps from any prior state - they persist
	// across runs so date-based incremental sync can read the
	// previous high-water mark on a fresh `dmt run`. Without this,
	// the second invocation's CreateRun (which fires before any
	// GetLastSyncTimestamp call) would overwrite the loaded map
	// with nil, silently degrading incremental sync to full
	// (Codex review on #255 PR).
	var carriedTimestamps map[string]map[string]map[string]time.Time
	if fs.state != nil && len(fs.state.SyncTimestamps) > 0 {
		carriedTimestamps = fs.state.SyncTimestamps
	}
	var carriedSnapshots map[string]map[string]schemaSnapshotState
	if fs.state != nil && len(fs.state.SchemaSnapshots) > 0 {
		carriedSnapshots = fs.state.SchemaSnapshots
	}

	fs.state = &fileStateData{
		RunID:           id,
		StartedAt:       time.Now(),
		Status:          "running",
		SourceSchema:    sourceSchema,
		TargetSchema:    targetSchema,
		ConfigHash:      hex.EncodeToString(hash[:8]), // First 8 bytes
		ProfileName:     profileName,
		ConfigPath:      configPath,
		Tables:          make(map[string]tableState),
		SyncTimestamps:  carriedTimestamps,
		SchemaSnapshots: carriedSnapshots,
	}

	return fs.save()
}

// UpdateRunConfig is a no-op for the file backend (it does not persist the full config).
func (fs *FileState) UpdateRunConfig(id string, config any) error {
	return nil
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
