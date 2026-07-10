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
	return fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}

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
		var carriedDeleteReconciliations map[string]map[string]deleteReconciliationState
		if fs.state != nil && len(fs.state.DeleteReconciliations) > 0 {
			carriedDeleteReconciliations = fs.state.DeleteReconciliations
		}
		carriedLeases := fs.state.MigrationLeases
		if carriedLeases == nil {
			carriedLeases = make(map[string]MigrationLease)
		}

		now := time.Now()
		fs.state = &fileStateData{
			RunID:                 id,
			StartedAt:             now,
			LastHeartbeat:         now,
			Status:                "running",
			Resumable:             boolPointer(true),
			ResumabilityReason:    RunResumabilityInProgress,
			SourceSchema:          sourceSchema,
			TargetSchema:          targetSchema,
			ConfigHash:            hex.EncodeToString(hash[:8]), // First 8 bytes
			ProfileName:           profileName,
			ConfigPath:            configPath,
			Tables:                make(map[string]tableState),
			SyncTimestamps:        carriedTimestamps,
			SchemaSnapshots:       carriedSnapshots,
			DeleteReconciliations: carriedDeleteReconciliations,
			MigrationLeases:       carriedLeases,
		}

		return fs.save()
	})
}

// UpdateRunConfig is a no-op for the file backend (it does not persist the full config).
func (fs *FileState) UpdateRunConfig(id string, config any) error {
	return nil
}

// CompleteRun marks the run as complete.
func (fs *FileState) CompleteRun(id string, status string, errorMsg string) error {
	return fs.completeRun(id, status, errorMsg, false, terminalResumabilityReason(status))
}

// CompleteRunResumable records an outcome that remains eligible for resume.
func (fs *FileState) CompleteRunResumable(id string, status string, errorMsg string, reason string) error {
	if reason == "" {
		reason = RunResumabilityPartialFailure
	}
	return fs.completeRun(id, status, errorMsg, true, reason)
}

func (fs *FileState) completeRun(id, status, errorMsg string, resumable bool, reason string) error {
	return fs.withRunLeaseMutation("complete run", func() error {
		if fs.state.RunID != id {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, id)
		}
		now := time.Now()
		fs.state.Status = status
		fs.state.CompletedAt = &now
		fs.state.Error = errorMsg
		fs.state.Resumable = boolPointer(resumable)
		fs.state.ResumabilityReason = reason
		return nil
	})
}

// AbandonRun removes the current run from automatic resume selection.
func (fs *FileState) AbandonRun(id string, reason string) error {
	reason = abandonResumabilityReason(reason)
	return fs.withRunLeaseMutation("abandon run", func() error {
		if fs.state.RunID != id {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, id)
		}
		if !fileRunResumable(fs.state) {
			return fmt.Errorf("run %s is not resumable", id)
		}
		if fs.state.Status == "running" {
			fs.state.Status = "failed"
			fs.state.Error = reason
		}
		if fs.state.CompletedAt == nil {
			now := time.Now()
			fs.state.CompletedAt = &now
		}
		fs.state.Resumable = boolPointer(false)
		fs.state.ResumabilityReason = reason
		return nil
	})
}

// UpdateRunHeartbeat records that a running process still owns a run.
func (fs *FileState) UpdateRunHeartbeat(runID string, at time.Time) error {
	return fs.withRunLeaseMutation("update run heartbeat", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		fs.state.LastHeartbeat = at.UTC()
		return nil
	})
}

// GetLastIncompleteRun returns the current run if it's incomplete.
func (fs *FileState) GetLastIncompleteRun() (*Run, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var run *Run
	err := fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		if fs.state.RunID == "" || !fileRunResumable(fs.state) {
			return nil
		}
		value := runFromFileState(fs.state)
		run = &value
		return nil
	})
	return run, err
}

// GetLastIncompleteRunForTarget filters the file backend's single current run
// by its durable target lease key. Legacy files fall back to target schema.
func (fs *FileState) GetLastIncompleteRunForTarget(target MigrationTarget) (*Run, error) {
	run, err := fs.GetLastIncompleteRun()
	if err != nil || run == nil {
		return run, err
	}
	target = target.Canonical()
	if run.LeaseTargetKey != "" {
		if run.LeaseTargetKey != target.Key() {
			return nil, nil
		}
	} else if run.TargetSchema != target.Schema && (target.Driver != "sqlite" || run.TargetSchema != "") {
		return nil, nil
	}
	return run, nil
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
	return fs.withRunLeaseMutation("mark run as resumed", func() error {
		if fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		for key, ts := range fs.state.Tables {
			if ts.Status == "running" {
				ts.Status = "pending"
				fs.state.Tables[key] = ts
			}
		}
		fs.state.Status = "running"
		fs.state.CompletedAt = nil
		fs.state.Error = ""
		fs.state.Resumable = boolPointer(true)
		fs.state.ResumabilityReason = RunResumabilityInProgress
		return nil
	})
}

func runFromFileState(state *fileStateData) Run {
	phase := state.Phase
	if phase == "" {
		phase = "initializing"
	}
	lastHeartbeat := state.LastHeartbeat
	if lastHeartbeat.IsZero() {
		lastHeartbeat = state.StartedAt
	}
	return Run{
		ID:                 state.RunID,
		StartedAt:          state.StartedAt,
		CompletedAt:        state.CompletedAt,
		LastHeartbeat:      lastHeartbeat,
		Status:             state.Status,
		Resumable:          fileRunResumable(state),
		ResumabilityReason: state.ResumabilityReason,
		Phase:              phase,
		Error:              state.Error,
		SourceSchema:       state.SourceSchema,
		TargetSchema:       state.TargetSchema,
		ConfigHash:         state.ConfigHash,
		ProfileName:        state.ProfileName,
		ConfigPath:         state.ConfigPath,
		LeaseTargetKey:     state.LeaseTargetKey,
		LeaseOwnerToken:    state.LeaseOwnerToken,
		LeaseGeneration:    state.LeaseGeneration,
	}
}

// UpdatePhase updates the current phase of a migration run.
func (fs *FileState) UpdatePhase(runID, phase string) error {
	return fs.withRunLeaseMutation("update run phase", func() error {
		if fs.state == nil || fs.state.RunID != runID {
			return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		fs.state.Phase = phase
		return nil
	})
}
