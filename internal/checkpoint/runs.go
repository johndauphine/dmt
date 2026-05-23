package checkpoint

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const sqliteRunTimeLayout = "2006-01-02 15:04:05"

// CreateRun creates a new migration run
func (s *State) CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	configJSON, _ := json.Marshal(config)

	// Compute config hash for change detection on resume (matches filestate behavior)
	hash := sha256.Sum256(configJSON)
	configHash := hex.EncodeToString(hash[:8])

	_, err := s.db.Exec(`
		INSERT INTO runs (id, started_at, last_heartbeat, status, source_schema, target_schema, config, profile_name, config_path, config_hash)
		VALUES (?, datetime('now'), datetime('now'), 'running', ?, ?, ?, ?, ?, ?)
	`, id, sourceSchema, targetSchema, string(configJSON), profileName, configPath, configHash)
	return err
}

// UpdateRunConfig overwrites the persisted config snapshot for a run.
// Called after AI tuning so history reflects the values actually used.
// config_hash is intentionally left unchanged — it's computed against the
// pre-AI config for resume validation against the user's YAML.
func (s *State) UpdateRunConfig(id string, config any) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal run config: %w", err)
	}
	result, err := s.db.Exec(`UPDATE runs SET config = ? WHERE id = ?`, string(configJSON), id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("update run config: no run with id %q", id)
	}
	return nil
}

// CompleteRun marks a run as complete
func (s *State) CompleteRun(id string, status string, errorMsg string) error {
	_, err := s.db.Exec(`
		UPDATE runs SET status = ?, completed_at = datetime('now'), error = ?
		WHERE id = ?
	`, status, errorMsg, id)
	return err
}

// UpdateRunHeartbeat records that a running process still owns a run.
func (s *State) UpdateRunHeartbeat(runID string, at time.Time) error {
	_, err := s.db.Exec(`UPDATE runs SET last_heartbeat = ? WHERE id = ?`,
		at.UTC().Format(sqliteRunTimeLayout), runID)
	return err
}

// GetLastIncompleteRun returns the most recent incomplete run
func (s *State) GetLastIncompleteRun() (*Run, error) {
	var r Run
	var startedAtStr string
	var profileName, configPath, phase, configHash, lastHeartbeat, configStr sql.NullString
	err := s.db.QueryRow(`
		SELECT id, started_at, status, COALESCE(phase, 'initializing'), source_schema, target_schema, profile_name, config_path, config_hash, last_heartbeat, config
		FROM runs WHERE status = 'running'
		ORDER BY started_at DESC, rowid DESC LIMIT 1
	`).Scan(&r.ID, &startedAtStr, &r.Status, &phase, &r.SourceSchema, &r.TargetSchema, &profileName, &configPath, &configHash, &lastHeartbeat, &configStr)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Parse SQLite datetime string
	r.StartedAt, _ = time.Parse(sqliteRunTimeLayout, startedAtStr)
	r.LastHeartbeat = r.StartedAt
	if lastHeartbeat.Valid && lastHeartbeat.String != "" {
		if t, err := time.Parse(sqliteRunTimeLayout, lastHeartbeat.String); err == nil {
			r.LastHeartbeat = t
		}
	}
	if profileName.Valid {
		r.ProfileName = profileName.String
	}
	if configPath.Valid {
		r.ConfigPath = configPath.String
	}
	if phase.Valid {
		r.Phase = phase.String
	}
	if configHash.Valid {
		r.ConfigHash = configHash.String
	}
	if configStr.Valid {
		r.Config = configStr.String
	}
	return &r, nil
}

// UpdatePhase updates the current phase of a migration run
func (s *State) UpdatePhase(runID, phase string) error {
	_, err := s.db.Exec(`UPDATE runs SET phase = ? WHERE id = ?`, phase, runID)
	return err
}

// SetRunConfigHash sets the config hash for a run (used for resume validation)
func (s *State) SetRunConfigHash(runID, configHash string) error {
	_, err := s.db.Exec(`UPDATE runs SET config_hash = ? WHERE id = ?`, configHash, runID)
	return err
}

// HasSuccessfulRunAfter checks if there's a successful run that supersedes the given incomplete run.
// A run is superseded if a later successful run exists with the same source and target schemas.
func (s *State) HasSuccessfulRunAfter(run *Run) (bool, error) {
	if run == nil {
		return false, nil
	}

	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*)
		FROM runs AS later
		JOIN runs AS current ON current.id = ?
		WHERE later.status = 'success'
		AND later.source_schema = ?
		AND later.target_schema = ?
		AND (
			later.started_at > current.started_at
			OR (later.started_at = current.started_at AND later.rowid > current.rowid)
		)
	`, run.ID, run.SourceSchema, run.TargetSchema).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetAllRuns returns all runs for history
func (s *State) GetAllRuns() ([]Run, error) {
	rows, err := s.db.Query(`
		SELECT id, started_at, completed_at, last_heartbeat, status, source_schema, target_schema, config, profile_name, config_path, error
		FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		var startedAtStr string
		var completedAtStr sql.NullString
		var lastHeartbeat sql.NullString
		var configStr sql.NullString
		var profileName, configPath, errorMsg sql.NullString
		if err := rows.Scan(&r.ID, &startedAtStr, &completedAtStr, &lastHeartbeat, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg); err != nil {
			return nil, err
		}
		r.StartedAt, _ = time.Parse(sqliteRunTimeLayout, startedAtStr)
		r.LastHeartbeat = r.StartedAt
		if completedAtStr.Valid {
			t, _ := time.Parse(sqliteRunTimeLayout, completedAtStr.String)
			r.CompletedAt = &t
		}
		if lastHeartbeat.Valid && lastHeartbeat.String != "" {
			if t, err := time.Parse(sqliteRunTimeLayout, lastHeartbeat.String); err == nil {
				r.LastHeartbeat = t
			}
		}
		if configStr.Valid {
			r.Config = configStr.String
		}
		if profileName.Valid {
			r.ProfileName = profileName.String
		}
		if configPath.Valid {
			r.ConfigPath = configPath.String
		}
		if errorMsg.Valid {
			r.Error = errorMsg.String
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// GetRunByID returns a specific run by ID
func (s *State) GetRunByID(runID string) (*Run, error) {
	var r Run
	var startedAtStr string
	var completedAtStr sql.NullString
	var lastHeartbeat sql.NullString
	var configStr sql.NullString

	var profileName, configPath, errorMsg sql.NullString
	err := s.db.QueryRow(`
		SELECT id, started_at, completed_at, last_heartbeat, status, source_schema, target_schema, config, profile_name, config_path, error
		FROM runs WHERE id = ?
	`, runID).Scan(&r.ID, &startedAtStr, &completedAtStr, &lastHeartbeat, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	r.StartedAt, _ = time.Parse(sqliteRunTimeLayout, startedAtStr)
	r.LastHeartbeat = r.StartedAt
	if completedAtStr.Valid {
		t, _ := time.Parse(sqliteRunTimeLayout, completedAtStr.String)
		r.CompletedAt = &t
	}
	if lastHeartbeat.Valid && lastHeartbeat.String != "" {
		if t, err := time.Parse(sqliteRunTimeLayout, lastHeartbeat.String); err == nil {
			r.LastHeartbeat = t
		}
	}
	if configStr.Valid {
		r.Config = configStr.String
	}
	if profileName.Valid {
		r.ProfileName = profileName.String
	}
	if configPath.Valid {
		r.ConfigPath = configPath.String
	}
	if errorMsg.Valid {
		r.Error = errorMsg.String
	}
	return &r, nil
}

// CleanupOldRuns removes completed runs older than retainDays.
// This prevents unbounded SQLite database growth.
func (s *State) CleanupOldRuns(retainDays int) (int64, error) {
	if retainDays <= 0 {
		return 0, nil
	}

	cutoff := time.Now().AddDate(0, 0, -retainDays).Format("2006-01-02 15:04:05")

	// Delete old progress records (cascade from tasks)
	_, err := s.db.Exec(`
		DELETE FROM transfer_progress WHERE task_id IN (
			SELECT id FROM tasks WHERE run_id IN (
				SELECT id FROM runs
				WHERE completed_at < ? AND status IN ('success', 'failed')
			)
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old progress: %w", err)
	}

	// Delete old task outputs
	_, err = s.db.Exec(`
		DELETE FROM task_outputs WHERE task_id IN (
			SELECT id FROM tasks WHERE run_id IN (
				SELECT id FROM runs
				WHERE completed_at < ? AND status IN ('success', 'failed')
			)
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old task outputs: %w", err)
	}

	// Delete old AI adjustments
	_, err = s.db.Exec(`
		DELETE FROM ai_adjustments WHERE run_id IN (
			SELECT id FROM runs
			WHERE completed_at < ? AND status IN ('success', 'failed')
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old ai adjustments: %w", err)
	}

	// Delete old fallback events. The schema declares ON DELETE
	// CASCADE, but the connection does not enable PRAGMA foreign_keys,
	// so the cascade would not actually fire — we delete explicitly
	// to match the rest of this function's manual-cascade pattern
	// (codex review on #176).
	_, err = s.db.Exec(`
		DELETE FROM fallback_events WHERE run_id IN (
			SELECT id FROM runs
			WHERE completed_at < ? AND status IN ('success', 'failed')
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old fallback events: %w", err)
	}

	// Delete old delete-reconciliation table summaries.
	_, err = s.db.Exec(`
		DELETE FROM delete_reconciliation_tables WHERE run_id IN (
			SELECT id FROM runs
			WHERE completed_at < ? AND status IN ('success', 'failed')
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old delete reconciliation tables: %w", err)
	}

	// Delete old tasks
	_, err = s.db.Exec(`
		DELETE FROM tasks WHERE run_id IN (
			SELECT id FROM runs
			WHERE completed_at < ? AND status IN ('success', 'failed')
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old tasks: %w", err)
	}

	// Delete old runs
	result, err := s.db.Exec(`
		DELETE FROM runs
		WHERE completed_at < ? AND status IN ('success', 'failed')
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old runs: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()
	return rowsDeleted, nil
}
