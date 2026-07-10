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

const (
	RunResumabilityInProgress     = "run is in progress or was interrupted"
	RunResumabilityPartialFailure = "one or more tables failed and can be retried"
	RunResumabilityAllowedPartial = "partial outcome accepted by migration.allow_partial"
	RunResumabilityAbandoned      = "run was explicitly abandoned by the operator"
)

// CreateRun creates a new migration run
func (s *State) CreateRun(id, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	configJSON, _ := json.Marshal(config)

	// Compute config hash for change detection on resume (matches filestate behavior)
	hash := sha256.Sum256(configJSON)
	configHash := hex.EncodeToString(hash[:8])

	_, err := s.db.Exec(`
		INSERT INTO runs (
			id, started_at, last_heartbeat, status, resumable, resumability_reason,
			source_schema, target_schema, config, profile_name, config_path, config_hash
		)
		VALUES (?, datetime('now'), datetime('now'), 'running', 1, ?, ?, ?, ?, ?, ?, ?)
	`, id, RunResumabilityInProgress, sourceSchema, targetSchema, string(configJSON), profileName, configPath, configHash)
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
	return s.withRunLeaseTx(id, "update run config", func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE runs SET config = ? WHERE id = ?`, string(configJSON), id)
		if err != nil {
			return err
		}
		return requireOneRun(result, id, "update run config")
	})
}

// CompleteRun marks a run as complete
func (s *State) CompleteRun(id string, status string, errorMsg string) error {
	return s.completeRun(id, status, errorMsg, false, terminalResumabilityReason(status))
}

// CompleteRunResumable records an outcome while preserving the run's durable
// checkpoints as eligible for a later resume. Partial transfer outcomes use
// this path; accepted partials use CompleteRun and are terminal.
func (s *State) CompleteRunResumable(id string, status string, errorMsg string, reason string) error {
	if reason == "" {
		reason = RunResumabilityPartialFailure
	}
	return s.completeRun(id, status, errorMsg, true, reason)
}

func (s *State) completeRun(id, status, errorMsg string, resumable bool, reason string) error {
	return s.withRunLeaseTx(id, "complete run", func(tx *sql.Tx) error {
		result, err := tx.Exec(`
			UPDATE runs
			SET status = ?, completed_at = datetime('now'), error = ?,
			    resumable = ?, resumability_reason = ?
			WHERE id = ?
		`, status, errorMsg, resumable, reason, id)
		if err != nil {
			return err
		}
		return requireOneRun(result, id, "complete run")
	})
}

// AbandonRun removes a recoverable run from automatic resume selection. A
// running/interrupted run becomes a failed terminal outcome; a partial run
// keeps its truthful partial outcome and completion timestamp.
func (s *State) AbandonRun(id string, reason string) error {
	reason = abandonResumabilityReason(reason)
	return s.withRunLeaseTx(id, "abandon run", func(tx *sql.Tx) error {
		result, err := tx.Exec(`
			UPDATE runs
			SET status = CASE WHEN status = 'running' THEN 'failed' ELSE status END,
			    completed_at = COALESCE(completed_at, datetime('now')),
			    error = CASE WHEN status = 'running' THEN ? ELSE error END,
			    resumable = 0,
			    resumability_reason = ?
			WHERE id = ? AND resumable = 1
		`, reason, reason, id)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("run %s is not resumable or does not exist", id)
		}
		return nil
	})
}

func terminalResumabilityReason(status string) string {
	if status == "partial" {
		return RunResumabilityAllowedPartial
	}
	return fmt.Sprintf("run completed with terminal outcome %s", status)
}

func abandonResumabilityReason(reason string) string {
	if reason == "" {
		return RunResumabilityAbandoned
	}
	return RunResumabilityAbandoned + ": " + reason
}

// UpdateRunHeartbeat records that a running process still owns a run.
func (s *State) UpdateRunHeartbeat(runID string, at time.Time) error {
	return s.withRunLeaseTx(runID, "update run heartbeat", func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE runs SET last_heartbeat = ? WHERE id = ?`,
			at.UTC().Format(sqliteRunTimeLayout), runID)
		if err != nil {
			return err
		}
		return requireOneRun(result, runID, "update run heartbeat")
	})
}

// GetLastIncompleteRun returns the most recent incomplete run
func (s *State) GetLastIncompleteRun() (*Run, error) {
	return s.getLastIncompleteRun("", nil)
}

// GetLastIncompleteRunForTarget returns the newest resumable run bound to the
// canonical target. Legacy runs predate target keys and fall back to the target
// schema so operators can still recover them under the heartbeat safeguards.
func (s *State) GetLastIncompleteRunForTarget(target MigrationTarget) (*Run, error) {
	target = target.Canonical()
	return s.getLastIncompleteRun(`
		AND (
			lease_target_key = ?
			OR (
				COALESCE(lease_generation, 0) = 0
				AND (target_schema = ? OR (? = 'sqlite' AND target_schema = ''))
			)
		)
	`, []any{target.Key(), target.Schema, target.Driver})
}

func (s *State) getLastIncompleteRun(targetPredicate string, args []any) (*Run, error) {
	var r Run
	var startedAtStr string
	var completedAt, profileName, configPath, phase, configHash, lastHeartbeat, configStr, resumabilityReason, errorMsg sql.NullString
	var resumable int
	query := `
		SELECT id, started_at, completed_at, status, COALESCE(phase, 'initializing'), source_schema, target_schema,
		       profile_name, config_path, config_hash, last_heartbeat, config,
		       COALESCE(resumable, 0), resumability_reason, error,
		       COALESCE(lease_target_key, ''), COALESCE(lease_owner_token, ''), COALESCE(lease_generation, 0)
		FROM runs WHERE resumable = 1
	` + targetPredicate + `
		ORDER BY started_at DESC, rowid DESC LIMIT 1
	`
	err := s.db.QueryRow(query, args...).Scan(&r.ID, &startedAtStr, &completedAt, &r.Status, &phase, &r.SourceSchema, &r.TargetSchema, &profileName, &configPath, &configHash, &lastHeartbeat, &configStr,
		&resumable, &resumabilityReason, &errorMsg,
		&r.LeaseTargetKey, &r.LeaseOwnerToken, &r.LeaseGeneration)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Parse SQLite datetime string
	r.StartedAt, _ = time.Parse(sqliteRunTimeLayout, startedAtStr)
	if completedAt.Valid {
		completed, _ := time.Parse(sqliteRunTimeLayout, completedAt.String)
		r.CompletedAt = &completed
	}
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
	r.Resumable = resumable != 0
	if resumabilityReason.Valid {
		r.ResumabilityReason = resumabilityReason.String
	}
	if errorMsg.Valid {
		r.Error = errorMsg.String
	}
	return &r, nil
}

// UpdatePhase updates the current phase of a migration run
func (s *State) UpdatePhase(runID, phase string) error {
	return s.withRunLeaseTx(runID, "update run phase", func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE runs SET phase = ? WHERE id = ?`, phase, runID)
		if err != nil {
			return err
		}
		return requireOneRun(result, runID, "update run phase")
	})
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
		AND (
			(
				COALESCE(current.lease_target_key, '') != ''
				AND later.lease_target_key = current.lease_target_key
			)
			OR (
				COALESCE(current.lease_target_key, '') = ''
				AND later.target_schema = ?
			)
		)
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

// runHistoryColumns is the shared SELECT list for history reads; scanRunRows
// scans exactly these columns in order.
const runHistoryColumns = "id, started_at, completed_at, last_heartbeat, status, COALESCE(resumable, 0), resumability_reason, phase, source_schema, target_schema, config, profile_name, config_path, error"

// scanRunRows materializes runs from a query over runHistoryColumns.
func scanRunRows(rows *sql.Rows) ([]Run, error) {
	var runs []Run
	for rows.Next() {
		var r Run
		var startedAtStr string
		var completedAtStr sql.NullString
		var lastHeartbeat sql.NullString
		var configStr sql.NullString
		var phase, profileName, configPath, errorMsg, resumabilityReason sql.NullString
		var resumable int
		if err := rows.Scan(&r.ID, &startedAtStr, &completedAtStr, &lastHeartbeat, &r.Status, &resumable, &resumabilityReason, &phase, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg); err != nil {
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
		if phase.Valid {
			r.Phase = phase.String
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
		r.Resumable = resumable != 0
		if resumabilityReason.Valid {
			r.ResumabilityReason = resumabilityReason.String
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// GetAllRuns returns the 20 most recent runs for history/AI features.
func (s *State) GetAllRuns() ([]Run, error) {
	rows, err := s.db.Query(`
		SELECT ` + runHistoryColumns + `
		FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRunRows(rows)
}

// GetRunsPage returns one page of runs (most recent first), optionally filtered
// by status, plus the total count matching the filter so callers can render
// pagination. An empty status means no filter. limit/offset are applied as-is;
// the caller is responsible for clamping them.
func (s *State) GetRunsPage(status string, limit, offset int) ([]Run, int, error) {
	where := ""
	var whereArgs []any
	if status != "" {
		where = " WHERE status = ?"
		whereArgs = append(whereArgs, status)
	}

	var total int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM runs"+where, whereArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	pageArgs := append(append([]any{}, whereArgs...), limit, offset)
	rows, err := s.db.Query(`
		SELECT `+runHistoryColumns+`
		FROM runs`+where+`
		ORDER BY started_at DESC, rowid DESC LIMIT ? OFFSET ?
	`, pageArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	runs, err := scanRunRows(rows)
	if err != nil {
		return nil, 0, err
	}
	return runs, total, nil
}

// GetRunByID returns a specific run by ID
func (s *State) GetRunByID(runID string) (*Run, error) {
	var r Run
	var startedAtStr string
	var completedAtStr sql.NullString
	var lastHeartbeat sql.NullString
	var configStr sql.NullString

	var profileName, configPath, errorMsg, phase, resumabilityReason sql.NullString
	var resumable int
	err := s.db.QueryRow(`
		SELECT id, started_at, completed_at, last_heartbeat, status,
		       COALESCE(resumable, 0), resumability_reason, phase,
		       source_schema, target_schema, config, profile_name, config_path, error,
		       COALESCE(lease_target_key, ''), COALESCE(lease_owner_token, ''), COALESCE(lease_generation, 0)
		FROM runs WHERE id = ?
	`, runID).Scan(&r.ID, &startedAtStr, &completedAtStr, &lastHeartbeat, &r.Status, &resumable, &resumabilityReason, &phase, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg,
		&r.LeaseTargetKey, &r.LeaseOwnerToken, &r.LeaseGeneration)

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
	r.Resumable = resumable != 0
	if resumabilityReason.Valid {
		r.ResumabilityReason = resumabilityReason.String
	}
	if phase.Valid {
		r.Phase = phase.String
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
				WHERE completed_at < ? AND resumable = 0
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
				WHERE completed_at < ? AND resumable = 0
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
				WHERE completed_at < ? AND resumable = 0
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
				WHERE completed_at < ? AND resumable = 0
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old fallback events: %w", err)
	}

	// Incremental fences are run-scoped and have no foreign-key cascade.
	_, err = s.db.Exec(`
		DELETE FROM incremental_fences WHERE run_id IN (
			SELECT id FROM runs
			WHERE completed_at < ? AND resumable = 0
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old incremental fences: %w", err)
	}

	// Delete old delete-reconciliation table summaries.
	_, err = s.db.Exec(`
		DELETE FROM delete_reconciliation_tables WHERE run_id IN (
			SELECT id FROM runs
				WHERE completed_at < ? AND resumable = 0
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old delete reconciliation tables: %w", err)
	}

	// Delete old tasks
	_, err = s.db.Exec(`
		DELETE FROM tasks WHERE run_id IN (
			SELECT id FROM runs
				WHERE completed_at < ? AND resumable = 0
		)
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old tasks: %w", err)
	}

	// Delete old runs
	result, err := s.db.Exec(`
		DELETE FROM runs
				WHERE completed_at < ? AND resumable = 0
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old runs: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()
	return rowsDeleted, nil
}
