package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Fixed-width UTC timestamps preserve chronological ordering in SQLite text
// comparisons. time.RFC3339Nano trims trailing zeroes and is not lexically
// ordered across whole-second/fractional-second values.
const leaseTimeLayout = "2006-01-02T15:04:05.000000000Z"

func (s *State) setActiveMigrationLease(lease MigrationLease) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	copy := lease
	s.lease = &copy
}

func (s *State) activeMigrationLease() (MigrationLease, bool) {
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	if s.lease == nil {
		return MigrationLease{}, false
	}
	return *s.lease, true
}

func (s *State) clearActiveMigrationLease(lease MigrationLease) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.lease != nil && s.lease.TargetKey == lease.TargetKey &&
		s.lease.OwnerToken == lease.OwnerToken && s.lease.Generation == lease.Generation {
		s.lease = nil
	}
}

func (s *State) AcquireMigrationLease(target MigrationTarget, ownerToken string, now time.Time, ttl time.Duration) (MigrationLease, error) {
	target = target.Canonical()
	if err := validateLeaseRequest(target, ownerToken, ttl); err != nil {
		return MigrationLease{}, err
	}
	now = now.UTC()
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return MigrationLease{}, fmt.Errorf("marshal migration target: %w", err)
	}
	targetKey := target.Key()
	expiresAt := now.Add(ttl)

	var generation int64
	err = s.db.QueryRow(`
		INSERT INTO migration_leases (
			target_key, target_identity, owner_token, generation, run_id,
			acquired_at, renewed_at, expires_at
		) VALUES (?, ?, ?, 1, NULL, ?, ?, ?)
		ON CONFLICT(target_key) DO UPDATE SET
			target_identity = excluded.target_identity,
			owner_token = excluded.owner_token,
			generation = CASE
				WHEN migration_leases.owner_token = excluded.owner_token
					THEN migration_leases.generation
				ELSE migration_leases.generation + 1
			END,
			run_id = CASE
				WHEN migration_leases.owner_token = excluded.owner_token
					THEN migration_leases.run_id
				ELSE NULL
			END,
			acquired_at = CASE
				WHEN migration_leases.owner_token = excluded.owner_token
					THEN migration_leases.acquired_at
				ELSE excluded.acquired_at
			END,
			renewed_at = excluded.renewed_at,
			expires_at = excluded.expires_at
		WHERE migration_leases.owner_token = excluded.owner_token
		   OR migration_leases.expires_at <= excluded.acquired_at
		RETURNING generation
	`, targetKey, string(targetJSON), ownerToken,
		now.Format(leaseTimeLayout), now.Format(leaseTimeLayout), expiresAt.Format(leaseTimeLayout)).Scan(&generation)
	if err == sql.ErrNoRows {
		return MigrationLease{}, s.sqliteLeaseHeldError(targetKey, target)
	}
	if err != nil {
		return MigrationLease{}, fmt.Errorf("acquire migration lease for %s: %w", target, err)
	}

	lease := MigrationLease{
		Target: target, TargetKey: targetKey, OwnerToken: ownerToken,
		Generation: generation, AcquiredAt: now, RenewedAt: now, ExpiresAt: expiresAt,
	}
	s.setActiveMigrationLease(lease)
	return lease, nil
}

func (s *State) sqliteLeaseHeldError(targetKey string, fallback MigrationTarget) error {
	var identityJSON, ownerToken, runID, expiresAt string
	var generation int64
	err := s.db.QueryRow(`
		SELECT target_identity, owner_token, generation, COALESCE(run_id, ''), expires_at
		FROM migration_leases WHERE target_key = ?
	`, targetKey).Scan(&identityJSON, &ownerToken, &generation, &runID, &expiresAt)
	if err != nil {
		return fmt.Errorf("read conflicting migration lease: %w", err)
	}
	target := fallback
	_ = json.Unmarshal([]byte(identityJSON), &target)
	expires, _ := time.Parse(leaseTimeLayout, expiresAt)
	return &LeaseHeldError{Target: target, OwnerToken: ownerToken, Generation: generation, RunID: runID, ExpiresAt: expires}
}

func (s *State) BindRunLease(runID string, lease MigrationLease) error {
	active, ok := s.activeMigrationLease()
	if !ok || !sameMigrationLease(active, lease) {
		return leaseLost(lease, "binding run to migration lease")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := verifySQLiteLeaseTx(tx, lease, time.Now().UTC(), "binding run to migration lease"); err != nil {
		return err
	}
	result, err := tx.Exec(`
		UPDATE runs
		SET lease_target_key = ?, lease_owner_token = ?, lease_generation = ?
		WHERE id = ?
	`, lease.TargetKey, lease.OwnerToken, lease.Generation, runID)
	if err != nil {
		return err
	}
	if err := requireOneRun(result, runID, "bind migration lease"); err != nil {
		return err
	}
	result, err = tx.Exec(`
		UPDATE migration_leases SET run_id = ?
		WHERE target_key = ? AND owner_token = ? AND generation = ?
	`, runID, lease.TargetKey, lease.OwnerToken, lease.Generation)
	if err != nil {
		return err
	}
	if err := requireOneLease(result, lease, "bind migration lease"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	lease.RunID = runID
	s.setActiveMigrationLease(lease)
	return nil
}

func (s *State) RenewMigrationLease(lease MigrationLease, now time.Time, ttl time.Duration) (MigrationLease, error) {
	if ttl <= 0 {
		return MigrationLease{}, fmt.Errorf("migration lease TTL must be positive")
	}
	now = now.UTC()
	expiresAt := now.Add(ttl)
	result, err := s.db.Exec(`
		UPDATE migration_leases
		SET renewed_at = ?, expires_at = ?
		WHERE target_key = ? AND owner_token = ? AND generation = ?
		  AND expires_at > ?
	`, now.Format(leaseTimeLayout), expiresAt.Format(leaseTimeLayout),
		lease.TargetKey, lease.OwnerToken, lease.Generation, now.Format(leaseTimeLayout))
	if err != nil {
		return MigrationLease{}, fmt.Errorf("renew migration lease: %w", err)
	}
	if err := requireOneLease(result, lease, "renew migration lease"); err != nil {
		return MigrationLease{}, err
	}
	lease.RenewedAt = now
	lease.ExpiresAt = expiresAt
	s.setActiveMigrationLease(lease)
	return lease, nil
}

func (s *State) ReleaseMigrationLease(lease MigrationLease) error {
	now := time.Now().UTC()
	result, err := s.db.Exec(`
		UPDATE migration_leases
		SET owner_token = '', run_id = NULL, renewed_at = ?, expires_at = ?
		WHERE target_key = ? AND owner_token = ? AND generation = ?
	`, now.Format(leaseTimeLayout), now.Format(leaseTimeLayout),
		lease.TargetKey, lease.OwnerToken, lease.Generation)
	if err != nil {
		return fmt.Errorf("release migration lease: %w", err)
	}
	if err := requireOneLease(result, lease, "release migration lease"); err != nil {
		return err
	}
	s.clearActiveMigrationLease(lease)
	return nil
}

func sameMigrationLease(a, b MigrationLease) bool {
	return a.TargetKey == b.TargetKey && a.OwnerToken == b.OwnerToken && a.Generation == b.Generation
}

func leaseLost(lease MigrationLease, operation string) error {
	return &LeaseLostError{TargetKey: lease.TargetKey, OwnerToken: lease.OwnerToken, Generation: lease.Generation, Operation: operation}
}

func requireOneRun(result sql.Result, runID, operation string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("%s: expected one run %q, updated %d", operation, runID, rows)
	}
	return nil
}

func requireOneLease(result sql.Result, lease MigrationLease, operation string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return leaseLost(lease, operation)
	}
	return nil
}

func verifySQLiteLeaseTx(tx *sql.Tx, lease MigrationLease, now time.Time, operation string) error {
	var count int
	err := tx.QueryRow(`
		SELECT COUNT(*) FROM migration_leases
		WHERE target_key = ? AND owner_token = ? AND generation = ? AND expires_at > ?
	`, lease.TargetKey, lease.OwnerToken, lease.Generation, now.UTC().Format(leaseTimeLayout)).Scan(&count)
	if err != nil {
		return err
	}
	if count != 1 {
		return leaseLost(lease, operation)
	}
	return nil
}

func (s *State) withRunLeaseTx(runID, operation string, mutate func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.verifyRunLeaseTx(tx, runID, operation); err != nil {
		return err
	}
	if err := mutate(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *State) withTaskLeaseTx(taskID int64, operation string, mutate func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var runID string
	if err := tx.QueryRow(`SELECT run_id FROM tasks WHERE id = ?`, taskID).Scan(&runID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%s: task not found: %d", operation, taskID)
		}
		return err
	}
	if err := s.verifyRunLeaseTx(tx, runID, operation); err != nil {
		return err
	}
	if err := mutate(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *State) verifyRunLeaseTx(tx *sql.Tx, runID, operation string) error {
	var targetKey, ownerToken sql.NullString
	var generation int64
	if err := tx.QueryRow(`
		SELECT lease_target_key, lease_owner_token, lease_generation
		FROM runs WHERE id = ?
	`, runID).Scan(&targetKey, &ownerToken, &generation); err != nil {
		return err
	}
	if generation == 0 {
		return nil
	}
	active, ok := s.activeMigrationLease()
	if !ok || active.TargetKey != targetKey.String || active.OwnerToken != ownerToken.String || active.Generation != generation {
		lease := MigrationLease{TargetKey: targetKey.String, OwnerToken: ownerToken.String, Generation: generation}
		if ok {
			lease = active
		}
		return leaseLost(lease, operation)
	}
	return verifySQLiteLeaseTx(tx, active, time.Now().UTC(), operation)
}

var _ MigrationLeaseBackend = (*State)(nil)
