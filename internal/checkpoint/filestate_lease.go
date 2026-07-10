package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"gopkg.in/yaml.v3"
)

// withProcessLock serializes a complete YAML read/compare/write cycle across
// FileState instances and operating-system processes. The advisory lock is
// released by the OS if a process crashes.
func (fs *FileState) withProcessLock(fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(fs.path), 0700); err != nil {
		return fmt.Errorf("creating file state lock directory: %w", err)
	}
	lock := flock.New(fs.path + ".lock")
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("locking file state: %w", err)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func (fs *FileState) reloadLocked() error {
	data, err := os.ReadFile(fs.path)
	if errors.Is(err, os.ErrNotExist) {
		fs.state = &fileStateData{
			Tables:          make(map[string]tableState),
			MigrationLeases: make(map[string]MigrationLease),
		}
		return fs.rebuildTaskIndexLocked()
	}
	if err != nil {
		return fmt.Errorf("reading state file: %w", err)
	}
	state := &fileStateData{}
	if err := yaml.Unmarshal(data, state); err != nil {
		return fmt.Errorf("parsing state file: %w", err)
	}
	if state.Tables == nil {
		state.Tables = make(map[string]tableState)
	}
	if state.MigrationLeases == nil {
		state.MigrationLeases = make(map[string]MigrationLease)
	}
	fs.state = state
	return fs.rebuildTaskIndexLocked()
}

func (fs *FileState) setActiveMigrationLeaseLocked(lease MigrationLease) {
	copy := lease
	fs.lease = &copy
}

func (fs *FileState) activeMigrationLeaseLocked() (MigrationLease, bool) {
	if fs.lease == nil {
		return MigrationLease{}, false
	}
	return *fs.lease, true
}

func (fs *FileState) AcquireMigrationLease(target MigrationTarget, ownerToken string, now time.Time, ttl time.Duration) (MigrationLease, error) {
	target = target.Canonical()
	if err := validateLeaseRequest(target, ownerToken, ttl); err != nil {
		return MigrationLease{}, err
	}
	now = now.UTC()
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var acquired MigrationLease
	err := fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		key := target.Key()
		current, exists := fs.state.MigrationLeases[key]
		if exists && current.OwnerToken != ownerToken && current.OwnerToken != "" && current.ExpiresAt.After(now) {
			return &LeaseHeldError{
				Target: current.Target, OwnerToken: current.OwnerToken,
				Generation: current.Generation, RunID: current.RunID, ExpiresAt: current.ExpiresAt,
			}
		}

		generation := int64(1)
		acquiredAt := now
		runID := ""
		if exists {
			generation = current.Generation
			if current.OwnerToken == ownerToken {
				acquiredAt = current.AcquiredAt
				runID = current.RunID
			} else {
				generation++
			}
		}
		acquired = MigrationLease{
			Target: target, TargetKey: key, OwnerToken: ownerToken,
			Generation: generation, RunID: runID, AcquiredAt: acquiredAt,
			RenewedAt: now, ExpiresAt: now.Add(ttl),
		}
		fs.state.MigrationLeases[key] = acquired
		if err := fs.save(); err != nil {
			return err
		}
		fs.setActiveMigrationLeaseLocked(acquired)
		return nil
	})
	if err != nil {
		return MigrationLease{}, err
	}
	return acquired, nil
}

func (fs *FileState) BindRunLease(runID string, lease MigrationLease) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		active, ok := fs.activeMigrationLeaseLocked()
		if !ok || !sameMigrationLease(active, lease) {
			return leaseLost(lease, "binding run to migration lease")
		}
		if err := fs.verifyMigrationLeaseLocked(lease, time.Now().UTC(), "binding run to migration lease"); err != nil {
			return err
		}
		if fs.state.RunID != runID {
			return fmt.Errorf("bind migration lease: run ID mismatch: expected %s, got %s", fs.state.RunID, runID)
		}
		fs.state.LeaseTargetKey = lease.TargetKey
		fs.state.LeaseOwnerToken = lease.OwnerToken
		fs.state.LeaseGeneration = lease.Generation
		lease.RunID = runID
		fs.state.MigrationLeases[lease.TargetKey] = lease
		if err := fs.save(); err != nil {
			return err
		}
		fs.setActiveMigrationLeaseLocked(lease)
		return nil
	})
}

func (fs *FileState) RenewMigrationLease(lease MigrationLease, now time.Time, ttl time.Duration) (MigrationLease, error) {
	if ttl <= 0 {
		return MigrationLease{}, fmt.Errorf("migration lease TTL must be positive")
	}
	now = now.UTC()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var renewed MigrationLease
	err := fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		if err := fs.verifyMigrationLeaseLocked(lease, now, "renew migration lease"); err != nil {
			return err
		}
		renewed = lease
		renewed.RenewedAt = now
		renewed.ExpiresAt = now.Add(ttl)
		fs.state.MigrationLeases[lease.TargetKey] = renewed
		if err := fs.save(); err != nil {
			return err
		}
		fs.setActiveMigrationLeaseLocked(renewed)
		return nil
	})
	if err != nil {
		return MigrationLease{}, err
	}
	return renewed, nil
}

func (fs *FileState) ReleaseMigrationLease(lease MigrationLease) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		current, ok := fs.state.MigrationLeases[lease.TargetKey]
		if !ok || !sameMigrationLease(current, lease) {
			return leaseLost(lease, "release migration lease")
		}
		now := time.Now().UTC()
		current.OwnerToken = ""
		current.RunID = ""
		current.RenewedAt = now
		current.ExpiresAt = now
		fs.state.MigrationLeases[lease.TargetKey] = current
		if err := fs.save(); err != nil {
			return err
		}
		if active, ok := fs.activeMigrationLeaseLocked(); ok && sameMigrationLease(active, lease) {
			fs.lease = nil
		}
		return nil
	})
}

func (fs *FileState) verifyMigrationLeaseLocked(lease MigrationLease, now time.Time, operation string) error {
	current, ok := fs.state.MigrationLeases[lease.TargetKey]
	if !ok || !sameMigrationLease(current, lease) || !current.ExpiresAt.After(now.UTC()) {
		return leaseLost(lease, operation)
	}
	return nil
}

func (fs *FileState) withRunLeaseMutation(operation string, mutate func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.withProcessLock(func() error {
		if err := fs.reloadLocked(); err != nil {
			return err
		}
		if err := fs.verifyBoundRunLeaseLocked(operation); err != nil {
			return err
		}
		if err := mutate(); err != nil {
			return err
		}
		return fs.save()
	})
}

func (fs *FileState) verifyBoundRunLeaseLocked(operation string) error {
	if fs.state.LeaseGeneration == 0 {
		return nil
	}
	active, ok := fs.activeMigrationLeaseLocked()
	if !ok || active.TargetKey != fs.state.LeaseTargetKey ||
		active.OwnerToken != fs.state.LeaseOwnerToken || active.Generation != fs.state.LeaseGeneration {
		lease := MigrationLease{
			TargetKey: fs.state.LeaseTargetKey, OwnerToken: fs.state.LeaseOwnerToken,
			Generation: fs.state.LeaseGeneration,
		}
		if ok {
			lease = active
		}
		return leaseLost(lease, operation)
	}
	return fs.verifyMigrationLeaseLocked(active, time.Now().UTC(), operation)
}

var _ MigrationLeaseBackend = (*FileState)(nil)
