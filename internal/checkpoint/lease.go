package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/v5/internal/exitcodes"
)

// MigrationTarget identifies the canonical target protected by a migration
// lease. Database and schema retain their exact spelling because quoted
// identifiers can be case-sensitive; driver and host are case-insensitive.
type MigrationTarget struct {
	Driver   string `json:"driver" yaml:"driver"`
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Database string `json:"database" yaml:"database"`
	Schema   string `json:"schema" yaml:"schema"`
}

// Canonical returns the normalized target identity used for lease keys.
func (t MigrationTarget) Canonical() MigrationTarget {
	t.Driver = strings.ToLower(strings.TrimSpace(t.Driver))
	t.Host = strings.ToLower(strings.TrimSpace(t.Host))
	t.Database = strings.TrimSpace(t.Database)
	t.Schema = strings.TrimSpace(t.Schema)
	switch {
	case t.Driver == "sqlite" && t.Schema == "":
		t.Schema = "main"
	case t.Driver == "mysql" && t.Schema == "":
		t.Schema = t.Database
	}
	return t
}

// Key returns a collision-free, stable representation of the target tuple.
func (t MigrationTarget) Key() string {
	b, _ := json.Marshal(t.Canonical())
	return string(b)
}

func (t MigrationTarget) String() string {
	t = t.Canonical()
	endpoint := t.Host
	if t.Port > 0 {
		endpoint = fmt.Sprintf("%s:%d", endpoint, t.Port)
	}
	if endpoint == "" {
		endpoint = "local"
	}
	return fmt.Sprintf("%s://%s/%s/%s", t.Driver, endpoint, t.Database, t.Schema)
}

// MigrationLease is an exclusive ownership grant for one target. Generation
// increases on every acquisition after release or expiry and is the fencing
// token attached to run, task, and progress writes.
type MigrationLease struct {
	Target     MigrationTarget `json:"target" yaml:"target"`
	TargetKey  string          `json:"target_key" yaml:"target_key"`
	OwnerToken string          `json:"owner_token" yaml:"owner_token"`
	Generation int64           `json:"generation" yaml:"generation"`
	RunID      string          `json:"run_id,omitempty" yaml:"run_id,omitempty"`
	AcquiredAt time.Time       `json:"acquired_at" yaml:"acquired_at"`
	RenewedAt  time.Time       `json:"renewed_at" yaml:"renewed_at"`
	ExpiresAt  time.Time       `json:"expires_at" yaml:"expires_at"`
}

// MigrationLeaseBackend is implemented by restartable state backends. An
// acquired lease becomes the backend instance's write credential; BindRunLease
// associates that generation with a new or resumed run.
type MigrationLeaseBackend interface {
	GetLastIncompleteRunForTarget(target MigrationTarget) (*Run, error)
	AcquireMigrationLease(target MigrationTarget, ownerToken string, now time.Time, ttl time.Duration) (MigrationLease, error)
	BindRunLease(runID string, lease MigrationLease) error
	RenewMigrationLease(lease MigrationLease, now time.Time, ttl time.Duration) (MigrationLease, error)
	ReleaseMigrationLease(lease MigrationLease) error
}

// LeaseHeldError reports a live conflicting owner. It is intentionally not
// bypassed by --force-resume; takeover is allowed only after expiry.
type LeaseHeldError struct {
	Target     MigrationTarget
	OwnerToken string
	Generation int64
	RunID      string
	ExpiresAt  time.Time
}

func (e *LeaseHeldError) Error() string {
	run := ""
	if e.RunID != "" {
		run = fmt.Sprintf(" for run %s", e.RunID)
	}
	return fmt.Sprintf("migration target %s is owned%s by lease generation %d until %s",
		e.Target, run, e.Generation, e.ExpiresAt.UTC().Format(time.RFC3339))
}

func (e *LeaseHeldError) ExitCode() int { return exitcodes.StateError }

// LeaseLostError reports that this process's owner token or fencing generation
// no longer matches the durable lease.
type LeaseLostError struct {
	TargetKey  string
	OwnerToken string
	Generation int64
	Operation  string
}

func (e *LeaseLostError) Error() string {
	op := e.Operation
	if op == "" {
		op = "state write"
	}
	return fmt.Sprintf("%s rejected: migration lease generation %d is no longer the active owner", op, e.Generation)
}

func (e *LeaseLostError) ExitCode() int { return exitcodes.StateError }

func IsLeaseHeldError(err error) bool {
	var target *LeaseHeldError
	return errors.As(err, &target)
}

func IsLeaseLostError(err error) bool {
	var target *LeaseLostError
	return errors.As(err, &target)
}

func validateLeaseRequest(target MigrationTarget, ownerToken string, ttl time.Duration) error {
	target = target.Canonical()
	if target.Driver == "" || target.Database == "" || target.Schema == "" {
		return fmt.Errorf("migration lease target requires driver, database, and schema")
	}
	if strings.TrimSpace(ownerToken) == "" {
		return fmt.Errorf("migration lease owner token is required")
	}
	if ttl <= 0 {
		return fmt.Errorf("migration lease TTL must be positive")
	}
	return nil
}
