package driver

import (
	"context"
	"database/sql"
)

// PreFlightSide identifies whether a preflight check is running against the
// source or target database. Some checks (free disk space, write privileges)
// only apply to one side.
type PreFlightSide string

const (
	PreFlightSideSource PreFlightSide = "source"
	PreFlightSideTarget PreFlightSide = "target"
)

// PreFlightSeverity classifies a finding's impact. The orchestrator treats
// SeverityError as a run-aborting condition (unless the operator opts the
// check out via --skip-preflight); SeverityWarn surfaces in logs but does
// not block; SeverityInfo is for observability only.
type PreFlightSeverity string

const (
	SeverityError PreFlightSeverity = "error"
	SeverityWarn  PreFlightSeverity = "warn"
	SeverityInfo  PreFlightSeverity = "info"
)

// PreFlightRequest carries everything a driver needs to run its preflight
// checks. Mirrors the configuration knobs that downstream phases depend on
// so the check can verify the actual setup the migration will use, not a
// generic "is the database alive" probe.
type PreFlightRequest struct {
	Side PreFlightSide

	// Schema is the source-side schema (when Side == source) or target-side
	// schema (when Side == target) the migration will read from / write to.
	// Privilege probes scope themselves to this schema.
	Schema string

	// TargetMode is the orchestrator's target_mode setting ("drop_recreate"
	// or "upsert"). Only meaningful when Side == target. drop_recreate
	// requires CREATE/DROP TABLE; upsert requires SELECT/INSERT/UPDATE.
	TargetMode string

	// Workers is the configured worker count. Pool-headroom checks compare
	// (max_connections - current_connections) against Workers + N margin.
	Workers int

	// StrictConsistency and ParallelReaders describe the source read shape.
	// MySQL uses them to probe LOCK TABLES only when the migration will
	// actually request more than one strict reader.
	StrictConsistency      bool
	StrictConsistencyScope string
	ParallelReaders        int
	Database               string

	// IncludeTables and ExcludeTables carry the migration's table filters to
	// source-side checks that must probe a real migrated table. Patterns use
	// the same filepath.Match semantics as orchestrator table filtering.
	IncludeTables []string
	ExcludeTables []string

	// EstimatedBytes is the estimated size of all data the migration will
	// transfer (sum of source tables' row_count * estimated_row_size).
	// Disk-space checks compare this × overhead factor against target free
	// space. Zero means "skip the disk check" (caller didn't compute it).
	EstimatedBytes int64

	// ConfirmBackup acknowledges that the operator understands drop_recreate
	// will destroy existing target data. Only meaningful when
	// Side == target AND TargetMode == "drop_recreate". When false, the
	// target-side check emits a SeverityError finding if any table in the
	// target schema is non-empty (#228 backup-acknowledgment).
	ConfirmBackup bool

	// ResumeOwnsTarget is true when preflight runs for a `dmt resume` whose run
	// already reached the transfer phase against a matching config. In that case
	// the target's tables were (re)created by this very run, so its non-empty
	// tables are the run's own output and the backup-acknowledgment gate must
	// not fire: resuming is the acknowledgment, and `resume` has no
	// --confirm-backup flag to satisfy it (#623).
	//
	// The orchestrator sets it deliberately narrowly (see runReachedTransfer and
	// the config-drift guard in Resume): a run killed before it created any
	// target tables — or a --force-resume against a drifted config whose table
	// set may not match the target — has NOT acknowledged the current target
	// contents, so its resume still faces the gate rather than silently
	// drop_recreate-ing over pre-existing data. Only meaningful when
	// Side == target.
	ResumeOwnsTarget bool
}

// PreFlightFinding is one observation from a preflight check — either OK
// (typically not emitted; absence of a finding is OK) or a warn/error with
// an actionable remedy. The Check field uses dotted names like
// "privileges.create_table" so operators can map skip flags and metrics
// to specific checks without parsing free-form messages.
type PreFlightFinding struct {
	Severity PreFlightSeverity `json:"severity"`
	Check    string            `json:"check"` // dotted check name, e.g. "privileges.create_table"
	Side     PreFlightSide     `json:"side"`
	Message  string            `json:"message"` // human-readable: what's wrong
	Remedy   string            `json:"remedy"`  // human-readable: what to do about it
}

// PreFlightChecker is the driver-side hook for preflight. Each driver
// implements its own set of checks since the SQL for "what's my version"
// or "do I have CREATE TABLE" is dialect-specific. Drivers that need no
// preflight (or aren't yet implemented) may return nil/empty.
//
// Implementations should:
//   - Never panic on a misconfigured DB; emit a SeverityError finding instead.
//   - Use the supplied db handle (don't open new connections).
//   - Honor ctx for cancellation/timeout.
//   - Return findings in stable order so test output is deterministic.
type PreFlightChecker interface {
	PreFlight(ctx context.Context, db *sql.DB, req PreFlightRequest) []PreFlightFinding
}
