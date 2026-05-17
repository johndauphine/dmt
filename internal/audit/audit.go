// Package audit writes an append-only NDJSON record of every dmt run
// for compliance auditing (#235).
//
// The audit log is structurally distinct from the SQLite checkpoint
// (`internal/checkpoint`):
//
//   - The checkpoint is *mutable working state* — rows are updated as a
//     run progresses, the database file is rewritten by every WAL
//     checkpoint, and the contract is "good enough to resume from."
//   - The audit log is an *immutable record of what happened* — once
//     a line is written it never moves; after the run ends the file
//     becomes 0444 (read-only); the only inputs are the events the
//     orchestrator emits.
//
// Each event is one NDJSON line. The file lives at
// `<audit_dir>/<run_id>.ndjson` (default `~/.dmt/audit`); the operator
// can ship the file to long-term storage with no further processing
// — it's already canonical, single-file, and tamper-evident-ready.
//
// Hash chaining is opt-in via TamperEvident at Logger construction.
// When enabled, every event carries `seq`, `prev_hash`, and `hash`
// fields; the operator can replay the chain via `sha256` to detect
// any retroactive modification. The hash includes a canonical JSON
// rendering of the event minus the hash itself, so the verification
// is deterministic.
//
// Sensitive values flow through `logging.Scrub` before write. Row
// content is never recorded by design — the audit log captures
// *operations*, not data.
package audit

import (
	"os"
	"sync"
)

// Event is the on-disk shape of one audit-log line. Required fields
// (Timestamp, Type, RunID) are filled by Logger.RecordEvent if the
// caller leaves them zero, so call sites stay terse:
//
//	auditor.RecordEvent(audit.Event{Type: "table_started", Fields: map[string]any{"table": t.Name}})
//
// Optional fields:
//   - Operator/DMTVersion/Source/Target/ConfigHash appear once on
//     `run_start` and are denormalized into every event by the
//     orchestrator's call-site decoration when needed.
//   - Seq/PrevHash/Hash appear ONLY when TamperEvident is enabled.
//     Setting them on a non-TamperEvident Logger is a no-op (Logger
//     does the calculation itself; caller-supplied values are
//     overwritten).
type Event struct {
	// Timestamp in RFC 3339 UTC. Filled by RecordEvent when zero.
	Timestamp string `json:"ts,omitempty"`

	// Type is the event kind: run_start, run_complete, table_started,
	// table_completed, validation_complete, error, retry, etc.
	// See docs/AUDIT-LOG.md for the canonical event-type list.
	Type string `json:"type"`

	// RunID is filled by RecordEvent from the Logger's run_id.
	RunID string `json:"run_id,omitempty"`

	// Fields holds the event's structured payload. Keys with secret-
	// looking names (password, api_key, etc.) flow through
	// logging.Scrub before serialization.
	Fields map[string]any `json:"-"`

	// Seq/PrevHash/Hash are written only when TamperEvident is on.
	Seq      int64  `json:"seq,omitempty"`
	PrevHash string `json:"prev_hash,omitempty"`
	Hash     string `json:"hash,omitempty"`
}

// Logger is the per-run audit-log writer. Concurrency-safe: the
// transfer pipeline can call RecordEvent from any goroutine.
type Logger struct {
	mu            sync.Mutex
	file          *os.File
	path          string
	runID         string
	tamperEvident bool
	seq           int64
	prevHash      string
	closed        bool
}

// Options configures a Logger at construction.
type Options struct {
	// Dir is the audit directory. If empty, defaults to
	// $HOME/.dmt/audit. Created with 0700 perms if missing.
	Dir string

	// RunID is the run identifier used both as the filename
	// (<run_id>.ndjson) and as the `run_id` field on every event.
	RunID string

	// TamperEvident enables hash-chained events. Off by default.
	TamperEvident bool
}

// Disabled returns the no-op sentinel used when the operator passed
// --no-audit (or the orchestrator is in a mode that can't safely
// audit, e.g. a dry-run). Every method on the Disabled logger is a
// no-op; callers don't need nil checks.
func Disabled() *Logger { return disabledLogger }

var disabledLogger = &Logger{closed: true}
