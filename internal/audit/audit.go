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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
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
	mu             sync.Mutex
	file           *os.File
	path           string
	runID          string
	tamperEvident  bool
	seq            int64
	prevHash       string
	closed         bool
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

// New constructs a Logger for the given run and opens the audit file
// in append-only mode. The file is created with 0600 perms (only the
// operator user can read it during the run) and chmod-ed to 0444 by
// Close so the immutability is filesystem-enforced once the run ends.
func New(opts Options) (*Logger, error) {
	if opts.RunID == "" {
		return nil, errors.New("audit: RunID is required")
	}
	dir, err := resolveDir(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("audit: create dir %q: %w", dir, err)
	}
	path := filepath.Join(dir, opts.RunID+".ndjson")
	// O_APPEND so every Write atomically lands at end-of-file (POSIX
	// guarantee for opens < PIPE_BUF; our JSON lines are well under).
	// O_EXCL would be stronger but breaks legitimate resume scenarios
	// where the same run_id is being recorded after an earlier crash.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("audit: open %q: %w", path, err)
	}
	l := &Logger{
		file:          f,
		path:          path,
		runID:         opts.RunID,
		tamperEvident: opts.TamperEvident,
		prevHash:      "GENESIS",
	}
	return l, nil
}

// Path returns the on-disk path of the audit log. Empty for the
// disabled logger.
func (l *Logger) Path() string {
	if l == nil || l == disabledLogger {
		return ""
	}
	return l.path
}

// RecordEvent appends one event. Safe for concurrent callers; serializes
// internally so the line ordering matches the event-emission ordering.
// Returns an error only if the underlying write or scrubbing fails;
// callers typically log and continue rather than aborting the run on an
// audit-log failure (compliance benefits less from a crashed migration
// than from a partial audit log).
func (l *Logger) RecordEvent(ev Event) error {
	if l == nil || l == disabledLogger {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("audit: logger is closed")
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if ev.RunID == "" {
		ev.RunID = l.runID
	}
	scrubbed := scrubFields(ev.Fields)

	if l.tamperEvident {
		l.seq++
		ev.Seq = l.seq
		ev.PrevHash = l.prevHash
		// Hash is computed over the canonical encoding of the event
		// MINUS the hash field itself, so the verifier can recompute
		// it without circular dependency.
		h, err := computeHash(ev, scrubbed)
		if err != nil {
			return fmt.Errorf("audit: hash event: %w", err)
		}
		ev.Hash = h
		l.prevHash = h
	}

	line, err := marshalLine(ev, scrubbed)
	if err != nil {
		return fmt.Errorf("audit: marshal event: %w", err)
	}
	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("audit: write event: %w", err)
	}
	return nil
}

// Close flushes any buffered data, closes the file, and chmod-s it to
// 0444 (read-only). Idempotent — repeat calls return nil. Close errors
// are returned but the file is always closed; callers can rely on the
// file being safe to ship after Close, even on error.
func (l *Logger) Close() error {
	if l == nil || l == disabledLogger {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	closeErr := l.file.Close()
	// Chmod is best-effort; some filesystems (e.g. mounted volumes
	// without execute bits) reject 0444 and we don't want that to
	// fail the run.
	chmodErr := os.Chmod(l.path, 0o444)
	if closeErr != nil {
		return closeErr
	}
	if chmodErr != nil {
		// Log instead of return — the audit data is on disk; the
		// chmod is enforcement-by-filesystem and degrades gracefully
		// on systems that can't honor it.
		logging.Warn("audit: chmod %q to 0444 failed: %v", l.path, chmodErr)
	}
	return nil
}

// resolveDir returns the absolute audit directory, defaulting to
// $HOME/.dmt/audit when dir is empty. Tilde expansion is handled
// because operators pass --audit-dir=~/audit and the shell may or
// may not expand it depending on how dmt was invoked.
func resolveDir(dir string) (string, error) {
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("audit: resolve $HOME: %w", err)
		}
		return filepath.Join(home, ".dmt", "audit"), nil
	}
	if len(dir) >= 2 && dir[:2] == "~/" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("audit: expand tilde: %w", err)
		}
		return filepath.Join(home, dir[2:]), nil
	}
	return dir, nil
}

// secretKeyNames is the set of field keys whose values are redacted
// regardless of value content. `logging.Scrub` catches secret patterns
// inside strings (DSNs, Bearer headers, etc.), but a structured event
// like `{source: {password: "hunter2"}}` carries the secret as the
// VALUE of a key named "password" — Scrub's regex doesn't see that
// because there's no `password=` prefix in the string itself. We redact
// by key name as a second line of defense.
//
// Match is case-insensitive on the trimmed key.
var secretKeyNames = map[string]struct{}{
	"password": {}, "passwd": {}, "pwd": {},
	"api_key": {}, "apikey": {}, "api-key": {},
	"secret": {}, "token": {},
	"webhook_url": {}, "webhook-url": {},
	"authorization": {},
}

// scrubFields walks the caller-supplied Fields map and returns a copy
// where:
//   - string values are passed through logging.Scrub (catches DSNs,
//     Bearer tokens, sk- API keys, Slack webhook URLs in free text)
//   - values whose KEY name matches secretKeyNames are replaced with
//     the redacted token regardless of value content (catches the
//     structured `{source: {password: "..."}}` case)
//   - nested maps recurse so secrets at any depth are caught
//
// Keys themselves are left intact — only values get redacted. Scrubbing
// keys would mangle the event schema and make audit replay harder.
func scrubFields(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if _, secret := secretKeyNames[strings.ToLower(strings.TrimSpace(k))]; secret {
			out[k] = logging.RedactedToken
			continue
		}
		switch typed := v.(type) {
		case string:
			out[k] = logging.Scrub(typed)
		case map[string]any:
			out[k] = scrubFields(typed)
		default:
			out[k] = v
		}
	}
	return out
}

// marshalLine encodes the event + its (already-scrubbed) fields as
// one canonical JSON object. Field keys are sorted for stable output
// (matters for hash chaining and golden-file tests).
func marshalLine(ev Event, scrubbed map[string]any) ([]byte, error) {
	// Build a single map combining the typed Event fields with the
	// arbitrary Fields. The typed fields win on key collision (so a
	// caller that puts "type" in Fields can't override the event's
	// own Type field).
	flat := make(map[string]any, len(scrubbed)+8)
	for k, v := range scrubbed {
		flat[k] = v
	}
	flat["ts"] = ev.Timestamp
	flat["type"] = ev.Type
	flat["run_id"] = ev.RunID
	if ev.Seq > 0 {
		flat["seq"] = ev.Seq
		flat["prev_hash"] = ev.PrevHash
		flat["hash"] = ev.Hash
	}
	return canonicalJSON(flat)
}

// canonicalJSON encodes a map with sorted keys and no extraneous
// whitespace. Required for hash chaining to be reproducible.
func canonicalJSON(m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf []byte
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf = append(buf, kb...)
		buf = append(buf, ':')
		vb, err := canonicalValue(m[k])
		if err != nil {
			return nil, err
		}
		buf = append(buf, vb...)
	}
	buf = append(buf, '}')
	return buf, nil
}

// canonicalValue recursively encodes a value. Maps go through
// canonicalJSON so nested objects also have sorted keys; everything
// else uses encoding/json, which is already deterministic for the
// primitive types we use.
func canonicalValue(v any) ([]byte, error) {
	switch typed := v.(type) {
	case map[string]any:
		return canonicalJSON(typed)
	case []any:
		var buf []byte
		buf = append(buf, '[')
		for i, item := range typed {
			if i > 0 {
				buf = append(buf, ',')
			}
			b, err := canonicalValue(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, b...)
		}
		buf = append(buf, ']')
		return buf, nil
	default:
		return json.Marshal(v)
	}
}

// computeHash returns sha256(prev_hash || canonicalJSON(event without
// hash)). The verifier replays this calculation across the file to
// detect retroactive edits.
func computeHash(ev Event, scrubbed map[string]any) (string, error) {
	// Construct the flat event WITHOUT the hash field — the verifier
	// must be able to compute the same hash by stripping the on-disk
	// hash and re-running this function.
	flat := make(map[string]any, len(scrubbed)+7)
	for k, v := range scrubbed {
		flat[k] = v
	}
	flat["ts"] = ev.Timestamp
	flat["type"] = ev.Type
	flat["run_id"] = ev.RunID
	flat["seq"] = ev.Seq
	flat["prev_hash"] = ev.PrevHash

	body, err := canonicalJSON(flat)
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	io.WriteString(sum, ev.PrevHash)
	sum.Write(body)
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}
