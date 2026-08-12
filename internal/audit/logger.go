package audit

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

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
	// O_APPEND so every write(2) lands at a unique end-of-file offset
	// regardless of concurrent writers (POSIX append semantics for
	// regular files — the kernel serializes the offset adjustment with
	// the write itself). O_EXCL would be stronger but breaks legitimate
	// resume scenarios where the same run_id is being recorded after an
	// earlier crash.
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
	// If we're appending to an existing file in tamper-evident mode,
	// pick up the chain from where it left off (Codex review on #235).
	// Otherwise a resume would restart seq=1/prev_hash=GENESIS and break
	// the documented verifier across the combined run+resume file.
	if opts.TamperEvident {
		if err := l.resumeChain(path); err != nil {
			// Close the file we just opened so we don't leak the fd.
			_ = f.Close()
			return nil, fmt.Errorf("audit: resume hash chain in %q: %w", path, err)
		}
	}
	return l, nil
}

// resumeChain scans the existing audit file for the last event's
// seq+hash so a resumed Logger continues the chain instead of restarting
// at seq=1/prev=GENESIS. No-op when the file is empty (fresh run) or
// not in tamper-evident mode (caller gates this).
func (l *Logger) resumeChain(path string) error {
	r, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // fresh file
		}
		return err
	}
	defer r.Close()
	var lastSeq int64
	lastHash := "GENESIS"
	scanner := bufio.NewScanner(r)
	// Audit lines can be large (config_resolved with the full sanitized
	// config); bump the buffer ceiling so the scanner doesn't refuse
	// long lines.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			return fmt.Errorf("audit: previous event not parseable: %w", err)
		}
		if s, ok := ev["seq"].(float64); ok {
			lastSeq = int64(s)
		}
		if h, ok := ev["hash"].(string); ok && h != "" {
			lastHash = h
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	l.seq = lastSeq
	l.prevHash = lastHash
	return nil
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
	// In tamper-evident mode, fsync after each event so the hash chain
	// is durable on disk before the next event extends it. Without
	// this, a crash between Write and the OS's eventual fsync could
	// leave the chain torn — the in-memory prevHash for event N+1
	// would reference an event that never reached the platter
	// (Copilot review on #235). For plain mode the OS page cache is
	// fine; audit is best-effort there and the perf cost of per-event
	// fsync isn't worth the marginally-stronger guarantee.
	if l.tamperEvident {
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("audit: fsync event: %w", err)
		}
	}
	return nil
}

// Close flushes any buffered data, closes the file, and chmod-s it to
// 0444 (read-only). Use this for terminal closes — successful run,
// hard-failed run, panic. For interrupted runs that the operator will
// resume, use CloseResumable instead so the file stays writable.
//
// Idempotent — repeat calls return nil. Close errors are returned but
// the file is always closed; callers can rely on the file being safe
// to ship after Close, even on error.
func (l *Logger) Close() error {
	return l.closeImpl(true /*chmodReadOnly*/)
}

// CloseResumable closes the file but DOES NOT chmod it to 0444, so a
// subsequent `dmt resume` can reopen the same file in O_APPEND mode
// and continue the audit log (Codex review on #235). Without this,
// Ctrl-C during a transfer would lock the audit file out of the
// resume that the user explicitly asked for.
func (l *Logger) CloseResumable() error {
	return l.closeImpl(false /*chmodReadOnly*/)
}

func (l *Logger) closeImpl(chmodReadOnly bool) error {
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
	if chmodReadOnly {
		// Chmod is best-effort; some filesystems (e.g. mounted volumes
		// without execute bits) reject 0444 and we don't want that to
		// fail the run.
		if chmodErr := os.Chmod(l.path, 0o444); chmodErr != nil {
			// Log instead of return — the audit data is on disk; the
			// chmod is enforcement-by-filesystem and degrades gracefully
			// on systems that can't honor it.
			logging.Warn("audit: chmod %q to 0444 failed: %v", l.path, chmodErr)
		}
	}
	return closeErr
}
