package audit

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNew_OpensAppendOnly verifies the basic happy path: New creates the
// audit directory, opens the file in append mode, and Path returns
// the on-disk location an operator can ship.
func TestNew_OpensAppendOnly(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Dir: dir, RunID: "abc123"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	want := filepath.Join(dir, "abc123.ndjson")
	if l.Path() != want {
		t.Errorf("Path() = %q, want %q", l.Path(), want)
	}
	if _, err := os.Stat(want); err != nil {
		t.Errorf("expected file %q to exist: %v", want, err)
	}
}

// TestRecordEvent_WritesNDJSON pins the on-disk shape — one JSON
// object per line, parseable, with the required fields populated.
func TestRecordEvent_WritesNDJSON(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "abc"})

	if err := l.RecordEvent(Event{
		Type:   "run_start",
		Fields: map[string]any{"operator": "alice@host"},
	}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	if err := l.RecordEvent(Event{Type: "run_complete"}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	l.Close()

	body, _ := os.ReadFile(l.Path())
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 0 not parseable: %v", err)
	}
	if first["type"] != "run_start" {
		t.Errorf("first event type = %v, want run_start", first["type"])
	}
	if first["run_id"] != "abc" {
		t.Errorf("first event run_id = %v, want abc", first["run_id"])
	}
	if first["operator"] != "alice@host" {
		t.Errorf("first event operator = %v, want alice@host", first["operator"])
	}
	if _, ok := first["ts"]; !ok {
		t.Errorf("first event missing ts")
	}
}

// TestRecordEvent_ScrubbingApplied pins the dependency on logging.Scrub.
// Connection strings (and other secret-looking fields) must never reach
// the audit file in plain form — credentials in audit logs are a
// compliance regression.
func TestRecordEvent_ScrubbingApplied(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "scrub"})
	if err := l.RecordEvent(Event{
		Type: "config_resolved",
		Fields: map[string]any{
			"dsn":      "postgres://alice:hunter2@db.example.com/orders",
			"api_key":  "sk-ant-AAAABBBBCCCCDDDDEEEEFFFF",
			"endpoint": "https://otel.example.com", // not a secret — should pass through
		},
	}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	l.Close()

	body, _ := os.ReadFile(l.Path())
	out := string(body)
	if strings.Contains(out, "hunter2") {
		t.Errorf("DSN password leaked into audit log: %s", out)
	}
	if strings.Contains(out, "AAAABBBBCCCCDDDDEEEEFFFF") {
		t.Errorf("API key leaked into audit log: %s", out)
	}
	if !strings.Contains(out, "https://otel.example.com") {
		t.Errorf("non-secret endpoint mistakenly scrubbed: %s", out)
	}
}

// TestClose_ChmodReadOnly pins the immutability guarantee: after Close,
// the file is 0444 so even the dmt process can't reopen-and-truncate.
// (chmod is best-effort on filesystems that don't support it; we don't
// fail the test if the result perm isn't 0444 — but we DO verify it's
// not writable, which is what compliance actually cares about.)
func TestClose_ChmodReadOnly(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "ro"})
	l.RecordEvent(Event{Type: "run_start"})
	l.Close()

	st, err := os.Stat(l.Path())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	mode := st.Mode().Perm()
	if mode&0o200 != 0 {
		t.Errorf("file remains writable after Close: mode=%v", mode)
	}
}

// TestClose_Idempotent ensures repeat Close() calls don't error or
// re-chmod (the latter could fail if the file is already 0444 and the
// FS rejects no-op chmod, which some don't).
func TestClose_Idempotent(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "idem"})
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestDisabled_NoOp pins the contract that the Disabled() sentinel is
// safe to use unconditionally: every method is a no-op, no file is
// created, Path() returns empty.
func TestDisabled_NoOp(t *testing.T) {
	l := Disabled()
	if l.Path() != "" {
		t.Errorf("disabled Path() = %q, want \"\"", l.Path())
	}
	if err := l.RecordEvent(Event{Type: "anything"}); err != nil {
		t.Errorf("disabled RecordEvent = %v, want nil", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("disabled Close = %v, want nil", err)
	}
}

// TestTamperEvident_HashChain verifies the chained-hash invariant for
// compliance scenarios. Manual replay of sha256(prev_hash || event_body)
// must equal the on-disk hash; an attacker who modifies any past event
// invalidates everything downstream.
func TestTamperEvident_HashChain(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "chain", TamperEvident: true})

	l.RecordEvent(Event{Type: "run_start", Fields: map[string]any{"k": "v1"}})
	l.RecordEvent(Event{Type: "table_started", Fields: map[string]any{"table": "Users"}})
	l.RecordEvent(Event{Type: "run_complete", Fields: map[string]any{"status": "success"}})
	l.Close()

	f, _ := os.Open(l.Path())
	defer f.Close()
	scanner := bufio.NewScanner(f)

	prevHash := "GENESIS"
	var seq int64 = 0
	for scanner.Scan() {
		seq++
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("line %d not parseable: %v", seq, err)
		}
		// Verify required fields.
		if got, _ := ev["seq"].(float64); int64(got) != seq {
			t.Errorf("line %d seq = %v, want %d", seq, ev["seq"], seq)
		}
		if ev["prev_hash"] != prevHash {
			t.Errorf("line %d prev_hash = %v, want %v", seq, ev["prev_hash"], prevHash)
		}
		gotHash, ok := ev["hash"].(string)
		if !ok || gotHash == "" {
			t.Fatalf("line %d missing hash", seq)
		}
		// Manually replay the hash. Strip the on-disk hash, canonicalize,
		// and recompute.
		hashOnDisk := gotHash
		delete(ev, "hash")
		body, err := canonicalJSON(ev)
		if err != nil {
			t.Fatalf("canonicalize line %d: %v", seq, err)
		}
		sum := sha256.New()
		io.WriteString(sum, prevHash)
		sum.Write(body)
		expected := "sha256:" + hex.EncodeToString(sum.Sum(nil))
		if expected != hashOnDisk {
			t.Errorf("line %d hash mismatch:\n  disk:     %s\n  recomputed: %s\n  body: %s",
				seq, hashOnDisk, expected, body)
		}
		prevHash = hashOnDisk
	}
	if seq != 3 {
		t.Errorf("expected 3 events, got %d", seq)
	}
}

// TestCloseResumable_KeepsFileWritable pins the codex-review fix:
// Ctrl-C / context cancellation during a transfer must leave the
// audit file writable so the eventual `dmt resume` can reopen it.
// Close() chmod-locks to 0444 (good for terminal closes); CloseResumable
// skips the chmod (good for interrupted runs).
func TestCloseResumable_KeepsFileWritable(t *testing.T) {
	dir := t.TempDir()
	l, _ := New(Options{Dir: dir, RunID: "resumable"})
	l.RecordEvent(Event{Type: "run_start"})
	if err := l.CloseResumable(); err != nil {
		t.Fatalf("CloseResumable: %v", err)
	}
	st, err := os.Stat(l.Path())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Mode().Perm()&0o200 == 0 {
		t.Errorf("CloseResumable should leave file writable; mode=%v", st.Mode())
	}
}

// TestResumeChain_ContinuesAcrossReopen pins the codex-review fix: in
// tamper-evident mode, opening an existing audit file for append must
// pick up the chain from where the prior run left off. Without this,
// the verifier breaks across a Run + Resume even when no tampering
// happened.
func TestResumeChain_ContinuesAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// First "run": write 2 events, close resumable (operator hit Ctrl-C).
	first, _ := New(Options{Dir: dir, RunID: "chain-resume", TamperEvident: true})
	first.RecordEvent(Event{Type: "run_start"})
	first.RecordEvent(Event{Type: "preflight"})
	firstSeq := first.seq
	firstHash := first.prevHash
	if err := first.CloseResumable(); err != nil {
		t.Fatalf("first CloseResumable: %v", err)
	}
	if firstSeq != 2 {
		t.Fatalf("expected seq=2 before reopen, got %d", firstSeq)
	}

	// "Resume": reopen the same file. Logger MUST continue the chain.
	second, err := New(Options{Dir: dir, RunID: "chain-resume", TamperEvident: true})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	if second.seq != firstSeq {
		t.Errorf("resumed seq = %d, want %d (continue from prior close)", second.seq, firstSeq)
	}
	if second.prevHash != firstHash {
		t.Errorf("resumed prevHash = %q, want %q", second.prevHash, firstHash)
	}
	second.RecordEvent(Event{Type: "resume_start"})
	second.Close()

	// Verify the combined file's chain replays cleanly.
	f, _ := os.Open(second.Path())
	defer f.Close()
	scanner := bufio.NewScanner(f)
	prevHash := "GENESIS"
	var seenSeqs []int64
	for scanner.Scan() {
		var ev map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &ev)
		seq := int64(ev["seq"].(float64))
		seenSeqs = append(seenSeqs, seq)
		if ev["prev_hash"] != prevHash {
			t.Errorf("seq %d prev_hash = %v, want %v", seq, ev["prev_hash"], prevHash)
		}
		prevHash = ev["hash"].(string)
	}
	// Expect seq 1,2,3 — strictly monotonic across the reopen.
	if len(seenSeqs) != 3 || seenSeqs[0] != 1 || seenSeqs[1] != 2 || seenSeqs[2] != 3 {
		t.Errorf("seqs across reopen = %v, want [1 2 3]", seenSeqs)
	}
}

// TestScrubFields_NestedMap pins the recursive scrubbing behavior. A
// config_resolved event commonly has nested {source: {password: ...}}
// — the audit log mustn't leak the password just because it's one
// level deep.
func TestScrubFields_NestedMap(t *testing.T) {
	in := map[string]any{
		"source": map[string]any{
			"host":     "db.example.com",
			"password": "hunter2",
		},
		"target": map[string]any{
			"api_key": "sk-ant-AAAABBBBCCCCDDDDEEEEFFFF",
		},
	}
	out := scrubFields(in)
	if got, _ := out["source"].(map[string]any)["password"]; got == "hunter2" {
		t.Errorf("nested password not scrubbed: %+v", out)
	}
	if got, _ := out["target"].(map[string]any)["api_key"]; strings.Contains(got.(string), "AAAABBBBCCCCDDDDEEEEFFFF") {
		t.Errorf("nested api_key not scrubbed: %+v", out)
	}
}
