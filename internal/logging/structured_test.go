package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestBaseAttrs_JSON pins the structured-attrs decoration of JSON output
// (#229). The orchestrator sets run_id/phase via SetBaseAttr and every
// subsequent log line must carry those fields so external aggregators
// (Datadog, Splunk, Loki) can filter by run.
func TestBaseAttrs_JSON(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("json")
	ClearBaseAttrs()
	defer func() {
		SetFormat("text")
		SetOutput(nil)
		ClearBaseAttrs()
	}()

	SetBaseAttr("run_id", "abc12345")
	SetBaseAttr("phase", "transfer")

	Info("chunk completed")

	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, buf.String())
	}
	if entry["run_id"] != "abc12345" {
		t.Errorf("run_id missing: %+v", entry)
	}
	if entry["phase"] != "transfer" {
		t.Errorf("phase missing: %+v", entry)
	}
	if entry["msg"] != "chunk completed" {
		t.Errorf("msg unexpected: %+v", entry)
	}
}

// TestEvent_StructuredFields pins the per-call structured pairs added in
// #229. Event() takes alternating key/value pairs and merges them with
// the base attrs in JSON output.
func TestEvent_StructuredFields(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("json")
	ClearBaseAttrs()
	defer func() {
		SetFormat("text")
		SetOutput(nil)
		ClearBaseAttrs()
	}()

	SetBaseAttr("run_id", "abc12345")
	InfoEvent("chunk completed", "table", "Users", "rows", 1000, "bytes", 4096)

	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, buf.String())
	}
	if entry["run_id"] != "abc12345" {
		t.Errorf("run_id missing: %+v", entry)
	}
	if entry["table"] != "Users" {
		t.Errorf("table = %v, want Users", entry["table"])
	}
	// JSON numbers come back as float64; both 1000 and 4096 must survive
	// the round-trip without becoming strings.
	if rows, ok := entry["rows"].(float64); !ok || rows != 1000 {
		t.Errorf("rows = %v (%T), want 1000", entry["rows"], entry["rows"])
	}
	if bb, ok := entry["bytes"].(float64); !ok || bb != 4096 {
		t.Errorf("bytes = %v (%T), want 4096", entry["bytes"], entry["bytes"])
	}
}

// TestEvent_DanglingKey covers the misuse case: odd number of args means
// the caller wrote `Event("msg", "key1", val1, "key2")`. We don't panic;
// the dangling arg lands under "_dangling" so it's still visible in the
// log without breaking the call site (#229).
func TestEvent_DanglingKey(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("json")
	ClearBaseAttrs()
	defer func() {
		SetFormat("text")
		SetOutput(nil)
		ClearBaseAttrs()
	}()

	InfoEvent("oops", "k1", "v1", "k2")

	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, buf.String())
	}
	if entry["k1"] != "v1" {
		t.Errorf("paired key missing: %+v", entry)
	}
	if entry["_dangling"] != "k2" {
		t.Errorf("_dangling = %v, want \"k2\"", entry["_dangling"])
	}
}

// TestBaseAttrs_ReservedCollision protects ts/level/msg from being
// accidentally overwritten by an attr with the same name. The collision
// lands under "attrs.<key>" so the original reserved field stays
// authoritative for log aggregators that index on those slots (#229).
func TestBaseAttrs_ReservedCollision(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("json")
	ClearBaseAttrs()
	defer func() {
		SetFormat("text")
		SetOutput(nil)
		ClearBaseAttrs()
	}()

	SetBaseAttr("level", "fancy")
	SetBaseAttr("msg", "fancy")
	Info("real msg")

	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, buf.String())
	}
	if entry["level"] != "info" {
		t.Errorf("reserved level overwritten: %+v", entry)
	}
	if entry["msg"] != "real msg" {
		t.Errorf("reserved msg overwritten: %+v", entry)
	}
	if entry["attrs.level"] != "fancy" {
		t.Errorf("collided level not preserved under attrs.level: %+v", entry)
	}
	if entry["attrs.msg"] != "fancy" {
		t.Errorf("collided msg not preserved under attrs.msg: %+v", entry)
	}
}

// TestBaseAttrs_Text appends sorted `key=value` pairs after the message
// in text mode. Order matters for log diffs and golden-file tests, so we
// pin alphabetical ordering on the keys.
func TestBaseAttrs_Text(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("text")
	ClearBaseAttrs()
	defer func() {
		SetOutput(nil)
		ClearBaseAttrs()
	}()

	SetBaseAttr("phase", "transfer")
	SetBaseAttr("run_id", "abc12345")

	Info("chunk completed")

	out := buf.String()
	if !strings.Contains(out, "chunk completed phase=transfer run_id=abc12345") {
		t.Errorf("expected sorted attrs after message, got: %q", out)
	}
}

// TestWithFields_Helper covers the batch setter. nil values delete keys;
// non-nil values set them. Caller can use a single map to update many
// attrs without the SetBaseAttr serial dance.
func TestWithFields_Helper(t *testing.T) {
	ClearBaseAttrs()
	defer ClearBaseAttrs()

	WithFields(map[string]any{
		"run_id":      "abc",
		"phase":       "preflight",
		"source_db":   "mssql",
		"will_delete": "soon",
	})
	WithFields(map[string]any{
		"will_delete": nil,
		"phase":       "transfer", // overwrite
	})

	got := BaseAttrs()
	if got["run_id"] != "abc" {
		t.Errorf("run_id missing/wrong: %+v", got)
	}
	if got["phase"] != "transfer" {
		t.Errorf("phase not overwritten: %+v", got)
	}
	if _, ok := got["will_delete"]; ok {
		t.Errorf("will_delete should be removed: %+v", got)
	}
	if got["source_db"] != "mssql" {
		t.Errorf("source_db missing: %+v", got)
	}
}
