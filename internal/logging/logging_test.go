package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestSetFormat_JSON(t *testing.T) {
	// Capture output
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("json")
	defer func() {
		SetFormat("text")
		SetOutput(nil)
	}()

	// Log a message
	Info("test message")

	// Parse the output
	output := buf.String()
	if output == "" {
		t.Fatal("expected output")
	}

	// Should be valid JSON
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &logEntry); err != nil {
		t.Fatalf("invalid JSON output: %v\nOutput: %s", err, output)
	}

	// Check required fields
	if _, ok := logEntry["ts"]; !ok {
		t.Error("missing 'ts' field in JSON log")
	}
	if level, ok := logEntry["level"]; !ok || level != "info" {
		t.Errorf("expected level='info', got %v", level)
	}
	if msg, ok := logEntry["msg"]; !ok || msg != "test message" {
		t.Errorf("expected msg='test message', got %v", msg)
	}
}

func TestSetFormat_Text(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LevelInfo)
	SetFormat("text")
	defer SetOutput(nil)

	Info("test message")

	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Errorf("expected [INFO] in text output: %s", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("expected 'test message' in output: %s", output)
	}
}

func TestJSONLogLevels(t *testing.T) {
	tests := []struct {
		name    string
		logFunc func(string, ...interface{})
		level   string
	}{
		{"debug", Debug, "debug"},
		{"info", Info, "info"},
		{"warn", Warn, "warn"},
		{"error", Error, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetOutput(&buf)
			SetLevel(LevelDebug) // Enable all levels
			SetFormat("json")
			defer func() {
				SetFormat("text")
				SetOutput(nil)
			}()

			tt.logFunc("test")

			var logEntry map[string]interface{}
			if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &logEntry); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}

			if logEntry["level"] != tt.level {
				t.Errorf("expected level=%s, got %v", tt.level, logEntry["level"])
			}
		})
	}
}
