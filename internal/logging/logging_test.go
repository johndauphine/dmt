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

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
		wantErr  bool
	}{
		// Valid lowercase
		{"debug", LevelDebug, false},
		{"info", LevelInfo, false},
		{"warn", LevelWarn, false},
		{"warning", LevelWarn, false},
		{"error", LevelError, false},

		// Valid uppercase
		{"DEBUG", LevelDebug, false},
		{"INFO", LevelInfo, false},
		{"WARN", LevelWarn, false},
		{"WARNING", LevelWarn, false},
		{"ERROR", LevelError, false},

		// Valid mixed case
		{"Debug", LevelDebug, false},
		{"Info", LevelInfo, false},
		{"Warn", LevelWarn, false},
		{"Warning", LevelWarn, false},
		{"Error", LevelError, false},

		// Invalid inputs
		{"", LevelInfo, true},
		{"invalid", LevelInfo, true},
		{"trace", LevelInfo, true},
		{"fatal", LevelInfo, true},
		{"INFO ", LevelInfo, true}, // trailing space
		{" info", LevelInfo, true}, // leading space
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			level, err := ParseLevel(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseLevel(%q) expected error, got nil", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("ParseLevel(%q) unexpected error: %v", tt.input, err)
				}
				if level != tt.expected {
					t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, level, tt.expected)
				}
			}
		})
	}
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarn, "WARN"},
		{LevelError, "ERROR"},
		{Level(99), "UNKNOWN"}, // unknown level
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("Level(%d).String() = %q, want %q", tt.level, got, tt.expected)
			}
		})
	}
}

func TestGetSetLevel(t *testing.T) {
	// Save original level
	original := GetLevel()
	defer SetLevel(original)

	levels := []Level{LevelDebug, LevelInfo, LevelWarn, LevelError}
	for _, level := range levels {
		SetLevel(level)
		if got := GetLevel(); got != level {
			t.Errorf("SetLevel(%v); GetLevel() = %v, want %v", level, got, level)
		}
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
