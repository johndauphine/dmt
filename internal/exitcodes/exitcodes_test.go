package exitcodes

import (
	"errors"
	"os"
	"testing"
)

func TestFromError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{"nil error", nil, Success},
		{"path error", &os.PathError{Op: "open", Path: "/foo", Err: errors.New("no such file")}, IOError},
		{"yaml parse error", errors.New("yaml: unmarshal error"), ConfigError},
		{"json parse error", errors.New("json: unmarshal error"), ConfigError},
		{"no such file", errors.New("open config.yaml: no such file or directory"), IOError},
		{"connection refused", errors.New("dial tcp: connection refused"), ConnectionError},
		{"login failed", errors.New("login failed for user"), ConnectionError},
		{"transfer error", errors.New("bulk copy failed"), TransferError},
		{"row count mismatch", errors.New("row count mismatch: expected 100, got 99"), ValidationError},
		{"row count validation failed", errors.New("row count validation failed"), ValidationError},
		{"no primary key", errors.New("table has no primary key for upsert"), ValidationError},
		{"context canceled", errors.New("context canceled"), Cancelled},
		{"state error", errors.New("checkpoint not found"), StateError},
		{"config changed", errors.New("config changed since last run"), StateError},
		{"unknown error", errors.New("something unexpected happened"), TransferError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromError(tt.err)
			if got != tt.expected {
				t.Errorf("FromError(%v) = %d (%s), want %d (%s)",
					tt.err, got, Description(got), tt.expected, Description(tt.expected))
			}
		})
	}
}

func TestExitError(t *testing.T) {
	inner := errors.New("inner error")
	exitErr := NewExitError(inner, ConnectionError)

	if exitErr.Code != ConnectionError {
		t.Errorf("expected code %d, got %d", ConnectionError, exitErr.Code)
	}

	if exitErr.Error() != "inner error" {
		t.Errorf("expected error message 'inner error', got '%s'", exitErr.Error())
	}

	if errors.Unwrap(exitErr) != inner {
		t.Error("Unwrap should return inner error")
	}

	// Test that FromError extracts the code from ExitError
	if FromError(exitErr) != ConnectionError {
		t.Errorf("FromError should extract code from ExitError")
	}
}

func TestIsRecoverable(t *testing.T) {
	recoverable := []int{ConnectionError, Cancelled, IOError}
	nonRecoverable := []int{Success, ConfigError, TransferError, ValidationError, StateError}

	for _, code := range recoverable {
		if !IsRecoverable(code) {
			t.Errorf("expected code %d (%s) to be recoverable", code, Description(code))
		}
	}

	for _, code := range nonRecoverable {
		if IsRecoverable(code) {
			t.Errorf("expected code %d (%s) to be non-recoverable", code, Description(code))
		}
	}
}

func TestDescription(t *testing.T) {
	tests := []struct {
		code     int
		expected string
	}{
		{Success, "success"},
		{ConfigError, "configuration error"},
		{ConnectionError, "connection error (recoverable)"},
		{TransferError, "transfer error"},
		{ValidationError, "validation error"},
		{Cancelled, "cancelled (recoverable)"},
		{StateError, "state error"},
		{IOError, "I/O error (recoverable)"},
		{99, "unknown error"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := Description(tt.code)
			if got != tt.expected {
				t.Errorf("Description(%d) = %q, want %q", tt.code, got, tt.expected)
			}
		})
	}
}
