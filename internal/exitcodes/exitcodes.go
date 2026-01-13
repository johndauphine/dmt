// Package exitcodes defines standard exit codes for CLI operations.
// These codes match the Rust data-transfer-tool implementation for consistency
// in Airflow, Kubernetes, and other orchestration environments.
package exitcodes

import (
	"errors"
	"os"
	"strings"
)

// Exit codes matching Rust implementation for Airflow/Kubernetes compatibility.
const (
	// Success - migration completed without errors
	Success = 0

	// ConfigError - configuration/YAML/JSON parsing errors (non-recoverable, don't retry)
	ConfigError = 1

	// ConnectionError - source/target database connection or pool errors (recoverable)
	ConnectionError = 2

	// TransferError - data transfer or schema extraction failed (non-recoverable)
	TransferError = 3

	// ValidationError - row count validation or missing primary key in upsert mode (non-recoverable)
	ValidationError = 4

	// Cancelled - user cancelled via SIGINT/SIGTERM (recoverable)
	Cancelled = 5

	// StateError - state file errors or config changed since last run (non-recoverable)
	StateError = 6

	// IOError - file I/O errors (recoverable)
	IOError = 7
)

// ExitError wraps an error with an exit code.
type ExitError struct {
	Err  error
	Code int
}

func (e *ExitError) Error() string {
	return e.Err.Error()
}

func (e *ExitError) Unwrap() error {
	return e.Err
}

// NewExitError creates a new ExitError with the given code.
func NewExitError(err error, code int) *ExitError {
	return &ExitError{Err: err, Code: code}
}

// FromError determines the appropriate exit code for an error.
// It examines error messages and types to classify the error.
func FromError(err error) int {
	if err == nil {
		return Success
	}

	// Check if it's already an ExitError
	var exitErr *ExitError
	if errors.As(err, &exitErr) {
		return exitErr.Code
	}

	// Check for os.PathError first (file not found, permission denied, etc.)
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return IOError
	}

	errStr := strings.ToLower(err.Error())

	// IO errors - check early for file-related errors (exit code 7)
	if containsAny(errStr, []string{
		"no such file",
		"file not found",
		"permission denied",
		"is a directory",
		"not a directory",
	}) {
		return IOError
	}

	// Validation errors (exit code 4) - check before ConfigError to avoid
	// "row count validation failed" matching ConfigError's "validation" keyword
	if containsAny(errStr, []string{
		"row count",
		"mismatch",
		"primary key",
		"no pk",
		"upsert requires",
		"validation failed",
	}) {
		return ValidationError
	}

	// Config errors (exit code 1) - parsing issues, not validation of data
	if containsAny(errStr, []string{
		"yaml:",
		"json:",
		"unmarshal",
		"invalid configuration",
		"missing required",
		"invalid value",
		"parsing config",
	}) && !containsAny(errStr, []string{"connection", "connect", "dial"}) {
		return ConfigError
	}

	// Connection errors (exit code 2)
	if containsAny(errStr, []string{
		"connection",
		"connect",
		"dial",
		"refused",
		"timeout",
		"unreachable",
		"no such host",
		"network",
		"pool",
		"ping",
		"login failed",
		"authentication",
	}) {
		return ConnectionError
	}

	// Transfer errors (exit code 3)
	if containsAny(errStr, []string{
		"transfer",
		"copy",
		"bulk",
		"insert",
		"schema extraction",
		"create table",
		"drop table",
		"truncate",
	}) {
		return TransferError
	}

	// Cancelled (exit code 5)
	if containsAny(errStr, []string{
		"cancel",
		"interrupt",
		"context canceled",
		"context deadline",
	}) {
		return Cancelled
	}

	// State errors (exit code 6)
	if containsAny(errStr, []string{
		"state",
		"checkpoint",
		"resume",
		"run not found",
		"already completed",
		"config changed",
	}) {
		return StateError
	}

	// Default to transfer error for unknown errors
	return TransferError
}

// IsRecoverable returns true if the error is recoverable (safe to retry).
func IsRecoverable(code int) bool {
	switch code {
	case ConnectionError, Cancelled, IOError:
		return true
	default:
		return false
	}
}

// Description returns a human-readable description of the exit code.
func Description(code int) string {
	switch code {
	case Success:
		return "success"
	case ConfigError:
		return "configuration error"
	case ConnectionError:
		return "connection error (recoverable)"
	case TransferError:
		return "transfer error"
	case ValidationError:
		return "validation error"
	case Cancelled:
		return "cancelled (recoverable)"
	case StateError:
		return "state error"
	case IOError:
		return "I/O error (recoverable)"
	default:
		return "unknown error"
	}
}

func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
