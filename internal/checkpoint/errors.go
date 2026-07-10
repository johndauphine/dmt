package checkpoint

import (
	"errors"
	"fmt"

	"github.com/johndauphine/dmt/internal/exitcodes"
)

// RequiredWriteError means a durability transition required for safe retry or
// resume did not persist. Callers must stop the success path when this error is
// returned; continuing could report success without a usable checkpoint.
type RequiredWriteError struct {
	Operation string
	Err       error
}

func (e *RequiredWriteError) Error() string {
	return fmt.Sprintf("required checkpoint write failed while %s: %v; repair checkpoint storage, then retry or resume", e.Operation, e.Err)
}

func (e *RequiredWriteError) Unwrap() error { return e.Err }

func (e *RequiredWriteError) ExitCode() int { return exitcodes.StateError }

// RequiredWrite adds durability and recovery context to a backend write
// failure. It preserves an existing RequiredWriteError to avoid duplicating
// guidance as the error crosses checkpoint, transfer, and orchestrator layers.
func RequiredWrite(operation string, err error) error {
	if err == nil {
		return nil
	}
	var required *RequiredWriteError
	if errors.As(err, &required) {
		return err
	}
	return &RequiredWriteError{Operation: operation, Err: err}
}

func IsRequiredWriteError(err error) bool {
	var required *RequiredWriteError
	return errors.As(err, &required)
}
