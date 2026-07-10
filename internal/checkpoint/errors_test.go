package checkpoint

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/exitcodes"
)

func TestRequiredWriteErrorCarriesStateExitCodeAndGuidance(t *testing.T) {
	cause := errors.New("disk full")
	err := RequiredWrite("saving progress for table orders task 42", cause)
	if !errors.Is(err, cause) || !IsRequiredWriteError(err) {
		t.Fatalf("RequiredWrite = %v, want wrapped typed error", err)
	}
	if code := exitcodes.FromError(err); code != exitcodes.StateError {
		t.Fatalf("exit code = %d, want StateError", code)
	}
	for _, want := range []string{"required checkpoint write", "repair checkpoint storage", "retry or resume"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
	if got := RequiredWrite("outer layer", err); got != err {
		t.Fatal("RequiredWrite double-wrapped an existing required-write error")
	}
}
