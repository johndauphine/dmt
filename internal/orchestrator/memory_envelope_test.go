package orchestrator

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/config"
)

func TestNewWithOptionsRejectsUnresolvedMemoryEnvelope(t *testing.T) {
	_, err := NewWithOptions(&config.Config{}, Options{})
	if err == nil || !strings.Contains(err.Error(), "memory envelope is unresolved") {
		t.Fatalf("NewWithOptions error = %v, want unresolved memory envelope", err)
	}
}
