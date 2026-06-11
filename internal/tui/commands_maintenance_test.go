package tui

import (
	"strings"
	"testing"
)

// #443: /cache clear is destructive, so without --confirm it must name
// the exact file and scope and not touch anything.
func TestHandleCacheCommand(t *testing.T) {
	out := runCmd(t, handleCacheCommand([]string{"/cache", "clear"}))
	for _, want := range []string{"This will remove the entire type cache file", "type-cache.json", "--confirm to proceed"} {
		if !strings.Contains(out, want) {
			t.Errorf("confirm prompt missing %q: %q", want, out)
		}
	}

	out = runCmd(t, handleCacheCommand([]string{"/cache", "clear", "--ai-only"}))
	for _, want := range []string{"AI-sourced entries", "deterministic typemap entries are kept", "/cache clear --ai-only --confirm"} {
		if !strings.Contains(out, want) {
			t.Errorf("ai-only prompt missing %q: %q", want, out)
		}
	}

	if out := runCmd(t, handleCacheCommand([]string{"/cache"})); !strings.Contains(out, "usage: /cache clear") {
		t.Fatalf("usage output: %q", out)
	}
	if out := runCmd(t, handleCacheCommand([]string{"/cache", "clear", "--bogus"})); !strings.Contains(out, "/cache clear: unknown flag --bogus") {
		t.Fatalf("unknown flag output: %q", out)
	}
}

// #443: /init-secrets rejects unknown flags through the shared parser.
// The happy path writes to ~/.secrets, so it is exercised by the CLI
// integration tests rather than here.
func TestHandleInitSecretsCommandParsing(t *testing.T) {
	if out := runCmd(t, handleInitSecretsCommand([]string{"/init-secrets", "--bogus"})); !strings.Contains(out, "/init-secrets: unknown flag --bogus") {
		t.Fatalf("unknown flag output: %q", out)
	}
}
