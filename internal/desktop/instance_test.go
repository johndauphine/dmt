package desktop

import (
	"os"
	"path/filepath"
	"testing"
)

// withHome redirects ~/.dmt into a fresh temp directory for the test, so
// instance state never touches the real user's home.
func withHome(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	// os.UserHomeDir consults USERPROFILE on Windows and HOME elsewhere; set
	// both so this test is meaningful under either.
	t.Setenv("USERPROFILE", os.Getenv("HOME"))
}

func TestInstanceAcquireFirstWins(t *testing.T) {
	withHome(t)
	in, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	defer in.Release()

	acquired, handoff, err := in.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !acquired {
		t.Fatalf("first Acquire() = false, want true (handoff=%q)", handoff)
	}
}

func TestInstanceSecondLaunchHandsOff(t *testing.T) {
	withHome(t)

	first, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (first): %v", err)
	}
	defer first.Release()
	if ok, _, err := first.Acquire(); err != nil || !ok {
		t.Fatalf("first Acquire() = (%v, %v), want (true, nil)", ok, err)
	}
	const url = "http://127.0.0.1:8484/?token=abc123"
	if err := first.PublishURL(url); err != nil {
		t.Fatalf("PublishURL: %v", err)
	}

	second, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (second): %v", err)
	}
	defer second.Release()
	acquired, handoff, err := second.Acquire()
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if acquired {
		t.Fatal("second Acquire() = true, want false (first instance still holds the lock)")
	}
	if handoff != url {
		t.Errorf("handoff URL = %q, want %q", handoff, url)
	}
}

func TestInstanceReleaseFreesLockForNextLaunch(t *testing.T) {
	withHome(t)

	first, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	if ok, _, err := first.Acquire(); err != nil || !ok {
		t.Fatalf("Acquire: (%v, %v)", ok, err)
	}
	first.Release()

	second, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (second): %v", err)
	}
	defer second.Release()
	acquired, _, err := second.Acquire()
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	if !acquired {
		t.Fatal("Acquire after Release() = false, want true")
	}
}

// A held lock with no sidecar (holder mid-startup, killed before publishing,
// or bound non-loopback and intentionally published nothing) must report "not
// acquired, no handoff" rather than erroring — the caller falls back to its
// own bind-failure path.
func TestInstanceSecondLaunchNoSidecarYet(t *testing.T) {
	withHome(t)

	first, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	defer first.Release()
	if ok, _, err := first.Acquire(); err != nil || !ok {
		t.Fatalf("Acquire: (%v, %v)", ok, err)
	}
	// Deliberately no PublishURL call.

	second, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (second): %v", err)
	}
	defer second.Release()
	acquired, handoff, err := second.Acquire()
	if err != nil {
		t.Fatalf("Acquire with no sidecar: %v", err)
	}
	if acquired {
		t.Fatal("Acquire() = true, want false")
	}
	if handoff != "" {
		t.Errorf("handoff = %q, want empty (no sidecar published)", handoff)
	}
}

func TestInstanceReleaseRemovesSidecar(t *testing.T) {
	withHome(t)

	in, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	if ok, _, err := in.Acquire(); err != nil || !ok {
		t.Fatalf("Acquire: (%v, %v)", ok, err)
	}
	if err := in.PublishURL("http://127.0.0.1:8484/"); err != nil {
		t.Fatalf("PublishURL: %v", err)
	}
	in.Release()

	if _, err := os.Stat(in.sidecarPath); !os.IsNotExist(err) {
		t.Errorf("sidecar still exists after Release: err=%v", err)
	}
}

func TestNewInstanceUsesDotDmtDir(t *testing.T) {
	withHome(t)
	in, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	home := os.Getenv("HOME")
	want := filepath.Join(home, ".dmt", "gui.lock")
	if in.lockPath != want {
		t.Errorf("lockPath = %q, want %q", in.lockPath, want)
	}
}
