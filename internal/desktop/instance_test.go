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

// Regression test for a review finding: a process killed after PublishURL but
// before Release (SIGKILL, an unrecovered panic) skips the deferred cleanup,
// so the sidecar file survives on disk even though the OS has already
// released the advisory lock. Without Acquire() clearing it, a fresh
// successor could sit on the lock for a while before its own PublishURL
// runs, and any third launch racing that window would read the first
// process's stale, now-dead URL instead of correctly seeing "no handoff yet".
func TestInstanceAcquireClearsStaleSidecarFromCrash(t *testing.T) {
	withHome(t)

	crashed, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (crashed): %v", err)
	}
	if ok, _, err := crashed.Acquire(); err != nil || !ok {
		t.Fatalf("Acquire: (%v, %v)", ok, err)
	}
	const staleURL = "http://127.0.0.1:8484/?token=dead-instance"
	if err := crashed.PublishURL(staleURL); err != nil {
		t.Fatalf("PublishURL: %v", err)
	}
	// Simulate a hard kill: only the OS-level lock is released, unlike a
	// graceful exit where the deferred Release() would also remove the
	// sidecar. crashed.lock.Unlock() (not crashed.Release()) mirrors exactly
	// that gap.
	if err := crashed.lock.Unlock(); err != nil {
		t.Fatalf("simulating crash unlock: %v", err)
	}

	successor, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (successor): %v", err)
	}
	defer successor.Release()
	acquired, _, err := successor.Acquire()
	if err != nil {
		t.Fatalf("Acquire after crash: %v", err)
	}
	if !acquired {
		t.Fatal("successor should acquire the lock the crashed process dropped")
	}
	if _, statErr := os.Stat(successor.sidecarPath); !os.IsNotExist(statErr) {
		t.Fatalf("stale sidecar from the crashed instance was not cleared on Acquire: statErr=%v", statErr)
	}

	// The real-world stakes: a third launch racing the successor's own
	// startup (before it has published its fresh URL) must not be handed the
	// dead process's URL.
	racer, err := NewInstance()
	if err != nil {
		t.Fatalf("NewInstance (racer): %v", err)
	}
	defer racer.Release()
	racerAcquired, racerHandoff, err := racer.Acquire()
	if err != nil {
		t.Fatalf("racer Acquire: %v", err)
	}
	if racerAcquired {
		t.Fatal("racer should not acquire while successor holds the lock")
	}
	if racerHandoff == staleURL {
		t.Fatal("racer was handed the crashed instance's stale URL instead of seeing no handoff yet")
	}
	if racerHandoff != "" {
		t.Errorf("racer handoff = %q, want empty (successor has not published yet)", racerHandoff)
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
