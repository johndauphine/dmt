package desktop

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

// dmtDir returns ~/.dmt, matching the convention used across the codebase
// (internal/audit/path.go, internal/config/defaults.go) for per-user state.
func dmtDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("desktop: resolve $HOME: %w", err)
	}
	return filepath.Join(home, ".dmt"), nil
}

// handoffInfo is the sidecar written by the instance holding the GUI lock, so
// a second `dmt --gui` launch can find the running server instead of failing
// to bind its port. Written only for a loopback bind with an auto-generated
// token (see Instance.Acquire) — an operator-supplied or remote token is never
// persisted to disk.
type handoffInfo struct {
	URL string `json:"url"`
}

// Instance coordinates single-instance behavior for `dmt --gui` across
// process launches, using the same advisory-lock pattern as
// internal/checkpoint/filestate_lease.go (gofrs/flock, released automatically
// if the process crashes).
type Instance struct {
	lock        *flock.Flock
	lockPath    string
	sidecarPath string
}

// NewInstance prepares (without acquiring) the GUI single-instance lock.
func NewInstance() (*Instance, error) {
	dir, err := dmtDir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("desktop: creating %s: %w", dir, err)
	}
	return &Instance{
		lock:        flock.New(filepath.Join(dir, "gui.lock")),
		lockPath:    filepath.Join(dir, "gui.lock"),
		sidecarPath: filepath.Join(dir, "gui.json"),
	}, nil
}

// Acquire tries to become the sole running `dmt --gui` instance.
//
// On success (acquired=true), the caller owns the lock until Release: it
// should start its server and then call PublishURL once bound.
//
// On failure (acquired=false, err=nil), another instance already holds the
// lock. handoffURL is populated from that instance's sidecar when available,
// so the caller can open a window against the existing server and exit
// instead of failing to bind the same port.
func (in *Instance) Acquire() (acquired bool, handoffURL string, err error) {
	ok, err := in.lock.TryLock()
	if err != nil {
		return false, "", fmt.Errorf("desktop: acquiring GUI instance lock: %w", err)
	}
	if ok {
		return true, "", nil
	}
	url, readErr := in.readSidecar()
	if readErr != nil {
		// The lock is held but the sidecar is missing, stale, or unreadable
		// (e.g. the holder is mid-startup, or was killed before writing it, or
		// bound a non-loopback address and intentionally wrote nothing — see
		// PublishURL). Report the failure without a handoff URL rather than
		// erroring: the caller falls back to printing its own bind failure.
		return false, "", nil //nolint:nilerr // sidecar absence is not a lock error
	}
	return false, url, nil
}

// PublishURL records the URL for other `dmt --gui` launches to hand off to.
// Call only after Acquire returns true and the server has bound its address.
//
// loopback must be true and the URL must not carry a remote or
// operator-chosen secret's worth of exposure beyond what the loopback bind
// already implies: callers pass includeToken=true only when the token was
// auto-generated for a loopback bind (mirroring the startup banner's own
// one-click-link rule in internal/webui/server.go printBanner), so an
// operator-supplied or remote-bind token is never written to disk.
func (in *Instance) PublishURL(url string) error {
	data, err := json.Marshal(handoffInfo{URL: url})
	if err != nil {
		return fmt.Errorf("desktop: encoding GUI sidecar: %w", err)
	}
	if err := os.WriteFile(in.sidecarPath, data, 0600); err != nil {
		return fmt.Errorf("desktop: writing GUI sidecar: %w", err)
	}
	return nil
}

// Release drops the lock and removes the sidecar. Safe to call even if
// Acquire never succeeded.
func (in *Instance) Release() {
	_ = os.Remove(in.sidecarPath)
	_ = in.lock.Unlock()
}

func (in *Instance) readSidecar() (string, error) {
	data, err := os.ReadFile(in.sidecarPath)
	if err != nil {
		return "", err
	}
	var info handoffInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return "", fmt.Errorf("desktop: parsing GUI sidecar: %w", err)
	}
	if info.URL == "" {
		return "", fmt.Errorf("desktop: GUI sidecar has no URL")
	}
	return info.URL, nil
}
