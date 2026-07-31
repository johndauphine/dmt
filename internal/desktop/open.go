// Package desktop turns the WebUI (internal/webui) into a desktop experience
// without a native shell. It launches the operator's own browser at the running
// server and coordinates single-instance behavior, so `dmt --gui` feels like an
// app while dmt itself stays a pure-Go, CGO-free, cross-compiled binary.
//
// Everything here is deliberately best-effort: a browser that cannot be found
// or refuses to start is logged, never fatal. The server keeps serving and the
// startup banner remains the fallback, so --gui degrades to exactly --webui.
package desktop

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

var (
	// ErrNoChromium reports that no Chromium-family browser was found, so the
	// chromeless --app window is unavailable on this machine.
	ErrNoChromium = errors.New("desktop: no Chromium-family browser found")
	// ErrNoDisplay reports a headless session where launching any browser is
	// guaranteed to fail (Linux/BSD without X11 or Wayland).
	ErrNoDisplay = errors.New("desktop: no graphical display available")
	// ErrUnsupportedOS reports a platform with no known browser launcher.
	ErrUnsupportedOS = errors.New("desktop: no browser launcher for this platform")
	// ErrNoOpener reports a supported platform (Linux/BSD) with a display, but
	// no opener utility (xdg-open, gio, etc.) found on PATH — distinct from
	// ErrUnsupportedOS, whose "for this platform" wording would otherwise be
	// misleading here: the platform is fine, a minimal userland just lacks the
	// binary (common in slim containers).
	ErrNoOpener = errors.New("desktop: no browser-opener utility found on PATH")
)

// Launcher opens URLs in a browser. The zero value targets the host platform;
// tests set goos, lookPath, and start to assert the constructed argv for every
// platform from a single machine, which per-OS build-tag files would prevent.
type Launcher struct {
	// goos overrides runtime.GOOS. Empty means the host platform.
	goos string
	// lookPath resolves an executable name to a path. Defaults to exec.LookPath.
	lookPath func(string) (string, error)
	// statFile reports whether an absolute path exists. Defaults to os.Stat.
	statFile func(string) error
	// start spawns the command. Defaults to startDetached.
	start func(*exec.Cmd) error
	// env reads an environment variable. Defaults to os.Getenv.
	env func(string) string
}

// New returns a Launcher bound to the host platform.
func New() *Launcher { return &Launcher{} }

func (l *Launcher) os() string {
	if l.goos != "" {
		return l.goos
	}
	return runtime.GOOS
}

func (l *Launcher) look(name string) (string, error) {
	if l.lookPath != nil {
		return l.lookPath(name)
	}
	return exec.LookPath(name)
}

func (l *Launcher) stat(path string) error {
	if l.statFile != nil {
		return l.statFile(path)
	}
	_, err := os.Stat(path)
	return err
}

func (l *Launcher) getenv(key string) string {
	if l.env != nil {
		return l.env(key)
	}
	return os.Getenv(key)
}

func (l *Launcher) run(cmd *exec.Cmd) error {
	if l.start != nil {
		return l.start(cmd)
	}
	return startDetached(cmd)
}

// startDetached spawns cmd without waiting for it. The browser typically
// outlives (or blocks in) the foreground, so dmt must never Wait on it, and its
// stdio must not interleave with the TUI/banner output on our terminal.
func startDetached(cmd *exec.Cmd) error {
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err == nil {
		cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, devNull, devNull
		defer devNull.Close()
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap the child so it doesn't linger as a zombie for the life of dmt.
	go func() { _ = cmd.Wait() }()
	return nil
}

// Open shows url in the operator's default browser.
func (l *Launcher) Open(url string) error {
	cmd, err := l.openCmd(url)
	if err != nil {
		return err
	}
	return l.run(cmd)
}

// openCmd builds (but does not run) the default-browser command. Split out so
// tests can assert argv without spawning anything.
func (l *Launcher) openCmd(url string) (*exec.Cmd, error) {
	switch l.os() {
	case "darwin":
		return exec.Command("open", url), nil
	case "windows":
		// rundll32 takes the URL as a single argument, unlike `cmd /c start`
		// which treats "&" in the ?token= query as a command separator and
		// silently truncates the URL.
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url), nil
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly":
		if !l.hasDisplay() {
			return nil, ErrNoDisplay
		}
		for _, opener := range []struct {
			bin  string
			args []string
		}{
			{"xdg-open", nil},
			{"gio", []string{"open"}},
			{"x-www-browser", nil},
			{"www-browser", nil},
		} {
			if path, err := l.look(opener.bin); err == nil {
				return exec.Command(path, append(append([]string{}, opener.args...), url)...), nil
			}
		}
		return nil, ErrNoOpener
	default:
		return nil, ErrUnsupportedOS
	}
}

// hasDisplay reports whether a graphical session is reachable. Only meaningful
// on Unix-likes; macOS and Windows always have one.
func (l *Launcher) hasDisplay() bool {
	return l.getenv("DISPLAY") != "" || l.getenv("WAYLAND_DISPLAY") != ""
}

// OpenAppWindow shows url in a chromeless, app-style window.
//
// Chromium-family browsers provide this directly via --app=. Safari has no
// equivalent and its standalone mode (File > Add to Dock, macOS 14+/Safari 17+)
// is user-initiated by design, so on a Mac with no Chromium installed this
// opens a dedicated Safari window instead and returns FallbackSafari so the
// caller can point the operator at Add to Dock.
func (l *Launcher) OpenAppWindow(url string) (Fallback, error) {
	if chrome, ok := l.findChromium(); ok {
		cmd := exec.Command(chrome, "--app="+url, "--window-size=1280,860")
		return FallbackNone, l.run(cmd)
	}
	if l.os() == "darwin" {
		// -n forces a new window rather than a tab in an existing Safari.
		cmd := exec.Command("open", "-n", "-a", "Safari", url)
		return FallbackSafari, l.run(cmd)
	}
	return FallbackNone, ErrNoChromium
}

// Fallback reports which path OpenAppWindow actually took, so the caller can
// tailor the hint it prints.
type Fallback int

const (
	// FallbackNone means a real chromeless app window was opened.
	FallbackNone Fallback = iota
	// FallbackSafari means Safari was opened as a normal window because no
	// Chromium-family browser was available.
	FallbackSafari
)

// chromiumNames are the PATH-resolvable Chromium-family executables, most
// common first.
var chromiumNames = []string{
	"google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
	"microsoft-edge", "microsoft-edge-stable", "brave-browser", "vivaldi",
}

// findChromium locates a Chromium-family browser: an explicit $BROWSER first
// (the operator's stated preference), then PATH, then the fixed install
// locations used on macOS and Windows where these apps are not on PATH.
func (l *Launcher) findChromium() (string, bool) {
	if b := l.getenv("BROWSER"); b != "" && isChromiumName(b) {
		if path, err := l.look(b); err == nil {
			return path, true
		}
	}
	for _, name := range chromiumNames {
		if path, err := l.look(name); err == nil {
			return path, true
		}
	}
	for _, path := range l.chromiumAppPaths() {
		if l.stat(path) == nil {
			return path, true
		}
	}
	return "", false
}

// isChromiumName reports whether a $BROWSER value names a Chromium-family
// browser. $BROWSER pointing at Firefox must not be handed --app=.
func isChromiumName(browser string) bool {
	base := filepath.Base(browser)
	for _, name := range chromiumNames {
		if base == name {
			return true
		}
	}
	return false
}

// chromiumAppPaths returns absolute install locations to probe on platforms
// where browsers are not on PATH.
func (l *Launcher) chromiumAppPaths() []string {
	switch l.os() {
	case "darwin":
		return []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
			"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
		}
	case "windows":
		var paths []string
		for _, root := range []string{
			l.getenv("ProgramFiles"),
			l.getenv("ProgramFiles(x86)"),
			l.getenv("LOCALAPPDATA"),
		} {
			if root == "" {
				continue
			}
			paths = append(paths,
				filepath.Join(root, "Google", "Chrome", "Application", "chrome.exe"),
				filepath.Join(root, "Microsoft", "Edge", "Application", "msedge.exe"),
				filepath.Join(root, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
			)
		}
		return paths
	default:
		return nil
	}
}

// SafariHint is the guidance shown when a Mac has no Chromium-family browser
// and the operator asked for an app window.
const SafariHint = "Safari has no command-line app mode — use File → Add to Dock " +
	"(macOS 14+/Safari 17+) to install dmt as a standalone desktop app."

// Describe renders a launch failure as an operator-facing sentence.
func Describe(err error) string {
	switch {
	case errors.Is(err, ErrNoDisplay):
		return "no graphical display detected (headless session)"
	case errors.Is(err, ErrNoChromium):
		return "no Chromium-family browser found for --app-window"
	case errors.Is(err, ErrNoOpener):
		return "no browser-opener utility (xdg-open, gio, etc.) found on PATH"
	case errors.Is(err, ErrUnsupportedOS):
		return "no known browser launcher for this platform"
	default:
		return err.Error()
	}
}
