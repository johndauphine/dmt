package desktop

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fakeLauncher builds a Launcher whose platform, PATH lookups, filesystem, and
// environment are all injected, so every platform's argv is assertable from a
// single host. onPath lists executables that resolve; onDisk lists absolute
// paths that exist.
func fakeLauncher(goos string, onPath []string, onDisk []string, env map[string]string) (*Launcher, *[]string) {
	var argv []string
	l := &Launcher{
		goos: goos,
		lookPath: func(name string) (string, error) {
			for _, p := range onPath {
				if p == name {
					return "/usr/bin/" + name, nil
				}
			}
			return "", exec.ErrNotFound
		},
		statFile: func(path string) error {
			for _, p := range onDisk {
				if p == path {
					return nil
				}
			}
			return errors.New("not found")
		},
		env: func(key string) string { return env[key] },
		start: func(cmd *exec.Cmd) error {
			argv = cmd.Args
			return nil
		},
	}
	return l, &argv
}

// linuxDisplay is the minimum env for a Linux launch to be considered viable.
var linuxDisplay = map[string]string{"DISPLAY": ":0"}

const testURL = "http://127.0.0.1:8484/?token=abc123"

// Windows probe paths are assembled with filepath.Join, matching the
// production code, so these expectations hold on any host OS.
var (
	winProgramFiles = filepath.Join("C:", "PF")
	winEdgePath     = filepath.Join(winProgramFiles, "Microsoft", "Edge", "Application", "msedge.exe")
)

func TestOpenDefaultBrowserArgv(t *testing.T) {
	cases := []struct {
		name   string
		goos   string
		onPath []string
		env    map[string]string
		want   []string
	}{
		{
			name: "darwin uses open and respects the default browser",
			goos: "darwin",
			want: []string{"open", testURL},
		},
		{
			// cmd /c start would treat "&" in the token query as a separator.
			name: "windows uses rundll32 so the token query survives",
			goos: "windows",
			want: []string{"rundll32", "url.dll,FileProtocolHandler", testURL},
		},
		{
			name:   "linux prefers xdg-open",
			goos:   "linux",
			onPath: []string{"xdg-open", "gio"},
			env:    linuxDisplay,
			want:   []string{"/usr/bin/xdg-open", testURL},
		},
		{
			name:   "linux falls back to gio open",
			goos:   "linux",
			onPath: []string{"gio"},
			env:    linuxDisplay,
			want:   []string{"/usr/bin/gio", "open", testURL},
		},
		{
			name:   "linux falls back to x-www-browser",
			goos:   "linux",
			onPath: []string{"x-www-browser"},
			env:    linuxDisplay,
			want:   []string{"/usr/bin/x-www-browser", testURL},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, argv := fakeLauncher(tc.goos, tc.onPath, nil, tc.env)
			if err := l.Open(testURL); err != nil {
				t.Fatalf("Open: %v", err)
			}
			if got := strings.Join(*argv, " "); got != strings.Join(tc.want, " ") {
				t.Errorf("argv = %q, want %q", got, tc.want)
			}
		})
	}
}

// A headless Linux box must be detected up front rather than spawning a
// process that is guaranteed to fail.
func TestOpenHeadlessLinux(t *testing.T) {
	l, argv := fakeLauncher("linux", []string{"xdg-open"}, nil, map[string]string{})
	err := l.Open(testURL)
	if !errors.Is(err, ErrNoDisplay) {
		t.Fatalf("Open on headless linux = %v, want ErrNoDisplay", err)
	}
	if len(*argv) != 0 {
		t.Errorf("headless launch spawned %v, want nothing", *argv)
	}
}

func TestOpenWaylandCountsAsDisplay(t *testing.T) {
	l, argv := fakeLauncher("linux", []string{"xdg-open"}, nil, map[string]string{"WAYLAND_DISPLAY": "wayland-0"})
	if err := l.Open(testURL); err != nil {
		t.Fatalf("Open under Wayland: %v", err)
	}
	if len(*argv) == 0 {
		t.Error("Wayland session did not launch a browser")
	}
}

func TestOpenUnsupportedPlatform(t *testing.T) {
	l, _ := fakeLauncher("plan9", nil, nil, nil)
	if err := l.Open(testURL); !errors.Is(err, ErrUnsupportedOS) {
		t.Fatalf("Open on plan9 = %v, want ErrUnsupportedOS", err)
	}
}

func TestOpenAppWindowChromium(t *testing.T) {
	cases := []struct {
		name   string
		goos   string
		onPath []string
		onDisk []string
		env    map[string]string
		want   []string
	}{
		{
			name:   "linux chromium on PATH",
			goos:   "linux",
			onPath: []string{"google-chrome"},
			env:    linuxDisplay,
			want:   []string{"/usr/bin/google-chrome", "--app=" + testURL, "--window-size=1280,860"},
		},
		{
			name:   "darwin finds Chrome at its fixed install path",
			goos:   "darwin",
			onDisk: []string{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"},
			want: []string{
				"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
				"--app=" + testURL, "--window-size=1280,860",
			},
		},
		{
			// Built with filepath.Join so the expectation matches the host's
			// separator: this asserts the path components, and on Windows CI it
			// additionally pins the backslash form.
			name:   "windows finds Edge under ProgramFiles",
			goos:   "windows",
			onDisk: []string{winEdgePath},
			env:    map[string]string{"ProgramFiles": winProgramFiles},
			want: []string{
				winEdgePath,
				"--app=" + testURL, "--window-size=1280,860",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, argv := fakeLauncher(tc.goos, tc.onPath, tc.onDisk, tc.env)
			fb, err := l.OpenAppWindow(testURL)
			if err != nil {
				t.Fatalf("OpenAppWindow: %v", err)
			}
			if fb != FallbackNone {
				t.Errorf("fallback = %v, want FallbackNone", fb)
			}
			if got := strings.Join(*argv, " "); got != strings.Join(tc.want, " ") {
				t.Errorf("argv = %q, want %q", got, tc.want)
			}
		})
	}
}

// Safari has no --app= equivalent, so a Mac without Chromium gets a dedicated
// Safari window plus a fallback signal the caller turns into an Add-to-Dock hint.
func TestOpenAppWindowSafariFallback(t *testing.T) {
	l, argv := fakeLauncher("darwin", nil, nil, nil)
	fb, err := l.OpenAppWindow(testURL)
	if err != nil {
		t.Fatalf("OpenAppWindow: %v", err)
	}
	if fb != FallbackSafari {
		t.Errorf("fallback = %v, want FallbackSafari", fb)
	}
	want := []string{"open", "-n", "-a", "Safari", testURL}
	if got := strings.Join(*argv, " "); got != strings.Join(want, " ") {
		t.Errorf("argv = %q, want %q", got, want)
	}
}

// Linux and Windows have no Safari, so no Chromium means no app window at all.
func TestOpenAppWindowNoChromium(t *testing.T) {
	for _, goos := range []string{"linux", "windows"} {
		t.Run(goos, func(t *testing.T) {
			l, _ := fakeLauncher(goos, nil, nil, linuxDisplay)
			if _, err := l.OpenAppWindow(testURL); !errors.Is(err, ErrNoChromium) {
				t.Fatalf("OpenAppWindow = %v, want ErrNoChromium", err)
			}
		})
	}
}

// $BROWSER is an operator preference, but only honored when it names a
// Chromium-family browser — handing --app= to Firefox would open a garbage tab.
func TestFindChromiumRespectsBrowserEnv(t *testing.T) {
	l, argv := fakeLauncher("linux",
		[]string{"firefox", "chromium"}, nil,
		map[string]string{"BROWSER": "firefox", "DISPLAY": ":0"})
	if _, err := l.OpenAppWindow(testURL); err != nil {
		t.Fatalf("OpenAppWindow: %v", err)
	}
	if got := (*argv)[0]; got != "/usr/bin/chromium" {
		t.Errorf("picked %q, want chromium (BROWSER=firefox must be ignored for --app)", got)
	}
}

func TestFindChromiumHonorsChromiumBrowserEnv(t *testing.T) {
	l, argv := fakeLauncher("linux",
		[]string{"google-chrome", "microsoft-edge"}, nil,
		map[string]string{"BROWSER": "microsoft-edge", "DISPLAY": ":0"})
	if _, err := l.OpenAppWindow(testURL); err != nil {
		t.Fatalf("OpenAppWindow: %v", err)
	}
	if got := (*argv)[0]; got != "/usr/bin/microsoft-edge" {
		t.Errorf("picked %q, want the $BROWSER choice microsoft-edge", got)
	}
}

func TestDescribe(t *testing.T) {
	cases := map[error]string{
		ErrNoDisplay:     "headless",
		ErrNoChromium:    "Chromium",
		ErrUnsupportedOS: "platform",
	}
	for err, want := range cases {
		if got := Describe(err); !strings.Contains(got, want) {
			t.Errorf("Describe(%v) = %q, want substring %q", err, got, want)
		}
	}
}
