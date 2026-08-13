package webui

import (
	"regexp"
	"strings"
	"testing"
)

// Accessibility guards for the SPA (#598). The front end has no build step and
// no JS test runner in this repo, so these assert the shape of the source the
// same way TestServiceWorkerVersionStamped does for sw.js. They are deliberately
// about structure a screen reader depends on — roles, labels, live regions —
// not about styling.

var (
	labelTagRe = regexp.MustCompile(`<label\b[^>]*>`)
	thTagRe    = regexp.MustCompile(`<th\b[^>]*>`)
	svgTagRe   = regexp.MustCompile(`<svg\b[^>]*>`)
)

// TestEveryLabelIsAssociated catches the most common form-accessibility defect:
// a <label> that merely sits next to its input. A wrapping .checkline label
// associates implicitly and needs no for=.
func TestEveryLabelIsAssociated(t *testing.T) {
	js := readAsset(t, "/app.js")
	for _, tag := range labelTagRe.FindAllString(js, -1) {
		if strings.Contains(tag, "for=") || strings.Contains(tag, `class="checkline"`) {
			continue
		}
		t.Errorf("unassociated label %q — add for=\"<input id>\" or wrap the control", tag)
	}
}

// TestEveryColumnHeaderHasScope keeps the data tables navigable: without scope,
// a screen reader cannot reliably tie a cell back to its column.
func TestEveryColumnHeaderHasScope(t *testing.T) {
	js := readAsset(t, "/app.js")
	for _, tag := range thTagRe.FindAllString(js, -1) {
		if !strings.Contains(tag, "scope=") {
			t.Errorf("column header without scope: %q", tag)
		}
	}
}

// TestDecorativeIconsAreHidden keeps the inline SVGs out of the accessibility
// tree — each one sits next to its own visible text label, so an unhidden icon
// is pure noise (and in some readers an unnamed "graphic" stop).
func TestDecorativeIconsAreHidden(t *testing.T) {
	js := readAsset(t, "/app.js")
	for _, tag := range svgTagRe.FindAllString(js, -1) {
		if !strings.Contains(tag, `aria-hidden="true"`) {
			t.Errorf("decorative svg not hidden from the a11y tree: %q", tag)
		}
	}
}

// TestModalsAreAccessibleDialogs pins the dialog contract for the two overlays:
// both announce as modal dialogs with a name, and both route through openModal,
// which is what supplies the focus trap, Escape, and focus restoration. A new
// overlay that skips openModal would strand keyboard focus behind it.
func TestModalsAreAccessibleDialogs(t *testing.T) {
	js := readAsset(t, "/app.js")

	if !strings.Contains(js, "function openModal(") {
		t.Fatal("openModal is gone — the shared focus trap/restore is what makes the overlays operable")
	}
	if n := strings.Count(js, "openModal("); n < 3 { // definition + both call sites
		t.Errorf("openModal referenced %d times, want >= 3 (definition + command palette + origin picker)", n)
	}
	// Both overlays declare themselves as named modal dialogs.
	if n := strings.Count(js, `role="dialog" aria-modal="true"`); n != 2 {
		t.Errorf(`found %d 'role="dialog" aria-modal="true"' overlays, want 2`, n)
	}
	for _, want := range []string{
		`aria-label="Command palette"`,   // palette's accessible name
		`aria-labelledby="picker-title"`, // origin picker names itself from its heading
	} {
		if !strings.Contains(js, want) {
			t.Errorf("modal missing accessible name: %s", want)
		}
	}
	// Focus restoration is the half of a trap that is easy to drop.
	if !strings.Contains(js, "returnTo.focus()") {
		t.Error("openModal no longer restores focus to the element that opened the modal")
	}
}

// TestCommandPaletteUsesComboboxPattern guards the palette's ARIA wiring: focus
// stays in the text input while Arrow keys move a virtual selection, so the
// highlighted command is only conveyed by aria-activedescendant. Without it the
// selection is silent to a screen reader.
func TestCommandPaletteUsesComboboxPattern(t *testing.T) {
	js := readAsset(t, "/app.js")
	for _, want := range []string{
		`role="combobox"`,
		`role="listbox"`,
		`role="option"`,
		"aria-activedescendant",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("command palette missing %s", want)
		}
	}
}

// TestRunProgressIsAnnounced guards the live-region plumbing for SSE progress:
// a sighted operator reads the telemetry panel, and this is the only channel a
// screen-reader user has. The throttle is part of the contract — an unthrottled
// region fires several times a second and makes the app unusable.
func TestRunProgressIsAnnounced(t *testing.T) {
	js := readAsset(t, "/app.js")
	for _, want := range []string{
		`id="run-live"`,
		`aria-live="polite"`,
		`role="progressbar"`,
		"aria-valuenow",
		"aria-valuetext",
		"function announceProgress(",
		"ANNOUNCE_EVERY_MS",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("run progress announcement missing %s", want)
		}
	}
	// One live region per kind of information: the run pill is a visual mirror
	// of state the toast stack already speaks, so it must not also be a region.
	if strings.Contains(js, `id="run-pill" class="badge idle" role="status"`) {
		t.Error(`#run-pill is a live region again — it duplicates the completed/failed toast`)
	}
}

// TestShellLandmarksAndSkipLink keeps the app shell navigable: a named primary
// nav, a focusable main, and a skip link past the sidebar's ten controls.
func TestShellLandmarksAndSkipLink(t *testing.T) {
	js := readAsset(t, "/app.js")
	css := readAsset(t, "/app.css")

	for _, want := range []string{
		`aria-label="Primary"`,                        // the nav landmark has a name
		`<main id="view" class="view" tabindex="-1">`, // skip-link target
		`class="skip-link"`,
		`aria-current`, // active nav item is stated, not just colored
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app shell missing %s", want)
		}
	}
	for _, want := range []string{".sr-only", ".skip-link"} {
		if !strings.Contains(css, want) {
			t.Errorf("app.css missing the %s utility", want)
		}
	}
	// The skip link must not navigate: the router owns location.hash and would
	// read "#view" as a view name.
	if !strings.Contains(js, `$(".skip-link").addEventListener("click", (e) => { e.preventDefault();`) {
		t.Error("skip link no longer suppresses hash navigation — it would bounce the router to the dashboard")
	}
}
