package webui

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// This file pins the WebUI's color palette to WCAG 2.1 AA (#598). The console
// is an operator tool people stare at during multi-hour migrations, and the
// palette is hand-authored in app.css with no build step to check it — so the
// ratios are asserted here instead. A palette edit that drops a pairing below
// its threshold fails the build with the measured number.
//
// Thresholds: 4.5:1 for body text (1.4.3), 3:1 for large text and for the
// boundary of a control the user must be able to find (1.4.11). Purely
// decorative seams (--border) carry no requirement and are not listed.

// relLuminance implements WCAG 2.1's relative luminance for an sRGB hex color.
func relLuminance(hex string) (float64, error) {
	hex = strings.TrimPrefix(hex, "#")
	if len(hex) != 6 {
		return 0, fmt.Errorf("not a 6-digit hex color: %q", hex)
	}
	chans := make([]float64, 3)
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseInt(hex[i*2:i*2+2], 16, 32)
		if err != nil {
			return 0, fmt.Errorf("bad hex %q: %w", hex, err)
		}
		s := float64(v) / 255.0
		if s <= 0.03928 {
			chans[i] = s / 12.92
		} else {
			chans[i] = math.Pow((s+0.055)/1.055, 2.4)
		}
	}
	return 0.2126*chans[0] + 0.7152*chans[1] + 0.0722*chans[2], nil
}

// contrastRatio is WCAG 2.1's (L1+0.05)/(L2+0.05), lighter color first.
func contrastRatio(t *testing.T, fg, bg string) float64 {
	t.Helper()
	lf, err := relLuminance(fg)
	if err != nil {
		t.Fatalf("foreground: %v", err)
	}
	lb, err := relLuminance(bg)
	if err != nil {
		t.Fatalf("background: %v", err)
	}
	if lf < lb {
		lf, lb = lb, lf
	}
	return (lf + 0.05) / (lb + 0.05)
}

// readAsset returns an embedded front-end file by its served path ("/app.css").
// Reading staticFS directly keeps the asset tests independent of routing.
func readAsset(t *testing.T, name string) string {
	t.Helper()
	b, err := staticFS.ReadFile("static" + name)
	if err != nil {
		t.Fatalf("read embedded asset %s: %v", name, err)
	}
	return string(b)
}

var tokenRe = regexp.MustCompile(`--([a-z0-9-]+):\s*(#[0-9a-fA-F]{6})`)

// cssBlock returns the declarations between marker and the next closing brace.
// The palette blocks in app.css contain no nested braces, so the first "}"
// after the marker ends the block.
func cssBlock(t *testing.T, css, marker string) string {
	t.Helper()
	i := strings.Index(css, marker)
	if i < 0 {
		t.Fatalf("app.css: block %q not found — did the palette get restructured?", marker)
	}
	rest := css[i+len(marker):]
	j := strings.Index(rest, "}")
	if j < 0 {
		t.Fatalf("app.css: block %q is not closed", marker)
	}
	return rest[:j]
}

func parseTokens(t *testing.T, block string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range tokenRe.FindAllStringSubmatch(block, -1) {
		out[m[1]] = strings.ToLower(m[2])
	}
	if len(out) == 0 {
		t.Fatal("app.css: palette block parsed to zero tokens")
	}
	return out
}

// palettes pulls the four palette blocks out of app.css: the light base, the
// dark media-query override, and the two explicit [data-theme] overrides.
func palettes(t *testing.T) (light, dark, lightAttr, darkAttr map[string]string) {
	t.Helper()
	css := readAsset(t, "/app.css")
	// The first ":root {" in the file is the light base.
	light = parseTokens(t, cssBlock(t, css, ":root {"))
	darkMedia := css[strings.Index(css, "@media (prefers-color-scheme: dark)"):]
	dark = parseTokens(t, cssBlock(t, darkMedia, ":root {"))
	lightAttr = parseTokens(t, cssBlock(t, css, `:root[data-theme="light"] {`))
	darkAttr = parseTokens(t, cssBlock(t, css, `:root[data-theme="dark"] {`))
	return
}

type pairing struct {
	what   string
	fg, bg string
	min    float64
}

// pairings lists every foreground/background combination app.css actually
// paints, with the threshold that combination has to clear.
var pairings = []pairing{
	// Body and card text.
	{"body text", "fg", "bg", 4.5},
	{"card text", "fg", "panel", 4.5},
	{"muted text on card", "fg-muted", "panel", 4.5},
	{"muted text on page", "fg-muted", "bg", 4.5},
	{"muted text on inset", "fg-muted", "panel-2", 4.5},
	// --fg-faint is small text everywhere it appears: .eyebrow (11px),
	// table.data th (11px), .stat .k (11px), .brand .ver (10px), .palette .hint.
	{"eyebrow on card", "fg-faint", "panel", 4.5},
	{"eyebrow on page", "fg-faint", "bg", 4.5},
	{"column header on inset", "fg-faint", "panel-2", 4.5},
	{"version on sidebar", "fg-faint", "bg-elev", 4.5},
	// Accent as text: links, .tabs .active, .setup-section, .brand .word b.
	{"link on card", "accent-strong", "panel", 4.5},
	{"link on page", "accent-strong", "bg", 4.5},
	// Accent as a surface: .btn.primary paints --on-accent on --accent.
	{"primary button label", "on-accent", "accent", 4.5},
	// Badges and cells paint a semantic color on its own soft background.
	{"running badge", "accent-strong", "accent-soft", 4.5},
	{"ok badge", "ok", "ok-soft", 4.5},
	{"warn badge", "warn", "warn-soft", 4.5},
	{"error badge", "danger", "danger-soft", 4.5},
	{"idle badge", "fg-muted", "bg-elev", 4.5},
	{"ok cell", "ok", "panel-2", 4.5},
	{"failed cell", "danger", "panel-2", 4.5},
	{"danger button label", "danger", "panel", 4.5},
	// Non-text (1.4.11). --border-strong is the boundary that identifies a text
	// input and a secondary button; the focus ring and progress fill have to be
	// findable against what they sit on.
	{"input border on page", "border-strong", "bg", 3.0},
	{"input border on card", "border-strong", "panel", 3.0},
	{"input border on inset", "border-strong", "panel-2", 3.0},
	{"focus ring on page", "accent", "bg", 3.0},
	{"focus ring on card", "accent", "panel", 3.0},
	{"progress fill", "accent", "bg-elev", 3.0},
}

func TestPaletteMeetsWCAGAA(t *testing.T) {
	light, dark, _, _ := palettes(t)

	for _, theme := range []struct {
		name   string
		tokens map[string]string
	}{{"light", light}, {"dark", dark}} {
		t.Run(theme.name, func(t *testing.T) {
			for _, p := range pairings {
				fg, ok := theme.tokens[p.fg]
				if !ok {
					t.Errorf("%s: token --%s not defined", theme.name, p.fg)
					continue
				}
				bg, ok := theme.tokens[p.bg]
				if !ok {
					t.Errorf("%s: token --%s not defined", theme.name, p.bg)
					continue
				}
				if r := contrastRatio(t, fg, bg); r < p.min {
					t.Errorf("%s: %s (--%s %s on --%s %s) = %.2f:1, want >= %.1f:1",
						theme.name, p.what, p.fg, fg, p.bg, bg, r, p.min)
				}
			}
		})
	}
}

// TestThemeOverridesMatchBasePalette guards the duplication app.css carries by
// design: the explicit [data-theme] blocks exist so the toggle can beat the
// media query in both directions, which means each one has to stay a verbatim
// copy of the palette it overrides. A value edited in one place and not the
// other would leave the toggled theme un-audited — and quietly off-brand.
func TestThemeOverridesMatchBasePalette(t *testing.T) {
	light, dark, lightAttr, darkAttr := palettes(t)

	for _, c := range []struct {
		name       string
		base, attr map[string]string
	}{
		{"light", light, lightAttr},
		{"dark", dark, darkAttr},
	} {
		for token, want := range c.base {
			got, ok := c.attr[token]
			if !ok {
				t.Errorf("%s: [data-theme] block is missing --%s (base defines it as %s)", c.name, token, want)
				continue
			}
			if got != want {
				t.Errorf("%s: --%s is %s in the base palette but %s in the [data-theme] block", c.name, token, want, got)
			}
		}
	}
}
