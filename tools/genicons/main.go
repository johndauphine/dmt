// Command genicons renders the dmt PWA/desktop icon set as PNGs, reproducing
// the "d" monogram from the WebUI login card (internal/webui/static/app.js,
// the .brand .logo rule in app.css) using only the standard library
// (image/color/draw/png + math) — no image tooling or external assets, matching
// the project's zero-external-dependency posture (see CLAUDE.md).
//
// Run manually and commit the output:
//
//	go run ./tools/genicons
//
// It writes into internal/webui/static/, which is embedded into the dmt
// binary by internal/webui/assets.go.
package main

import (
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"path/filepath"
)

// accent / accentStrong reproduce --accent / --accent-strong from app.css's
// light theme; onAccent is --on-accent (white). The icons use one fixed
// palette rather than following the browser's color scheme, the same way any
// static app icon does.
var (
	accent       = color.RGBA{0x11, 0x98, 0xb6, 0xff}
	accentStrong = color.RGBA{0x0e, 0x7d, 0x97, 0xff}
	onAccent     = color.RGBA{0xff, 0xff, 0xff, 0xff}
)

// icon is one output file: size in pixels and how much of the canvas the "d"
// glyph is drawn within. Maskable icons must keep all content inside a
// centered "safe zone" (Google's maskable-icon guidance: ~80% of the canvas)
// since the OS applies its own mask/crop shape outside it; regular icons and
// apple-touch-icon use the full canvas. Per Apple's HIG, apple-touch-icon
// must be a plain full-bleed square with no pre-baked rounding or
// transparency — true of every icon here, since iOS/Android both apply their
// own corner masking on top.
type icon struct {
	name         string
	size         int
	safeZoneFrac float64
}

var icons = []icon{
	{"icon-192.png", 192, 1.0},
	{"icon-512.png", 512, 1.0},
	{"icon-512-maskable.png", 512, 0.8},
	{"apple-touch-icon.png", 180, 1.0},
}

func main() {
	outDir := filepath.Join("internal", "webui", "static")
	if _, err := os.Stat(outDir); err != nil {
		fmt.Fprintf(os.Stderr, "genicons: %s not found — run from the repository root: %v\n", outDir, err)
		os.Exit(1)
	}
	for _, ic := range icons {
		img := render(ic.size, ic.safeZoneFrac)
		path := filepath.Join(outDir, ic.name)
		if err := writePNG(path, img); err != nil {
			fmt.Fprintf(os.Stderr, "genicons: %s: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Println("wrote", path)
	}
}

func writePNG(path string, img image.Image) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

// render draws one icon: a full-bleed diagonal-gradient square (matching the
// CSS `linear-gradient(150deg, --accent, --accent-strong)`) with a white "d"
// monogram — a filled ring ("o" bowl) plus a vertical stem — confined to the
// centered safeZoneFrac fraction of the canvas. Edges are supersampled 2x for
// anti-aliasing since the glyph is drawn from circles at icon resolutions
// small enough that hard edges would look noticeably jagged.
func render(size int, safeZoneFrac float64) *image.RGBA {
	const supersample = 2
	hi := size * supersample
	big := image.NewRGBA(image.Rect(0, 0, hi, hi))

	// linear-gradient(150deg, ...): CSS angles are measured clockwise from
	// "up". Convert to a unit direction vector in image space (x right, y
	// down) for a simple dot-product gradient.
	const angleDeg = 150.0
	rad := (angleDeg - 180) * math.Pi / 180 // CSS 180deg = left-to-right in image space
	dir := [2]float64{math.Sin(rad), -math.Cos(rad)}

	fsz := float64(hi)
	for y := 0; y < hi; y++ {
		for x := 0; x < hi; x++ {
			// Project (x,y) onto the gradient axis, normalized to [0,1]
			// across the canvas diagonal extent along that axis.
			nx, ny := float64(x)/fsz, float64(y)/fsz
			t := nx*dir[0] + ny*dir[1]
			t = clamp01((t + 1) / 2)
			big.Set(x, y, lerpColor(accent, accentStrong, t))
		}
	}

	// Glyph geometry, all relative to a centered safe-zone box so maskable
	// padding only shrinks the box the glyph is drawn within.
	box := fsz * safeZoneFrac
	pad := (fsz - box) / 2
	cx, cy := fsz/2, pad+box*0.56 // bowl center biased slightly down, like a real 'd'
	bowlOuter := box * 0.30
	stroke := box * 0.15
	bowlInner := bowlOuter - stroke
	stemW := stroke
	stemX0 := cx + bowlOuter - stemW*0.9 // stem overlaps the bowl's right edge
	stemX1 := stemX0 + stemW
	stemY0 := pad + box*0.08
	stemY1 := cy + bowlOuter*0.55

	for y := 0; y < hi; y++ {
		for x := 0; x < hi; x++ {
			fx, fy := float64(x)+0.5, float64(y)+0.5
			d := math.Hypot(fx-cx, fy-cy)
			inBowl := d <= bowlOuter && d >= bowlInner
			inStem := fx >= stemX0 && fx <= stemX1 && fy >= stemY0 && fy <= stemY1
			if inBowl || inStem {
				big.Set(x, y, onAccent)
			}
		}
	}

	return downsample(big, size, supersample)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func lerpColor(a, b color.RGBA, t float64) color.RGBA {
	l := func(x, y uint8) uint8 { return uint8(float64(x) + (float64(y)-float64(x))*t) }
	return color.RGBA{l(a.R, b.R), l(a.G, b.G), l(a.B, b.B), 0xff}
}

// downsample box-filters a supersample x supersample block per output pixel,
// the anti-aliasing step for the hard-edged shapes drawn in render.
func downsample(src *image.RGBA, size, supersample int) *image.RGBA {
	out := image.NewRGBA(image.Rect(0, 0, size, size))
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			var r, g, b, a int
			for sy := 0; sy < supersample; sy++ {
				for sx := 0; sx < supersample; sx++ {
					c := src.RGBAAt(x*supersample+sx, y*supersample+sy)
					r += int(c.R)
					g += int(c.G)
					b += int(c.B)
					a += int(c.A)
				}
			}
			n := supersample * supersample
			out.SetRGBA(x, y, color.RGBA{uint8(r / n), uint8(g / n), uint8(b / n), uint8(a / n)})
		}
	}
	return out
}
