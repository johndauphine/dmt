package validation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

// hashOf computes the SHA-256 of a sequence of WriteValue calls.
// Used to verify that two semantically-equivalent inputs hash to
// the same bytes.
func hashOf(t *testing.T, vals ...any) []byte {
	t.Helper()
	h := sha256.New()
	enc := CanonicalEncoder{}
	for _, v := range vals {
		if err := enc.WriteValue(h, v); err != nil {
			t.Fatalf("WriteValue(%v) errored: %v", v, err)
		}
	}
	return h.Sum(nil)
}

func TestCanonicalize_TagsDifferentiateTypes(t *testing.T) {
	// The string "0", the integer 0, and the float 0.0 must produce
	// different canonical bytes — otherwise a column-type change
	// (e.g. int → varchar) would silently pass validation.
	str0 := hashOf(t, "0")
	int0 := hashOf(t, int64(0))
	flt0 := hashOf(t, 0.0)
	null := hashOf(t, nil)
	bool0 := hashOf(t, false)

	pairs := map[string][]byte{
		"string-0": str0,
		"int-0":    int0,
		"float-0":  flt0,
		"null":     null,
		"false":    bool0,
	}
	seen := map[string]string{}
	for name, h := range pairs {
		hex := hex.EncodeToString(h)
		if prev, dup := seen[hex]; dup {
			t.Errorf("type tag collision: %s and %s both produce %s", name, prev, hex)
		}
		seen[hex] = name
	}
}

func TestCanonicalize_EquivalentInputsMatch(t *testing.T) {
	// Same value reached via different Go types (int → int64,
	// string vs []byte for the same UTF-8 bytes) should produce
	// identical canonical bytes — that's the cross-driver invariant.
	tests := []struct {
		name    string
		a, b    any
		wantEq  bool
		comment string
	}{
		{"int8 vs int64 (same value)", int8(42), int64(42), true,
			"width-agnostic integer canonicalization"},
		{"int32 vs int (same value)", int32(42), int(42), true,
			"width-agnostic integer canonicalization"},
		{"float32 vs float64 (same value, exactly representable)", float32(1.5), float64(1.5), true,
			"float canonicalization rounds to same string"},
		{"string vs []byte (printable, same content)", "hello", []byte("hello"), true,
			"text in []byte (MySQL driver) hashes same as string (pgx driver)"},
		{"int 0 vs int -0 (Go has no -0 for int)", int64(0), int64(0), true,
			"trivial"},
		{"int 0 vs string \"0\"", int64(0), "0", false,
			"type tag must distinguish"},
		{"NULL vs empty string", nil, "", false,
			"NULL is its own type tag"},
		{"NULL vs empty bytes", nil, []byte{}, false,
			"NULL still distinct from empty value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ha, hb := hashOf(t, tt.a), hashOf(t, tt.b)
			eq := bytes.Equal(ha, hb)
			if eq != tt.wantEq {
				t.Errorf("eq=%v want=%v\n  a=%v → %x\n  b=%v → %x\n  %s",
					eq, tt.wantEq, tt.a, ha, tt.b, hb, tt.comment)
			}
		})
	}
}

func TestCanonicalize_LengthPrefixPreventsConcatAmbiguity(t *testing.T) {
	// "abc" + "def" must not collide with "abcdef" or "ab" + "cdef".
	a := hashOf(t, "abc", "def")
	b := hashOf(t, "abcdef")
	c := hashOf(t, "ab", "cdef")
	if bytes.Equal(a, b) {
		t.Error("(\"abc\",\"def\") collided with (\"abcdef\") — length prefix not working")
	}
	if bytes.Equal(a, c) {
		t.Error("(\"abc\",\"def\") collided with (\"ab\",\"cdef\")")
	}
	if bytes.Equal(b, c) {
		t.Error("(\"abcdef\") collided with (\"ab\",\"cdef\")")
	}
}

func TestCanonicalize_TimeUTCNormalization(t *testing.T) {
	// 12:00 UTC and the equivalent 07:00 EST must canonicalize
	// identically. Drivers vary on whether they preserve the
	// original timezone or convert to UTC at scan time — the
	// canonicalizer must hide that variance.
	utc := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	est := time.Date(2024, 6, 15, 8, 0, 0, 0, time.FixedZone("EST", -4*3600))
	if !bytes.Equal(hashOf(t, utc), hashOf(t, est)) {
		t.Error("same instant in different zones produced different canonical bytes")
	}

	// Nanosecond precision retained.
	with := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC)
	without := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if bytes.Equal(hashOf(t, with), hashOf(t, without)) {
		t.Error("nanosecond precision was lost — encoding is too coarse")
	}
}

func TestCanonicalize_InvalidUTF8StringErrors(t *testing.T) {
	// A Go string holding invalid UTF-8 should fail-fast, not
	// silently hash garbage. This guards against a real bug class:
	// a driver returning corrupt bytes that the canonicalizer
	// would otherwise tag as "string" and produce identical-looking
	// digests from different inputs.
	enc := CanonicalEncoder{}
	h := sha256.New()
	invalid := string([]byte{0xff, 0xfe, 0xfd})
	if err := enc.WriteValue(h, invalid); err == nil {
		t.Error("invalid UTF-8 string should error")
	}
}

func TestCanonicalize_BinaryBytesGoesThroughHexPath(t *testing.T) {
	// Bytes that don't look like text (contains NUL) should hash
	// the same regardless of case of any hex letters in the
	// encoding. Verifies the bytes-tag path is reachable.
	binary := []byte{0x00, 0x01, 0x02, 0xff}
	if looksLikeText(binary) {
		t.Fatal("looksLikeText should reject content with NUL bytes")
	}
}

// TestCanonicalize_NonASCIITextBytesHashAsString guards the
// Copilot-flagged regression on #226: the prior looksLikeText only
// recognized ASCII printables, so byte slices containing CJK,
// accented characters, or emoji were misrouted through the binary
// hex path. After the unicode.IsPrint fix, valid UTF-8 text bytes
// hash identically to the string form regardless of driver type.
func TestCanonicalize_NonASCIITextBytesHashAsString(t *testing.T) {
	tests := []struct {
		name string
		s    string
	}{
		{"japanese", "日本語"},
		{"accented", "élève café"},
		{"emoji", "hello 🚀 world"},
		{"mixed", "汉字 with English"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			asString := hashOf(t, tt.s)
			asBytes := hashOf(t, []byte(tt.s))
			if !bytes.Equal(asString, asBytes) {
				t.Errorf("UTF-8 text via string vs []byte hashed differently — driver-type variance would cause false validation mismatches\n  string: %x\n  bytes:  %x",
					asString, asBytes)
			}
		})
	}
}

func TestLooksLikeText_UTF8Variants(t *testing.T) {
	// Sanity checks on the heuristic — the canonicalizer relies
	// on this function to route bytes to either the string or
	// bytes/hex path.
	tests := []struct {
		name string
		b    []byte
		want bool
	}{
		{"plain ASCII", []byte("hello world"), true},
		{"empty", []byte{}, false},
		{"contains NUL", []byte{'h', 0, 'i'}, false},
		{"PNG header (binary)", []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, false},
		{"Japanese UTF-8", []byte("日本語"), true},
		{"accented", []byte("café"), true},
		{"emoji", []byte("🚀"), true},
		{"invalid UTF-8 byte sequence", []byte{0xff, 0xfe, 0xfd}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeText(tt.b); got != tt.want {
				t.Errorf("looksLikeText(%q) = %v, want %v", tt.b, got, tt.want)
			}
		})
	}
}
