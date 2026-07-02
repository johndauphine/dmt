package ident

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitizePG(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty", "", "col_"},
		{"simple lowercase", "userid", "userid"},
		{"uppercase", "PackedByPersonID", "packedbypersonid"},
		{"with underscore", "last_edited_by", "last_edited_by"},
		{"special chars", "User-Id", "user_id"},
		{"starts with digit", "1column", "col_1column"},
		{"accented chars", "Ñoño", "ñoño"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizePG(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizePG(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// suffixRe matches the disambiguating "_<8 hex>" suffix appended on truncation.
var suffixRe = regexp.MustCompile(`_[0-9a-f]{8}$`)

func TestSanitizePGTruncatesTo63Bytes(t *testing.T) {
	// Regression for #553: PostgreSQL silently truncates identifiers to 63
	// bytes, so two long names identical in the first 63 bytes would collapse
	// into the same relation and destroy each other's data in drop_recreate.
	// SanitizePG must truncate to <=63 bytes with a disambiguating suffix.

	// 64-byte ASCII name (over the limit by one).
	long := strings.Repeat("a", 64)
	got := SanitizePG(long)
	if len(got) > maxPGIdentBytes {
		t.Fatalf("SanitizePG(64 a's) = %q (%d bytes), want <= %d", got, len(got), maxPGIdentBytes)
	}
	if !suffixRe.MatchString(got) {
		t.Fatalf("expected disambiguating hash suffix, got %q", got)
	}

	// A name that is exactly 63 bytes must be left untouched.
	exact := strings.Repeat("b", 63)
	if got := SanitizePG(exact); got != exact {
		t.Fatalf("SanitizePG(63 b's) = %q, want it unchanged", got)
	}
}

func TestSanitizePGDisambiguatesLongNamesSharingPrefix(t *testing.T) {
	// Two names that share the first 63 bytes but differ afterwards must
	// sanitize to *different* identifiers (the hash suffix is over the full
	// original name), otherwise PostgreSQL truncation collides them.
	prefix := strings.Repeat("a", 60)
	one := SanitizePG(prefix + "_ONE_suffix")
	two := SanitizePG(prefix + "_TWO_suffix")

	if len(one) > maxPGIdentBytes || len(two) > maxPGIdentBytes {
		t.Fatalf("truncated names exceed limit: %q (%d), %q (%d)", one, len(one), two, len(two))
	}
	if one == two {
		t.Fatalf("distinct long names collided after truncation: both -> %q", one)
	}

	// Deterministic: same input always yields the same output.
	if again := SanitizePG(prefix + "_ONE_suffix"); again != one {
		t.Fatalf("SanitizePG not deterministic: %q vs %q", one, again)
	}
}

func TestSanitizePGTruncationNeverSplitsRune(t *testing.T) {
	// PostgreSQL counts bytes, but truncating mid-rune would corrupt the
	// identifier. 40 'é' = 80 bytes / 40 runes; truncation must land on a
	// rune boundary and stay valid UTF-8 within 63 bytes.
	got := SanitizePG(strings.Repeat("é", 40))
	if len(got) > maxPGIdentBytes {
		t.Fatalf("truncated multibyte name = %d bytes, want <= %d", len(got), maxPGIdentBytes)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncation split a rune, result not valid UTF-8: %q", got)
	}
}
