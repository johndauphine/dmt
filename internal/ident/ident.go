// Package ident provides identifier sanitization shared across driver and target packages.
package ident

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxPGIdentBytes is PostgreSQL's identifier length limit (NAMEDATALEN-1).
// PostgreSQL silently truncates longer identifiers to this many bytes.
const maxPGIdentBytes = 63

// SanitizePG converts an identifier to PostgreSQL-friendly lowercase format.
// Lowercases, replaces non-alphanumeric/underscore chars with underscores,
// and prefixes digit-leading names with "col_".
//
// Names longer than 63 bytes are truncated (at a rune boundary) with a short
// hash of the full original appended, so two long names that PostgreSQL would
// otherwise truncate to the same relation stay distinct. This does not by
// itself resolve collisions between *short* names that sanitize identically
// (e.g. "Order Items" vs "Order-Items") — those are detected at the set level
// by target.DetectPGIdentifierCollisions.
//
// Examples: VoteTypes -> votetypes, UserId -> userid, User-Id -> user_id
func SanitizePG(name string) string {
	if name == "" {
		return "col_"
	}
	s := strings.ToLower(name)
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}
	if s == "" {
		return "col_"
	}
	if len(s) > maxPGIdentBytes {
		s = truncatePGIdent(s, name)
	}
	return s
}

// truncatePGIdent shortens an over-length sanitized identifier to fit in 63
// bytes, appending "_" + an 8-hex-char hash of the full original name as a
// disambiguating suffix. The prefix is cut on a rune boundary so multi-byte
// letters (PostgreSQL counts bytes, not runes) are never split.
func truncatePGIdent(s, original string) string {
	const hashLen = 8
	// Reserve "_" + hashLen bytes for the suffix.
	budget := maxPGIdentBytes - (1 + hashLen)

	sum := sha256.Sum256([]byte(original))
	suffix := "_" + hex.EncodeToString(sum[:])[:hashLen]

	var b strings.Builder
	used := 0
	for _, r := range s {
		rl := utf8.RuneLen(r)
		if used+rl > budget {
			break
		}
		b.WriteRune(r)
		used += rl
	}
	return b.String() + suffix
}
