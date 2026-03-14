// Package ident provides identifier sanitization shared across driver and target packages.
package ident

import (
	"strings"
	"unicode"
)

// SanitizePG converts an identifier to PostgreSQL-friendly lowercase format.
// Lowercases, replaces non-alphanumeric/underscore chars with underscores,
// and prefixes digit-leading names with "col_".
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
	return s
}
