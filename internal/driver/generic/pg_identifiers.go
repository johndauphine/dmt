package generic

import (
	"strings"
	"unicode"
)

// sanitizePGIdentifier converts an identifier to PostgreSQL-friendly lowercase format.
// Simply lowercases and replaces special chars with underscores.
// Example: VoteTypes -> votetypes, UserId -> userid, User-Id -> user_id
func sanitizePGIdentifier(ident string) string {
	if ident == "" {
		return "col_"
	}
	s := strings.ToLower(ident)
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()
	// Prefix with col_ if starts with digit
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}
	if s == "" {
		return "col_"
	}
	return s
}

// sanitizePGTableName is an alias for sanitizePGIdentifier for table names.
func sanitizePGTableName(ident string) string {
	return sanitizePGIdentifier(ident)
}
