// DEFAULT expression translation across dialects.
//
// Source DDL DEFAULT expressions carry vendor-specific syntax that
// doesn't survive verbatim transfer:
//
//	PG     "0::integer"                  ← type cast
//	PG     "'hello'::character varying"  ← type cast on a literal
//	MSSQL  "((0))"                       ← double parens around literal
//	MSSQL  "(N'hello')"                  ← N prefix + parens
//	MySQL  "now()" / "CURRENT_TIMESTAMP" ← function name varies
//
// FormatDDLDefault is the entry point. It strips dialect-specific syntax
// from the source expression, optionally translates 0/1 ↔ true/false
// for boolean columns, translates common function names to the target
// dialect, and ensures the result is properly quoted.
//
// Ported from UVG ddl.rs lines 374-498 + the strip helpers in
// codegen/mod.rs lines 154-201.

package ddl

import (
	"strings"
)

// FormatDDLDefault returns a target-dialect DEFAULT expression derived
// from the source's verbatim default. The fourth argument carries the
// boolean-column hint so 0 / 1 / true / false get translated correctly
// (they look identical to integers otherwise).
//
// Empty input returns empty — caller should guard against this and not
// emit "DEFAULT " for columns with no default.
func FormatDDLDefault(sourceDefault, sourceDialect, targetDialect string, isBoolean bool) string {
	cleaned := stripSourceSyntax(sourceDefault, sourceDialect)

	if isBoolean {
		if translated, ok := translateBooleanLiteral(cleaned, targetDialect); ok {
			return translated
		}
	}

	translated := translateDefaultFunction(cleaned, targetDialect)
	return ensureDefaultQuoting(translated)
}

// stripSourceSyntax strips dialect-specific decoration from a default
// expression so the rest of the translator sees the bare value.
//
//	PG    → strip ::TYPE casts (recursive: handles nested casts)
//	MSSQL → strip wrapping parens and the N'…' prefix on Unicode literals
//	MySQL → just trim whitespace; defaults are stored as the raw value
func stripSourceSyntax(expr, sourceDialect string) string {
	switch sourceDialect {
	case DialectPostgres:
		return stripPostgresTypecast(expr)
	case DialectMSSQL:
		return stripMSSQLParens(expr)
	default:
		return strings.TrimSpace(expr)
	}
}

// stripPostgresTypecast removes ::TYPE casts from a default expression.
// Postgres reports DEFAULT 0 as "0::integer" and DEFAULT 'hello' as
// "'hello'::character varying" — the cast metadata is the type system's
// concern, not the value's.
//
// The implementation finds the LAST :: that's not inside quotes or
// parens (so we don't strip a :: inside a function call's argument)
// and trims everything from that position on. Mirrors UVG's
// find_typecast_pos byte-walk.
func stripPostgresTypecast(expr string) string {
	pos := findTypecastPos(expr)
	if pos < 0 {
		return strings.TrimSpace(expr)
	}
	return strings.TrimSpace(expr[:pos])
}

// findTypecastPos walks the expression byte-by-byte, tracking quote and
// paren depth, and returns the byte index of the last `::` that's
// outside both. Returns -1 when no such `::` exists.
func findTypecastPos(expr string) int {
	inQuotes := false
	parenDepth := 0
	lastCast := -1

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		switch {
		case ch == '\'':
			inQuotes = !inQuotes
		case ch == '(' && !inQuotes:
			parenDepth++
		case ch == ')' && !inQuotes:
			if parenDepth > 0 {
				parenDepth--
			}
		case ch == ':' && !inQuotes && parenDepth == 0:
			if i+1 < len(expr) && expr[i+1] == ':' {
				lastCast = i
				i++ // skip the second ':'
			}
		}
	}
	return lastCast
}

// stripMSSQLParens removes the wrapping parentheses MSSQL adds to
// almost every stored default ("((0))" instead of "0") and the N
// prefix on Unicode string literals ("(N'hello')" → "'hello'"). MSSQL's
// information_schema returns defaults in this decorated form even
// though the source DDL didn't necessarily write them that way.
//
// Critical: only strip an outer pair when it actually encloses the
// ENTIRE remaining expression, not just when the string starts with `(`
// and ends with `)`. Without that check, "((1)+(2))" would over-strip
// to "1)+(2" — the first inner ')' rebalances the depth before the
// final '(' on the right side (Codex review on PR #188). Quote state is
// tracked so a `)` inside a string literal doesn't fool the depth
// counter.
func stripMSSQLParens(expr string) string {
	s := strings.TrimSpace(expr)
	for outerParensWrapWhole(s) {
		s = s[1 : len(s)-1]
	}
	if strings.HasPrefix(s, "N'") || strings.HasPrefix(s, `N"`) {
		s = s[1:]
	}
	return strings.TrimSpace(s)
}

// outerParensWrapWhole returns true when s starts with '(' and the
// matching ')' for that opening paren is the final character. Walks
// paren depth + quote state byte-by-byte; the matching close drops
// depth back to zero — if that happens before the end, the outer pair
// does NOT enclose the whole expression and stripping would corrupt it.
func outerParensWrapWhole(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	inQuote := false
	depth := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '\'':
			inQuote = !inQuote
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
			// If depth hit zero before the last char, the outer '(' was
			// already closed — the trailing ')' belongs to a different
			// pair. Stripping would merge two unrelated parts.
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// translateBooleanLiteral handles the 0/1 ↔ true/false translation for
// boolean columns. Returns (value, true) when a translation applies;
// (_, false) means "not a boolean literal, fall through to normal
// translation."
//
// Postgres prefers the keyword form (true/false). MSSQL and MySQL store
// booleans as 1/0 (BIT and TINYINT(1) respectively); their DEFAULT
// clauses accept the integer form.
func translateBooleanLiteral(cleaned, targetDialect string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(cleaned))
	switch lower {
	case "1", "true":
		if targetDialect == DialectPostgres {
			return "true", true
		}
		if targetDialect == DialectMSSQL || targetDialect == DialectMySQL {
			return "1", true
		}
	case "0", "false":
		if targetDialect == DialectPostgres {
			return "false", true
		}
		if targetDialect == DialectMSSQL || targetDialect == DialectMySQL {
			return "0", true
		}
	}
	return "", false
}

// translateDefaultFunction maps common SQL functions across dialects.
// Currently covers the two most-used cases: current-timestamp and UUID
// generation. Anything else passes through unchanged (string literals,
// numeric literals, and vendor-specific functions are caller's problem
// — those route to AI fallback at the wiring layer when needed).
func translateDefaultFunction(expr, targetDialect string) string {
	lower := strings.ToLower(strings.TrimSpace(expr))

	if isCurrentTimestampSynonym(lower) {
		switch targetDialect {
		case DialectPostgres:
			return "now()"
		case DialectMySQL:
			return "CURRENT_TIMESTAMP"
		case DialectMSSQL:
			return "GETDATE()"
		}
	}

	if isUUIDGeneratorSynonym(lower) {
		switch targetDialect {
		case DialectPostgres:
			return "gen_random_uuid()"
		case DialectMySQL:
			return "(UUID())"
		case DialectMSSQL:
			return "NEWID()"
		}
	}

	return expr
}

func isCurrentTimestampSynonym(lower string) bool {
	switch lower {
	case "now()", "current_timestamp", "getdate()", "sysdatetimeoffset()", "sysdatetime()":
		return true
	}
	return false
}

func isUUIDGeneratorSynonym(lower string) bool {
	switch lower {
	case "gen_random_uuid()", "uuid()", "newid()":
		return true
	}
	return false
}

// ensureDefaultQuoting wraps a bare string default in single quotes.
// MySQL stores string defaults without quotes (a DEFAULT 'member'
// shows up as "member" in information_schema), so the value needs
// re-quoting before re-emission. Numbers, NULL, boolean keywords,
// function calls (anything containing parens), and already-quoted
// strings pass through unchanged.
func ensureDefaultQuoting(expr string) string {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return ""
	}

	if isAlreadyQuoted(trimmed) || isUnquotedKeywordDefault(trimmed) {
		return trimmed
	}

	if strings.Contains(trimmed, "(") {
		return trimmed
	}

	if isNumericLiteral(trimmed) {
		return trimmed
	}

	return "'" + strings.ReplaceAll(trimmed, "'", "''") + "'"
}

func isAlreadyQuoted(s string) bool {
	if len(s) < 2 {
		return false
	}
	first, last := s[0], s[len(s)-1]
	return (first == '\'' && last == '\'') || (first == '"' && last == '"')
}

// isUnquotedKeywordDefault recognises SQL keywords that are valid as
// bare default expressions (no quoting needed). Matches case-
// insensitively.
func isUnquotedKeywordDefault(s string) bool {
	upper := strings.ToUpper(s)
	switch upper {
	case "NULL",
		"TRUE", "FALSE",
		"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_USER":
		return true
	}
	return false
}

// isNumericLiteral returns true when s parses as a finite number. Used
// to skip quoting for integer / decimal defaults (DEFAULT 0, DEFAULT
// 3.14). Sign is allowed.
func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	hasDigit := false
	hasDot := false
	hasExp := false
	for i, ch := range s {
		switch {
		case ch >= '0' && ch <= '9':
			hasDigit = true
		case ch == '.' && !hasDot && !hasExp:
			hasDot = true
		case (ch == 'e' || ch == 'E') && hasDigit && !hasExp:
			hasExp = true
		case (ch == '+' || ch == '-') && (i == 0 || s[i-1] == 'e' || s[i-1] == 'E'):
			// signed prefix or exponent sign
		default:
			return false
		}
	}
	return hasDigit
}
