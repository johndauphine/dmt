package driver

import (
	"fmt"
	"strings"
)

// validateFinalizationDDLResponse validates an AI-generated finalization DDL
// statement (index / foreign key / check constraint). It enforces the expected
// statement kind, rejects multi-statement responses, and confirms the
// statement targets the expected table — so a prompt-injected or misbehaving
// model cannot smuggle a trailing statement (e.g. "...; DROP TABLE users;") or
// retarget another table (#561). The prompts embed untrusted source metadata
// (constraint definitions, index filters, names), so the response is treated
// as untrusted and structurally validated before execution.
func validateFinalizationDDLResponse(ddl string, req FinalizationDDLRequest, validatePrefix string) error {
	ddl = strings.TrimSpace(ddl)
	if ddl == "" {
		return fmt.Errorf("response is empty")
	}
	upperDDL := strings.ToUpper(ddl)

	// Reject SQL comments outright. A single generated finalization statement
	// never needs one, and a comment lets an attacker desync this validator
	// from the database: an unbalanced quote inside a `--`/`/* */`/`#` comment
	// (e.g. the apostrophe in "-- it's fine") would open a fake string region
	// here that swallows a following real `;` — while the DB treats the comment
	// as a comment and executes the trailing statement. This must run before
	// the statement/target scans, which are themselves comment-blind (#561).
	if hasUnquotedSQLComment(ddl) {
		return fmt.Errorf("finalization DDL must not contain SQL comments: %s", truncateString(ddl, 100))
	}

	// Reject string-literal syntax whose parsing diverges between this validator
	// and a target database, since that divergence lets a real `;` hide from the
	// single-statement scan while statement 1 stays valid and the trailing
	// statement executes. Confirmed executable bypasses: a backslash escape
	// inside a MySQL string (`'\''`, DSN has multiStatements=true) and Postgres
	// dollar-quoting (`$$'$$`, simple-query runs multiple commands). A generated
	// index/FK/check statement never needs either, and rejecting is safe on this
	// AI fallback path — it fails loud rather than losing data (#561). Only the
	// string syntax every target agrees on (`'...'` with `''` doubling, quoted/
	// bracketed identifiers) is allowed through to the scans below.
	if reason := unvalidatableStringSyntax(ddl); reason != "" {
		return fmt.Errorf("finalization DDL uses %s, which cannot be safely validated: %s", reason, truncateString(ddl, 100))
	}

	// A complete statement has balanced quotes/identifiers. Rejecting an
	// unterminated one fails with a clear message instead of a raw driver
	// syntax error, and removes any reliance on the database to refuse
	// malformed DDL that the scans above treat as "still inside a string".
	if hasUnterminatedQuote(ddl) {
		return fmt.Errorf("finalization DDL has an unterminated quoted string or identifier: %s", truncateString(ddl, 100))
	}

	// Exactly one statement — blocks trailing-statement injection regardless of
	// whether the response ends with a semicolon.
	if isMultiStatementSQL(ddl) {
		return fmt.Errorf("finalization DDL must contain exactly one SQL statement: %s", truncateString(ddl, 100))
	}

	expectedTable := targetIdentifier(req.Table.Name, req.TargetDBType)

	switch req.Type {
	case DDLTypeIndex:
		if !strings.HasPrefix(upperDDL, "CREATE") || !strings.Contains(upperDDL, "INDEX") {
			return fmt.Errorf("response does not contain valid CREATE INDEX statement: %s", truncateString(ddl, 100))
		}
		target, ok := indexTargetTable(ddl)
		if !ok {
			return fmt.Errorf("could not verify target table of CREATE INDEX statement: %s", truncateString(ddl, 100))
		}
		if !identifierEqual(target, expectedTable) {
			return fmt.Errorf("CREATE INDEX targets table %q, expected %q", target, expectedTable)
		}

	default: // DDLTypeForeignKey / DDLTypeCheckConstraint → ALTER TABLE ... ADD
		if !strings.HasPrefix(upperDDL, validatePrefix) {
			return fmt.Errorf("response does not contain valid %s statement: %s", validatePrefix, truncateString(ddl, 100))
		}
		target, ok := alterTableTargetTable(ddl)
		if !ok {
			return fmt.Errorf("could not verify target table of %s statement: %s", validatePrefix, truncateString(ddl, 100))
		}
		if !identifierEqual(target, expectedTable) {
			return fmt.Errorf("%s targets table %q, expected %q", validatePrefix, target, expectedTable)
		}
	}

	return nil
}

// unvalidatableStringSyntax returns a non-empty reason if s contains string
// syntax whose meaning differs across target databases and that this validator
// cannot faithfully model — specifically a backslash (MySQL escape ambiguity;
// also invalid outside a string) or a dollar-quote (`$tag$...$tag$`, Postgres).
// Both are unnecessary in finalization DDL, so rejecting them keeps the
// validator's quote model a safe superset of every target's.
func unvalidatableStringSyntax(s string) string {
	if strings.IndexByte(s, '\\') >= 0 {
		return "a backslash escape"
	}
	if containsDollarQuote(s) {
		return "dollar-quoting"
	}
	return ""
}

// containsDollarQuote reports whether s contains the start of a Postgres
// dollar-quoted string: a '$', an optional tag of [A-Za-z0-9_], then a '$'.
// A lone '$' in an identifier (price$, $1) is not flagged.
func containsDollarQuote(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		j := i + 1
		for j < len(s) && isDollarTagChar(s[j]) {
			j++
		}
		if j < len(s) && s[j] == '$' {
			return true
		}
	}
	return false
}

func isDollarTagChar(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// hasUnterminatedQuote reports whether s ends inside an unclosed string or
// bracketed identifier, using the same quote model as the statement/target
// scans. Such a response is not a complete statement.
func hasUnterminatedQuote(s string) bool {
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if (quote == '\'' || quote == '"') && i+1 < len(s) && s[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		}
	}
	return quote != 0
}

// hasUnquotedSQLComment reports whether s contains a SQL comment introducer
// (`--` line, `/*` block, or `#` MySQL line) outside of a quoted string or
// bracketed identifier. Comment markers inside string/identifier literals
// (e.g. a check expression `note <> '-- x'`) are not comments and are allowed.
func hasUnquotedSQLComment(s string) bool {
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if (quote == '\'' || quote == '"') && i+1 < len(s) && s[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		case '-':
			if i+1 < len(s) && s[i+1] == '-' {
				return true
			}
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				return true
			}
		case '#':
			return true
		}
	}
	return false
}

// isMultiStatementSQL reports whether s contains more than one SQL statement:
// a ';' outside of quotes/brackets followed by further non-whitespace content.
// A single optional trailing ';' is allowed, so the check does not require the
// statement to end with a semicolon.
func isMultiStatementSQL(s string) bool {
	s = strings.TrimSpace(s)
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if (quote == '\'' || quote == '"') && i+1 < len(s) && s[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '[':
			quote = ']'
		case ';':
			if strings.TrimSpace(s[i+1:]) != "" {
				return true
			}
		}
	}
	return false
}

// alterTableTargetTable extracts the table an ALTER TABLE statement targets:
// the qualified name between the TABLE keyword and ADD, returning its final
// (table) part. Tolerates ONLY / IF EXISTS prefixes and schema qualification.
func alterTableTargetTable(ddl string) (string, bool) {
	tokens := sqlIdentifierTokens(ddl)
	tableIdx := -1
	for i, tk := range tokens {
		if !tk.quoted && strings.EqualFold(tk.value, "TABLE") {
			tableIdx = i
			break
		}
	}
	if tableIdx < 0 {
		return "", false
	}
	last := ""
	for i := tableIdx + 1; i < len(tokens); i++ {
		if !tokens[i].quoted && strings.EqualFold(tokens[i].value, "ADD") {
			if last == "" {
				return "", false
			}
			return last, true
		}
		last = tokens[i].value
	}
	return "", false
}

// indexTargetTable extracts the table a CREATE INDEX statement targets: the
// qualified name following the ON keyword (skipping an optional ONLY), and
// returns its final (table) part. Tolerates schema qualification, quoting,
// USING clauses, and the column/filter parentheses that follow the table.
func indexTargetTable(ddl string) (string, bool) {
	pos, ok := keywordEnd(ddl, "ON")
	if !ok {
		return "", false
	}
	pos = skipSpaces(ddl, pos)
	if word, next := peekWord(ddl, pos); strings.EqualFold(word, "ONLY") {
		pos = skipSpaces(ddl, next)
	}
	return lastQualifiedNamePart(ddl, pos)
}

// keywordEnd finds the first standalone occurrence of kw (case-insensitive,
// outside quotes/brackets) in s and returns the index just past it.
func keywordEnd(s, kw string) (int, bool) {
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if (quote == '\'' || quote == '"') && i+1 < len(s) && s[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
			continue
		case '[':
			quote = ']'
			continue
		}
		if isIdentifierStart(c) {
			j := i + 1
			for j < len(s) && isIdentifierPart(s[j]) {
				j++
			}
			if strings.EqualFold(s[i:j], kw) {
				return j, true
			}
			i = j - 1
		}
	}
	return 0, false
}

// lastQualifiedNamePart reads a (possibly schema-qualified, possibly quoted)
// identifier starting at i and returns its final part, e.g. schema.table ->
// "table". Reading stops at the first character that is neither part of an
// identifier nor a '.' separator (whitespace, '(', etc.).
func lastQualifiedNamePart(s string, i int) (string, bool) {
	last := ""
	for i < len(s) {
		var part string
		switch c := s[i]; {
		case c == '"', c == '`':
			part, i = readQuotedIdentifier(s, i, c)
		case c == '[':
			part, i = readBracketIdentifier(s, i)
		case isIdentifierStart(c):
			part, i = peekWord(s, i)
		default:
			if last == "" {
				return "", false
			}
			return last, true
		}
		last = part
		if i < len(s) && s[i] == '.' {
			i++
			continue
		}
		break
	}
	if last == "" {
		return "", false
	}
	return last, true
}

func skipSpaces(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func peekWord(s string, i int) (string, int) {
	if i >= len(s) || !isIdentifierStart(s[i]) {
		return "", i
	}
	j := i + 1
	for j < len(s) && isIdentifierPart(s[j]) {
		j++
	}
	return s[i:j], j
}
