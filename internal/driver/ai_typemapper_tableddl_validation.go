package driver

import (
	"fmt"
	"strings"
)

type tableColumnSegment struct {
	name    string
	segment string
}

type sqlIdentToken struct {
	value  string
	quoted bool
}

func validateTableDDLResponse(ddl string, req TableDDLRequest) error {
	ddl = strings.TrimSpace(ddl)
	if ddl == "" {
		return fmt.Errorf("response is empty")
	}
	upperDDL := strings.ToUpper(ddl)
	if !strings.HasPrefix(upperDDL, "CREATE TABLE") {
		return fmt.Errorf("response does not contain valid CREATE TABLE statement: %s", truncateString(ddl, 100))
	}
	if !strings.HasSuffix(ddl, ";") {
		return fmt.Errorf("CREATE TABLE response must end with a semicolon")
	}
	if hasSemicolonBeforeFinal(ddl) {
		return fmt.Errorf("CREATE TABLE response must contain exactly one SQL statement")
	}

	tableParts, err := extractCreateTableNameParts(ddl)
	if err != nil {
		return err
	}
	expectedTable := targetIdentifier(req.SourceTable.Name, req.TargetDBType)
	if len(tableParts) == 0 || !identifierEqual(tableParts[len(tableParts)-1], expectedTable) {
		return fmt.Errorf("expected target table %q in AI DDL, got %q", expectedTable, strings.Join(tableParts, "."))
	}
	if req.TargetSchema != "" && (len(tableParts) < 2 || !identifierEqual(tableParts[len(tableParts)-2], req.TargetSchema)) {
		return fmt.Errorf("expected target schema %q in AI DDL, got %q", req.TargetSchema, strings.Join(tableParts, "."))
	}

	segments, err := extractCreateTableSegments(ddl)
	if err != nil {
		return err
	}
	actualColumns := make(map[string]tableColumnSegment)
	for _, segment := range segments {
		token, ok := leadingSQLIdentifier(segment)
		if !ok {
			continue
		}
		if !token.quoted && isTableConstraintKeyword(token.value) {
			if containsForbiddenTableConstraint(segment) {
				return fmt.Errorf("AI DDL includes disallowed constraint/index clause: %s", truncateString(strings.TrimSpace(segment), 120))
			}
			continue
		}
		if containsUnquotedKeyword(segment, "CHECK") || containsUnquotedKeyword(segment, "REFERENCES") ||
			containsUnquotedKeyword(segment, "FOREIGN") {
			return fmt.Errorf("AI DDL includes disallowed inline constraint on column %q", token.value)
		}
		actualColumns[identifierKey(token.value)] = tableColumnSegment{name: token.value, segment: segment}
	}

	expectedColumns := make(map[string]string, len(req.SourceTable.Columns))
	for _, col := range req.SourceTable.Columns {
		targetName := targetIdentifier(col.Name, req.TargetDBType)
		expectedColumns[identifierKey(targetName)] = targetName
		if _, ok := actualColumns[identifierKey(targetName)]; !ok {
			return fmt.Errorf("AI DDL missing expected column %q", targetName)
		}
	}
	for key, actual := range actualColumns {
		if _, ok := expectedColumns[key]; !ok {
			return fmt.Errorf("AI DDL contains unexpected column %q", actual.name)
		}
	}

	if len(req.SourceTable.PrimaryKey) > 0 && !containsUnquotedKeywordSequence(ddl, "PRIMARY", "KEY") {
		return fmt.Errorf("AI DDL missing PRIMARY KEY constraint")
	}
	for _, pk := range req.SourceTable.PrimaryKey {
		targetPK := targetIdentifier(pk, req.TargetDBType)
		col, ok := actualColumns[identifierKey(targetPK)]
		if !ok {
			return fmt.Errorf("AI DDL missing primary key column %q", targetPK)
		}
		if !containsUnquotedKeywordSequence(col.segment, "NOT", "NULL") &&
			!containsUnquotedKeywordSequence(col.segment, "PRIMARY", "KEY") {
			return fmt.Errorf("AI DDL primary key column %q must be NOT NULL", targetPK)
		}
	}

	return nil
}

func extractCreateTableNameParts(ddl string) ([]string, error) {
	open, err := findFirstOpenParen(ddl)
	if err != nil {
		return nil, err
	}
	header := strings.TrimSpace(ddl[:open])
	rest, ok := stripCreateTablePrefix(header)
	if !ok {
		return nil, fmt.Errorf("response does not contain valid CREATE TABLE statement: %s", truncateString(ddl, 100))
	}
	rest = stripIfNotExists(rest)
	tokens := sqlIdentifierTokens(rest)
	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		parts = append(parts, token.value)
	}
	return parts, nil
}

func extractCreateTableSegments(ddl string) ([]string, error) {
	open, err := findFirstOpenParen(ddl)
	if err != nil {
		return nil, err
	}
	close, err := findMatchingParen(ddl, open)
	if err != nil {
		return nil, err
	}
	return splitTopLevelComma(ddl[open+1 : close]), nil
}

func stripCreateTablePrefix(s string) (string, bool) {
	fields := strings.Fields(s)
	if len(fields) < 3 || !strings.EqualFold(fields[0], "CREATE") || !strings.EqualFold(fields[1], "TABLE") {
		return "", false
	}
	idx := strings.Index(strings.ToUpper(s), strings.ToUpper(fields[2]))
	if idx < 0 {
		return "", false
	}
	return strings.TrimSpace(s[idx:]), true
}

func stripIfNotExists(s string) string {
	fields := strings.Fields(s)
	if len(fields) >= 4 &&
		strings.EqualFold(fields[0], "IF") &&
		strings.EqualFold(fields[1], "NOT") &&
		strings.EqualFold(fields[2], "EXISTS") {
		idx := strings.Index(strings.ToUpper(s), strings.ToUpper(fields[3]))
		if idx >= 0 {
			return strings.TrimSpace(s[idx:])
		}
	}
	return s
}

func findFirstOpenParen(s string) (int, error) {
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
		case '(':
			return i, nil
		}
	}
	return -1, fmt.Errorf("CREATE TABLE response missing column list")
}

func findMatchingParen(s string, open int) (int, error) {
	depth := 0
	quote := byte(0)
	for i := open; i < len(s); i++ {
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
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("CREATE TABLE response has unbalanced parentheses")
}

func splitTopLevelComma(s string) []string {
	var parts []string
	start := 0
	depth := 0
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
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(s[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func hasSemicolonBeforeFinal(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) == 0 || trimmed[len(trimmed)-1] != ';' {
		return false
	}
	quote := byte(0)
	for i := 0; i < len(trimmed)-1; i++ {
		c := trimmed[i]
		if quote != 0 {
			if c == quote {
				if (quote == '\'' || quote == '"') && i+1 < len(trimmed) && trimmed[i+1] == quote {
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
			return true
		}
	}
	return false
}

func leadingSQLIdentifier(s string) (sqlIdentToken, bool) {
	tokens := sqlIdentifierTokens(s)
	if len(tokens) == 0 {
		return sqlIdentToken{}, false
	}
	return tokens[0], true
}

func sqlIdentifierTokens(s string) []sqlIdentToken {
	var tokens []sqlIdentToken
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\'':
			i = skipQuoted(s, i, '\'')
		case c == '"':
			value, next := readQuotedIdentifier(s, i, '"')
			tokens = append(tokens, sqlIdentToken{value: value, quoted: true})
			i = next
		case c == '`':
			value, next := readQuotedIdentifier(s, i, '`')
			tokens = append(tokens, sqlIdentToken{value: value, quoted: true})
			i = next
		case c == '[':
			value, next := readBracketIdentifier(s, i)
			tokens = append(tokens, sqlIdentToken{value: value, quoted: true})
			i = next
		case isIdentifierStart(c):
			start := i
			i++
			for i < len(s) && isIdentifierPart(s[i]) {
				i++
			}
			tokens = append(tokens, sqlIdentToken{value: s[start:i]})
		default:
			i++
		}
	}
	return tokens
}

func readQuotedIdentifier(s string, start int, quote byte) (string, int) {
	var b strings.Builder
	for i := start + 1; i < len(s); i++ {
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			return b.String(), i + 1
		}
		b.WriteByte(s[i])
	}
	return b.String(), len(s)
}

func readBracketIdentifier(s string, start int) (string, int) {
	var b strings.Builder
	for i := start + 1; i < len(s); i++ {
		if s[i] == ']' {
			if i+1 < len(s) && s[i+1] == ']' {
				b.WriteByte(']')
				i++
				continue
			}
			return b.String(), i + 1
		}
		b.WriteByte(s[i])
	}
	return b.String(), len(s)
}

func skipQuoted(s string, start int, quote byte) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] == quote {
			if i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(s)
}

func isIdentifierStart(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isIdentifierPart(c byte) bool {
	return isIdentifierStart(c) || (c >= '0' && c <= '9') || c == '$'
}

func isTableConstraintKeyword(s string) bool {
	switch strings.ToUpper(s) {
	case "PRIMARY", "CONSTRAINT", "FOREIGN", "CHECK", "UNIQUE", "INDEX", "KEY":
		return true
	default:
		return false
	}
}

func containsForbiddenTableConstraint(segment string) bool {
	return containsUnquotedKeywordSequence(segment, "FOREIGN", "KEY") ||
		containsUnquotedKeyword(segment, "CHECK") ||
		containsUnquotedKeyword(segment, "UNIQUE") ||
		containsUnquotedKeyword(segment, "INDEX") ||
		containsUnquotedKeyword(segment, "KEY") && !containsUnquotedKeywordSequence(segment, "PRIMARY", "KEY")
}

func containsUnquotedKeyword(segment, keyword string) bool {
	for _, token := range sqlIdentifierTokens(segment) {
		if !token.quoted && strings.EqualFold(token.value, keyword) {
			return true
		}
	}
	return false
}

func containsUnquotedKeywordSequence(segment string, words ...string) bool {
	if len(words) == 0 {
		return false
	}
	tokens := sqlIdentifierTokens(segment)
	for i := 0; i <= len(tokens)-len(words); i++ {
		matched := true
		for j, word := range words {
			if tokens[i+j].quoted || !strings.EqualFold(tokens[i+j].value, word) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func identifierKey(s string) string {
	return strings.ToLower(s)
}

func identifierEqual(a, b string) bool {
	return strings.EqualFold(a, b)
}
