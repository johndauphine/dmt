package mssql

import (
	"strings"
	"unicode/utf8"
)

func convertRowForBulkCopy(row []any, columnTypes []string) []any {
	result := make([]any, len(row))
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			if isTextColumn(columnTypes, i) && utf8.Valid(b) {
				result[i] = string(b)
			} else if isASCIINumeric(b) {
				result[i] = string(b)
			} else {
				result[i] = v
			}
		} else {
			result[i] = v
		}
	}
	return result
}

func isTextColumn(columnTypes []string, index int) bool {
	if index >= len(columnTypes) {
		return false
	}

	columnType := normalizeColumnType(columnTypes[index])
	switch columnType {
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"nchar", "nvarchar", "ntext", "string", "uniqueidentifier", "uuid",
		"json", "jsonb", "xml", "enum", "set":
		return true
	default:
		return false
	}
}

func normalizeColumnType(columnType string) string {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	if idx := strings.IndexAny(columnType, "( \t\r\n"); idx >= 0 {
		columnType = columnType[:idx]
	}
	return columnType
}

func isASCIINumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	hasDigit := false
	hasDot := false
	hasE := false
	i := 0

	if b[i] == '+' || b[i] == '-' {
		i++
		if i >= len(b) {
			return false
		}
	}

	for i < len(b) {
		c := b[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '.':
			if hasDot || hasE {
				return false
			}
			hasDot = true
		case c == 'E' || c == 'e':
			if hasE || !hasDigit {
				return false
			}
			hasE = true
			i++
			if i < len(b) && (b[i] == '+' || b[i] == '-') {
				i++
			}
			if i >= len(b) || b[i] < '0' || b[i] > '9' {
				return false
			}
			continue
		default:
			return false
		}
		i++
	}

	return hasDigit
}

func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}

	if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
		return mssqlErr.SQLErrorNumber() == 1205
	}

	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "1205")
}
