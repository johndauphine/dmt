// MySQL / MariaDB dialect mapper.
//
// Port of UVG src/ddl_typemap/mysql.rs (Apache-2.0 / MIT, see UVG NOTICE):
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/mysql.rs

package typemap

import (
	"fmt"
	"strings"
)

// mysqlToCanonical normalizes a MySQL column to canonical form. Two
// dialect-specific bits stand out: tinyint(1) collapses to Boolean
// (the de-facto MySQL convention), and ENUM types parse their values
// out of the data_type string.
func mysqlToCanonical(col ColumnInfo) CanonicalType {
	udt := col.UDTName

	switch udt {
	case "tinyint":
		if isTinyintBool(col) {
			return CanonicalType{Kind: KindBoolean}
		}
		return CanonicalType{Kind: KindSmallInt}
	case "smallint":
		return CanonicalType{Kind: KindSmallInt}
	case "mediumint", "int":
		return CanonicalType{Kind: KindInteger}
	case "bigint":
		return CanonicalType{Kind: KindBigInt}
	case "float":
		return CanonicalType{Kind: KindFloat}
	case "double":
		return CanonicalType{Kind: KindDouble}
	case "decimal", "numeric":
		return CanonicalType{
			Kind:      KindDecimal,
			Precision: col.NumericPrecision,
			Scale:     col.NumericScale,
		}
	case "varchar":
		return CanonicalType{Kind: KindVarchar, Length: col.CharacterMaximumLength}
	case "char":
		return CanonicalType{Kind: KindChar, Length: col.CharacterMaximumLength}
	case "text", "tinytext", "mediumtext", "longtext":
		return CanonicalType{Kind: KindText}
	case "binary", "varbinary":
		return CanonicalType{Kind: KindBytes, Length: col.CharacterMaximumLength}
	case "blob", "tinyblob", "mediumblob", "longblob":
		return CanonicalType{Kind: KindBytes}
	case "date":
		return CanonicalType{Kind: KindDate}
	case "time":
		return CanonicalType{Kind: KindTime, WithTZ: false}
	case "datetime", "timestamp":
		return CanonicalType{Kind: KindTimestamp, WithTZ: false}
	case "year":
		// MySQL YEAR is a 1-byte integer 1901-2155; SmallInt is the
		// closest portable equivalent.
		return CanonicalType{Kind: KindSmallInt}
	case "json":
		return CanonicalType{Kind: KindJSON}
	case "enum":
		// Parse failure (malformed data_type, missing parens) → fall
		// back to Raw passthrough so mysqlFromCanonical doesn't end up
		// emitting an invalid bare ENUM() (Copilot review on PR #185).
		values := parseMySQLEnumValues(col.DataType)
		if len(values) == 0 {
			return CanonicalType{Kind: KindRaw, TypeName: col.DataType}
		}
		return CanonicalType{Kind: KindEnum, Values: values}
	case "set":
		// MySQL SET is comma-separated multi-value; no portable
		// canonical equivalent. Preserve the data_type verbatim — the
		// quoted string literals inside (e.g. set('a','b')) must NOT
		// be uppercased, since their values are case-sensitive
		// (Copilot review on PR #185).
		return CanonicalType{Kind: KindRaw, TypeName: col.DataType}
	case "bit":
		// BIT(1) is the conventional Boolean; BIT(N) for N>1 has no
		// portable equivalent.
		precision := 1
		if col.NumericPrecision != nil {
			precision = *col.NumericPrecision
		}
		if precision == 1 {
			return CanonicalType{Kind: KindBoolean}
		}
		return CanonicalType{Kind: KindRaw, TypeName: col.DataType}
	case "boolean", "bool":
		return CanonicalType{Kind: KindBoolean}
	default:
		return CanonicalType{Kind: KindRaw, TypeName: strings.ToUpper(udt)}
	}
}

// mysqlFromCanonical emits a canonical type as MySQL DDL.
func mysqlFromCanonical(ct CanonicalType) DdlType {
	switch ct.Kind {
	case KindBoolean:
		// TINYINT(1) is the de-facto MySQL Boolean — round-trips
		// through to_canonical's tinyint(1)→Boolean detection.
		return exactDDL("TINYINT(1)")
	case KindSmallInt:
		return exactDDL("SMALLINT")
	case KindInteger:
		return exactDDL("INT")
	case KindBigInt:
		return exactDDL("BIGINT")
	case KindFloat:
		return exactDDL("FLOAT")
	case KindDouble:
		return exactDDL("DOUBLE")
	case KindDecimal:
		switch {
		case ct.Precision != nil && ct.Scale != nil:
			return exactDDL(fmt.Sprintf("DECIMAL(%d, %d)", *ct.Precision, *ct.Scale))
		case ct.Precision != nil:
			return exactDDL(fmt.Sprintf("DECIMAL(%d)", *ct.Precision))
		default:
			return exactDDL("DECIMAL")
		}
	case KindVarchar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("VARCHAR(%d)", *ct.Length))
		}
		return exactDDL("VARCHAR(255)")
	case KindChar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("CHAR(%d)", *ct.Length))
		}
		return exactDDL("CHAR(1)")
	case KindText:
		return exactDDL("TEXT")
	case KindBytes:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("VARBINARY(%d)", *ct.Length))
		}
		return exactDDL("BLOB")
	case KindDate:
		return exactDDL("DATE")
	case KindTime:
		// MySQL TIME has no time-zone variant; with_tz collapses.
		return exactDDL("TIME")
	case KindTimestamp:
		// MySQL TIMESTAMP has implicit-timezone behavior that's
		// poorly portable; DATETIME is the safer canonical target.
		return exactDDL("DATETIME")
	case KindInterval:
		return approxDDL("VARCHAR(255)", "No INTERVAL type in MySQL")
	case KindUUID:
		return exactDDL("CHAR(36)")
	case KindJSON:
		return exactDDL("JSON")
	case KindJSONB:
		return approxDDL("JSON", "JSONB binary indexing not available in MySQL")
	case KindEnum:
		// An empty value list would emit invalid bare `ENUM()` —
		// upstream filtering ensures Values is non-empty at the
		// to_canonical layer (parse failure routes to Raw), but guard
		// here too so a hand-built CanonicalType{Kind: KindEnum} can't
		// produce broken DDL (Copilot review on PR #185).
		if len(ct.Values) == 0 {
			return approxDDL("VARCHAR(255)", "Enum has no values; defaulting to VARCHAR(255)")
		}
		quoted := make([]string, len(ct.Values))
		for i, v := range ct.Values {
			quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return exactDDL(fmt.Sprintf("ENUM(%s)", strings.Join(quoted, ", ")))
	case KindArray:
		return approxDDL("JSON", "No array type in MySQL; using JSON")
	case KindRaw:
		return exactDDL(ct.TypeName)
	default:
		return exactDDL("TEXT")
	}
}

// isTinyintBool detects MySQL's de-facto Boolean convention: a tinyint
// column whose data_type starts with "tinyint(1)". Other tinyint widths
// stay SmallInt.
func isTinyintBool(col ColumnInfo) bool {
	return col.UDTName == "tinyint" && strings.HasPrefix(col.DataType, "tinyint(1)")
}

// parseMySQLEnumValues extracts the enum value list from a column_type
// string of the form "enum('a','b','c')". Handles single-quote escaping
// (MySQL doubles single quotes inside enum values: enum('it''s'))
// byte by byte to match UVG's parser.
func parseMySQLEnumValues(columnType string) []string {
	openIdx := strings.Index(columnType, "(")
	closeIdx := strings.LastIndex(columnType, ")")
	if openIdx < 0 || closeIdx < 0 || openIdx+1 >= closeIdx {
		return nil
	}
	inner := columnType[openIdx+1 : closeIdx]

	var values []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if !inQuote {
			if ch == '\'' {
				inQuote = true
			}
			continue
		}
		if ch == '\'' {
			// Escaped quote (MySQL's '' inside a quoted string)?
			if i+1 < len(inner) && inner[i+1] == '\'' {
				current.WriteByte('\'')
				i++
				continue
			}
			// End of value.
			inQuote = false
			values = append(values, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}

	return values
}
