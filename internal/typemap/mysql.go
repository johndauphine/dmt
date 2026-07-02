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
		if isMySQLUnsigned(col) {
			return CanonicalType{Kind: KindInteger}
		}
		return CanonicalType{Kind: KindSmallInt}
	case "mediumint", "int":
		if isMySQLUnsigned(col) && udt == "int" {
			return CanonicalType{Kind: KindBigInt}
		}
		return CanonicalType{Kind: KindInteger}
	case "bigint":
		if isMySQLUnsigned(col) {
			return CanonicalType{Kind: KindDecimal, Precision: IntPtr(20), Scale: IntPtr(0)}
		}
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
	case "tinytext":
		// #206: preserve the source-side byte capacity so a MySQL →
		// MySQL round-trip emits TINYTEXT again (was widening to TEXT
		// pre-#196 and to LONGTEXT post-#196).
		return CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(255)}
	case "text":
		return CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(65_535)}
	case "mediumtext":
		return CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(16_777_215)}
	case "longtext":
		// LONGTEXT is the unbounded tier — no MaxBytes set (matches
		// the canonical convention that nil = unbounded).
		return CanonicalType{Kind: KindText}
	case "binary", "varbinary":
		return CanonicalType{Kind: KindBytes, Length: col.CharacterMaximumLength}
	case "tinyblob":
		// #206: same shape as the text tiers.
		return CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(255)}
	case "blob":
		return CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(65_535)}
	case "mediumblob":
		return CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(16_777_215)}
	case "longblob":
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
		precision := mysqlBitPrecision(col)
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
		// Issue #196: nil Length on KindVarchar means the source column
		// was unbounded (e.g. MSSQL nvarchar(max), PG VARCHAR with no
		// limit). MySQL has no unbounded VARCHAR — VARCHAR(255) was the
		// prior default and silently truncated wide-text columns at
		// write time (Error 1406 "Data too long"). LONGTEXT (4 GB,
		// stored off-row) is the only correct portable target for
		// "unbounded source text" on MySQL; row-size budgets sized for
		// inline VARCHAR don't apply.
		return exactDDL("LONGTEXT")
	case KindChar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("CHAR(%d)", *ct.Length))
		}
		return exactDDL("CHAR(1)")
	case KindText:
		// Issue #206 builds on #196: pick the smallest MySQL text tier
		// that fits the source's MaxBytes capacity. nil MaxBytes
		// (unbounded source — MSSQL nvarchar(max), PG TEXT, MySQL
		// LONGTEXT) → LONGTEXT. Sized source (MySQL TINYTEXT/TEXT/
		// MEDIUMTEXT) round-trips faithfully via mysqlToCanonical
		// setting MaxBytes to the source byte cap.
		return exactDDL(mysqlTextTierFor(ct.MaxBytes))
	case KindBytes:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("VARBINARY(%d)", *ct.Length))
		}
		// Issue #206 / #196: same shape as KindText — pick the smallest
		// blob tier that fits MaxBytes. Unbounded source (MSSQL
		// varbinary(max)/image, PG bytea, MySQL LONGBLOB) → LONGBLOB.
		return exactDDL(mysqlBlobTierFor(ct.MaxBytes))
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
		// Defensive fallback for any future Kind that's added without
		// updating this switch. Same #196 rationale as KindText:
		// LONGTEXT minimizes truncation risk for an unknown Kind whose
		// width characteristics we don't know yet.
		return exactDDL("LONGTEXT")
	}
}

// isTinyintBool detects MySQL's de-facto Boolean convention: a tinyint
// column whose data_type starts with "tinyint(1)". Other tinyint widths
// stay SmallInt.
func isTinyintBool(col ColumnInfo) bool {
	return col.UDTName == "tinyint" && strings.HasPrefix(col.DataType, "tinyint(1)")
}

func isMySQLUnsigned(col ColumnInfo) bool {
	return strings.Contains(strings.ToLower(col.DataType), " unsigned")
}

func mysqlBitPrecision(col ColumnInfo) int {
	if col.NumericPrecision != nil {
		return *col.NumericPrecision
	}
	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	if !strings.HasPrefix(dt, "bit(") {
		return 1
	}
	end := strings.IndexByte(dt, ')')
	if end <= len("bit(") {
		return 1
	}
	var precision int
	if _, err := fmt.Sscanf(dt[len("bit("):end], "%d", &precision); err == nil && precision > 0 {
		return precision
	}
	return 1
}

// MySQL text/blob tier byte caps. Matches the MySQL data dictionary:
// TINYTEXT/TINYBLOB hold up to 255 bytes; TEXT/BLOB up to 64 KB - 1;
// MEDIUMTEXT/MEDIUMBLOB up to 16 MB - 1; LONGTEXT/LONGBLOB up to 4 GB - 1.
const (
	mysqlTinyTierBytes   = 255
	mysqlTextTierBytes   = 65_535
	mysqlMediumTierBytes = 16_777_215
)

// mysqlTextTierFor picks the smallest MySQL text tier that fits the
// source's MaxBytes capacity (#206). nil MaxBytes means unbounded
// source — emit LONGTEXT (matches the #196 default for unbounded
// PG/MSSQL text).
func mysqlTextTierFor(maxBytes *int64) string {
	if maxBytes == nil {
		return "LONGTEXT"
	}
	switch {
	case *maxBytes <= mysqlTinyTierBytes:
		return "TINYTEXT"
	case *maxBytes <= mysqlTextTierBytes:
		return "TEXT"
	case *maxBytes <= mysqlMediumTierBytes:
		return "MEDIUMTEXT"
	default:
		return "LONGTEXT"
	}
}

// mysqlBlobTierFor is the binary companion to mysqlTextTierFor (#206).
// Same byte-cap thresholds; emits the BLOB-family type at each tier.
func mysqlBlobTierFor(maxBytes *int64) string {
	if maxBytes == nil {
		return "LONGBLOB"
	}
	switch {
	case *maxBytes <= mysqlTinyTierBytes:
		return "TINYBLOB"
	case *maxBytes <= mysqlTextTierBytes:
		return "BLOB"
	case *maxBytes <= mysqlMediumTierBytes:
		return "MEDIUMBLOB"
	default:
		return "LONGBLOB"
	}
}

// parseMySQLEnumValues extracts the enum value list from a column_type
// string of the form "enum('a','b','c')". Handles single-quote escaping
// byte by byte to match UVG's parser — MySQL doubles a single quote
// inside an enum value:
//
//	enum('it''s')
//
// The example lives in a code block because Go 1.26 gofmt applies
// TeX-style quote substitution to doc-comment prose, silently turning
// a doubled straight quote into a typographic one.
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
