// SQLite dialect mapper.
//
// SQLite uses dynamic type affinity rather than strict types — the declared
// column type is treated as a hint that maps to one of five storage classes
// (INTEGER, REAL, TEXT, BLOB, NUMERIC). The mapper here is conservative: it
// accepts the common declared-type names (INT, INTEGER, VARCHAR, TEXT, REAL,
// BLOB, DATETIME, BOOLEAN, NUMERIC/DECIMAL) and falls back to TEXT for
// anything unrecognized. Test-only driver — fidelity loss across exotic
// types is acceptable.

package typemap

import (
	"fmt"
	"strings"
)

// sqliteToCanonical normalizes a SQLite column to canonical form. SQLite's
// declared type strings are matched case-insensitively against affinity
// keywords per SQLite's type-affinity rules.
func sqliteToCanonical(col ColumnInfo) CanonicalType {
	// SQLite stores the declared type verbatim from CREATE TABLE; the
	// UDTName comes from PRAGMA table_info(t).type, which is the same
	// string. Match case-insensitively against known affinity keywords.
	udt := strings.ToUpper(strings.TrimSpace(col.UDTName))

	// Strip any parameter list — "VARCHAR(255)" → "VARCHAR".
	if i := strings.Index(udt, "("); i >= 0 {
		udt = strings.TrimSpace(udt[:i])
	}

	switch udt {
	case "INTEGER", "INT", "BIGINT", "INT8", "ROWID":
		return CanonicalType{Kind: KindBigInt}
	case "SMALLINT", "INT2":
		return CanonicalType{Kind: KindSmallInt}
	case "MEDIUMINT", "INT4":
		return CanonicalType{Kind: KindInteger}
	case "TINYINT":
		// SQLite doesn't have a Boolean type — TINYINT could be either.
		// Default to SmallInt; explicit BOOLEAN below catches the
		// boolean case.
		return CanonicalType{Kind: KindSmallInt}
	case "BOOL", "BOOLEAN":
		return CanonicalType{Kind: KindBoolean}
	case "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT":
		return CanonicalType{Kind: KindDouble}
	case "DECIMAL", "NUMERIC":
		return CanonicalType{
			Kind:      KindDecimal,
			Precision: col.NumericPrecision,
			Scale:     col.NumericScale,
		}
	case "VARCHAR", "NVARCHAR", "VARYING CHARACTER", "CHARACTER VARYING":
		return CanonicalType{Kind: KindVarchar, Length: col.CharacterMaximumLength}
	case "CHAR", "NCHAR", "CHARACTER":
		return CanonicalType{Kind: KindChar, Length: col.CharacterMaximumLength}
	case "TEXT", "CLOB":
		return CanonicalType{Kind: KindText}
	case "BLOB":
		return CanonicalType{Kind: KindBytes}
	case "DATE":
		return CanonicalType{Kind: KindDate}
	case "TIME":
		return CanonicalType{Kind: KindTime}
	case "DATETIME", "TIMESTAMP":
		return CanonicalType{Kind: KindTimestamp}
	case "UUID":
		return CanonicalType{Kind: KindUUID}
	case "JSON":
		return CanonicalType{Kind: KindJSON}
	case "":
		// SQLite allows declaring a column with no type ("CREATE TABLE
		// t (x)"). Affinity falls back to BLOB but in practice these
		// columns hold mixed types. Use TEXT for portability.
		return CanonicalType{Kind: KindText}
	default:
		// Fall back to SQLite's affinity rules (substring match per
		// the SQLite docs).
		switch {
		case strings.Contains(udt, "INT"):
			return CanonicalType{Kind: KindBigInt}
		case strings.Contains(udt, "CHAR") || strings.Contains(udt, "CLOB") || strings.Contains(udt, "TEXT"):
			return CanonicalType{Kind: KindText}
		case strings.Contains(udt, "BLOB"):
			return CanonicalType{Kind: KindBytes}
		case strings.Contains(udt, "REAL") || strings.Contains(udt, "FLOA") || strings.Contains(udt, "DOUB"):
			return CanonicalType{Kind: KindDouble}
		}
		return CanonicalType{Kind: KindRaw, TypeName: col.UDTName}
	}
}

// sqliteFromCanonical emits a canonical type as SQLite DDL. SQLite has
// type affinity, not strict types, so any declared type is accepted —
// we use names that round-trip cleanly through sqliteToCanonical.
func sqliteFromCanonical(ct CanonicalType) DdlType {
	switch ct.Kind {
	case KindBoolean:
		// SQLite has no boolean type; BOOLEAN is recognized by name
		// only (stored as INTEGER 0/1). Use BOOLEAN so round-tripping
		// preserves intent.
		return exactDDL("BOOLEAN")
	case KindSmallInt:
		return exactDDL("SMALLINT")
	case KindInteger:
		return exactDDL("INTEGER")
	case KindBigInt:
		return exactDDL("BIGINT")
	case KindFloat, KindDouble:
		return exactDDL("REAL")
	case KindDecimal:
		switch {
		case ct.Precision != nil && ct.Scale != nil:
			return exactDDL(fmt.Sprintf("NUMERIC(%d, %d)", *ct.Precision, *ct.Scale))
		case ct.Precision != nil:
			return exactDDL(fmt.Sprintf("NUMERIC(%d)", *ct.Precision))
		default:
			return exactDDL("NUMERIC")
		}
	case KindVarchar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("VARCHAR(%d)", *ct.Length))
		}
		return exactDDL("TEXT")
	case KindChar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("CHAR(%d)", *ct.Length))
		}
		return exactDDL("CHAR(1)")
	case KindText:
		return exactDDL("TEXT")
	case KindBytes:
		return exactDDL("BLOB")
	case KindDate:
		return exactDDL("DATE")
	case KindTime:
		return exactDDL("TIME")
	case KindTimestamp:
		return exactDDL("DATETIME")
	case KindInterval:
		return approxDDL("TEXT", "No INTERVAL type in SQLite; stored as TEXT")
	case KindUUID:
		// SQLite has no UUID type — TEXT is the conventional storage.
		return approxDDL("TEXT", "No UUID type in SQLite; stored as TEXT")
	case KindJSON:
		// SQLite has a JSON1 extension but the column type is just
		// TEXT — JSON validation happens via functions.
		return exactDDL("TEXT")
	case KindJSONB:
		return approxDDL("TEXT", "No JSONB type in SQLite; stored as TEXT")
	case KindEnum:
		return approxDDL("TEXT", "No ENUM type in SQLite; stored as TEXT")
	case KindArray:
		return approxDDL("TEXT", "No array type in SQLite; stored as TEXT/JSON")
	case KindRaw:
		return exactDDL(ct.TypeName)
	default:
		return exactDDL("TEXT")
	}
}
