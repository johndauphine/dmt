// PostgreSQL dialect mapper.
//
// Port of /Users/john/repos/uvg/src/ddl_typemap/pg.rs (Apache-2.0 / MIT,
// see UVG NOTICE). Behavior preserved verbatim — Rust match arms become
// Go switch cases; Option<i32> becomes *int; the recursive Array case
// recurses through ToCanonical/FromCanonical the same way.

package typemap

import (
	"fmt"
	"strings"
)

// postgresToCanonical normalizes a PG column to canonical form. Array
// types (PG's `_text` = "array of text") are detected via the leading
// underscore and recursed.
func postgresToCanonical(col ColumnInfo) CanonicalType {
	udt := col.UDTName

	// Array: PG names array types with a leading underscore (_text,
	// _int4, etc.). Strip the prefix and recurse so nested arrays
	// work without further special-casing.
	if elementUDT, ok := stripPrefix(udt, "_"); ok {
		element := postgresToCanonical(ColumnInfo{
			UDTName:                elementUDT,
			DataType:               col.DataType,
			CharacterMaximumLength: col.CharacterMaximumLength,
			NumericPrecision:       col.NumericPrecision,
			NumericScale:           col.NumericScale,
		})
		return CanonicalType{Kind: KindArray, Element: &element}
	}

	switch udt {
	case "bool":
		return CanonicalType{Kind: KindBoolean}
	case "int2", "smallserial":
		return CanonicalType{Kind: KindSmallInt}
	case "int4", "serial":
		return CanonicalType{Kind: KindInteger}
	case "int8", "bigserial":
		return CanonicalType{Kind: KindBigInt}
	case "float4", "real":
		return CanonicalType{Kind: KindFloat}
	case "float8", "double precision":
		return CanonicalType{Kind: KindDouble}
	case "numeric", "decimal":
		return CanonicalType{
			Kind:      KindDecimal,
			Precision: col.NumericPrecision,
			Scale:     col.NumericScale,
		}
	case "varchar", "character varying":
		return CanonicalType{Kind: KindVarchar, Length: col.CharacterMaximumLength}
	case "char", "character", "bpchar":
		return CanonicalType{Kind: KindChar, Length: col.CharacterMaximumLength}
	case "text":
		return CanonicalType{Kind: KindText}
	case "bytea":
		return CanonicalType{Kind: KindBytes}
	case "date":
		return CanonicalType{Kind: KindDate}
	case "time", "time without time zone":
		return CanonicalType{Kind: KindTime, WithTZ: false}
	case "timetz", "time with time zone":
		return CanonicalType{Kind: KindTime, WithTZ: true}
	case "timestamp", "timestamp without time zone":
		return CanonicalType{Kind: KindTimestamp, WithTZ: false}
	case "timestamptz", "timestamp with time zone":
		return CanonicalType{Kind: KindTimestamp, WithTZ: true}
	case "interval":
		return CanonicalType{Kind: KindInterval}
	case "uuid":
		return CanonicalType{Kind: KindUUID}
	case "json":
		return CanonicalType{Kind: KindJSON}
	case "jsonb":
		return CanonicalType{Kind: KindJSONB}
	case "inet", "cidr", "macaddr":
		// PG-specific network types — preserved as Raw so a same-
		// dialect PG → PG migration round-trips, and cross-dialect
		// migrations route through the AI fallback at #170 wiring time.
		return CanonicalType{Kind: KindRaw, TypeName: strings.ToUpper(udt)}
	default:
		return CanonicalType{Kind: KindRaw, TypeName: strings.ToUpper(udt)}
	}
}

// postgresFromCanonical emits a canonical type as PostgreSQL DDL.
func postgresFromCanonical(ct CanonicalType) DdlType {
	switch ct.Kind {
	case KindBoolean:
		return exactDDL("BOOLEAN")
	case KindSmallInt:
		return exactDDL("SMALLINT")
	case KindInteger:
		return exactDDL("INTEGER")
	case KindBigInt:
		return exactDDL("BIGINT")
	case KindFloat:
		return exactDDL("REAL")
	case KindDouble:
		return exactDDL("DOUBLE PRECISION")
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
		return exactDDL("VARCHAR")
	case KindChar:
		if ct.Length != nil {
			return exactDDL(fmt.Sprintf("CHAR(%d)", *ct.Length))
		}
		return exactDDL("CHAR")
	case KindText:
		return exactDDL("TEXT")
	case KindBytes:
		return exactDDL("BYTEA")
	case KindDate:
		return exactDDL("DATE")
	case KindTime:
		if ct.WithTZ {
			return exactDDL("TIME WITH TIME ZONE")
		}
		return exactDDL("TIME")
	case KindTimestamp:
		if ct.WithTZ {
			return exactDDL("TIMESTAMP WITH TIME ZONE")
		}
		return exactDDL("TIMESTAMP")
	case KindInterval:
		return exactDDL("INTERVAL")
	case KindUUID:
		return exactDDL("UUID")
	case KindJSON:
		return exactDDL("JSON")
	case KindJSONB:
		return exactDDL("JSONB")
	case KindEnum:
		// Cross-dialect Enum becomes a fallback VARCHAR — PG enums
		// require a separate CREATE TYPE statement which #169 will
		// emit alongside the column definition. The column itself
		// here is a placeholder.
		return approxDDL("VARCHAR(255)", "Enum mapped to VARCHAR; use CREATE TYPE for native PG enum")
	case KindArray:
		if ct.Element == nil {
			// Defensive — Array with nil Element shouldn't reach
			// here under normal use, but don't panic.
			return approxDDL("TEXT[]", "Array with nil element; defaulting to TEXT[]")
		}
		inner := postgresFromCanonical(*ct.Element)
		out := exactDDL(inner.SQLType + "[]")
		out.IsApproximate = inner.IsApproximate
		if inner.Warning != "" {
			out.Warning = "array element: " + inner.Warning
		}
		return out
	case KindRaw:
		return exactDDL(ct.TypeName)
	default:
		return exactDDL("TEXT")
	}
}

// stripPrefix is a Go convenience over strings.TrimPrefix's "did it
// strip" semantic — strings.TrimPrefix returns the original string
// unchanged when the prefix doesn't match, which makes "did the strip
// happen" hard to detect. Returns (rest, true) on prefix match,
// ("", false) otherwise.
func stripPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return "", false
}
