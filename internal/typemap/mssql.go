// SQL Server dialect mapper.
//
// Port of UVG src/ddl_typemap/mssql.rs (Apache-2.0 / MIT, see UVG NOTICE):
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/mssql.rs

package typemap

import (
	"fmt"
	"strings"
)

// mssqlToCanonical normalizes an MSSQL column to canonical form.
func mssqlToCanonical(col ColumnInfo) CanonicalType {
	udt := col.UDTName

	switch udt {
	case "bit":
		return CanonicalType{Kind: KindBoolean}
	case "tinyint", "smallint":
		return CanonicalType{Kind: KindSmallInt}
	case "int":
		return CanonicalType{Kind: KindInteger}
	case "bigint":
		return CanonicalType{Kind: KindBigInt}
	case "real":
		return CanonicalType{Kind: KindFloat}
	case "float":
		return CanonicalType{Kind: KindDouble}
	case "decimal", "numeric":
		return CanonicalType{
			Kind:      KindDecimal,
			Precision: col.NumericPrecision,
			Scale:     col.NumericScale,
		}
	case "money":
		// MSSQL money has a fixed shape: 19 digits with 4-digit scale.
		return CanonicalType{Kind: KindDecimal, Precision: IntPtr(19), Scale: IntPtr(4)}
	case "smallmoney":
		return CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(4)}
	case "varchar":
		return CanonicalType{Kind: KindVarchar, Length: mssqlMaxLengthOrNil(col.CharacterMaximumLength)}
	case "char":
		return CanonicalType{Kind: KindChar, Length: mssqlMaxLengthOrNil(col.CharacterMaximumLength)}
	case "nvarchar":
		// Canonical drops the wide-char distinction. Target dialect
		// chooses NVARCHAR vs VARCHAR based on its own conventions.
		return CanonicalType{Kind: KindVarchar, Length: mssqlMaxLengthOrNil(col.CharacterMaximumLength)}
	case "nchar":
		return CanonicalType{Kind: KindChar, Length: mssqlMaxLengthOrNil(col.CharacterMaximumLength)}
	case "text", "ntext":
		return CanonicalType{Kind: KindText}
	case "binary", "varbinary", "image":
		return CanonicalType{Kind: KindBytes, Length: mssqlMaxLengthOrNil(col.CharacterMaximumLength)}
	case "date":
		return CanonicalType{Kind: KindDate}
	case "time":
		return CanonicalType{Kind: KindTime, WithTZ: false}
	case "datetime", "datetime2", "smalldatetime":
		return CanonicalType{Kind: KindTimestamp, WithTZ: false}
	case "datetimeoffset":
		return CanonicalType{Kind: KindTimestamp, WithTZ: true}
	case "uniqueidentifier":
		return CanonicalType{Kind: KindUUID}
	default:
		return CanonicalType{Kind: KindRaw, TypeName: strings.ToUpper(udt)}
	}
}

// mssqlMaxLengthOrNil normalizes MSSQL's -1 "MAX" sentinel for
// CHARACTER_MAXIMUM_LENGTH to nil so the canonical IR doesn't carry a
// dialect-specific magic number downstream. Negative or zero lengths
// are not meaningful as concrete sizes; treating them as nil makes the
// IR semantically clean (canonical Length means "explicitly N
// characters/bytes," nil means "unbounded / target chooses default").
//
// Codex review on PR #185 caught the original implementation passing
// -1 straight through to mssqlFromCanonical, producing invalid
// NVARCHAR(-1) / VARBINARY(-1) DDL.
func mssqlMaxLengthOrNil(length *int) *int {
	if length == nil || *length <= 0 {
		return nil
	}
	return length
}

// mssqlFromCanonical emits a canonical type as MSSQL DDL.
//
// MSSQL prefers NVARCHAR over VARCHAR (UTF-16 is the safer default for
// cross-dialect data; PG/MySQL UTF-8 source columns survive the round
// trip without encoding loss). Same logic for NCHAR vs CHAR.
func mssqlFromCanonical(ct CanonicalType) DdlType {
	switch ct.Kind {
	case KindBoolean:
		return exactDDL("BIT")
	case KindSmallInt:
		return exactDDL("SMALLINT")
	case KindInteger:
		return exactDDL("INT")
	case KindBigInt:
		return exactDDL("BIGINT")
	case KindFloat:
		return exactDDL("REAL")
	case KindDouble:
		return exactDDL("FLOAT")
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
		// Defensive: a non-positive Length (e.g. MSSQL's -1 MAX
		// sentinel leaking through) emits MAX rather than the invalid
		// NVARCHAR(-1) DDL (Codex review on PR #185).
		if ct.Length != nil && *ct.Length > 0 {
			return exactDDL(fmt.Sprintf("NVARCHAR(%d)", *ct.Length))
		}
		return exactDDL("NVARCHAR(MAX)")
	case KindChar:
		if ct.Length != nil && *ct.Length > 0 {
			return exactDDL(fmt.Sprintf("NCHAR(%d)", *ct.Length))
		}
		return exactDDL("NCHAR(1)")
	case KindText:
		return exactDDL("NVARCHAR(MAX)")
	case KindBytes:
		if ct.Length != nil && *ct.Length > 0 {
			return exactDDL(fmt.Sprintf("VARBINARY(%d)", *ct.Length))
		}
		return exactDDL("VARBINARY(MAX)")
	case KindDate:
		return exactDDL("DATE")
	case KindTime:
		// MSSQL TIME has no time-zone variant; with_tz collapses.
		return exactDDL("TIME")
	case KindTimestamp:
		if ct.WithTZ {
			return exactDDL("DATETIMEOFFSET")
		}
		return exactDDL("DATETIME2")
	case KindInterval:
		return approxDDL("NVARCHAR(255)", "No INTERVAL type in MSSQL")
	case KindUUID:
		return exactDDL("UNIQUEIDENTIFIER")
	case KindJSON, KindJSONB:
		return approxDDL("NVARCHAR(MAX)", "No native JSON type in MSSQL; using NVARCHAR(MAX)")
	case KindEnum:
		return approxDDL("NVARCHAR(255)", "No ENUM type in MSSQL; consider CHECK constraint")
	case KindArray:
		return approxDDL("NVARCHAR(MAX)", "No array type in MSSQL; using NVARCHAR(MAX)")
	case KindRaw:
		return exactDDL(ct.TypeName)
	default:
		return exactDDL("NVARCHAR(MAX)")
	}
}
