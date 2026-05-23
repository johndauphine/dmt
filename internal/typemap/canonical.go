// Package typemap is the deterministic cross-database type mapper that
// replaces the AI-driven path in internal/driver/ai_typemapper.go (#168
// of the AI-optional epic #167).
//
// Architecture: canonical type IR + per-dialect mappers. Two mechanical
// translation passes:
//
//	to_canonical(col, source) → CanonicalType
//	from_canonical(ct, target) → DdlType { sql_type, is_approximate, warning }
//
// Lossy mappings (e.g. PG `interval` → MSSQL `NVARCHAR(255) /* No
// INTERVAL type */`) stay deterministic-with-caveat via DdlType's
// IsApproximate + Warning fields, rather than falling through to AI.
// AI fallback narrows to Raw types only — vendor-specific types not in
// the canonical catalog (PG inet/cidr/macaddr, MSSQL hierarchyid, etc.).
//
// Ported from UVG src/ddl_typemap/ (Apache-2.0 / MIT dual-licensed),
// pinned at https://github.com/johndauphine/uvg/tree/3106f79c. Per-file
// attribution headers point at the source .rs files. Standard porting
// pattern: Rust enum → Go tagged struct, Option<i32> → *int,
// Vec<String> → []string, Box<T> → *T.
package typemap

import "fmt"

// Kind tags a CanonicalType variant. Maps 1:1 to UVG's CanonicalType
// enum variants.
type Kind int

const (
	KindBoolean Kind = iota
	KindSmallInt
	KindInteger
	KindBigInt
	KindFloat
	KindDouble
	KindDecimal
	KindVarchar
	KindChar
	KindText
	KindBytes
	KindDate
	KindTime
	KindTimestamp
	KindInterval
	KindUUID
	KindJSON
	KindJSONB
	KindEnum
	KindArray
	KindRaw
)

// CanonicalType is the dialect-neutral type representation. Each variant
// reads its relevant fields and ignores the others (Decimal reads
// Precision + Scale; Varchar reads Length; Enum reads Values; Array
// reads Element; Raw reads TypeName; Time/Timestamp read WithTZ).
//
// The tagged-struct shape (one type for all variants) was chosen over a
// sealed-interface family because Go's switch is more ergonomic for
// dispatch and the field count stays bounded.
type CanonicalType struct {
	Kind Kind

	// Length is the source-side declared length. Unit depends on Kind:
	// characters for KindVarchar / KindChar (matches the SQL standard's
	// CHARACTER_MAXIMUM_LENGTH); bytes for KindBytes (sizing for
	// VARBINARY/BINARY columns is byte-defined). nil = "no length
	// specified," which means LONGTEXT/LONGBLOB on MySQL targets after
	// the #196 fix.
	Length    *int
	Precision *int // Decimal — nil = "default precision"
	Scale     *int // Decimal — nil = "default scale"
	WithTZ    bool // Time, Timestamp

	// MaxBytes is the source-side capacity in BYTES for KindText /
	// KindBytes. Used by mysqlFromCanonical to pick the smallest text/
	// blob tier (TINYTEXT / TEXT / MEDIUMTEXT / LONGTEXT and their BLOB
	// counterparts) that fits the source's documented capacity, so a
	// MySQL → MySQL same-dialect round-trip preserves the original
	// type instead of widening to LONGTEXT/LONGBLOB (#206 — fixes a
	// pre-existing IR collapse made worse by #196).
	//
	// Other dialects' from_canonical paths ignore MaxBytes — PG and
	// MSSQL each have a single unbounded text type (TEXT and
	// nvarchar(max) respectively), so there's nothing to tier against.
	// Round-tripping MySQL TINYTEXT through PG and back loses the size
	// (PG TEXT becomes KindText{MaxBytes: nil}); acceptable per #206
	// because multi-hop migrations across dialects with different type
	// systems are inherently lossy.
	//
	// Unit is BYTES (not characters) because MySQL's tier definitions
	// are byte-defined. KindVarchar's Length stays in characters
	// (matches the SQL standard's CHARACTER_MAXIMUM_LENGTH semantics).
	// The unit difference is intentional — same field name across
	// kinds with different units would be a footgun.
	MaxBytes *int64 // Text, Bytes — nil = unbounded (LONGTEXT/LONGBLOB tier)

	Values   []string       // Enum
	Element  *CanonicalType // Array (recursive)
	TypeName string         // Raw — verbatim source-vendor type name
}

// DdlType is the result of mapping a canonical type to a target dialect's
// DDL type string. IsApproximate marks cases where the target lacks an
// exact equivalent (PG interval → NVARCHAR(255) on MSSQL); Warning
// carries the human-readable reason for the approximation.
type DdlType struct {
	SQLType       string
	IsApproximate bool
	Warning       string
}

// Dialect names. Matches the canonical names from internal/driver
// (postgres, mssql, mysql) so callers can pass the same identifier.
// Kept as string constants rather than an enum because the package
// boundary needs to stay clean of any internal/driver import.
const (
	DialectPostgres = "postgres"
	DialectMSSQL    = "mssql"
	DialectMySQL    = "mysql"
	DialectSQLite   = "sqlite"
)

// ColumnInfo is the per-column metadata the mappers consume. Mirrors the
// relevant subset of UVG's schema::ColumnInfo plus the fields dmt's
// driver Reader implementations already produce.
type ColumnInfo struct {
	Name string

	// UDTName is the underlying type name as the driver reports it
	// ("int4", "uniqueidentifier", "tinyint", etc.). The primary
	// dispatch key for to_canonical.
	UDTName string

	// DataType is the dialect's normalized type string (PG
	// "integer", MSSQL "nvarchar(max)", MySQL "tinyint(1)"). MySQL's
	// to_canonical needs this for the tinyint(1)→Boolean detection
	// and for ENUM value parsing.
	DataType string

	// CharacterMaximumLength: nil = "no length specified" (analogous
	// to Rust's Option<i32>). Drives Varchar(N) / Char(N) / Bytes(N).
	CharacterMaximumLength *int

	// NumericPrecision / NumericScale: nil = "default". Drives
	// Decimal(p, s).
	NumericPrecision *int
	NumericScale     *int

	IsNullable bool
}

// IntPtr is a small helper to construct *int values inline. Used heavily
// in tests that mirror UVG's #[cfg(test)] blocks.
func IntPtr(v int) *int { return &v }

// Int64Ptr is the *int64 companion to IntPtr. Used by mysqlToCanonical
// to set the MaxBytes capacity for sized text/blob variants (#206).
func Int64Ptr(v int64) *int64 { return &v }

// exactDDL is the constructor for the non-lossy result shape. Used by
// per-dialect from_canonical implementations.
func exactDDL(sqlType string) DdlType {
	return DdlType{SQLType: sqlType}
}

// approxDDL is the constructor for the lossy result shape. The warning
// surfaces in DDL-emission logs when #170 wires this in (see issue body).
func approxDDL(sqlType, warning string) DdlType {
	return DdlType{SQLType: sqlType, IsApproximate: true, Warning: warning}
}

// ToCanonical normalizes a source column to a CanonicalType using the
// source dialect's mapper.
func ToCanonical(col ColumnInfo, dialect string) CanonicalType {
	switch dialect {
	case DialectPostgres:
		return postgresToCanonical(col)
	case DialectMSSQL:
		return mssqlToCanonical(col)
	case DialectMySQL:
		return mysqlToCanonical(col)
	case DialectSQLite:
		return sqliteToCanonical(col)
	default:
		return CanonicalType{Kind: KindRaw, TypeName: col.UDTName}
	}
}

// FromCanonical emits a canonical type as a DDL fragment for the target
// dialect.
//
// On an unknown dialect, falls back to the Raw type name when one is
// present (only Kind=KindRaw populates TypeName; for other variants
// TypeName is empty). When neither applies, emits an approximate TEXT
// with a warning so callers don't silently produce an empty SQLType
// (Copilot review on PR #185).
func FromCanonical(ct CanonicalType, dialect string) DdlType {
	switch dialect {
	case DialectPostgres:
		return postgresFromCanonical(ct)
	case DialectMSSQL:
		return mssqlFromCanonical(ct)
	case DialectMySQL:
		return mysqlFromCanonical(ct)
	case DialectSQLite:
		return sqliteFromCanonical(ct)
	default:
		if ct.TypeName != "" {
			return exactDDL(ct.TypeName)
		}
		return approxDDL("TEXT", fmt.Sprintf("unknown target dialect %q; defaulting to TEXT for kind %d", dialect, ct.Kind))
	}
}

// MapDDLType is the convenience composition: source column → canonical →
// target DDL. Equivalent to FromCanonical(ToCanonical(col, source), target).
func MapDDLType(col ColumnInfo, source, target string) DdlType {
	return FromCanonical(ToCanonical(col, source), target)
}
