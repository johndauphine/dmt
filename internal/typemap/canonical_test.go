// Cross-dialect MapDDLType tests.
//
// Ported from UVG src/ddl_typemap/mod.rs (embedded `#[cfg(test)] mod
// tests` block — the deterministic regression corpus for end-to-end
// source→target mapping):
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/mod.rs
//
// Same expectations as the Rust tests, swapped to dmt's "postgres" /
// "mssql" / "mysql" dialect names.

package typemap

import (
	"reflect"
	"testing"
)

func TestMapDDLType_PG_To_MySQL_Int(t *testing.T) {
	col := ColumnInfo{Name: "id", UDTName: "int4", DataType: "integer"}
	got := MapDDLType(col, DialectPostgres, DialectMySQL)
	if got.SQLType != "INT" {
		t.Errorf("got %q, want INT", got.SQLType)
	}
	if got.IsApproximate {
		t.Errorf("int4 → INT should be exact, got approximate (%q)", got.Warning)
	}
}

func TestMapDDLType_PG_To_MySQL_JsonB_Approximate(t *testing.T) {
	col := ColumnInfo{Name: "data", UDTName: "jsonb", DataType: "jsonb"}
	got := MapDDLType(col, DialectPostgres, DialectMySQL)
	if got.SQLType != "JSON" {
		t.Errorf("got %q, want JSON", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("jsonb → JSON should be approximate (no binary indexing)")
	}
	if got.Warning == "" {
		t.Error("approximate result should carry a warning string")
	}
}

func TestMapDDLType_PG_To_MySQL_UUID(t *testing.T) {
	col := ColumnInfo{Name: "id", UDTName: "uuid", DataType: "uuid"}
	got := MapDDLType(col, DialectPostgres, DialectMySQL)
	if got.SQLType != "CHAR(36)" {
		t.Errorf("got %q, want CHAR(36)", got.SQLType)
	}
}

func TestMapDDLType_PG_To_MSSQL_UUID(t *testing.T) {
	col := ColumnInfo{Name: "id", UDTName: "uuid", DataType: "uuid"}
	got := MapDDLType(col, DialectPostgres, DialectMSSQL)
	if got.SQLType != "UNIQUEIDENTIFIER" {
		t.Errorf("got %q, want UNIQUEIDENTIFIER", got.SQLType)
	}
}

func TestMapDDLType_PG_To_MySQL_TimestampTZ_Drops_TZ(t *testing.T) {
	// MySQL's DATETIME has no time-zone variant — the canonical
	// WithTZ flag is silently dropped. UVG marks this exact route as
	// the canonical example of "lossy but deterministic": the result
	// is the chosen target type, not an approximation warning.
	col := ColumnInfo{Name: "ts", UDTName: "timestamptz", DataType: "timestamp with time zone"}
	got := MapDDLType(col, DialectPostgres, DialectMySQL)
	if got.SQLType != "DATETIME" {
		t.Errorf("got %q, want DATETIME", got.SQLType)
	}
}

func TestMapDDLType_PG_To_MSSQL_Interval_Approximate(t *testing.T) {
	col := ColumnInfo{Name: "duration", UDTName: "interval", DataType: "interval"}
	got := MapDDLType(col, DialectPostgres, DialectMSSQL)
	if got.SQLType != "NVARCHAR(255)" {
		t.Errorf("got %q, want NVARCHAR(255)", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("interval → NVARCHAR should be approximate")
	}
}

func TestMapDDLType_MSSQL_To_PG_Bit(t *testing.T) {
	col := ColumnInfo{Name: "active", UDTName: "bit", DataType: "bit"}
	got := MapDDLType(col, DialectMSSQL, DialectPostgres)
	if got.SQLType != "BOOLEAN" {
		t.Errorf("got %q, want BOOLEAN", got.SQLType)
	}
}

func TestMapDDLType_MSSQL_To_PG_DatetimeOffset(t *testing.T) {
	col := ColumnInfo{Name: "ts", UDTName: "datetimeoffset", DataType: "datetimeoffset"}
	got := MapDDLType(col, DialectMSSQL, DialectPostgres)
	if got.SQLType != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("got %q, want TIMESTAMP WITH TIME ZONE", got.SQLType)
	}
}

func TestMapDDLType_MSSQL_To_MySQL_Money(t *testing.T) {
	col := ColumnInfo{Name: "price", UDTName: "money", DataType: "money"}
	got := MapDDLType(col, DialectMSSQL, DialectMySQL)
	if got.SQLType != "DECIMAL(19, 4)" {
		t.Errorf("got %q, want DECIMAL(19, 4)", got.SQLType)
	}
}

func TestMapDDLType_MySQL_To_PG_TinyintBool(t *testing.T) {
	col := ColumnInfo{Name: "active", UDTName: "tinyint", DataType: "tinyint(1)"}
	got := MapDDLType(col, DialectMySQL, DialectPostgres)
	if got.SQLType != "BOOLEAN" {
		t.Errorf("got %q, want BOOLEAN", got.SQLType)
	}
}

func TestMapDDLType_MySQL_To_MSSQL_Enum(t *testing.T) {
	// Enum routes through MSSQL's approximate path (no native ENUM).
	col := ColumnInfo{Name: "status", UDTName: "enum", DataType: "enum('active','inactive','pending')"}
	got := MapDDLType(col, DialectMySQL, DialectMSSQL)
	if got.SQLType != "NVARCHAR(255)" {
		t.Errorf("got %q, want NVARCHAR(255)", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("enum → MSSQL should be approximate")
	}
}

func TestMapDDLType_PG_Array_RoundTrip(t *testing.T) {
	// _int4 → KindArray{KindInteger} → INTEGER[]
	col := ColumnInfo{Name: "tags", UDTName: "_int4", DataType: "integer[]"}
	got := MapDDLType(col, DialectPostgres, DialectPostgres)
	if got.SQLType != "INTEGER[]" {
		t.Errorf("got %q, want INTEGER[]", got.SQLType)
	}
}

func TestMapDDLType_PG_Array_To_MySQL_Approximate(t *testing.T) {
	col := ColumnInfo{Name: "tags", UDTName: "_text", DataType: "text[]"}
	got := MapDDLType(col, DialectPostgres, DialectMySQL)
	if got.SQLType != "JSON" {
		t.Errorf("got %q, want JSON", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("array → MySQL JSON should be approximate")
	}
}

// TestMapDDLType_MSSQL_NvarcharMax_RoundTrips — Codex review on PR #185.
// MSSQL nvarchar(max) survives the full source→canonical→target round
// trip (and cross-dialect mappings) without producing invalid DDL with
// the -1 sentinel.
func TestMapDDLType_MSSQL_NvarcharMax_RoundTrips(t *testing.T) {
	col := ColumnInfo{Name: "body", UDTName: "nvarchar", CharacterMaximumLength: IntPtr(-1)}

	if got := MapDDLType(col, DialectMSSQL, DialectMSSQL); got.SQLType != "NVARCHAR(MAX)" {
		t.Errorf("MSSQL → MSSQL: got %q, want NVARCHAR(MAX)", got.SQLType)
	}
	if got := MapDDLType(col, DialectMSSQL, DialectPostgres); got.SQLType != "VARCHAR" {
		t.Errorf("MSSQL → PG: got %q, want VARCHAR (no length)", got.SQLType)
	}
	if got := MapDDLType(col, DialectMSSQL, DialectMySQL); got.SQLType != "VARCHAR(255)" {
		t.Errorf("MSSQL → MySQL: got %q, want VARCHAR(255)", got.SQLType)
	}
}

// TestMapDDLType_MSSQL_VarbinaryMax_RoundTrips — paired with the
// nvarchar(max) test above; the -1 sentinel applies to varbinary too.
func TestMapDDLType_MSSQL_VarbinaryMax_RoundTrips(t *testing.T) {
	col := ColumnInfo{Name: "blob", UDTName: "varbinary", CharacterMaximumLength: IntPtr(-1)}

	if got := MapDDLType(col, DialectMSSQL, DialectMSSQL); got.SQLType != "VARBINARY(MAX)" {
		t.Errorf("MSSQL → MSSQL: got %q, want VARBINARY(MAX)", got.SQLType)
	}
	if got := MapDDLType(col, DialectMSSQL, DialectPostgres); got.SQLType != "BYTEA" {
		t.Errorf("MSSQL → PG: got %q, want BYTEA", got.SQLType)
	}
	if got := MapDDLType(col, DialectMSSQL, DialectMySQL); got.SQLType != "BLOB" {
		t.Errorf("MSSQL → MySQL: got %q, want BLOB", got.SQLType)
	}
}

func TestMapDDLType_UnknownDialect_Falls_Through(t *testing.T) {
	col := ColumnInfo{Name: "x", UDTName: "weirdtype"}
	got := MapDDLType(col, "oracle", "snowflake")
	// Unknown source dialect → Raw{TypeName: col.UDTName}
	// Unknown target dialect → SQLType: ct.TypeName (Raw passthrough)
	if got.SQLType != "weirdtype" {
		t.Errorf("got %q, want weirdtype", got.SQLType)
	}
}

// TestFromCanonical_UnknownDialect_NonRawFallsBackToText — Copilot review
// regression on PR #185. Without the fallback, FromCanonical's default
// branch returned ct.TypeName, which is empty for any non-Raw kind:
// silent empty-SQLType bug. Now: emit TEXT with an approximate warning.
func TestFromCanonical_UnknownDialect_NonRawFallsBackToText(t *testing.T) {
	got := FromCanonical(CanonicalType{Kind: KindBoolean}, "oracle")
	if got.SQLType != "TEXT" {
		t.Errorf("unknown dialect + non-Raw kind should emit TEXT, got %q", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("unknown-dialect fallback should be marked approximate")
	}
	if got.Warning == "" {
		t.Error("unknown-dialect fallback should carry a warning")
	}
}

// TestFromCanonical_UnknownDialect_RawPassthroughExact — paired with the
// non-Raw test above: when TypeName IS populated (Raw kind), the unknown-
// dialect fallback should emit it exact, not approximate.
func TestFromCanonical_UnknownDialect_RawPassthroughExact(t *testing.T) {
	got := FromCanonical(CanonicalType{Kind: KindRaw, TypeName: "GEOMETRY"}, "oracle")
	if got.SQLType != "GEOMETRY" {
		t.Errorf("Raw passthrough should emit verbatim, got %q", got.SQLType)
	}
	if got.IsApproximate {
		t.Error("Raw passthrough should be exact, not approximate")
	}
}

func TestCanonicalType_DeepEqual_With_Pointers(t *testing.T) {
	// Sanity check: CanonicalType comparison via reflect.DeepEqual
	// dereferences *int fields rather than comparing pointer
	// addresses. The test corpus relies on this throughout.
	a := CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}
	b := CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("DeepEqual should treat *int fields by value, not by address")
	}
}
