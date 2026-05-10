// MySQL / MariaDB dialect tests.
//
// Ported from UVG src/ddl_typemap/mysql.rs `#[cfg(test)] mod tests`:
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/mysql.rs

package typemap

import (
	"reflect"
	"testing"
)

func TestMySQLToCanonical(t *testing.T) {
	tests := []struct {
		name string
		col  ColumnInfo
		want CanonicalType
	}{
		{"tinyint_bool", ColumnInfo{UDTName: "tinyint", DataType: "tinyint(1)"},
			CanonicalType{Kind: KindBoolean}},
		{"tinyint_not_bool", ColumnInfo{UDTName: "tinyint", DataType: "tinyint(4)"},
			CanonicalType{Kind: KindSmallInt}},
		{"int", ColumnInfo{UDTName: "int", DataType: "int"}, CanonicalType{Kind: KindInteger}},
		{"bigint", ColumnInfo{UDTName: "bigint", DataType: "bigint"}, CanonicalType{Kind: KindBigInt}},
		{"smallint", ColumnInfo{UDTName: "smallint", DataType: "smallint"}, CanonicalType{Kind: KindSmallInt}},
		{"mediumint_to_integer", ColumnInfo{UDTName: "mediumint", DataType: "mediumint"},
			CanonicalType{Kind: KindInteger}},
		{"varchar_with_length", ColumnInfo{UDTName: "varchar", CharacterMaximumLength: IntPtr(255)},
			CanonicalType{Kind: KindVarchar, Length: IntPtr(255)}},
		{"text", ColumnInfo{UDTName: "text"}, CanonicalType{Kind: KindText}},
		{"longtext", ColumnInfo{UDTName: "longtext"}, CanonicalType{Kind: KindText}},
		{"json", ColumnInfo{UDTName: "json"}, CanonicalType{Kind: KindJSON}},
		{"datetime", ColumnInfo{UDTName: "datetime"},
			CanonicalType{Kind: KindTimestamp, WithTZ: false}},
		{"date", ColumnInfo{UDTName: "date"}, CanonicalType{Kind: KindDate}},
		{"blob", ColumnInfo{UDTName: "blob"}, CanonicalType{Kind: KindBytes}},
		{"varbinary_with_length", ColumnInfo{UDTName: "varbinary", CharacterMaximumLength: IntPtr(64)},
			CanonicalType{Kind: KindBytes, Length: IntPtr(64)}},
		{"year_to_smallint", ColumnInfo{UDTName: "year"}, CanonicalType{Kind: KindSmallInt}},
		{"bit_1_to_bool", ColumnInfo{UDTName: "bit", NumericPrecision: IntPtr(1)},
			CanonicalType{Kind: KindBoolean}},
		{"bit_default_to_bool", ColumnInfo{UDTName: "bit"}, CanonicalType{Kind: KindBoolean}},
		{"boolean_alias", ColumnInfo{UDTName: "boolean"}, CanonicalType{Kind: KindBoolean}},
		{"decimal_p_s", ColumnInfo{UDTName: "decimal", NumericPrecision: IntPtr(10), NumericScale: IntPtr(2)},
			CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlToCanonical(tc.col)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestMySQLToCanonical_Enum(t *testing.T) {
	col := ColumnInfo{
		UDTName:  "enum",
		DataType: "enum('active','inactive','pending')",
	}
	got := mysqlToCanonical(col)
	if got.Kind != KindEnum {
		t.Fatalf("expected KindEnum, got %v", got.Kind)
	}
	want := []string{"active", "inactive", "pending"}
	if !reflect.DeepEqual(got.Values, want) {
		t.Errorf("got values %v, want %v", got.Values, want)
	}
}

func TestMySQLToCanonical_Enum_With_Escaped_Quote(t *testing.T) {
	// MySQL doubles single quotes inside enum values: 'it''s'
	col := ColumnInfo{
		UDTName:  "enum",
		DataType: "enum('it''s','foo','bar''baz')",
	}
	got := mysqlToCanonical(col)
	want := []string{"it's", "foo", "bar'baz"}
	if !reflect.DeepEqual(got.Values, want) {
		t.Errorf("got %v, want %v", got.Values, want)
	}
}

func TestMySQLFromCanonical_Exact(t *testing.T) {
	tests := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"boolean_to_tinyint_1", CanonicalType{Kind: KindBoolean}, "TINYINT(1)"},
		{"smallint", CanonicalType{Kind: KindSmallInt}, "SMALLINT"},
		{"integer", CanonicalType{Kind: KindInteger}, "INT"},
		{"bigint", CanonicalType{Kind: KindBigInt}, "BIGINT"},
		{"float", CanonicalType{Kind: KindFloat}, "FLOAT"},
		{"double", CanonicalType{Kind: KindDouble}, "DOUBLE"},
		{"varchar_with_length", CanonicalType{Kind: KindVarchar, Length: IntPtr(100)}, "VARCHAR(100)"},
		// Issue #196: nil Length means source was unbounded (e.g. MSSQL
		// nvarchar(max)). Pre-fix this emitted VARCHAR(255) and silently
		// truncated wide-text data at write time. LONGTEXT is the only
		// correct portable target for unbounded source text on MySQL.
		{"varchar_no_length_to_longtext_196", CanonicalType{Kind: KindVarchar}, "LONGTEXT"},
		// Issue #196: KindText models unbounded source text; MySQL TEXT
		// is the *smallest* of four sized text types (64 KB) — wrong
		// default. LONGTEXT preserves fidelity.
		{"text_to_longtext_196", CanonicalType{Kind: KindText}, "LONGTEXT"},
		{"date", CanonicalType{Kind: KindDate}, "DATE"},
		{"time", CanonicalType{Kind: KindTime}, "TIME"},
		{"timestamp_to_datetime", CanonicalType{Kind: KindTimestamp}, "DATETIME"},
		{"timestamp_with_tz_to_datetime", CanonicalType{Kind: KindTimestamp, WithTZ: true}, "DATETIME"},
		{"uuid_to_char_36", CanonicalType{Kind: KindUUID}, "CHAR(36)"},
		{"json", CanonicalType{Kind: KindJSON}, "JSON"},
		// Issue #196 parallel: nil Length on KindBytes means unbounded
		// source bytes (MSSQL varbinary(max)). MySQL BLOB caps at 64 KB;
		// LONGBLOB is the safe default for unbounded.
		{"bytes_no_length_to_longblob_196", CanonicalType{Kind: KindBytes}, "LONGBLOB"},
		{"bytes_with_length", CanonicalType{Kind: KindBytes, Length: IntPtr(64)}, "VARBINARY(64)"},
		{"decimal_p_s", CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}, "DECIMAL(10, 2)"},
		{"raw_passthrough", CanonicalType{Kind: KindRaw, TypeName: "GEOMETRY"}, "GEOMETRY"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlFromCanonical(tc.ct)
			if got.SQLType != tc.want {
				t.Errorf("got %q, want %q", got.SQLType, tc.want)
			}
			if got.IsApproximate {
				t.Errorf("expected exact, got approximate (warning %q)", got.Warning)
			}
		})
	}
}

func TestMySQLFromCanonical_Enum(t *testing.T) {
	ct := CanonicalType{Kind: KindEnum, Values: []string{"active", "inactive", "pending"}}
	got := mysqlFromCanonical(ct)
	want := "ENUM('active', 'inactive', 'pending')"
	if got.SQLType != want {
		t.Errorf("got %q, want %q", got.SQLType, want)
	}
}

func TestMySQLFromCanonical_Enum_Escapes_Quotes(t *testing.T) {
	// Single quotes inside enum values must be doubled on emit so the
	// result round-trips through to_canonical.
	ct := CanonicalType{Kind: KindEnum, Values: []string{"it's", "foo"}}
	got := mysqlFromCanonical(ct)
	want := "ENUM('it''s', 'foo')"
	if got.SQLType != want {
		t.Errorf("got %q, want %q", got.SQLType, want)
	}
}

func TestMySQLFromCanonical_Approximate(t *testing.T) {
	tests := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"interval", CanonicalType{Kind: KindInterval}, "VARCHAR(255)"},
		{"jsonb", CanonicalType{Kind: KindJSONB}, "JSON"},
		{"array", CanonicalType{Kind: KindArray, Element: &CanonicalType{Kind: KindText}}, "JSON"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlFromCanonical(tc.ct)
			if got.SQLType != tc.want {
				t.Errorf("got %q, want %q", got.SQLType, tc.want)
			}
			if !got.IsApproximate {
				t.Error("expected approximate, got exact")
			}
			if got.Warning == "" {
				t.Error("approximate result must carry a warning")
			}
		})
	}
}

func TestMySQLEnumValues_Empty(t *testing.T) {
	if got := parseMySQLEnumValues(""); got != nil {
		t.Errorf("empty input → nil, got %v", got)
	}
	if got := parseMySQLEnumValues("not_an_enum"); got != nil {
		t.Errorf("no parens → nil, got %v", got)
	}
}

// TestMySQLToCanonical_EnumParseFailure_FallsBackToRaw — Copilot review
// regression on PR #185. A malformed enum data_type would previously
// return KindEnum with nil Values, which downstream emits as invalid
// bare ENUM(). Now it routes to Raw passthrough so the original
// data_type survives.
func TestMySQLToCanonical_EnumParseFailure_FallsBackToRaw(t *testing.T) {
	col := ColumnInfo{UDTName: "enum", DataType: "enum"} // missing parens
	got := mysqlToCanonical(col)
	if got.Kind != KindRaw {
		t.Errorf("malformed enum should fall back to Raw, got Kind=%v", got.Kind)
	}
	if got.TypeName != "enum" {
		t.Errorf("TypeName should preserve original data_type verbatim, got %q", got.TypeName)
	}
}

// TestMySQLToCanonical_SetPreservesCase — Copilot review regression on
// PR #185. SET values are case-sensitive; uppercasing the data_type
// would mutate the quoted literals and change semantics. Verifies the
// passthrough preserves them verbatim.
func TestMySQLToCanonical_SetPreservesCase(t *testing.T) {
	col := ColumnInfo{UDTName: "set", DataType: "set('Active','InActive','PENDING')"}
	got := mysqlToCanonical(col)
	if got.Kind != KindRaw {
		t.Errorf("set should map to Raw, got Kind=%v", got.Kind)
	}
	if got.TypeName != "set('Active','InActive','PENDING')" {
		t.Errorf("set values must be preserved verbatim (case-sensitive); got %q", got.TypeName)
	}
}

// TestMySQLFromCanonical_EmptyEnumApproximate — Copilot review regression
// on PR #185. An empty Values list must NOT emit invalid bare ENUM();
// fall back to VARCHAR(255) with an IsApproximate warning.
func TestMySQLFromCanonical_EmptyEnumApproximate(t *testing.T) {
	got := mysqlFromCanonical(CanonicalType{Kind: KindEnum})
	if got.SQLType != "VARCHAR(255)" {
		t.Errorf("empty enum should fall back to VARCHAR(255), got %q", got.SQLType)
	}
	if !got.IsApproximate {
		t.Error("empty enum fallback should be marked approximate")
	}
}

// TestMySQLFromCanonical_UnboundedSourceText_Issue196 is the explicit
// regression guard for issue #196: an unbounded source text column
// (MSSQL nvarchar(max)/text/ntext, PG TEXT) must round-trip to
// MySQL LONGTEXT, not VARCHAR(255) or TEXT. Discovery context: SO2010
// migration mssql→mysql produced "Error 1406: Data too long for
// column 'Body' at row 1" because Posts.Body (nvarchar(max), real
// values up to ~12 MB) was created as VARCHAR(255) on MySQL.
//
// The table-driven Exact test above exercises the same canonical-side
// inputs; this test additionally documents the source-side case
// (MSSQL nvarchar(max) with CHARACTER_MAXIMUM_LENGTH=-1) that
// produces the nil-Length canonical, and exercises the full
// round-trip via mssqlToCanonical → mysqlFromCanonical.
func TestMySQLFromCanonical_UnboundedSourceText_Issue196(t *testing.T) {
	cases := []struct {
		name string
		col  ColumnInfo
		want string
	}{
		{
			"mssql_nvarchar_max",
			ColumnInfo{UDTName: "nvarchar", CharacterMaximumLength: IntPtr(-1)},
			"LONGTEXT",
		},
		{
			"mssql_varchar_max",
			ColumnInfo{UDTName: "varchar", CharacterMaximumLength: IntPtr(-1)},
			"LONGTEXT",
		},
		{
			"mssql_text",
			ColumnInfo{UDTName: "text"},
			"LONGTEXT",
		},
		{
			"mssql_ntext",
			ColumnInfo{UDTName: "ntext"},
			"LONGTEXT",
		},
		// MSSQL varbinary(max) → KindBytes{nil} → LONGBLOB on MySQL
		// (parallel #196 fix; same bug class for byte columns).
		{
			"mssql_varbinary_max",
			ColumnInfo{UDTName: "varbinary", CharacterMaximumLength: IntPtr(-1)},
			"LONGBLOB",
		},
		{
			"mssql_image",
			ColumnInfo{UDTName: "image"},
			"LONGBLOB",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			canonical := mssqlToCanonical(tc.col)
			ddl := mysqlFromCanonical(canonical)
			if ddl.SQLType != tc.want {
				t.Errorf("mssql %q → MySQL %q, want %q (issue #196 round-trip)",
					tc.col.UDTName, ddl.SQLType, tc.want)
			}
			// Bounded source columns still emit bounded MySQL DDL —
			// the fix only changes the unbounded default. Exact
			// (non-approximate) is the right shape: LONGTEXT is the
			// faithful representation, not a lossy fallback.
			if ddl.IsApproximate {
				t.Errorf("LONGTEXT/LONGBLOB for unbounded source should be exact (faithful), not approximate; got warning %q",
					ddl.Warning)
			}
		})
	}
}

// TestMySQLFromCanonical_BoundedTextStillBounded is the regression-guard
// companion to the #196 fix: explicit-length varchar/varbinary columns
// must continue to emit VARCHAR(N)/VARBINARY(N), not get widened to
// LONGTEXT/LONGBLOB. Catches a hypothetical over-correction of the
// #196 fix that would also widen bounded columns.
func TestMySQLFromCanonical_BoundedTextStillBounded(t *testing.T) {
	cases := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"varchar_50", CanonicalType{Kind: KindVarchar, Length: IntPtr(50)}, "VARCHAR(50)"},
		{"varchar_4000", CanonicalType{Kind: KindVarchar, Length: IntPtr(4000)}, "VARCHAR(4000)"},
		{"varbinary_128", CanonicalType{Kind: KindBytes, Length: IntPtr(128)}, "VARBINARY(128)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlFromCanonical(tc.ct)
			if got.SQLType != tc.want {
				t.Errorf("got %q, want %q (#196 fix must not widen bounded columns)", got.SQLType, tc.want)
			}
		})
	}
}
