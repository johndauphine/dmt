// MySQL / MariaDB dialect tests.
//
// Ported from UVG src/ddl_typemap/mysql.rs `#[cfg(test)] mod tests`:
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/mysql.rs

package typemap

import (
	"reflect"
	"strings"
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
		{"smallint_unsigned", ColumnInfo{UDTName: "smallint", DataType: "smallint unsigned"},
			CanonicalType{Kind: KindInteger}},
		{"mediumint_to_integer", ColumnInfo{UDTName: "mediumint", DataType: "mediumint"},
			CanonicalType{Kind: KindInteger}},
		{"int_unsigned", ColumnInfo{UDTName: "int", DataType: "int unsigned"},
			CanonicalType{Kind: KindBigInt}},
		{"bigint_unsigned", ColumnInfo{UDTName: "bigint", DataType: "bigint unsigned"},
			CanonicalType{Kind: KindDecimal, Precision: IntPtr(20), Scale: IntPtr(0)}},
		{"varchar_with_length", ColumnInfo{UDTName: "varchar", CharacterMaximumLength: IntPtr(255)},
			CanonicalType{Kind: KindVarchar, Length: IntPtr(255)}},
		// Issue #206: sized text variants now carry MaxBytes so a
		// MySQL → MySQL round-trip emits the original tier instead of
		// widening to LONGTEXT. tinytext=255B, text=64KB-1, mediumtext=16MB-1.
		{"tinytext_206", ColumnInfo{UDTName: "tinytext"},
			CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(255)}},
		{"text_206", ColumnInfo{UDTName: "text"},
			CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(65_535)}},
		{"mediumtext_206", ColumnInfo{UDTName: "mediumtext"},
			CanonicalType{Kind: KindText, MaxBytes: Int64Ptr(16_777_215)}},
		{"longtext_unbounded", ColumnInfo{UDTName: "longtext"},
			CanonicalType{Kind: KindText}}, // nil MaxBytes = unbounded tier
		{"json", ColumnInfo{UDTName: "json"}, CanonicalType{Kind: KindJSON}},
		{"datetime", ColumnInfo{UDTName: "datetime"},
			CanonicalType{Kind: KindTimestamp, WithTZ: false}},
		{"date", ColumnInfo{UDTName: "date"}, CanonicalType{Kind: KindDate}},
		// Issue #206: same shape for the BLOB family.
		{"tinyblob_206", ColumnInfo{UDTName: "tinyblob"},
			CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(255)}},
		{"blob_206", ColumnInfo{UDTName: "blob"},
			CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(65_535)}},
		{"mediumblob_206", ColumnInfo{UDTName: "mediumblob"},
			CanonicalType{Kind: KindBytes, MaxBytes: Int64Ptr(16_777_215)}},
		{"longblob_unbounded", ColumnInfo{UDTName: "longblob"},
			CanonicalType{Kind: KindBytes}},
		{"varbinary_with_length", ColumnInfo{UDTName: "varbinary", CharacterMaximumLength: IntPtr(64)},
			CanonicalType{Kind: KindBytes, Length: IntPtr(64)}},
		{"year_to_smallint", ColumnInfo{UDTName: "year"}, CanonicalType{Kind: KindSmallInt}},
		{"bit_1_to_bool", ColumnInfo{UDTName: "bit", NumericPrecision: IntPtr(1)},
			CanonicalType{Kind: KindBoolean}},
		{"bit_8_to_raw_from_column_type", ColumnInfo{UDTName: "bit", DataType: "bit(8)"},
			CanonicalType{Kind: KindRaw, TypeName: "bit(8)"}},
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

// --- Issue #206: text/blob tier preservation -----------------------

// TestMySQLTextTierFor walks the tier picker's full input domain.
// Boundary values matter: ≤255 → TINYTEXT, ≤65535 → TEXT,
// ≤16777215 → MEDIUMTEXT, anything larger or nil → LONGTEXT.
func TestMySQLTextTierFor(t *testing.T) {
	cases := []struct {
		name     string
		maxBytes *int64
		want     string
	}{
		{"nil_unbounded", nil, "LONGTEXT"},
		{"1_byte", Int64Ptr(1), "TINYTEXT"},
		{"255_at_tiny_ceiling", Int64Ptr(255), "TINYTEXT"},
		{"256_just_above_tiny", Int64Ptr(256), "TEXT"},
		{"65535_at_text_ceiling", Int64Ptr(65_535), "TEXT"},
		{"65536_just_above_text", Int64Ptr(65_536), "MEDIUMTEXT"},
		{"16777215_at_medium_ceiling", Int64Ptr(16_777_215), "MEDIUMTEXT"},
		{"16777216_just_above_medium", Int64Ptr(16_777_216), "LONGTEXT"},
		{"4_GB", Int64Ptr(4_000_000_000), "LONGTEXT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mysqlTextTierFor(tc.maxBytes); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMySQLBlobTierFor mirrors the text picker for the BLOB family.
// Same byte caps, BLOB-family emissions.
func TestMySQLBlobTierFor(t *testing.T) {
	cases := []struct {
		name     string
		maxBytes *int64
		want     string
	}{
		{"nil_unbounded", nil, "LONGBLOB"},
		{"255_at_tiny_ceiling", Int64Ptr(255), "TINYBLOB"},
		{"65535_at_blob_ceiling", Int64Ptr(65_535), "BLOB"},
		{"16777215_at_medium_ceiling", Int64Ptr(16_777_215), "MEDIUMBLOB"},
		{"16777216_just_above_medium", Int64Ptr(16_777_216), "LONGBLOB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mysqlBlobTierFor(tc.maxBytes); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMySQLTextRoundTrip_PreservesTier is the #206 motivating
// regression: each MySQL sized text/blob variant must survive the
// full source → canonical → MySQL round-trip without widening. Was
// always widening pre-#206 (TINYTEXT → TEXT pre-#196, then → LONGTEXT
// post-#196 made it worse).
func TestMySQLTextRoundTrip_PreservesTier(t *testing.T) {
	cases := []struct{ udt string }{
		{"tinytext"},
		{"text"},
		{"mediumtext"},
		{"longtext"},
		{"tinyblob"},
		{"blob"},
		{"mediumblob"},
		{"longblob"},
	}
	for _, tc := range cases {
		t.Run(tc.udt, func(t *testing.T) {
			canonical := mysqlToCanonical(ColumnInfo{UDTName: tc.udt})
			ddl := mysqlFromCanonical(canonical)
			want := strings.ToUpper(tc.udt)
			if ddl.SQLType != want {
				t.Errorf("MySQL %q round-trip → %q, want %q (#206)",
					tc.udt, ddl.SQLType, want)
			}
		})
	}
}

// TestMySQLTextTierFor_CrossDialectFromMSSQL verifies the #196 win is
// preserved: MSSQL nvarchar(max) (KindVarchar{Length: nil}) still
// becomes LONGTEXT on MySQL — that's KindVarchar's path, not KindText,
// so it's unaffected by the #206 changes. PG TEXT (KindText{nil}) also
// unchanged. Both go through the unbounded LONGTEXT branch.
func TestMySQLTextTierFor_CrossDialectFromMSSQL(t *testing.T) {
	// MSSQL nvarchar(max) → KindVarchar{nil} (handled outside the
	// KindText switch; verified by TestMySQLFromCanonical_UnboundedSourceText_Issue196)
	mssqlMax := mssqlToCanonical(ColumnInfo{UDTName: "nvarchar", CharacterMaximumLength: IntPtr(-1)})
	if mssqlMax.Kind != KindVarchar || mssqlMax.Length != nil {
		t.Fatalf("MSSQL nvarchar(max) → %+v, want KindVarchar{nil} (the #196 path; #206 must not regress this)", mssqlMax)
	}
	// MSSQL text → KindText{nil} (truly unbounded; no MaxBytes hint set
	// because MSSQL text is deprecated and treated as unbounded).
	mssqlText := mssqlToCanonical(ColumnInfo{UDTName: "text"})
	if mssqlText.Kind != KindText || mssqlText.MaxBytes != nil {
		t.Errorf("MSSQL text → %+v, want KindText{nil MaxBytes} (no source-side capacity hint)", mssqlText)
	}
	if got := mysqlFromCanonical(mssqlText).SQLType; got != "LONGTEXT" {
		t.Errorf("MSSQL text → MySQL → %q, want LONGTEXT (#196 unbounded default preserved)", got)
	}
}
