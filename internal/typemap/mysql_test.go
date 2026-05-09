// MySQL / MariaDB dialect tests.
//
// Ported from UVG /Users/john/repos/uvg/src/ddl_typemap/mysql.rs
// `#[cfg(test)] mod tests`.

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
		{"varchar_no_length", CanonicalType{Kind: KindVarchar}, "VARCHAR(255)"},
		{"text", CanonicalType{Kind: KindText}, "TEXT"},
		{"date", CanonicalType{Kind: KindDate}, "DATE"},
		{"time", CanonicalType{Kind: KindTime}, "TIME"},
		{"timestamp_to_datetime", CanonicalType{Kind: KindTimestamp}, "DATETIME"},
		{"timestamp_with_tz_to_datetime", CanonicalType{Kind: KindTimestamp, WithTZ: true}, "DATETIME"},
		{"uuid_to_char_36", CanonicalType{Kind: KindUUID}, "CHAR(36)"},
		{"json", CanonicalType{Kind: KindJSON}, "JSON"},
		{"bytes_no_length", CanonicalType{Kind: KindBytes}, "BLOB"},
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
