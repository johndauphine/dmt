// PostgreSQL dialect tests.
//
// Ported from UVG src/ddl_typemap/pg.rs `#[cfg(test)] mod tests`:
// https://github.com/johndauphine/uvg/blob/3106f79c/src/ddl_typemap/pg.rs

package typemap

import (
	"reflect"
	"testing"
)

func TestPostgresToCanonical(t *testing.T) {
	tests := []struct {
		name string
		col  ColumnInfo
		want CanonicalType
	}{
		{"int4", ColumnInfo{UDTName: "int4"}, CanonicalType{Kind: KindInteger}},
		{"int8", ColumnInfo{UDTName: "int8"}, CanonicalType{Kind: KindBigInt}},
		{"int2", ColumnInfo{UDTName: "int2"}, CanonicalType{Kind: KindSmallInt}},
		{"serial", ColumnInfo{UDTName: "serial"}, CanonicalType{Kind: KindInteger}},
		{"bigserial", ColumnInfo{UDTName: "bigserial"}, CanonicalType{Kind: KindBigInt}},
		{"bool", ColumnInfo{UDTName: "bool"}, CanonicalType{Kind: KindBoolean}},
		{"varchar_with_length", ColumnInfo{UDTName: "varchar", CharacterMaximumLength: IntPtr(100)},
			CanonicalType{Kind: KindVarchar, Length: IntPtr(100)}},
		{"text", ColumnInfo{UDTName: "text"}, CanonicalType{Kind: KindText}},
		{"timestamptz", ColumnInfo{UDTName: "timestamptz"},
			CanonicalType{Kind: KindTimestamp, WithTZ: true}},
		{"timestamp", ColumnInfo{UDTName: "timestamp"},
			CanonicalType{Kind: KindTimestamp, WithTZ: false}},
		{"uuid", ColumnInfo{UDTName: "uuid"}, CanonicalType{Kind: KindUUID}},
		{"jsonb", ColumnInfo{UDTName: "jsonb"}, CanonicalType{Kind: KindJSONB}},
		{"json", ColumnInfo{UDTName: "json"}, CanonicalType{Kind: KindJSON}},
		{"bytea", ColumnInfo{UDTName: "bytea"}, CanonicalType{Kind: KindBytes}},
		{"numeric_p_s", ColumnInfo{UDTName: "numeric", NumericPrecision: IntPtr(10), NumericScale: IntPtr(2)},
			CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}},
		{"interval", ColumnInfo{UDTName: "interval"}, CanonicalType{Kind: KindInterval}},
		{"inet_preserved_as_raw", ColumnInfo{UDTName: "inet"},
			CanonicalType{Kind: KindRaw, TypeName: "INET"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := postgresToCanonical(tc.col)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPostgresToCanonical_Array_Recursion(t *testing.T) {
	// PG _int4 → Array{Integer}
	col := ColumnInfo{UDTName: "_int4"}
	got := postgresToCanonical(col)
	if got.Kind != KindArray {
		t.Fatalf("expected KindArray, got %v", got.Kind)
	}
	if got.Element == nil {
		t.Fatal("Array.Element should not be nil")
	}
	if got.Element.Kind != KindInteger {
		t.Errorf("expected element=Integer, got %v", got.Element.Kind)
	}
}

func TestPostgresFromCanonical(t *testing.T) {
	tests := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"boolean", CanonicalType{Kind: KindBoolean}, "BOOLEAN"},
		{"integer", CanonicalType{Kind: KindInteger}, "INTEGER"},
		{"bigint", CanonicalType{Kind: KindBigInt}, "BIGINT"},
		{"smallint", CanonicalType{Kind: KindSmallInt}, "SMALLINT"},
		{"varchar_with_length", CanonicalType{Kind: KindVarchar, Length: IntPtr(255)}, "VARCHAR(255)"},
		{"varchar_no_length", CanonicalType{Kind: KindVarchar}, "VARCHAR"},
		{"text", CanonicalType{Kind: KindText}, "TEXT"},
		{"timestamp_tz", CanonicalType{Kind: KindTimestamp, WithTZ: true}, "TIMESTAMP WITH TIME ZONE"},
		{"timestamp_no_tz", CanonicalType{Kind: KindTimestamp}, "TIMESTAMP"},
		{"uuid", CanonicalType{Kind: KindUUID}, "UUID"},
		{"jsonb", CanonicalType{Kind: KindJSONB}, "JSONB"},
		{"json", CanonicalType{Kind: KindJSON}, "JSON"},
		{"interval", CanonicalType{Kind: KindInterval}, "INTERVAL"},
		{"bytea", CanonicalType{Kind: KindBytes}, "BYTEA"},
		{"decimal_p_s", CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}, "NUMERIC(10, 2)"},
		{"decimal_p_only", CanonicalType{Kind: KindDecimal, Precision: IntPtr(10)}, "NUMERIC(10)"},
		{"decimal_default", CanonicalType{Kind: KindDecimal}, "NUMERIC"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := postgresFromCanonical(tc.ct)
			if got.SQLType != tc.want {
				t.Errorf("got %q, want %q", got.SQLType, tc.want)
			}
			if got.IsApproximate {
				t.Errorf("expected exact, got approximate (warning %q)", got.Warning)
			}
		})
	}
}

func TestPostgresFromCanonical_Array(t *testing.T) {
	// Integer[] is the basic case, recurses through the same path.
	intElem := CanonicalType{Kind: KindInteger}
	ct := CanonicalType{Kind: KindArray, Element: &intElem}
	got := postgresFromCanonical(ct)
	if got.SQLType != "INTEGER[]" {
		t.Errorf("got %q, want INTEGER[]", got.SQLType)
	}
}

func TestPostgresFromCanonical_Array_Nested(t *testing.T) {
	// Array{Array{Text}} → TEXT[][]
	textElem := CanonicalType{Kind: KindText}
	innerArr := CanonicalType{Kind: KindArray, Element: &textElem}
	ct := CanonicalType{Kind: KindArray, Element: &innerArr}
	got := postgresFromCanonical(ct)
	if got.SQLType != "TEXT[][]" {
		t.Errorf("got %q, want TEXT[][]", got.SQLType)
	}
}
