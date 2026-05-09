// SQL Server dialect tests.
//
// Ported from UVG /Users/john/repos/uvg/src/ddl_typemap/mssql.rs
// `#[cfg(test)] mod tests`.

package typemap

import (
	"reflect"
	"testing"
)

func TestMSSQLToCanonical(t *testing.T) {
	tests := []struct {
		name string
		col  ColumnInfo
		want CanonicalType
	}{
		{"int", ColumnInfo{UDTName: "int"}, CanonicalType{Kind: KindInteger}},
		{"bigint", ColumnInfo{UDTName: "bigint"}, CanonicalType{Kind: KindBigInt}},
		{"smallint", ColumnInfo{UDTName: "smallint"}, CanonicalType{Kind: KindSmallInt}},
		{"tinyint", ColumnInfo{UDTName: "tinyint"}, CanonicalType{Kind: KindSmallInt}},
		{"bit", ColumnInfo{UDTName: "bit"}, CanonicalType{Kind: KindBoolean}},
		{"uniqueidentifier", ColumnInfo{UDTName: "uniqueidentifier"}, CanonicalType{Kind: KindUUID}},
		{"money", ColumnInfo{UDTName: "money"},
			CanonicalType{Kind: KindDecimal, Precision: IntPtr(19), Scale: IntPtr(4)}},
		{"smallmoney", ColumnInfo{UDTName: "smallmoney"},
			CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(4)}},
		{"datetimeoffset", ColumnInfo{UDTName: "datetimeoffset"},
			CanonicalType{Kind: KindTimestamp, WithTZ: true}},
		{"datetime2", ColumnInfo{UDTName: "datetime2"},
			CanonicalType{Kind: KindTimestamp, WithTZ: false}},
		{"datetime", ColumnInfo{UDTName: "datetime"},
			CanonicalType{Kind: KindTimestamp, WithTZ: false}},
		{"nvarchar_with_length", ColumnInfo{UDTName: "nvarchar", CharacterMaximumLength: IntPtr(255)},
			CanonicalType{Kind: KindVarchar, Length: IntPtr(255)}},
		{"varchar_with_length", ColumnInfo{UDTName: "varchar", CharacterMaximumLength: IntPtr(50)},
			CanonicalType{Kind: KindVarchar, Length: IntPtr(50)}},
		{"text", ColumnInfo{UDTName: "text"}, CanonicalType{Kind: KindText}},
		{"ntext", ColumnInfo{UDTName: "ntext"}, CanonicalType{Kind: KindText}},
		{"date", ColumnInfo{UDTName: "date"}, CanonicalType{Kind: KindDate}},
		{"varbinary_with_length", ColumnInfo{UDTName: "varbinary", CharacterMaximumLength: IntPtr(64)},
			CanonicalType{Kind: KindBytes, Length: IntPtr(64)}},
		{"hierarchyid_falls_through_to_raw", ColumnInfo{UDTName: "hierarchyid"},
			CanonicalType{Kind: KindRaw, TypeName: "HIERARCHYID"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mssqlToCanonical(tc.col)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestMSSQLFromCanonical_Exact(t *testing.T) {
	tests := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"boolean", CanonicalType{Kind: KindBoolean}, "BIT"},
		{"smallint", CanonicalType{Kind: KindSmallInt}, "SMALLINT"},
		{"integer", CanonicalType{Kind: KindInteger}, "INT"},
		{"bigint", CanonicalType{Kind: KindBigInt}, "BIGINT"},
		{"float_real", CanonicalType{Kind: KindFloat}, "REAL"},
		{"double_float", CanonicalType{Kind: KindDouble}, "FLOAT"},
		{"decimal_p_s", CanonicalType{Kind: KindDecimal, Precision: IntPtr(10), Scale: IntPtr(2)}, "DECIMAL(10, 2)"},
		{"decimal_p_only", CanonicalType{Kind: KindDecimal, Precision: IntPtr(10)}, "DECIMAL(10)"},
		{"decimal_default", CanonicalType{Kind: KindDecimal}, "DECIMAL"},
		{"varchar_with_length", CanonicalType{Kind: KindVarchar, Length: IntPtr(50)}, "NVARCHAR(50)"},
		{"varchar_no_length", CanonicalType{Kind: KindVarchar}, "NVARCHAR(MAX)"},
		{"char_with_length", CanonicalType{Kind: KindChar, Length: IntPtr(10)}, "NCHAR(10)"},
		{"text_to_nvarchar_max", CanonicalType{Kind: KindText}, "NVARCHAR(MAX)"},
		{"bytes_with_length", CanonicalType{Kind: KindBytes, Length: IntPtr(64)}, "VARBINARY(64)"},
		{"bytes_no_length", CanonicalType{Kind: KindBytes}, "VARBINARY(MAX)"},
		{"date", CanonicalType{Kind: KindDate}, "DATE"},
		{"time", CanonicalType{Kind: KindTime}, "TIME"},
		{"time_with_tz_collapses", CanonicalType{Kind: KindTime, WithTZ: true}, "TIME"},
		{"timestamp", CanonicalType{Kind: KindTimestamp}, "DATETIME2"},
		{"timestamp_with_tz", CanonicalType{Kind: KindTimestamp, WithTZ: true}, "DATETIMEOFFSET"},
		{"uuid", CanonicalType{Kind: KindUUID}, "UNIQUEIDENTIFIER"},
		{"raw_passthrough", CanonicalType{Kind: KindRaw, TypeName: "HIERARCHYID"}, "HIERARCHYID"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mssqlFromCanonical(tc.ct)
			if got.SQLType != tc.want {
				t.Errorf("got %q, want %q", got.SQLType, tc.want)
			}
			if got.IsApproximate {
				t.Errorf("expected exact, got approximate (warning %q)", got.Warning)
			}
		})
	}
}

func TestMSSQLFromCanonical_Approximate(t *testing.T) {
	// All of these target an MSSQL surface that lacks the canonical
	// type — they must come back IsApproximate=true with a non-empty
	// warning so #170's DDL emitter can surface the loss.
	tests := []struct {
		name string
		ct   CanonicalType
		want string
	}{
		{"interval", CanonicalType{Kind: KindInterval}, "NVARCHAR(255)"},
		{"json", CanonicalType{Kind: KindJSON}, "NVARCHAR(MAX)"},
		{"jsonb", CanonicalType{Kind: KindJSONB}, "NVARCHAR(MAX)"},
		{"enum", CanonicalType{Kind: KindEnum, Values: []string{"a", "b"}}, "NVARCHAR(255)"},
		{"array", CanonicalType{Kind: KindArray, Element: &CanonicalType{Kind: KindInteger}}, "NVARCHAR(MAX)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mssqlFromCanonical(tc.ct)
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
