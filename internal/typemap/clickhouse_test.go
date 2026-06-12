package typemap

import "testing"

func intp(n int) *int { return &n }

// Wrapper stripping and parameterized-type parsing are where the
// ClickHouse mapper differs structurally from the row-store mappers.
func TestClickHouseToCanonical(t *testing.T) {
	cases := []struct {
		udt  string
		want Kind
	}{
		{"String", KindText},
		{"Nullable(String)", KindText},
		{"LowCardinality(String)", KindText},
		{"Nullable(LowCardinality(String))", KindText},
		{"LowCardinality(Nullable(String))", KindText},
		{"LowCardinality(Nullable(FixedString(4)))", KindChar},
		{"Int8", KindSmallInt},
		{"Int16", KindSmallInt},
		{"UInt8", KindSmallInt},
		{"UInt16", KindInteger},
		{"Int32", KindInteger},
		{"UInt32", KindBigInt},
		{"Int64", KindBigInt},
		{"UInt64", KindRaw}, // no lossless canonical home
		{"Int128", KindRaw},
		{"Float32", KindFloat},
		{"Float64", KindDouble},
		{"Bool", KindBoolean},
		{"Date", KindDate},
		{"Date32", KindDate},
		{"DateTime", KindTimestamp},
		{"DateTime64(3)", KindTimestamp},
		{"DateTime64(9, 'UTC')", KindTimestamp},
		{"UUID", KindUUID},
		{"Enum8('a' = 1, 'b' = 2)", KindEnum},
		{"Array(String)", KindArray},
		{"IPv4", KindText},
		{"AggregateFunction(sum, UInt64)", KindRaw},
	}
	for _, c := range cases {
		got := clickhouseToCanonical(ColumnInfo{UDTName: c.udt})
		if got.Kind != c.want {
			t.Errorf("ToCanonical(%q).Kind = %v, want %v", c.udt, got.Kind, c.want)
		}
	}

	if got := clickhouseToCanonical(ColumnInfo{UDTName: "FixedString(16)"}); got.Kind != KindChar || got.Length == nil || *got.Length != 16 {
		t.Errorf("FixedString(16) = %+v", got)
	}
	if got := clickhouseToCanonical(ColumnInfo{UDTName: "Decimal(10, 2)"}); got.Kind != KindDecimal || *got.Precision != 10 || *got.Scale != 2 {
		t.Errorf("Decimal(10,2) = %+v", got)
	}
	if got := clickhouseToCanonical(ColumnInfo{UDTName: "Decimal64(4)"}); got.Kind != KindDecimal || *got.Precision != 18 || *got.Scale != 4 {
		t.Errorf("Decimal64(4) = %+v", got)
	}
	if got := clickhouseToCanonical(ColumnInfo{UDTName: "Nullable(Decimal(20, 5))"}); got.Kind != KindDecimal || *got.Precision != 20 {
		t.Errorf("Nullable(Decimal(20,5)) = %+v", got)
	}
}

func TestClickHouseFromCanonical(t *testing.T) {
	cases := []struct {
		ct     CanonicalType
		want   string
		approx bool
	}{
		{CanonicalType{Kind: KindBoolean}, "Bool", false},
		{CanonicalType{Kind: KindSmallInt}, "Int16", false},
		{CanonicalType{Kind: KindInteger}, "Int32", false},
		{CanonicalType{Kind: KindBigInt}, "Int64", false},
		{CanonicalType{Kind: KindFloat}, "Float32", false},
		{CanonicalType{Kind: KindDouble}, "Float64", false},
		{CanonicalType{Kind: KindDecimal, Precision: intp(10), Scale: intp(2)}, "Decimal(10, 2)", false},
		{CanonicalType{Kind: KindDecimal}, "Decimal(38, 9)", true},
		{CanonicalType{Kind: KindVarchar, Length: intp(255)}, "String", false},
		{CanonicalType{Kind: KindChar, Length: intp(8)}, "FixedString(8)", false},
		{CanonicalType{Kind: KindText}, "String", false},
		{CanonicalType{Kind: KindBytes}, "String", false},
		{CanonicalType{Kind: KindDate}, "Date32", false},
		{CanonicalType{Kind: KindTime}, "String", true},
		{CanonicalType{Kind: KindTimestamp}, "DateTime64(3)", false},
		{CanonicalType{Kind: KindUUID}, "UUID", false},
		{CanonicalType{Kind: KindJSON}, "String", true},
		{CanonicalType{Kind: KindRaw, TypeName: "Map(String, UInt64)"}, "Map(String, UInt64)", false},
	}
	for _, c := range cases {
		got := clickhouseFromCanonical(c.ct)
		if got.SQLType != c.want {
			t.Errorf("FromCanonical(%v) = %q, want %q", c.ct.Kind, got.SQLType, c.want)
		}
		if got.IsApproximate != c.approx {
			t.Errorf("FromCanonical(%v) approximate = %v (%q), want %v", c.ct.Kind, got.IsApproximate, got.Warning, c.approx)
		}
	}
}

// Cross-engine spot checks through the public mapping entry points.
func TestClickHouseCrossEngine(t *testing.T) {
	// mssql nvarchar(100) → clickhouse String
	ct := ToCanonical(ColumnInfo{UDTName: "nvarchar", DataType: "nvarchar", CharacterMaximumLength: intp(100)}, DialectMSSQL)
	if got := FromCanonical(ct, DialectClickHouse).SQLType; got != "String" {
		t.Errorf("mssql nvarchar(100) → clickhouse %q, want String", got)
	}
	// clickhouse DateTime64(3) → postgres timestamp-ish
	ct = ToCanonical(ColumnInfo{UDTName: "DateTime64(3)"}, DialectClickHouse)
	if ct.Kind != KindTimestamp {
		t.Fatalf("DateTime64 canonical = %v", ct.Kind)
	}
	if got := FromCanonical(ct, DialectPostgres).SQLType; got == "" {
		t.Error("postgres emission for timestamp is empty")
	}
}
