package generic

import (
	"bytes"
	"strings"
	"testing"
)

// Tests ported from the hand-written mssql driver with its removal
// (#509 cleanup) — they pin the TDS bulk-copy value conversion, the
// #227 MERGE shapes, and the staging-name uniqueness contract on the
// generic strategy implementations.

func TestMSSQLCheckidentStatementQuotingAndInjection(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatalf("LoadCatalog(mssql): %v", err)
	}
	d := NewDialect(cat)

	t.Run("apostrophe in name is escaped for the string literal", func(t *testing.T) {
		got := mssqlCheckidentStatement(d, "dbo", "It's Data", 42)
		want := "DBCC CHECKIDENT ('[dbo].[It''s Data]', RESEED, 42)"
		if got != want {
			t.Fatalf("got  %q\nwant %q", got, want)
		}
	})

	t.Run("apostrophe in schema is escaped too", func(t *testing.T) {
		got := mssqlCheckidentStatement(d, "O'Brien", "t", 7)
		want := "DBCC CHECKIDENT ('[O''Brien].[t]', RESEED, 7)"
		if got != want {
			t.Fatalf("got  %q\nwant %q", got, want)
		}
	})

	t.Run("injection payload cannot break out of the literal", func(t *testing.T) {
		// A hostile source table name that tries to terminate the literal and
		// append its own statement. After escaping, every apostrophe in the
		// name must be doubled so the whole thing stays one string argument.
		evil := "x]', RESEED, 1); DROP TABLE [y]--"
		got := mssqlCheckidentStatement(d, "dbo", evil, 1)

		// The statement is the fixed DBCC template with exactly one opening and
		// one closing literal quote around the (escaped) argument.
		if !strings.HasPrefix(got, "DBCC CHECKIDENT ('") || !strings.HasSuffix(got, "', RESEED, 1)") {
			t.Fatalf("statement shape changed unexpectedly: %q", got)
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(got, "DBCC CHECKIDENT ('"), "', RESEED, 1)")
		// No bare (un-doubled) single quote may remain inside the literal —
		// that is exactly what would let the payload close the string early.
		if strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
			t.Fatalf("un-escaped single quote survived in literal argument: %q", inner)
		}
	})
}

func TestConvertRowForBulkCopyConvertsUTF8TextBytes(t *testing.T) {
	row := []any{
		[]byte("München"),
		[]byte("日本語 \U0001f680"),
		[]byte("plain ASCII"),
		[]byte("active"),
		[]byte("read,write"),
	}

	got := convertRowForBulkCopy(row, []string{"varchar(255)", "longtext", "nvarchar(max)", "enum", "set"})

	want := []string{"München", "日本語 \U0001f680", "plain ASCII", "active", "read,write"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("column %d = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestConvertRowForBulkCopyKeepsBinaryBytes(t *testing.T) {
	want := []byte{0x01, 0x02, 0x03}

	got := convertRowForBulkCopy([]any{want}, []string{"blob"})

	gotBytes, ok := got[0].([]byte)
	if !ok {
		t.Fatalf("binary column converted to %T, want []byte", got[0])
	}
	if !bytes.Equal(gotBytes, want) {
		t.Fatalf("binary column = %v, want %v", gotBytes, want)
	}
}

// ASCII-digit payloads in BINARY-typed columns must stay []byte —
// go-mssqldb bulk copy rejects strings there (codex on #509; the
// hand-written writer converted them).
func TestConvertRowForBulkCopyKeepsNumericLookingBinaryBytes(t *testing.T) {
	want := []byte("123")

	got := convertRowForBulkCopy([]any{want}, []string{"varbinary(16)"})

	gotBytes, ok := got[0].([]byte)
	if !ok {
		t.Fatalf("varbinary column converted to %T, want []byte", got[0])
	}
	if !bytes.Equal(gotBytes, want) {
		t.Fatalf("varbinary column = %v, want %v", gotBytes, want)
	}
}

func TestConvertRowForBulkCopyPreservesLegacyNumericBytes(t *testing.T) {
	got := convertRowForBulkCopy([]any{[]byte("123.45")}, nil)

	if got[0] != "123.45" {
		t.Fatalf("numeric bytes = %#v, want string", got[0])
	}
}

func TestConvertRowForBulkCopyLeavesUnknownNonNumericBytes(t *testing.T) {
	want := []byte("München")

	got := convertRowForBulkCopy([]any{want}, nil)

	gotBytes, ok := got[0].([]byte)
	if !ok {
		t.Fatalf("unknown nonnumeric bytes converted to %T, want []byte", got[0])
	}
	if !bytes.Equal(gotBytes, want) {
		t.Fatalf("unknown nonnumeric bytes = %v, want %v", gotBytes, want)
	}
}

func TestConvertRowForBulkCopyKeepsInvalidUTF8TextBytes(t *testing.T) {
	want := []byte{0xff, 0xfe}

	got := convertRowForBulkCopy([]any{want}, []string{"varchar(255)"})

	gotBytes, ok := got[0].([]byte)
	if !ok {
		t.Fatalf("invalid UTF-8 bytes converted to %T, want []byte", got[0])
	}
	if !bytes.Equal(gotBytes, want) {
		t.Fatalf("invalid UTF-8 bytes = %v, want %v", gotBytes, want)
	}
}

func TestIsASCIINumeric(t *testing.T) {
	tests := []struct {
		input    []byte
		expected bool
	}{
		{[]byte("123"), true},
		{[]byte("-45.67"), true},
		{[]byte("+0.5"), true},
		{[]byte("1.5E+10"), true},
		{[]byte("1e-5"), true},
		{[]byte(".5"), true},
		{[]byte(""), false},
		{[]byte("."), false},
		{[]byte("+-1"), false},
		{[]byte("1.2.3"), false},
		{[]byte("abc"), false},
		{[]byte{0x01, 0x02, 0x03}, false}, // binary data
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := isASCIINumeric(tt.input)
			if result != tt.expected {
				t.Errorf("isASCIINumeric(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestMssqlBuildMerge_InsertOnlyOmitsMatchedBranch pins the resume-safe
// shape added for #227: when insertOnly=true, the MERGE must contain
// only WHEN NOT MATCHED (replayed already-present rows are silent
// no-ops). When insertOnly=false the upsert path's WHEN MATCHED ...
// THEN UPDATE branch must still be emitted.
func TestMssqlBuildMerge_InsertOnlyOmitsMatchedBranch(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDialect(cat)

	cols := []string{"id", "title", "body"}
	pkCols := []string{"id"}
	target := "[dbo].[posts]"
	staging := "#stg_posts_w0"

	insertOnly := mssqlBuildMerge(d, target, staging, cols, pkCols, nil, false, true)
	if strings.Contains(insertOnly, "WHEN MATCHED") {
		t.Errorf("insertOnly=true MERGE must not contain WHEN MATCHED:\n%s", insertOnly)
	}
	if !strings.Contains(insertOnly, "WHEN NOT MATCHED THEN INSERT") {
		t.Errorf("insertOnly=true MERGE must contain WHEN NOT MATCHED THEN INSERT:\n%s", insertOnly)
	}
	if !strings.Contains(insertOnly, "ON target.[id] = source.[id]") {
		t.Errorf("insertOnly=true MERGE must contain ON-clause for PK:\n%s", insertOnly)
	}

	upsert := mssqlBuildMerge(d, target, staging, cols, pkCols, nil, false, false)
	if !strings.Contains(upsert, "WHEN MATCHED") {
		t.Errorf("insertOnly=false (upsert) MERGE must contain WHEN MATCHED — regression:\n%s", upsert)
	}
	if !strings.Contains(upsert, "WHEN NOT MATCHED THEN INSERT") {
		t.Errorf("insertOnly=false MERGE must contain WHEN NOT MATCHED THEN INSERT:\n%s", upsert)
	}
}

// Spatial MERGE shapes (codex on #509, fixed relative to the
// hand-written writer): any mutable spatial column forces an
// unconditional update — change detection can't compare spatial types,
// so the predicate would skip spatial-only changes (and rendered an
// empty "AND ()" syntax error for all-spatial tables).
func TestMssqlBuildMerge_SpatialForcesUnconditionalUpdate(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDialect(cat)
	spatial := []spatialColumn{{Name: "geo", TypeName: "geography"}}

	mixed := mssqlBuildMerge(d, "[dbo].[t]", "#stg", []string{"id", "name", "geo"}, []string{"id"}, spatial, true, false)
	if strings.Contains(mixed, "WHEN MATCHED AND") {
		t.Errorf("mixed spatial table must update unconditionally:\n%s", mixed)
	}
	if !strings.Contains(mixed, "WHEN MATCHED THEN UPDATE") {
		t.Errorf("mixed spatial table must still update:\n%s", mixed)
	}
	if !strings.Contains(mixed, "geography::STGeomFromText(source.[geo], 4326)") {
		t.Errorf("cross-engine spatial source must convert WKT:\n%s", mixed)
	}

	allSpatial := mssqlBuildMerge(d, "[dbo].[t]", "#stg", []string{"id", "geo"}, []string{"id"}, spatial, true, false)
	if strings.Contains(allSpatial, "AND ()") {
		t.Errorf("all-spatial table rendered the empty predicate:\n%s", allSpatial)
	}
}

// TestMssqlBuildMerge_ChangeDetectionIsByteExact pins #547: the WHEN
// MATCHED change-detection predicate must compare values byte-exactly
// (VARBINARY) rather than with a bare `<>`, otherwise SQL Server's
// default case-insensitive, ANSI-padded collation drops case-only and
// trailing-space changes and the sync silently diverges.
func TestMssqlBuildMerge_ChangeDetectionIsByteExact(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDialect(cat)

	cols := []string{"id", "name"}
	pkCols := []string{"id"}
	upsert := mssqlBuildMerge(d, "[dbo].[users]", "#stg", cols, pkCols, nil, false, false)

	// The non-PK column must be compared byte-exact, both sides converted.
	if !strings.Contains(upsert, "CONVERT(VARBINARY(MAX), target.[name]) <> CONVERT(VARBINARY(MAX), source.[name])") {
		t.Errorf("change detection must compare [name] byte-exact via VARBINARY:\n%s", upsert)
	}
	// A bare collation-sensitive comparison of the column must NOT appear.
	if strings.Contains(upsert, "target.[name] <> source.[name]") {
		t.Errorf("change detection still uses a collation-sensitive `<>` for [name]:\n%s", upsert)
	}
	// NULL-transition disjuncts preserved.
	if !strings.Contains(upsert, "target.[name] IS NULL AND source.[name] IS NOT NULL") ||
		!strings.Contains(upsert, "target.[name] IS NOT NULL AND source.[name] IS NULL") {
		t.Errorf("NULL-transition predicates must be preserved:\n%s", upsert)
	}
	// The PK is not part of change detection.
	if strings.Contains(upsert, "CONVERT(VARBINARY(MAX), target.[id])") {
		t.Errorf("PK column must not appear in change detection:\n%s", upsert)
	}
}

// TestMssqlSafeStagingName guards the resume-safe path: concurrent
// partitions on the same writer connection must produce different
// staging table names, and quoted-identifier characters must be
// sanitized out — the name is interpolated unquoted into SELECT INTO /
// ALTER TABLE (codex on #509; the hand-written writer embedded the raw
// name).
func TestMssqlSafeStagingName(t *testing.T) {
	p1, p2 := 1, 2
	n1 := mssqlSafeStagingName("posts", 0, &p1)
	n2 := mssqlSafeStagingName("posts", 0, &p2)
	if n1 == n2 {
		t.Errorf("partitioned staging names must differ: p1=%q p2=%q", n1, n2)
	}
	nNil := mssqlSafeStagingName("posts", 0, nil)
	if nNil == n1 {
		t.Errorf("partitioned and unpartitioned staging names must differ: nil=%q p1=%q", nNil, n1)
	}

	spaced := mssqlSafeStagingName("Order Details", 0, nil)
	if strings.ContainsAny(spaced, " ]['\"") {
		t.Errorf("staging name must be quoting-free: %q", spaced)
	}
	if !strings.HasPrefix(spaced, "#stg_Order_Details") {
		t.Errorf("sanitized name unexpected: %q", spaced)
	}
}
