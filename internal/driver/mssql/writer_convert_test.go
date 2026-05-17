package mssql

import (
	"bytes"
	"testing"
)

func TestConvertRowForBulkCopyConvertsUTF8TextBytes(t *testing.T) {
	row := []any{
		[]byte("M\u00fcnchen"),
		[]byte("\u65e5\u672c\u8a9e \U0001f680"),
		[]byte("plain ASCII"),
		[]byte("active"),
		[]byte("read,write"),
	}

	got := convertRowForBulkCopy(row, []string{"varchar(255)", "longtext", "nvarchar(max)", "enum", "set"})

	want := []string{"M\u00fcnchen", "\u65e5\u672c\u8a9e \U0001f680", "plain ASCII", "active", "read,write"}
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

func TestConvertRowForBulkCopyPreservesLegacyNumericBytes(t *testing.T) {
	got := convertRowForBulkCopy([]any{[]byte("123.45")}, nil)

	if got[0] != "123.45" {
		t.Fatalf("numeric bytes = %#v, want string", got[0])
	}
}

func TestConvertRowForBulkCopyLeavesUnknownNonNumericBytes(t *testing.T) {
	want := []byte("M\u00fcnchen")

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
