package generic

import (
	"strings"
	"testing"
	"time"
)

func TestRenderInfileTSV(t *testing.T) {
	identity := func(r []any) []any { return r }
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456000, time.UTC)

	rows := [][]any{
		{int64(1), "plain", nil, 1.5},
		{int64(2), "tab\there", []byte("bin\x00ary"), ts},
		{int64(3), "line\nbreak \\ slash\rcr", true, float64(0)},
	}
	got, err := renderInfileTSV(rows, identity)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"1\tplain\t\\N\t1.5",
		"2\ttab\\there\tbin\\0ary\t2024-06-15 10:30:00.123456",
		"3\tline\\nbreak \\\\ slash\\rcr\t1\t0",
	}, "\n") + "\n"
	if string(got) != want {
		t.Errorf("TSV mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

func TestRenderInfileTSVTimeUsesUTC(t *testing.T) {
	loc := time.FixedZone("source-local", -4*60*60)
	ts := time.Date(2024, 6, 15, 8, 30, 0, 123456000, loc)

	got, err := renderInfileTSV([][]any{{ts}}, func(r []any) []any { return r })
	if err != nil {
		t.Fatal(err)
	}

	want := "2024-06-15 12:30:00.123456\n"
	if string(got) != want {
		t.Fatalf("rendered timestamp = %q, want %q", got, want)
	}
}

// Round-trip property: escaping must be reversible under LOAD DATA's
// default ESCAPED BY '\' scheme for every special byte.
func TestWriteInfileEscapedSpecials(t *testing.T) {
	in := "a\tb\nc\rd\\e\x00f"
	rows := [][]any{{in}}
	got, _ := renderInfileTSV(rows, func(r []any) []any { return r })
	if strings.ContainsAny(strings.TrimSuffix(string(got), "\n"), "\t\n\r\x00") {
		t.Errorf("unescaped special bytes survive: %q", got)
	}
}
