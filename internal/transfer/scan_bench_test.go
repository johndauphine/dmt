package transfer

import (
	"database/sql"
	"fmt"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"testing"

	_ "modernc.org/sqlite"
)

// BenchmarkScanRows measures the scan hot path over a mixed-type row shape
// (#466). The fixture mixes converter-relevant types (bit, datetime,
// uniqueidentifier) with pass-through types so the benchmark reflects the
// real cost balance: most columns need no conversion.
func BenchmarkScanRows(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT, d REAL, e INTEGER, f TEXT, g TEXT)`); err != nil {
		b.Fatal(err)
	}
	tx, _ := db.Begin()
	for i := 0; i < 5000; i++ {
		if _, err := tx.Exec(`INSERT INTO t VALUES (?,?,?,?,?,?,?,?)`,
			i, fmt.Sprintf("name-%d", i), i%2, "2024-01-02 03:04:05", float64(i)*1.5, i*7, "some longer text value for realism", "C56A4180-65AA-42EC-A945-5FD21DEC0538"); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	colTypes := []string{"int", "varchar", "bit", "datetime", "float", "int", "text", "uniqueidentifier"}
	convs := driver.DefaultValueConverters(colTypes)
	convIdx := buildConvIdx(convs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT id,a,b,c,d,e,f,g FROM t`)
		if err != nil {
			b.Fatal(err)
		}
		chunk, _, err := scanRows(rows, len(colTypes), convs, convIdx)
		rows.Close()
		if err != nil {
			b.Fatal(err)
		}
		if len(chunk) != 5000 {
			b.Fatalf("got %d rows", len(chunk))
		}
	}
}
