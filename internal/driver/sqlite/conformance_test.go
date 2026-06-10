package sqlite

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

func TestDriverConformance(t *testing.T) {
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "sqlite",
		Driver:  &Driver{},
		Aliases: []string{"sqlite3", "sqlitedb"},

		QuoteName:     `weird"name`,
		QuoteNameWant: `"weird""name"`,

		QualifiedSchema:   "ignored",
		QualifiedTable:    "Users",
		QualifiedNameWant: `"Users"`,

		PlaceholderIndex: 1,
		PlaceholderWant:  "?",

		ColumnList:     []string{"id", `weird"name`},
		ColumnListWant: `"id", "weird""name"`,

		DateType: "text",

		SchemaIgnoredInQualification: true,
		RequireAIPromptAugmentation:  true,

		Pagination: sqlitePaginationCase(),
	})
}

func sqlitePaginationCase() *conformance.PaginationCase {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	return &conformance.PaginationCase{
		Columns:    `"id", "name", "updated_at"`,
		PKColumn:   "id",
		Schema:     "ignored",
		Table:      "Users",
		OrderBy:    `"id"`,
		DateFilter: &driver.DateFilter{Column: "updated_at", Timestamp: ts},

		LastPK: int64(10),
		MaxPK:  int64(100),
		Limit:  25,
		RowNum: int64(50),

		KeysetNoMaxQuery: `
			SELECT "id", "name", "updated_at" FROM "Users"
			WHERE "id" > ? AND "updated_at" > ?
			ORDER BY "id"
			LIMIT ?
		`,
		KeysetNoMaxArgs: []any{int64(10), ts, 25},

		KeysetWithMaxQuery: `
			SELECT "id", "name", "updated_at" FROM "Users"
			WHERE "id" > ? AND "id" <= ? AND "updated_at" > ?
			ORDER BY "id"
			LIMIT ?
		`,
		KeysetWithMaxArgs: []any{int64(10), int64(100), ts, 25},

		RowNumberQuery: `
			SELECT "id", "name", "updated_at" FROM (
				SELECT "id", "name", "updated_at", ROW_NUMBER() OVER (ORDER BY "id") as __rn
				FROM "Users" WHERE "updated_at" > ?
			)
			WHERE __rn > ? AND __rn <= ?
			ORDER BY __rn
		`,
		RowNumberArgs: []any{ts, int64(50), int64(75)},
	}
}

// TestWriterCapabilities pins the #460 capability matrix: SQLite cannot
// ADD CONSTRAINT after CREATE TABLE, so it deliberately does not
// implement driver.ConstraintWriter — finalization skips FK/CHECK with
// one audited message instead of per-constraint stub warnings.
func TestWriterCapabilities(t *testing.T) {
	conformance.CheckWriterCapabilities(t, (*Writer)(nil), conformance.WriterCapabilities{
		ConstraintWriter: false,
	})
}
