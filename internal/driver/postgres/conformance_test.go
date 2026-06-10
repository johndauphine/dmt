package postgres

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

func TestDriverConformance(t *testing.T) {
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "postgres",
		Driver:  &Driver{},
		Aliases: []string{"postgresql", "pg"},

		QuoteName:     `weird"name`,
		QuoteNameWant: `"weird""name"`,

		QualifiedSchema:   "public",
		QualifiedTable:    "Users",
		QualifiedNameWant: `"public"."Users"`,

		PlaceholderIndex: 1,
		PlaceholderWant:  "$1",

		ColumnList:     []string{"id", `weird"name`},
		ColumnListWant: `"id", "weird""name"`,

		DateType: "timestamp",

		RequireSchemaQualification:  true,
		RequireAIPromptAugmentation: true,

		Pagination: postgresPaginationCase(),
	})
}

func postgresPaginationCase() *conformance.PaginationCase {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	return &conformance.PaginationCase{
		Columns:    `"id", "name", "updated_at"`,
		PKColumn:   "id",
		Schema:     "public",
		Table:      "Users",
		OrderBy:    `"id"`,
		DateFilter: &driver.DateFilter{Column: "updated_at", Timestamp: ts},

		LastPK: int64(10),
		MaxPK:  int64(100),
		Limit:  25,
		RowNum: int64(50),

		KeysetNoMaxQuery: `
			SELECT "id", "name", "updated_at" FROM "public"."Users"
			WHERE "id" > $1 AND "updated_at" > $3
			ORDER BY "id"
			LIMIT $2
		`,
		KeysetNoMaxArgs: []any{int64(10), 25, ts},

		KeysetWithMaxQuery: `
			SELECT "id", "name", "updated_at" FROM "public"."Users"
			WHERE "id" > $1 AND "id" <= $2 AND "updated_at" > $4
			ORDER BY "id"
			LIMIT $3
		`,
		KeysetWithMaxArgs: []any{int64(10), int64(100), 25, ts},

		RowNumberQuery: `
			WITH numbered AS (
				SELECT "id", "name", "updated_at", ROW_NUMBER() OVER (ORDER BY "id") as __rn
				FROM "public"."Users" WHERE "updated_at" > $3
			)
			SELECT "id", "name", "updated_at" FROM numbered
			WHERE __rn > $1 AND __rn <= $2
			ORDER BY __rn
		`,
		RowNumberArgs: []any{int64(50), int64(75), ts},
	}
}

// TestWriterCapabilities pins the #460 capability matrix: postgres supports
// post-transfer FK/CHECK creation.
func TestWriterCapabilities(t *testing.T) {
	conformance.CheckWriterCapabilities(t, (*Writer)(nil), conformance.WriterCapabilities{
		ConstraintWriter: true,
		Upserter:         true,
		SequenceResetter: true,
	})
	conformance.CheckReaderCapabilities(t, (*Reader)(nil), conformance.ReaderCapabilities{
		IncrementalDateReader: true,
	})
}
