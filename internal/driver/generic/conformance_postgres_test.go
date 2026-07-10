package generic

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

// The postgres DriverCase against the CATALOG engine (#509). Identical
// to the hand-written case except placeholder NUMBERING: the catalog
// renderer numbers {?} by occurrence, the hand-written dialect used a
// fixed scheme — the (placeholder, argument) bindings are semantically
// identical; only the SQL text differs.
func TestPostgresCatalogConformance(t *testing.T) {
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "postgres",
		Driver:  NewDriver(cat),
		Aliases: []string{"postgresql", "pg"},

		QuoteName:     `weird"name`,
		QuoteNameWant: `"weird""name"`,

		QualifiedSchema:   "public",
		QualifiedTable:    "Users",
		QualifiedNameWant: `"public"."Users"`,

		PlaceholderIndex: 2,
		PlaceholderWant:  "$2",

		ColumnList:     []string{"id", `weird"name`},
		ColumnListWant: `"id", "weird""name"`,

		DateType: "timestamp",

		RequireSchemaQualification: true,

		Pagination: postgresCatalogPaginationCase(),
	})
}

func postgresCatalogPaginationCase() *conformance.PaginationCase {
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
			WHERE "id" > $1 AND "updated_at" > $2
			ORDER BY "id"
			LIMIT $3
		`,
		KeysetNoMaxArgs: []any{int64(10), ts, 25},

		KeysetWithMaxQuery: `
			SELECT "id", "name", "updated_at" FROM "public"."Users"
			WHERE "id" > $1 AND "id" <= $2 AND "updated_at" > $3
			ORDER BY "id"
			LIMIT $4
		`,
		KeysetWithMaxArgs: []any{int64(10), int64(100), ts, 25},

		RowNumberQuery: `
			WITH numbered AS (
				SELECT "id", "name", "updated_at", ROW_NUMBER() OVER (ORDER BY "id") as __rn
				FROM "public"."Users" WHERE "updated_at" > $1
			)
			SELECT "id", "name", "updated_at" FROM numbered
			WHERE __rn > $2 AND __rn <= $3
			ORDER BY __rn
		`,
		RowNumberArgs: []any{ts, int64(50), int64(75)},
	}
}

func TestPostgresCatalogCapabilities(t *testing.T) {
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	conformance.CheckWriterCapabilities(t, (*writerFull)(nil), conformance.WriterCapabilities{
		ConstraintWriter: true,
		Upserter:         true,
		SequenceResetter: true,
	})
	conformance.CheckDialectCapabilities(t, NewDialect(cat), conformance.DialectCapabilities{
		StrictParallelStrategy: "exported_snapshot",
	})
}

// The hand-written Defaults() pinned a secure TLS mode and the measured
// COPY chunk plateau — the catalog must not silently relax either when
// it takes over registration.
func TestPostgresCatalogDefaults(t *testing.T) {
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDriver(cat).Defaults()
	if d.SSLMode != "require" {
		t.Errorf("Defaults().SSLMode = %q, want %q (fail-closed TLS)", d.SSLMode, "require")
	}
	if d.OptimumBulkChunkBytes != 25_000_000 {
		t.Errorf("Defaults().OptimumBulkChunkBytes = %d, want 25000000 (#164 measured plateau)", d.OptimumBulkChunkBytes)
	}
	if d.Port != 5432 || d.Schema != "public" {
		t.Errorf("Defaults() port/schema = %d/%q, want 5432/public", d.Port, d.Schema)
	}
	if !d.ScaleWritersWithCores || d.WriteAheadWriters != 2 {
		t.Errorf("Defaults() writers = %d (scale=%v), want 2 (scale=true)", d.WriteAheadWriters, d.ScaleWritersWithCores)
	}
}
