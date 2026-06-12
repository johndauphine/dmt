package generic

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

// The ClickHouse DriverCase (#507), written from ClickHouse's
// documented SQL semantics BEFORE the catalog (#478 rule: the case is
// the catalog's definition of done):
//   - identifiers quote with backticks, escaped by doubling
//   - the "schema" is the database: qualification is `db`.`table`
//   - positional ? placeholders (clickhouse-go std interface)
//   - no table hints
//   - keyset and ROW_NUMBER() OVER pagination both valid (window
//     functions are GA since 21.x); LIMIT takes a ? parameter
func TestClickHouseDriverConformance(t *testing.T) {
	cat, err := LoadCatalog("clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "clickhouse",
		Driver:  NewDriver(cat),
		Aliases: []string{"ch"},

		QuoteName:     "weird`name",
		QuoteNameWant: "`weird``name`",

		QualifiedSchema:   "analytics",
		QualifiedTable:    "events",
		QualifiedNameWant: "`analytics`.`events`",

		PlaceholderIndex: 1,
		PlaceholderWant:  "?",

		ColumnList:     []string{"id", "weird`name"},
		ColumnListWant: "`id`, `weird``name`",

		DateType: "datetime",

		RequireSchemaQualification: true,

		Pagination: clickhousePaginationCase(),
	})
}

func clickhousePaginationCase() *conformance.PaginationCase {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	return &conformance.PaginationCase{
		Columns:    "`id`, `name`, `updated_at`",
		PKColumn:   "id",
		Schema:     "analytics",
		Table:      "events",
		OrderBy:    "`id`",
		DateFilter: &driver.DateFilter{Column: "updated_at", Timestamp: ts},

		LastPK: int64(10),
		MaxPK:  int64(100),
		Limit:  25,
		RowNum: int64(50),

		KeysetNoMaxQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM ` + "`analytics`.`events`" + `
			WHERE ` + "`id`" + ` > ? AND ` + "`updated_at`" + ` > ?
			ORDER BY ` + "`id`" + `
			LIMIT ?
		`,
		KeysetNoMaxArgs: []any{int64(10), ts, 25},

		KeysetWithMaxQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM ` + "`analytics`.`events`" + `
			WHERE ` + "`id`" + ` > ? AND ` + "`id`" + ` <= ? AND ` + "`updated_at`" + ` > ?
			ORDER BY ` + "`id`" + `
			LIMIT ?
		`,
		KeysetWithMaxArgs: []any{int64(10), int64(100), ts, 25},

		RowNumberQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM (
				SELECT ` + "`id`, `name`, `updated_at`" + `, ROW_NUMBER() OVER (ORDER BY ` + "`id`" + `) as __rn
				FROM ` + "`analytics`.`events`" + ` WHERE ` + "`updated_at`" + ` > ?
			)
			WHERE __rn > ? AND __rn <= ?
			ORDER BY __rn
		`,
		RowNumberArgs: []any{ts, int64(50), int64(75)},
	}
}

// The capability matrix (#460): ClickHouse has no upsert (MergeTree
// dedup is eventual via ReplacingMergeTree, not transactional), no
// sequences, and no post-transfer FK/CHECK DDL. Incremental date
// reads work fine.
func TestClickHouseCapabilities(t *testing.T) {
	conformance.CheckWriterCapabilities(t, (*Writer)(nil), conformance.WriterCapabilities{
		ConstraintWriter: false,
		Upserter:         false,
		SequenceResetter: false,
	})
	conformance.CheckReaderCapabilities(t, (*readerWithDates)(nil), conformance.ReaderCapabilities{
		IncrementalDateReader: true,
	})
}
