package generic

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

// The mysql DriverCase against the CATALOG engine (#509) — identical
// expectations to the hand-written driver's conformance test, which
// stays in place as the oracle until the registration flip. Note: the
// harness's "registered" subtest resolves driver.Get("mysql") to
// whichever driver is registered; both report the same name, so this
// case is valid before and after the flip.
func TestMySQLCatalogConformance(t *testing.T) {
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "mysql",
		Driver:  NewDriver(cat),
		Aliases: []string{"mariadb", "maria"},

		QuoteName:     "weird`name",
		QuoteNameWant: "`weird``name`",

		QualifiedSchema:   "app",
		QualifiedTable:    "Users",
		QualifiedNameWant: "`app`.`Users`",

		PlaceholderIndex: 1,
		PlaceholderWant:  "?",

		ColumnList:     []string{"id", "weird`name"},
		ColumnListWant: "`id`, `weird``name`",

		DateType: "datetime",

		RequireSchemaQualification: true,

		Pagination: mysqlCatalogPaginationCase(),
	})
}

func mysqlCatalogPaginationCase() *conformance.PaginationCase {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	return &conformance.PaginationCase{
		Columns:    "`id`, `name`, `updated_at`",
		PKColumn:   "id",
		Schema:     "app",
		Table:      "Users",
		OrderBy:    "`id`",
		DateFilter: &driver.DateFilter{Column: "updated_at", Timestamp: ts},

		LastPK: int64(10),
		MaxPK:  int64(100),
		Limit:  25,
		RowNum: int64(50),

		KeysetNoMaxQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM ` + "`app`.`Users`" + `
			WHERE ` + "`id`" + ` > ? AND ` + "`updated_at`" + ` > ?
			ORDER BY ` + "`id`" + `
			LIMIT ?
		`,
		KeysetNoMaxArgs: []any{int64(10), ts, 25},

		KeysetWithMaxQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM ` + "`app`.`Users`" + `
			WHERE ` + "`id`" + ` > ? AND ` + "`id`" + ` <= ? AND ` + "`updated_at`" + ` > ?
			ORDER BY ` + "`id`" + `
			LIMIT ?
		`,
		KeysetWithMaxArgs: []any{int64(10), int64(100), ts, 25},

		RowNumberQuery: `
			SELECT ` + "`id`, `name`, `updated_at`" + ` FROM (
				SELECT ` + "`id`, `name`, `updated_at`" + `, ROW_NUMBER() OVER (ORDER BY ` + "`id`" + `) as __rn
				FROM ` + "`app`.`Users`" + ` WHERE ` + "`updated_at`" + ` > ?
			) AS numbered
			WHERE __rn > ? AND __rn <= ?
			ORDER BY __rn
		`,
		RowNumberArgs: []any{ts, int64(50), int64(75)},
	}
}

// Full capability matrix (#460): the writerFull combo.
func TestMySQLCatalogCapabilities(t *testing.T) {
	cat, err := LoadCatalog("mysql")
	if err != nil {
		t.Fatal(err)
	}
	conformance.CheckWriterCapabilities(t, (*writerFull)(nil), conformance.WriterCapabilities{
		ConstraintWriter: true,
		Upserter:         true,
		SequenceResetter: true,
	})
	conformance.CheckDialectCapabilities(t, NewDialect(cat), conformance.DialectCapabilities{
		StrictParallelStrategy: "none",
	})
}
