package generic

import (
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

// The mssql DriverCase against the CATALOG engine (#509). Identical to
// the hand-written case except PARAMETER STYLE: the hand-written
// dialect used sql.Named arguments (@limit, @lastPK, @lastSyncDate);
// the catalog renderer numbers {?} by occurrence as @p1.. with
// positional binding — go-mssqldb binds @pN positionally, so the
// (placeholder, argument) pairings are semantically identical.
func TestMssqlCatalogConformance(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "mssql",
		Driver:  NewDriver(cat),
		Aliases: []string{"sqlserver", "sql-server"},

		QuoteName:     `weird]name`,
		QuoteNameWant: `[weird]]name]`,

		QualifiedSchema:   "dbo",
		QualifiedTable:    "Users",
		QualifiedNameWant: `[dbo].[Users]`,

		PlaceholderIndex: 1,
		PlaceholderWant:  "@p1",

		ColumnList:     []string{"id", `weird]name`},
		ColumnListWant: `[id], [weird]]name]`,

		DateType: "datetime2",

		RequireSchemaQualification: true,

		Pagination: mssqlCatalogPaginationCase(),
	})
}

func mssqlCatalogPaginationCase() *conformance.PaginationCase {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	lastSync := "2024-06-15T10:30:00.1234567"
	return &conformance.PaginationCase{
		Columns:    `[id], [name], [updated_at]`,
		PKColumn:   "id",
		Schema:     "dbo",
		Table:      "Users",
		TableHint:  "WITH (NOLOCK)",
		OrderBy:    `[id]`,
		DateFilter: &driver.DateFilter{Column: "updated_at", Timestamp: ts},

		LastPK: int64(10),
		MaxPK:  int64(100),
		Limit:  25,
		RowNum: int64(50),

		KeysetNoMaxQuery: `
			SELECT TOP (@p1) [id], [name], [updated_at]
			FROM [dbo].[Users] WITH (NOLOCK)
			WHERE [id] > @p2 AND CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @p3, 126)
			ORDER BY [id]
		`,
		KeysetNoMaxArgs: []any{25, int64(10), lastSync},

		KeysetWithMaxQuery: `
			SELECT TOP (@p1) [id], [name], [updated_at]
			FROM [dbo].[Users] WITH (NOLOCK)
			WHERE [id] > @p2 AND [id] <= @p3 AND CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @p4, 126)
			ORDER BY [id]
		`,
		KeysetWithMaxArgs: []any{25, int64(10), int64(100), lastSync},

		RowNumberQuery: `
			WITH numbered AS (
				SELECT [id], [name], [updated_at], ROW_NUMBER() OVER (ORDER BY [id]) as __rn
				FROM [dbo].[Users] WITH (NOLOCK) WHERE CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @p1, 126)
			)
			SELECT [id], [name], [updated_at] FROM numbered
			WHERE __rn > @p2 AND __rn <= @p3
			ORDER BY __rn
		`,
		RowNumberArgs: []any{lastSync, int64(50), int64(75)},
	}
}

// Capability matrix + defaults: the catalog must not relax the
// hand-written driver's secure encrypt default or the 32KB packet size.
func TestMssqlCatalogCapabilities(t *testing.T) {
	conformance.CheckWriterCapabilities(t, (*writerFull)(nil), conformance.WriterCapabilities{
		ConstraintWriter: true,
		Upserter:         true,
		SequenceResetter: true,
	})
}

func TestMssqlCatalogDefaults(t *testing.T) {
	cat, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	d := NewDriver(cat).Defaults()
	if !d.Encrypt {
		t.Error("Defaults().Encrypt = false, want true (fail-closed TLS)")
	}
	if d.PacketSize != 32767 {
		t.Errorf("Defaults().PacketSize = %d, want 32767", d.PacketSize)
	}
	if d.Port != 1433 || d.Schema != "dbo" {
		t.Errorf("Defaults() port/schema = %d/%q, want 1433/dbo", d.Port, d.Schema)
	}
	if !d.ScaleWritersWithCores || d.WriteAheadWriters != 2 {
		t.Errorf("Defaults() writers = %d (scale=%v), want 2 (scale=true)", d.WriteAheadWriters, d.ScaleWritersWithCores)
	}
}
