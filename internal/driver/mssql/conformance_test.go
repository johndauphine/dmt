package mssql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/conformance"
)

func TestDriverConformance(t *testing.T) {
	conformance.RunDriverConformance(t, conformance.DriverCase{
		Name:    "mssql",
		Driver:  &Driver{},
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

		Pagination: mssqlPaginationCase(),
	})
}

func mssqlPaginationCase() *conformance.PaginationCase {
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
			SELECT TOP (@limit) [id], [name], [updated_at]
			FROM [dbo].[Users] WITH (NOLOCK)
			WHERE [id] > @lastPK
				AND CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @lastSyncDate, 126)
			ORDER BY [id]
		`,
		KeysetNoMaxArgs: []any{
			sql.Named("limit", 25),
			sql.Named("lastPK", int64(10)),
			sql.Named("lastSyncDate", lastSync),
		},

		KeysetWithMaxQuery: `
			SELECT TOP (@limit) [id], [name], [updated_at]
			FROM [dbo].[Users] WITH (NOLOCK)
			WHERE [id] > @lastPK AND [id] <= @maxPK
				AND CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @lastSyncDate, 126)
			ORDER BY [id]
		`,
		KeysetWithMaxArgs: []any{
			sql.Named("limit", 25),
			sql.Named("lastPK", int64(10)),
			sql.Named("maxPK", int64(100)),
			sql.Named("lastSyncDate", lastSync),
		},

		RowNumberQuery: `
			WITH numbered AS (
				SELECT [id], [name], [updated_at], ROW_NUMBER() OVER (ORDER BY [id]) as __rn
				FROM [dbo].[Users] WITH (NOLOCK)
				WHERE CONVERT(datetime2(7), [updated_at]) > CONVERT(datetime2(7), @lastSyncDate, 126)
			)
			SELECT [id], [name], [updated_at] FROM numbered
			WHERE __rn > @rowNum AND __rn <= @rowNumEnd
			ORDER BY __rn
		`,
		RowNumberArgs: []any{
			sql.Named("rowNum", int64(50)),
			sql.Named("rowNumEnd", int64(75)),
			sql.Named("lastSyncDate", lastSync),
		},
	}
}

// TestWriterCapabilities pins the #460 capability matrix: mssql supports
// post-transfer FK/CHECK creation.
func TestWriterCapabilities(t *testing.T) {
	conformance.CheckWriterCapabilities(t, (*Writer)(nil), conformance.WriterCapabilities{
		ConstraintWriter: true,
	})
}
