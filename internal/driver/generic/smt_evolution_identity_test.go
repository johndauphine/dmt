package generic

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/ident"
	"github.com/johndauphine/dmt/internal/smtddl"
)

func TestPostgresSMTEvolutionIdentifiersMatchCreatePath(t *testing.T) {
	tests := []struct {
		name           string
		targetSchema   string
		physicalSchema string
		renderSchema   string
		tableName      string
		columnName     string
	}{
		{
			name:           "mixed punctuation and default schema",
			targetSchema:   "Public",
			physicalSchema: "public",
			tableName:      "9 Sales.Order History",
			columnName:     "New Status-Code",
		},
		{
			name:           "long names and custom schema retain DMT identity",
			targetSchema:   "Sales Ops",
			physicalSchema: "sales_ops",
			renderSchema:   "sales_ops",
			tableName:      strings.Repeat("Long Mixed.Table-Name ", 6),
			columnName:     strings.Repeat("Long Mixed.Column-Name ", 6),
		},
	}

	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	w := &Writer{
		cat:        cat,
		sourceType: "postgres",
		ident:      identifierSanitizers[cat.Quoting.IdentifierSanitizer],
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkName := "1 Primary-Key"
			table := &driver.Table{
				Name: tt.tableName,
				Columns: []driver.Column{
					{Name: pkName, DataType: "bigint", IsNullable: false},
					{Name: tt.columnName, DataType: "character varying", MaxLength: 160, IsNullable: true},
				},
				PrimaryKey: []string{pkName},
			}
			createSQL, err := driver.PlanCreateTable(driver.TableDDLRequest{
				SourceDBType: "postgres",
				TargetDBType: "postgres",
				SourceTable:  table,
				TargetSchema: tt.targetSchema,
			})
			if err != nil {
				t.Fatalf("PlanCreateTable: %v", err)
			}

			wantTable := ident.SanitizePG(tt.tableName)
			wantPK := ident.SanitizePG(pkName)
			wantColumn := ident.SanitizePG(tt.columnName)
			wantTableSQL := `"` + wantTable + `"`
			if tt.renderSchema != "" {
				wantTableSQL = `"` + tt.renderSchema + `".` + wantTableSQL
			}
			for _, want := range []string{
				wantTableSQL,
				`"` + wantPK + `"`,
				`"` + wantColumn + `"`,
			} {
				if !strings.Contains(createSQL, want) {
					t.Fatalf("CREATE SQL is missing %q:\n%s", want, createSQL)
				}
			}

			req := w.smtEvolutionRequest(table, tt.targetSchema)
			if req.TargetSchema != tt.renderSchema ||
				req.Table.Name != wantTable ||
				req.Table.Columns[0].Name != wantPK ||
				req.Table.Columns[1].Name != wantColumn ||
				req.Table.PrimaryKey[0] != wantPK {
				t.Fatalf("evolution identifiers = schema:%q table:%q columns:%q/%q pk:%q; want %q/%q/%q/%q",
					req.TargetSchema,
					req.Table.Name,
					req.Table.Columns[0].Name,
					req.Table.Columns[1].Name,
					req.Table.PrimaryKey[0],
					tt.renderSchema,
					wantTable,
					wantPK,
					wantColumn,
				)
			}
			probeArgs := w.introArgs(tt.targetSchema, wantTable, wantColumn)
			if got := probeArgs[0]; got != tt.physicalSchema {
				t.Fatalf("physical probe schema = %q, want %q", got, tt.physicalSchema)
			}

			column := w.smtEvolutionColumn(table.Columns[1])
			type namedBatch struct {
				name  string
				batch smtddl.Batch
			}
			var batches []namedBatch
			add, err := smtddl.RenderAddColumn(req, column)
			if err != nil {
				t.Fatalf("RenderAddColumn: %v", err)
			}
			batches = append(batches, namedBatch{"add", add})
			nullability, err := smtddl.RenderAlterColumnNullability(req, column)
			if err != nil {
				t.Fatalf("RenderAlterColumnNullability: %v", err)
			}
			batches = append(batches, namedBatch{"nullability", nullability})
			alterType, err := smtddl.RenderAlterColumnType(req, column)
			if err != nil {
				t.Fatalf("RenderAlterColumnType: %v", err)
			}
			batches = append(batches, namedBatch{"type", alterType})
			drop, err := smtddl.RenderDropTable(req, true)
			if err != nil {
				t.Fatalf("RenderDropTable: %v", err)
			}
			batches = append(batches, namedBatch{"drop", drop})
			truncate, err := smtddl.RenderTruncateTable(req, true)
			if err != nil {
				t.Fatalf("RenderTruncateTable: %v", err)
			}
			batches = append(batches, namedBatch{"truncate", truncate})

			wantColumnSQL := `"` + wantColumn + `"`
			for _, artifact := range batches {
				if len(artifact.batch.Statements) == 0 {
					t.Fatalf("%s batch is empty", artifact.name)
				}
				sql := artifact.batch.Statements[0].SQL
				if !strings.Contains(sql, wantTableSQL) {
					t.Fatalf("%s SQL targets a different table identity:\n%s\nCREATE:\n%s", artifact.name, sql, createSQL)
				}
				if artifact.name != "drop" && artifact.name != "truncate" && !strings.Contains(sql, wantColumnSQL) {
					t.Fatalf("%s SQL targets a different column identity:\n%s\nCREATE:\n%s", artifact.name, sql, createSQL)
				}
			}
		})
	}
}
