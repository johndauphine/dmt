package generic

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/smtddl"
)

const smtBatchCleanupTimeout = 5 * time.Second

type smtBatchExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// executeSMTBatch honors SMT's execution contract without interpreting or
// rewriting its SQL. Connection-affine batches pin one physical connection
// for the complete statement sequence and failure cleanup.
func (w *Writer) executeSMTBatch(ctx context.Context, batch smtddl.Batch) error {
	if batch.IsEmpty() {
		return nil
	}
	if !batch.RequiresSingleConnection {
		return executeSMTBatchOn(ctx, w.db, batch)
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pinning connection for SMT DDL batch: %w", err)
	}
	defer conn.Close()
	return executeSMTBatchOn(ctx, conn, batch)
}

func executeSMTBatchOn(ctx context.Context, execer smtBatchExecer, batch smtddl.Batch) error {
	for index, statement := range batch.Statements {
		logging.DebugEvent("Executing SMT evolution DDL",
			"statement_index", index,
			"statement_kind", statement.Kind,
			"ddl", statement.SQL,
		)
		if _, err := execer.ExecContext(ctx, statement.SQL); err != nil {
			if batch.IsBestEffort(index) {
				logging.WarnEvent("SMT best-effort DDL statement failed",
					"statement_index", index,
					"statement_kind", statement.Kind,
					"error", err,
				)
				continue
			}

			primary := fmt.Errorf("SMT DDL statement %d (%s): %w", index, statement.Kind, err)
			runSMTBatchCleanup(execer, batch.Cleanup)
			return primary
		}
	}
	return nil
}

// runSMTBatchCleanup uses an independent bounded context so cancellation of
// the migration cannot strand connection-local state. Cleanup failures are
// logged in order and never replace the required statement's primary error.
func runSMTBatchCleanup(execer smtBatchExecer, cleanup []smtddl.Statement) {
	if len(cleanup) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), smtBatchCleanupTimeout)
	defer cancel()

	for index, statement := range cleanup {
		if _, err := execer.ExecContext(ctx, statement.SQL); err != nil {
			logging.WarnEvent("SMT DDL failure cleanup failed",
				"cleanup_index", index,
				"statement_kind", statement.Kind,
				"error", err,
			)
		}
	}
}

func (w *Writer) smtEvolutionRequest(table *driver.Table, targetSchema string) smtddl.Request {
	req := smtddl.Request{
		SourceDialect: w.sourceType,
		TargetDialect: w.cat.Name,
		TargetSchema:  w.smtEvolutionTargetSchema(targetSchema),
		Table: smtddl.Table{
			Name:       w.smtEvolutionIdentifier(table.Name),
			Columns:    make([]smtddl.Column, len(table.Columns)),
			PrimaryKey: make([]string, len(table.PrimaryKey)),
		},
	}
	for index, column := range table.Columns {
		req.Table.Columns[index] = w.smtEvolutionColumn(column)
	}
	for index, column := range table.PrimaryKey {
		req.Table.PrimaryKey[index] = w.smtEvolutionIdentifier(column)
	}
	return req
}

func (w *Writer) smtEvolutionColumn(column driver.Column) smtddl.Column {
	return smtddl.Column{
		Name:              w.smtEvolutionIdentifier(column.Name),
		DataType:          column.DataType,
		FullDataType:      column.FullDataType,
		MaxLength:         column.MaxLength,
		Precision:         column.Precision,
		Scale:             column.Scale,
		IsNullable:        column.IsNullable,
		IsIdentity:        column.IsIdentity,
		DefaultExpression: column.DefaultValue,
		HasDefault:        column.DefaultValue != "",
		SRID:              column.SRID,
	}
}

func (w *Writer) smtEvolutionIdentifier(name string) string {
	if w.ident == nil {
		return name
	}
	return w.ident(name)
}

// smtEvolutionTargetSchema mirrors the create path's target-schema contract:
// PostgreSQL identifiers use DMT's sanitizer, connection-selected
// MySQL/SQLite schemas stay unqualified, and default public/dbo schemas are
// suppressed. This keeps evolution on the exact relation identity CREATE used.
func (w *Writer) smtEvolutionTargetSchema(schema string) string {
	schema = w.smtEvolutionIdentifier(schema)
	switch w.cat.Name {
	case "mysql", "sqlite":
		return ""
	case "postgres":
		if schema == "public" {
			return ""
		}
	case "mssql":
		if schema == "dbo" {
			return ""
		}
	}
	return schema
}
