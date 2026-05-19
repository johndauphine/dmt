package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// CreateSchema creates the target schema if it doesn't exist.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	var exists int
	err := w.db.QueryRowContext(ctx,
		"SELECT 1 FROM sys.schemas WHERE name = @schema",
		sql.Named("schema", schema)).Scan(&exists)
	if err == sql.ErrNoRows {
		_, err = w.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", w.dialect.QuoteIdentifier(schema)))
		return err
	}
	return err
}

// CreateTable creates a table from source metadata.
func (w *Writer) CreateTable(ctx context.Context, t *driver.Table, targetSchema string) error {
	return w.CreateTableWithOptions(ctx, t, targetSchema, driver.TableOptions{})
}

// CreateTableWithOptions creates a table with options using AI-generated DDL.
func (w *Writer) CreateTableWithOptions(ctx context.Context, t *driver.Table, targetSchema string, opts driver.TableOptions) error {
	// Use table-level AI DDL generation with full database context
	req := driver.TableDDLRequest{
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}

	resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("AI DDL generation failed for table %s: %w", t.FullName(), err)
	}

	logging.Debug("AI generated DDL for %s:\n%s", t.FullName(), resp.CreateTableDDL)

	// Log column type mappings
	for colName, colType := range resp.ColumnTypes {
		logging.Debug("  Column %s -> %s", colName, colType)
	}

	_, err = w.db.ExecContext(ctx, resp.CreateTableDDL)
	if err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, resp.CreateTableDDL)
	}

	return nil
}

// AddColumn adds a nullable column to an existing target table. It is
// idempotent so interrupted schema-evolution runs can be retried safely.
func (w *Writer) AddColumn(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	if t == nil {
		return fmt.Errorf("table is required")
	}
	if column == nil {
		return fmt.Errorf("column is required")
	}

	exists, err := w.columnExists(ctx, targetSchema, t.Name, column.Name)
	if err != nil {
		return fmt.Errorf("checking column %s.%s.%s: %w", targetSchema, t.Name, column.Name, err)
	}
	if exists {
		return nil
	}

	ddl, err := w.buildAddColumnSQL(t, column, targetSchema)
	if err != nil {
		return err
	}
	logging.Debug("Adding SQL Server column with DDL: %s", ddl)
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) columnExists(ctx context.Context, schema, table, column string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table), sql.Named("column", column)).Scan(&exists)
	return exists == 1, err
}

func (w *Writer) buildAddColumnSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "mssql", *column)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ADD %s %s NULL",
		w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(column.Name),
		mappedType), nil
}

// DropColumnNotNull relaxes a target column from NOT NULL to NULL.
func (w *Writer) DropColumnNotNull(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	ddl, err := w.buildDropColumnNotNullSQL(t, column, targetSchema)
	if err != nil {
		return err
	}
	logging.Debug("Relaxing SQL Server column nullability with DDL: %s", ddl)
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) buildDropColumnNotNullSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	if t == nil {
		return "", fmt.Errorf("table is required")
	}
	if column == nil {
		return "", fmt.Errorf("column is required")
	}

	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "mssql", *column)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s NULL",
		w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(column.Name),
		mappedType), nil
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, table)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// SetTableLogged is a no-op for SQL Server.
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// CreatePrimaryKey is a no-op because PK is created with the table.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key constraint.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
			WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
			AND TABLE_SCHEMA = @schema
			AND TABLE_NAME = @table
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	return exists == 1, err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// Returns empty string if DDL cannot be retrieved.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	// Build DDL from information_schema
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			IS_NULLABLE,
			COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION
	`, sql.Named("schema", schema), sql.Named("table", table))
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", schema, table, err)
		return ""
	}
	defer rows.Close()

	var sb strings.Builder
	// Use dialect's QuoteIdentifier for proper escaping
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n",
		w.dialect.QuoteIdentifier(schema),
		w.dialect.QuoteIdentifier(table)))

	first := true
	for rows.Next() {
		var colName, dataType, isNullable string
		var charMaxLen, numPrecision, numScale sql.NullInt64
		var colDefault sql.NullString

		if err := rows.Scan(&colName, &dataType, &charMaxLen, &numPrecision, &numScale, &isNullable, &colDefault); err != nil {
			logging.Debug("Failed to scan column for %s.%s: %v", schema, table, err)
			continue
		}

		if !first {
			sb.WriteString(",\n")
		}
		first = false

		sb.WriteString(fmt.Sprintf("    %s ", w.dialect.QuoteIdentifier(colName)))

		// Build type with precision
		typeStr := dataType
		if charMaxLen.Valid && charMaxLen.Int64 > 0 {
			if charMaxLen.Int64 == -1 {
				typeStr = fmt.Sprintf("%s(MAX)", dataType)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", dataType, charMaxLen.Int64)
			}
		} else if numPrecision.Valid && numPrecision.Int64 > 0 {
			if numScale.Valid && numScale.Int64 > 0 {
				typeStr = fmt.Sprintf("%s(%d,%d)", dataType, numPrecision.Int64, numScale.Int64)
			} else {
				typeStr = fmt.Sprintf("%s(%d)", dataType, numPrecision.Int64)
			}
		}
		sb.WriteString(typeStr)

		if isNullable == "NO" {
			sb.WriteString(" NOT NULL")
		}
		if colDefault.Valid && colDefault.String != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", colDefault.String))
		}
	}

	// Check if any columns were found
	if first {
		logging.Debug("No columns found for table %s.%s", schema, table)
		return ""
	}

	sb.WriteString("\n);")
	return sb.String()
}

// ResetSequence resets identity sequence to max value.
func (w *Writer) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
	var identityCol string
	for _, c := range t.Columns {
		if c.IsIdentity {
			identityCol = c.Name
			break
		}
	}

	if identityCol == "" {
		return nil
	}

	var maxVal int64
	err := w.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", w.dialect.QuoteIdentifier(identityCol), w.dialect.QualifyTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil
	}

	_, err = w.db.ExecContext(ctx, fmt.Sprintf("DBCC CHECKIDENT ('%s', RESEED, %d)", w.dialect.QualifyTable(schema, t.Name), maxVal))
	return err
}

// CreateIndex creates an index on the target table using AI-generated DDL.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for index creation")
	}

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeIndex,
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		Table:         t,
		Index:         idx,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("AI index DDL generation failed for %s.%s: %w", t.Name, idx.Name, err)
	}

	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

// CreateForeignKey creates a foreign key constraint using AI-generated DDL.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for foreign key creation")
	}

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:          driver.DDLTypeForeignKey,
		SourceDBType:  w.sourceType,
		TargetDBType:  "mssql",
		Table:         t,
		ForeignKey:    fk,
		TargetSchema:  targetSchema,
		TargetContext: w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("AI FK DDL generation failed for %s.%s: %w", t.Name, fk.Name, err)
	}

	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

// CreateCheckConstraint creates a check constraint using AI-generated DDL.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for check constraint creation")
	}

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:            driver.DDLTypeCheckConstraint,
		SourceDBType:    w.sourceType,
		TargetDBType:    "mssql",
		Table:           t,
		CheckConstraint: chk,
		TargetSchema:    targetSchema,
		TargetContext:   w.dbContext,
	})
	if err != nil {
		return fmt.Errorf("AI check constraint DDL generation failed for %s.%s: %w", t.Name, chk.Name, err)
	}

	_, err = w.db.ExecContext(ctx, ddl)
	return err
}
