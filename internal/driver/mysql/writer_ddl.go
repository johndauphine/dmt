package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// CreateSchema creates the target schema (database) if it doesn't exist.
// Note: In MySQL, schema = database.
func (w *Writer) CreateSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil // Using default database
	}
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", w.dialect.QuoteIdentifier(schema)))
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
		TargetDBType:  "mysql",
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
	logging.Debug("Adding MySQL column with DDL: %s", ddl)
	_, err = w.db.ExecContext(ctx, ddl)
	return err
}

func (w *Writer) columnExists(ctx context.Context, schema, table, column string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
	`, dbName, table, column).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (w *Writer) buildAddColumnSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "mysql", *column)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL",
		w.dialect.QualifyTable(targetSchema, t.Name),
		w.dialect.QuoteIdentifier(column.Name),
		mappedType), nil
}

// DropTable drops a table using AI-generated DDL that handles foreign key constraints.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	// Use AI-generated DROP DDL if available
	if w.dropDDLMapper != nil {
		ddl, err := w.dropDDLMapper.GenerateDropTableDDL(ctx, driver.DropTableDDLRequest{
			TargetDBType:  "mysql",
			TargetSchema:  schema,
			TableName:     table,
			TargetContext: w.dbContext,
		})
		if err != nil {
			logging.Warn("AI DROP DDL generation failed, using fallback: %v", err)
		} else {
			logging.Debug("Executing AI-generated DROP DDL: %s", ddl)
			_, err := w.db.ExecContext(ctx, ddl)
			return err
		}
	}

	// Fallback: Disable FK checks, drop table, re-enable FK checks in a single statement
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS %s; SET FOREIGN_KEY_CHECKS = 1;",
		qualifiedTable))
	return err
}

// TruncateTable truncates a table, disabling foreign key checks to allow
// truncating tables that are referenced by other tables.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	qualifiedTable := w.dialect.QualifyTable(schema, table)
	_, err := w.db.ExecContext(ctx, fmt.Sprintf(
		"SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE %s; SET FOREIGN_KEY_CHECKS = 1;",
		qualifiedTable))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// SetTableLogged is a no-op for MySQL (no unlogged tables).
func (w *Writer) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// CreatePrimaryKey is a no-op because PK is created with the table.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	return nil
}

// HasPrimaryKey checks if a table has a primary key constraint.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var exists int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// Returns empty string if DDL cannot be retrieved.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	// Use dialect's QualifyTable for proper identifier escaping (prevents SQL injection)
	qualifiedTable := w.dialect.QualifyTable(dbName, table)
	var tableName, createStmt string
	err := w.db.QueryRowContext(ctx, "SHOW CREATE TABLE "+qualifiedTable).Scan(&tableName, &createStmt)
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", dbName, table, err)
		return ""
	}
	return createStmt
}

// ResetSequence resets AUTO_INCREMENT to max value.
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
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
			w.dialect.QuoteIdentifier(identityCol),
			w.dialect.QualifyTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil
	}

	_, err = w.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d",
			w.dialect.QualifyTable(schema, t.Name), maxVal+1))
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
		TargetDBType:  "mysql",
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
		TargetDBType:  "mysql",
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
// Note: MySQL 8.0.16+ supports CHECK constraints, earlier versions ignore them.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for check constraint creation")
	}

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:            driver.DDLTypeCheckConstraint,
		SourceDBType:    w.sourceType,
		TargetDBType:    "mysql",
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
