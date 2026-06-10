package postgres

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
	_, err := w.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", w.dialect.QuoteIdentifier(schema)))
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
		TargetDBType:  "postgres",
		SourceTable:   t,
		TargetSchema:  targetSchema,
		SourceContext: opts.SourceContext,
		TargetContext: w.dbContext,
	}

	resp, err := w.tableMapper.GenerateTableDDL(ctx, req)
	if err != nil {
		return fmt.Errorf("AI DDL generation failed for table %s: %w", t.FullName(), err)
	}

	ddl := resp.CreateTableDDL

	// Handle unlogged option - modify the DDL if needed
	if opts.Unlogged && !strings.Contains(strings.ToUpper(ddl), "UNLOGGED") {
		ddl = strings.Replace(ddl, "CREATE TABLE", "CREATE UNLOGGED TABLE", 1)
	}

	logging.Debug("AI generated DDL for %s:\n%s", t.FullName(), ddl)

	// Log column type mappings
	for colName, colType := range resp.ColumnTypes {
		logging.Debug("  Column %s -> %s", colName, colType)
	}

	_, err = w.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w\nDDL: %s", t.FullName(), err, ddl)
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

	sanitizedTable := sanitizePGTableName(t.Name)
	sanitizedColumn := sanitizePGIdentifier(column.Name)
	exists, err := w.columnExists(ctx, targetSchema, sanitizedTable, sanitizedColumn)
	if err != nil {
		return fmt.Errorf("checking column %s.%s.%s: %w", targetSchema, sanitizedTable, sanitizedColumn, err)
	}
	if exists {
		return nil
	}

	ddl, err := w.buildAddColumnSQL(t, column, targetSchema)
	if err != nil {
		return err
	}
	logging.Debug("Adding PostgreSQL column with DDL: %s", ddl)
	_, err = w.pool.Exec(ctx, ddl)
	return err
}

func (w *Writer) columnExists(ctx context.Context, schema, table, column string) (bool, error) {
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
		)
	`, schema, table, column).Scan(&exists)
	return exists, err
}

func (w *Writer) buildAddColumnSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "postgres", *column)
	if err != nil {
		return "", err
	}

	sanitizedTable := sanitizePGTableName(t.Name)
	sanitizedColumn := sanitizePGIdentifier(column.Name)
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NULL",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(sanitizedColumn),
		mappedType), nil
}

// DropColumnNotNull relaxes a target column from NOT NULL to NULL.
func (w *Writer) DropColumnNotNull(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	ddl, err := w.buildDropColumnNotNullSQL(t, column, targetSchema)
	if err != nil {
		return err
	}
	logging.Debug("Relaxing PostgreSQL column nullability with DDL: %s", ddl)
	_, err = w.pool.Exec(ctx, ddl)
	return err
}

func (w *Writer) buildDropColumnNotNullSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	if t == nil {
		return "", fmt.Errorf("table is required")
	}
	if column == nil {
		return "", fmt.Errorf("column is required")
	}

	sanitizedTable := sanitizePGTableName(t.Name)
	sanitizedColumn := sanitizePGIdentifier(column.Name)
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(sanitizedColumn)), nil
}

// AlterColumnType changes a target column to the mapped current source type.
func (w *Writer) AlterColumnType(ctx context.Context, t *driver.Table, column *driver.Column, targetSchema string) error {
	ddl, err := w.buildAlterColumnTypeSQL(t, column, targetSchema)
	if err != nil {
		return err
	}
	logging.Debug("Altering PostgreSQL column type with DDL: %s", ddl)
	_, err = w.pool.Exec(ctx, ddl)
	return err
}

func (w *Writer) buildAlterColumnTypeSQL(t *driver.Table, column *driver.Column, targetSchema string) (string, error) {
	if t == nil {
		return "", fmt.Errorf("table is required")
	}
	if column == nil {
		return "", fmt.Errorf("column is required")
	}

	mappedType, err := driver.MapColumnType(w.typeMapper, w.sourceType, "postgres", *column)
	if err != nil {
		return "", err
	}

	sanitizedTable := sanitizePGTableName(t.Name)
	sanitizedColumn := sanitizePGIdentifier(column.Name)
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(sanitizedColumn),
		mappedType), nil
}

// DropTable drops a table.
func (w *Writer) DropTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TruncateTable truncates a table.
func (w *Writer) TruncateTable(ctx context.Context, schema, table string) error {
	sanitizedTable := sanitizePGTableName(table)
	_, err := w.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", w.dialect.QualifyTable(schema, sanitizedTable)))
	return err
}

// TableExists checks if a table exists.
func (w *Writer) TableExists(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGTableName(table)
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// CreatePrimaryKey creates the primary key constraint.
// This is idempotent - it checks if a PK already exists before creating one.
// AI-generated DDL includes the PK inline, so this check is necessary.
func (w *Writer) CreatePrimaryKey(ctx context.Context, t *driver.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	// Check if PK already exists (AI-generated DDL includes PK inline)
	hasPK, err := w.HasPrimaryKey(ctx, targetSchema, t.Name)
	if err != nil {
		return fmt.Errorf("checking for existing PK: %w", err)
	}
	if hasPK {
		return nil // PK already exists, nothing to do
	}

	sanitizedTable := sanitizePGTableName(t.Name)
	cols := make([]string, len(t.PrimaryKey))
	for i, c := range t.PrimaryKey {
		cols[i] = w.dialect.QuoteIdentifier(sanitizePGIdentifier(c))
	}

	pkName := fmt.Sprintf("pk_%s", sanitizedTable)
	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
		w.dialect.QualifyTable(targetSchema, sanitizedTable),
		w.dialect.QuoteIdentifier(pkName),
		strings.Join(cols, ", "))

	_, err = w.pool.Exec(ctx, sql)
	return err
}

// GetTableDDL retrieves the CREATE TABLE DDL for an existing table.
// This provides context to AI for generating indexes, FKs, etc.
func (w *Writer) GetTableDDL(ctx context.Context, schema, table string) string {
	// Use pg_get_tabledef extension if available, otherwise build from catalog
	var ddl string

	// First try the extension (if installed)
	err := w.pool.QueryRow(ctx,
		`SELECT pg_get_tabledef($1, $2)`,
		schema, table,
	).Scan(&ddl)
	if err == nil && ddl != "" {
		return ddl
	}

	// Fallback: build DDL from information_schema
	rows, err := w.pool.Query(ctx, `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		logging.Debug("Could not get table DDL for %s.%s: %v", schema, table, err)
		return ""
	}
	defer rows.Close()

	var sb strings.Builder
	// Use dialect's QuoteIdentifier for proper escaping
	fmt.Fprintf(&sb, "CREATE TABLE %s.%s (\n",
		w.dialect.QuoteIdentifier(schema),
		w.dialect.QuoteIdentifier(table))

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

		fmt.Fprintf(&sb, "    %s ", w.dialect.QuoteIdentifier(colName))

		// Build type with precision
		typeStr := dataType
		if charMaxLen.Valid && charMaxLen.Int64 > 0 {
			typeStr = fmt.Sprintf("%s(%d)", dataType, charMaxLen.Int64)
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
			fmt.Fprintf(&sb, " DEFAULT %s", colDefault.String)
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

// CreateIndex creates an index using AI-generated DDL.
func (w *Writer) CreateIndex(ctx context.Context, t *driver.Table, idx *driver.Index, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for index creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedIdx := &driver.Index{
		Name:     sanitizePGIdentifier(idx.Name),
		Columns:  make([]string, len(idx.Columns)),
		IsUnique: idx.IsUnique,
		Filter:   idx.Filter,
	}
	for i, col := range idx.Columns {
		sanitizedIdx.Columns[i] = sanitizePGIdentifier(col)
	}
	if len(idx.IncludeCols) > 0 {
		sanitizedIdx.IncludeCols = make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			sanitizedIdx.IncludeCols[i] = sanitizePGIdentifier(col)
		}
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:           driver.DDLTypeIndex,
		SourceDBType:   w.sourceType,
		TargetDBType:   "postgres",
		Table:          sanitizedTable,
		Index:          sanitizedIdx,
		TargetSchema:   targetSchema,
		TargetContext:  w.dbContext,
		TargetTableDDL: targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI index DDL generation failed for %s.%s: %w", t.Name, idx.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}

// CreateForeignKey creates a foreign key constraint using AI-generated DDL.
func (w *Writer) CreateForeignKey(ctx context.Context, t *driver.Table, fk *driver.ForeignKey, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for foreign key creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedFK := &driver.ForeignKey{
		Name:       sanitizePGIdentifier(fk.Name),
		Columns:    make([]string, len(fk.Columns)),
		RefSchema:  fk.RefSchema,
		RefTable:   sanitizePGIdentifier(fk.RefTable),
		RefColumns: make([]string, len(fk.RefColumns)),
		OnDelete:   fk.OnDelete,
		OnUpdate:   fk.OnUpdate,
	}
	for i, col := range fk.Columns {
		sanitizedFK.Columns[i] = sanitizePGIdentifier(col)
	}
	for i, col := range fk.RefColumns {
		sanitizedFK.RefColumns[i] = sanitizePGIdentifier(col)
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:           driver.DDLTypeForeignKey,
		SourceDBType:   w.sourceType,
		TargetDBType:   "postgres",
		Table:          sanitizedTable,
		ForeignKey:     sanitizedFK,
		TargetSchema:   targetSchema,
		TargetContext:  w.dbContext,
		TargetTableDDL: targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI FK DDL generation failed for %s.%s: %w", t.Name, fk.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}

// CreateCheckConstraint creates a check constraint using AI-generated DDL.
func (w *Writer) CreateCheckConstraint(ctx context.Context, t *driver.Table, chk *driver.CheckConstraint, targetSchema string) error {
	if w.finalizationMapper == nil {
		return fmt.Errorf("finalization mapper not available for check constraint creation")
	}

	// Create copies with sanitized (lowercase) names for PostgreSQL
	sanitizedTableName := sanitizePGIdentifier(t.Name)
	sanitizedTable := &driver.Table{Name: sanitizedTableName}
	sanitizedChk := &driver.CheckConstraint{
		Name:       sanitizePGIdentifier(chk.Name),
		Definition: chk.Definition,
	}

	// Get target table DDL for AI context
	targetTableDDL := w.GetTableDDL(ctx, targetSchema, sanitizedTableName)

	ddl, err := w.finalizationMapper.GenerateFinalizationDDL(ctx, driver.FinalizationDDLRequest{
		Type:            driver.DDLTypeCheckConstraint,
		SourceDBType:    w.sourceType,
		TargetDBType:    "postgres",
		Table:           sanitizedTable,
		CheckConstraint: sanitizedChk,
		TargetSchema:    targetSchema,
		TargetContext:   w.dbContext,
		TargetTableDDL:  targetTableDDL,
	})
	if err != nil {
		return fmt.Errorf("AI check constraint DDL generation failed for %s.%s: %w", t.Name, chk.Name, err)
	}

	_, err = w.pool.Exec(ctx, ddl)
	return err
}
