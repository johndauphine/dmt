package mssql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/stats"
	mssql "github.com/microsoft/go-mssqldb"
)

// Writer implements driver.Writer for SQL Server.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	chunkSize          int
	compatLevel        int
	sourceType         string
	dialect            *Dialect
	typeMapper         driver.TypeMapper
	tableMapper        driver.TableTypeMapper       // Table-level DDL generation
	finalizationMapper driver.FinalizationDDLMapper // AI-driven finalization DDL
	dbContext          *driver.DatabaseContext      // Cached database context for AI
}

// NewWriter creates a new SQL Server writer.
func NewWriter(cfg *dbconfig.TargetConfig, maxConns int, opts driver.WriterOptions) (*Writer, error) {
	dialect := &Dialect{}
	dsn := dialect.BuildDSN(cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.DSNOptions())

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	// Query database compatibility level
	var compatLevel int
	err = db.QueryRow(`
		SELECT compatibility_level
		FROM sys.databases
		WHERE name = DB_NAME()
	`).Scan(&compatLevel)
	if err != nil {
		compatLevel = 0
	}

	logging.Debug("Connected to MSSQL target: %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)

	// Validate type mapper is provided
	if opts.TypeMapper == nil {
		db.Close()
		return nil, fmt.Errorf("TypeMapper is required")
	}

	// Require TableTypeMapper for table-level AI DDL generation
	tableMapper, ok := opts.TypeMapper.(driver.TableTypeMapper)
	if !ok {
		db.Close()
		return nil, fmt.Errorf("TypeMapper must implement TableTypeMapper interface for table-level DDL generation")
	}

	// Log AI mapper initialization
	if aiMapper, ok := opts.TypeMapper.(*driver.AITypeMapper); ok {
		logging.Debug("AI Table-Level Type Mapping enabled (provider: %s, model: %s)",
			aiMapper.ProviderName(), aiMapper.Model())
		if aiMapper.CacheSize() > 0 {
			logging.Debug("Loaded %d cached AI type mappings", aiMapper.CacheSize())
		}
	}

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	w := &Writer{
		db:                 db,
		config:             cfg,
		maxConns:           maxConns,
		chunkSize:          opts.ChunkSize,
		compatLevel:        compatLevel,
		sourceType:         opts.SourceType,
		dialect:            dialect,
		typeMapper:         opts.TypeMapper,
		tableMapper:        tableMapper,
		finalizationMapper: finalizationMapper,
	}

	// Gather database context for AI
	w.dbContext = w.gatherDatabaseContext()

	return w, nil
}

// gatherDatabaseContext collects SQL Server database metadata for AI context.
func (w *Writer) gatherDatabaseContext() *driver.DatabaseContext {
	ctx := &driver.DatabaseContext{
		DatabaseName:             w.config.Database,
		ServerName:               w.config.Host,
		IdentifierCase:           "insensitive",
		CaseSensitiveIdentifiers: false,
		MaxIdentifierLength:      128,
		VarcharSemantics:         "byte", // VARCHAR = bytes, NVARCHAR = chars
		BytesPerChar:             2,      // NVARCHAR uses 2 bytes per char
	}

	// Query server version
	var version string
	if w.db.QueryRow("SELECT @@VERSION").Scan(&version) == nil {
		ctx.Version = version
		// Parse major version using regex
		// @@VERSION returns something like "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6"
		// Try to match the product year first (2016, 2017, 2019, 2022, etc.)
		yearRegex := regexp.MustCompile(`SQL Server (\d{4})`)
		if matches := yearRegex.FindStringSubmatch(version); len(matches) > 1 {
			if year, err := strconv.Atoi(matches[1]); err == nil {
				// Map year to major version number
				switch {
				case year >= 2022:
					ctx.MajorVersion = 16
				case year >= 2019:
					ctx.MajorVersion = 15
				case year >= 2017:
					ctx.MajorVersion = 14
				case year >= 2016:
					ctx.MajorVersion = 13
				case year >= 2014:
					ctx.MajorVersion = 12
				default:
					ctx.MajorVersion = 11
				}
			}
		}
		// Fallback: try to parse version number directly (e.g., "16.0.1000.6")
		if ctx.MajorVersion == 0 {
			verNumRegex := regexp.MustCompile(`- (\d+)\.`)
			if matches := verNumRegex.FindStringSubmatch(version); len(matches) > 1 {
				if majorVer, err := strconv.Atoi(matches[1]); err == nil {
					ctx.MajorVersion = majorVer
				}
			}
		}
		if ctx.MajorVersion == 0 {
			logging.Warn("Could not parse SQL Server version from '%s', version-specific features may not be detected", version)
		}
	}

	// Query database collation
	var collation sql.NullString
	if w.db.QueryRow("SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation')").Scan(&collation) == nil && collation.Valid {
		ctx.Collation = collation.String
		// Parse collation for case sensitivity
		upperCollation := strings.ToUpper(collation.String)
		if strings.Contains(upperCollation, "_CS_") {
			ctx.CaseSensitiveData = true
		} else if strings.Contains(upperCollation, "_CI_") {
			ctx.CaseSensitiveData = false
		}
		// Parse for accent sensitivity
		if strings.Contains(upperCollation, "_AS") {
			ctx.Notes = "Accent-sensitive collation"
		}
	}

	// Query code page from collation
	var codePage sql.NullInt64
	if w.db.QueryRow(`
		SELECT COLLATIONPROPERTY(DATABASEPROPERTYEX(DB_NAME(), 'Collation'), 'CodePage')
	`).Scan(&codePage) == nil && codePage.Valid {
		ctx.CodePage = int(codePage.Int64)
		switch ctx.CodePage {
		case 65001:
			ctx.Encoding = "UTF-8"
		case 1252:
			ctx.Encoding = "Latin1 (Windows-1252)"
		case 1200:
			ctx.Encoding = "UTF-16LE"
		default:
			ctx.Encoding = fmt.Sprintf("CP%d", ctx.CodePage)
		}
	}

	// Set charset based on typical SQL Server setup
	ctx.Charset = "SQL_Latin1_General_CP1"
	if ctx.CodePage == 65001 {
		ctx.Charset = "UTF-8"
	}
	ctx.NationalCharset = "UTF-16"

	// Max varchar lengths
	ctx.MaxVarcharLength = 8000 // VARCHAR max, NVARCHAR max is 4000 chars

	// Features based on compatibility level
	ctx.Features = []string{"NVARCHAR", "VARCHAR_MAX", "DATETIME2", "JSON"}
	if w.compatLevel >= 130 { // SQL Server 2016+
		ctx.Features = append(ctx.Features, "JSON_FUNCTIONS", "TEMPORAL_TABLES")
	}
	if w.compatLevel >= 150 { // SQL Server 2019+
		ctx.Features = append(ctx.Features, "UTF8_SUPPORT")
	}

	logging.Debug("MSSQL context: collation=%s, code_page=%d, compat_level=%d",
		ctx.Collation, ctx.CodePage, w.compatLevel)

	return ctx
}

// Close closes all connections.
func (w *Writer) Close() {
	w.db.Close()
}

// Ping tests the connection.
func (w *Writer) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

// DB returns the underlying database connection for tuning analysis.
func (w *Writer) DB() *sql.DB {
	return w.db
}

// MaxConns returns the configured maximum connections.
func (w *Writer) MaxConns() int {
	return w.maxConns
}

// DBType returns the database type.
func (w *Writer) DBType() string {
	return "mssql"
}

// PoolStats returns connection pool statistics.
func (w *Writer) PoolStats() stats.PoolStats {
	dbStats := w.db.Stats()
	return stats.PoolStats{
		DBType:      "mssql",
		MaxConns:    dbStats.MaxOpenConnections,
		ActiveConns: dbStats.InUse,
		IdleConns:   dbStats.Idle,
		WaitCount:   dbStats.WaitCount,
		WaitTimeMs:  dbStats.WaitDuration.Milliseconds(),
	}
}

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

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	err = w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`
	err := w.db.QueryRowContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table)).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// This may be slow on large tables.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
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

// WriteBatch writes a batch of rows using TDS bulk copy.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	fullTableName := fmt.Sprintf("[%s].[%s]", opts.Schema, opts.Table)

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	err = conn.Raw(func(driverConn any) error {
		mssqlConn, ok := driverConn.(*mssql.Conn)
		if !ok {
			return fmt.Errorf("expected *mssql.Conn, got %T", driverConn)
		}

		batchSize := w.chunkSize
		if batchSize <= 0 || batchSize > len(opts.Rows) {
			batchSize = len(opts.Rows)
		}

		bulk := mssqlConn.CreateBulkContext(ctx, fullTableName, opts.Columns)
		bulk.Options.Tablock = true
		bulk.Options.RowsPerBatch = batchSize

		for _, row := range opts.Rows {
			err := bulk.AddRow(convertRowForBulkCopy(row))
			if err != nil {
				return fmt.Errorf("adding row: %w", err)
			}
		}

		rowsAffected, err := bulk.Done()
		if err != nil {
			return fmt.Errorf("finalizing bulk insert: %w", err)
		}

		if rowsAffected != int64(len(opts.Rows)) {
			return fmt.Errorf("bulk insert: expected %d rows, got %d", len(opts.Rows), rowsAffected)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("bulk copy: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("committing bulk copy: %w", err)
	}

	return nil
}

// UpsertBatch performs upsert using staging table + MERGE.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	// Generate staging table name
	stagingTable := w.safeStagingName(opts.Table, opts.WriterID, nil)

	// Create temp table
	targetTable := w.dialect.QualifyTable(opts.Schema, opts.Table)
	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// Detect spatial columns
	isCrossEngine := w.sourceType == "postgres"
	spatialCols, err := w.getSpatialColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("detecting spatial columns: %w", err)
	}

	// Populate SRIDs
	if len(spatialCols) > 0 && len(opts.ColumnSRIDs) == len(opts.Columns) {
		sridMap := make(map[string]int, len(opts.Columns))
		for i, col := range opts.Columns {
			sridMap[strings.ToLower(col)] = opts.ColumnSRIDs[i]
		}
		for i := range spatialCols {
			if srid, ok := sridMap[strings.ToLower(spatialCols[i].Name)]; ok && srid > 0 {
				spatialCols[i].SRID = srid
			}
		}
	}

	// Alter spatial columns for cross-engine
	if isCrossEngine && len(spatialCols) > 0 {
		if err := w.alterSpatialColumnsToText(ctx, conn, stagingTable, spatialCols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	// Get actual column names (case handling)
	actualCols, err := w.getStagingTableColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("getting staging table columns: %w", err)
	}

	colMapping := make(map[string]string, len(actualCols))
	for _, ac := range actualCols {
		colMapping[strings.ToLower(ac)] = ac
	}

	mappedCols := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		if actual, ok := colMapping[strings.ToLower(c)]; ok {
			mappedCols[i] = actual
		} else {
			mappedCols[i] = c
		}
	}

	mappedPKCols := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		if actual, ok := colMapping[strings.ToLower(pk)]; ok {
			mappedPKCols[i] = actual
		} else {
			mappedPKCols[i] = pk
		}
	}

	// Check for identity columns
	var hasIdentity bool
	identitySQL := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.columns c
			JOIN sys.tables t ON c.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND c.is_identity = 1
		) THEN 1 ELSE 0 END`
	if err := conn.QueryRowContext(ctx, identitySQL, opts.Schema, opts.Table).Scan(&hasIdentity); err != nil {
		hasIdentity = false
	}

	// Bulk insert to staging
	if err := w.bulkInsertToTemp(ctx, conn, stagingTable, mappedCols, opts.Rows); err != nil {
		return fmt.Errorf("bulk insert to staging: %w", err)
	}

	// Execute MERGE
	mergeSQL := w.buildMerge(targetTable, stagingTable, mappedCols, mappedPKCols, spatialCols, isCrossEngine)
	if err := w.executeMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	return nil
}

func (w *Writer) safeStagingName(table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	base := fmt.Sprintf("#stg_%s", table)
	maxLen := 116

	if len(base)+len(suffix) > maxLen {
		hash := sha256.Sum256([]byte(table))
		base = fmt.Sprintf("#stg_%x", hash[:8])
	}
	return base + suffix
}

func (w *Writer) getStagingTableColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]string, error) {
	query := `
		SELECT c.name
		FROM tempdb.sys.columns c
		WHERE c.object_id = OBJECT_ID('tempdb..' + @p1)
		ORDER BY c.column_id`

	rows, err := conn.QueryContext(ctx, query, stagingTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		cols = append(cols, colName)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for staging table %s", stagingTable)
	}

	return cols, nil
}

type spatialColumn struct {
	Name     string
	TypeName string
	SRID     int
}

func (w *Writer) getSpatialColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]spatialColumn, error) {
	query := `
		SELECT c.name, t.name AS type_name
		FROM tempdb.sys.columns c
		JOIN tempdb.sys.types t ON c.user_type_id = t.user_type_id
		WHERE c.object_id = OBJECT_ID('tempdb..' + @p1)
		AND t.name IN ('geography', 'geometry')
		ORDER BY c.column_id`

	rows, err := conn.QueryContext(ctx, query, stagingTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var spatialCols []spatialColumn
	for rows.Next() {
		var col spatialColumn
		if err := rows.Scan(&col.Name, &col.TypeName); err != nil {
			return nil, err
		}
		spatialCols = append(spatialCols, col)
	}

	return spatialCols, nil
}

func (w *Writer) alterSpatialColumnsToText(ctx context.Context, conn *sql.Conn, stagingTable string, spatialCols []spatialColumn) error {
	// SQL Server doesn't allow ALTER COLUMN from geography/geometry to nvarchar(max)
	// (implicit conversion not allowed). Instead, we DROP and ADD the column.
	for _, col := range spatialCols {
		quotedCol := w.dialect.QuoteIdentifier(col.Name)

		// Drop the geography/geometry column
		dropSQL := fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("dropping column %s: %w", col.Name, err)
		}

		// Add it back as nvarchar(max) for WKT text data
		addSQL := fmt.Sprintf(`ALTER TABLE %s ADD %s nvarchar(max)`, stagingTable, quotedCol)
		if _, err := conn.ExecContext(ctx, addSQL); err != nil {
			return fmt.Errorf("adding column %s as nvarchar: %w", col.Name, err)
		}
	}
	return nil
}

func (w *Writer) bulkInsertToTemp(ctx context.Context, conn *sql.Conn, tempTable string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(tempTable, mssql.BulkOptions{
		RowsPerBatch: w.chunkSize,
	}, cols...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, convertRowForBulkCopy(row)...); err != nil {
			return err
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *Writer) buildMerge(targetTable, stagingTable string, cols, pkCols []string, spatialCols []spatialColumn, isCrossEngine bool) string {
	spatialMap := make(map[string]spatialColumn, len(spatialCols))
	for _, col := range spatialCols {
		spatialMap[strings.ToLower(col.Name)] = col
	}

	var onClauses []string
	for _, pk := range pkCols {
		onClauses = append(onClauses, fmt.Sprintf("target.%s = source.%s",
			w.dialect.QuoteIdentifier(pk), w.dialect.QuoteIdentifier(pk)))
	}

	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var changeDetection []string
	for _, col := range cols {
		if !pkSet[col] {
			quotedCol := w.dialect.QuoteIdentifier(col)
			sourceExpr := fmt.Sprintf("source.%s", quotedCol)

			spatialCol, isSpatial := spatialMap[strings.ToLower(col)]
			if isSpatial && isCrossEngine {
				srid := spatialCol.SRID
				if srid == 0 {
					srid = 4326
				}
				sourceExpr = fmt.Sprintf("%s::STGeomFromText(source.%s, %d)", spatialCol.TypeName, quotedCol, srid)
			}

			setClauses = append(setClauses, fmt.Sprintf("%s = %s", quotedCol, sourceExpr))

			if isSpatial {
				continue
			}

			changeDetection = append(changeDetection, fmt.Sprintf(
				"(target.%s <> source.%s OR "+
					"(target.%s IS NULL AND source.%s IS NOT NULL) OR "+
					"(target.%s IS NOT NULL AND source.%s IS NULL))",
				quotedCol, quotedCol, quotedCol, quotedCol, quotedCol, quotedCol))
		}
	}

	quotedCols := make([]string, len(cols))
	sourceCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCol := w.dialect.QuoteIdentifier(col)
		quotedCols[i] = quotedCol

		spatialCol, isSpatial := spatialMap[strings.ToLower(col)]
		if isSpatial && isCrossEngine {
			srid := spatialCol.SRID
			if srid == 0 {
				srid = 4326
			}
			sourceCols[i] = fmt.Sprintf("%s::STGeomFromText(source.%s, %d)", spatialCol.TypeName, quotedCol, srid)
		} else {
			sourceCols[i] = fmt.Sprintf("source.%s", quotedCol)
		}
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("MERGE INTO %s WITH (TABLOCK) AS target\n", targetTable))
	sb.WriteString(fmt.Sprintf("USING %s AS source\n", stagingTable))
	sb.WriteString(fmt.Sprintf("ON %s\n", strings.Join(onClauses, " AND ")))

	if len(setClauses) > 0 {
		sb.WriteString(fmt.Sprintf("WHEN MATCHED AND (%s) THEN UPDATE SET %s\n",
			strings.Join(changeDetection, " OR "),
			strings.Join(setClauses, ", ")))
	}

	sb.WriteString(fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(quotedCols, ", "),
		strings.Join(sourceCols, ", ")))

	return sb.String()
}

func (w *Writer) executeMergeWithRetry(ctx context.Context, conn *sql.Conn, targetTable, mergeSQL string, hasIdentity bool, maxRetries int) error {
	const baseDelayMs = 200

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error

		if hasIdentity {
			if _, err = conn.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s ON", targetTable)); err != nil {
				return fmt.Errorf("enabling identity insert: %w", err)
			}
			_, err = conn.ExecContext(ctx, mergeSQL)
			if _, disableErr := conn.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s OFF", targetTable)); disableErr != nil {
				logging.Warn("Failed to disable IDENTITY_INSERT on %s: %v", targetTable, disableErr)
			}
		} else {
			_, err = conn.ExecContext(ctx, mergeSQL)
		}

		if err == nil {
			return nil
		}

		if !isDeadlockError(err) || attempt == maxRetries {
			return err
		}

		logging.Warn("Deadlock on %s, retry %d/%d", targetTable, attempt, maxRetries)
		time.Sleep(time.Duration(baseDelayMs*attempt) * time.Millisecond)
	}

	return fmt.Errorf("merge failed after %d retries", maxRetries)
}

func convertRowForBulkCopy(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			if isASCIINumeric(b) {
				result[i] = string(b)
			} else {
				result[i] = v
			}
		} else {
			result[i] = v
		}
	}
	return result
}

func isASCIINumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	hasDigit := false
	hasDot := false
	hasE := false
	i := 0

	if b[i] == '+' || b[i] == '-' {
		i++
		if i >= len(b) {
			return false
		}
	}

	for i < len(b) {
		c := b[i]
		switch {
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '.':
			if hasDot || hasE {
				return false
			}
			hasDot = true
		case c == 'E' || c == 'e':
			if hasE || !hasDigit {
				return false
			}
			hasE = true
			i++
			if i < len(b) && (b[i] == '+' || b[i] == '-') {
				i++
			}
			if i >= len(b) || b[i] < '0' || b[i] > '9' {
				return false
			}
			continue
		default:
			return false
		}
		i++
	}

	return hasDigit
}

func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}

	if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
		return mssqlErr.SQLErrorNumber() == 1205
	}

	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "1205")
}

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.db.QueryRowContext(ctx, query, args...).Scan(dest)
}
