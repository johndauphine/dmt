package target

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/typemap"
	mssql "github.com/microsoft/go-mssqldb"
)

// MSSQLPool manages a pool of SQL Server target connections
type MSSQLPool struct {
	db           *sql.DB
	config       *config.TargetConfig
	maxConns     int
	rowsPerBatch int    // Hint for bulk copy optimizer
	compatLevel  int    // Database compatibility level (e.g., 130 for SQL Server 2016)
	sourceType   string // "mssql" or "postgres" - used for DDL generation
}

// NewMSSQLPool creates a new SQL Server target connection pool
// sourceType indicates the source database type ("mssql" or "postgres") for DDL generation
func NewMSSQLPool(cfg *config.TargetConfig, maxConns int, rowsPerBatch int, sourceType string) (*MSSQLPool, error) {
	trustCert := "false"
	if cfg.TrustServerCert {
		trustCert = "true"
	}
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.Encrypt, trustCert)

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

	// Query database compatibility level (used for MERGE/EXCEPT support check)
	var compatLevel int
	err = db.QueryRow(`
		SELECT compatibility_level
		FROM sys.databases
		WHERE name = DB_NAME()
	`).Scan(&compatLevel)
	if err != nil {
		// Non-fatal - just log and continue with level 0
		compatLevel = 0
	}

	return &MSSQLPool{
		db:           db,
		config:       cfg,
		maxConns:     maxConns,
		rowsPerBatch: rowsPerBatch,
		compatLevel:  compatLevel,
		sourceType:   sourceType,
	}, nil
}

// Close closes all connections in the pool
func (p *MSSQLPool) Close() {
	p.db.Close()
}

// Ping tests the connection to the database
func (p *MSSQLPool) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

// DB returns the underlying database connection
func (p *MSSQLPool) DB() *sql.DB {
	return p.db
}

// MaxConns returns the configured maximum connections
func (p *MSSQLPool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *MSSQLPool) DBType() string {
	return "mssql"
}

// CompatLevel returns the database compatibility level (e.g., 130 for SQL Server 2016)
func (p *MSSQLPool) CompatLevel() int {
	return p.compatLevel
}

// CreateSchema creates the target schema if it doesn't exist
func (p *MSSQLPool) CreateSchema(ctx context.Context, schema string) error {
	// Check if schema exists
	var exists int
	err := p.db.QueryRowContext(ctx,
		"SELECT 1 FROM sys.schemas WHERE name = @schema",
		sql.Named("schema", schema)).Scan(&exists)
	if err == sql.ErrNoRows {
		// Create schema
		_, err = p.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", quoteMSSQLIdent(schema)))
		return err
	}
	return err
}

// CreateTable creates a table from source metadata
func (p *MSSQLPool) CreateTable(ctx context.Context, t *source.Table, targetSchema string) error {
	return p.CreateTableWithOptions(ctx, t, targetSchema, false)
}

// CreateTableWithOptions creates a table (unlogged option ignored for MSSQL)
func (p *MSSQLPool) CreateTableWithOptions(ctx context.Context, t *source.Table, targetSchema string, unlogged bool) error {
	ddl := GenerateMSSQLDDL(t, targetSchema, p.sourceType)

	_, err := p.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}

	return nil
}

// SetTableLogged is a no-op for SQL Server (all tables are logged)
func (p *MSSQLPool) SetTableLogged(ctx context.Context, schema, table string) error {
	return nil
}

// TruncateTable truncates a table
func (p *MSSQLPool) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualifyMSSQLTable(schema, table)))
	return err
}

// DropTable drops a table if it exists
func (p *MSSQLPool) DropTable(ctx context.Context, schema, table string) error {
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifyMSSQLTable(schema, table)))
	return err
}

// TableExists checks if a table exists in the schema
func (p *MSSQLPool) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := p.db.QueryRowContext(ctx, `
		SELECT 1 FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// CreatePrimaryKey is a no-op for MSSQL because we create the PK with the table DDL.
// This enables efficient sorted bulk insert when data arrives in PK order.
func (p *MSSQLPool) CreatePrimaryKey(ctx context.Context, t *source.Table, targetSchema string) error {
	// PK is already created in GenerateMSSQLDDL - nothing to do here
	return nil
}

// GetRowCount returns the row count for a table
func (p *MSSQLPool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifyMSSQLTable(schema, table))).Scan(&count)
	return count, err
}

// HasPrimaryKey checks if a table has a primary key constraint
func (p *MSSQLPool) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	var exists int
	err := p.db.QueryRowContext(ctx, `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
			WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
			AND TABLE_SCHEMA = @schema
			AND TABLE_NAME = @table
		) THEN 1 ELSE 0 END
	`, sql.Named("schema", schema), sql.Named("table", table)).Scan(&exists)
	return exists == 1, err
}

// ResetSequence resets identity sequence to max value
func (p *MSSQLPool) ResetSequence(ctx context.Context, schema string, t *source.Table) error {
	// Find identity column
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

	// Get max value from the table
	var maxVal int64
	err := p.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", quoteMSSQLIdent(identityCol), qualifyMSSQLTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil // Empty table, no need to reset
	}

	// Reseed identity
	_, err = p.db.ExecContext(ctx, fmt.Sprintf("DBCC CHECKIDENT ('%s', RESEED, %d)", qualifyMSSQLTable(schema, t.Name), maxVal))
	return err
}

// CreateIndex creates an index on the target table
func (p *MSSQLPool) CreateIndex(ctx context.Context, t *source.Table, idx *source.Index, targetSchema string) error {
	// Build column list
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = quoteMSSQLIdent(col)
	}

	unique := ""
	if idx.IsUnique {
		unique = "UNIQUE "
	}

	// Generate index name (SQL Server has length limits)
	idxName := fmt.Sprintf("idx_%s_%s", t.Name, idx.Name)
	if len(idxName) > 128 {
		idxName = idxName[:128]
	}

	sqlStmt := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, quoteMSSQLIdent(idxName), qualifyMSSQLTable(targetSchema, t.Name), strings.Join(cols, ", "))

	// Add included columns if any
	if len(idx.IncludeCols) > 0 {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			includeCols[i] = quoteMSSQLIdent(col)
		}
		sqlStmt += fmt.Sprintf(" INCLUDE (%s)", strings.Join(includeCols, ", "))
	}

	_, err := p.db.ExecContext(ctx, sqlStmt)
	return err
}

// CreateForeignKey creates a foreign key constraint on the target table
func (p *MSSQLPool) CreateForeignKey(ctx context.Context, t *source.Table, fk *source.ForeignKey, targetSchema string) error {
	// Build column lists
	cols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		cols[i] = quoteMSSQLIdent(col)
	}

	refCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		refCols[i] = quoteMSSQLIdent(col)
	}

	// Map referential actions
	onDelete := mapReferentialActionMSSQL(fk.OnDelete)
	onUpdate := mapReferentialActionMSSQL(fk.OnUpdate)

	// Generate FK name
	fkName := fmt.Sprintf("fk_%s_%s", t.Name, fk.Name)
	if len(fkName) > 128 {
		fkName = fkName[:128]
	}

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s (%s)
		ON DELETE %s
		ON UPDATE %s
	`, qualifyMSSQLTable(targetSchema, t.Name), quoteMSSQLIdent(fkName),
		strings.Join(cols, ", "),
		qualifyMSSQLTable(targetSchema, fk.RefTable), strings.Join(refCols, ", "),
		onDelete, onUpdate)

	_, err := p.db.ExecContext(ctx, sqlStmt)
	return err
}

// CreateCheckConstraint creates a check constraint on the target table
func (p *MSSQLPool) CreateCheckConstraint(ctx context.Context, t *source.Table, chk *source.CheckConstraint, targetSchema string) error {
	// Convert PostgreSQL CHECK syntax to SQL Server
	definition := convertCheckDefinitionMSSQL(chk.Definition)

	// Generate constraint name
	chkName := fmt.Sprintf("chk_%s_%s", t.Name, chk.Name)
	if len(chkName) > 128 {
		chkName = chkName[:128]
	}

	sqlStmt := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		CHECK %s
	`, qualifyMSSQLTable(targetSchema, t.Name), quoteMSSQLIdent(chkName), definition)

	_, err := p.db.ExecContext(ctx, sqlStmt)
	return err
}

// WriteChunk writes a chunk of data to the target table using TDS bulk copy protocol.
// Uses the direct bulk API (CreateBulkContext) for better performance by avoiding
// database/sql prepared statement overhead. Wrapped in explicit transaction for atomicity.
func (p *MSSQLPool) WriteChunk(ctx context.Context, schema, table string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	// Create fully qualified table name
	fullTableName := fmt.Sprintf("[%s].[%s]", schema, table)

	// Get a connection from the pool
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("getting connection: %w", err)
	}
	defer conn.Close()

	// Begin transaction for atomicity - ensures all-or-nothing insert
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call after commit - returns ErrTxDone

	// Use the direct bulk API via Raw() for better performance
	// This bypasses database/sql prepared statement overhead
	err = conn.Raw(func(driverConn any) error {
		mssqlConn, ok := driverConn.(*mssql.Conn)
		if !ok {
			return fmt.Errorf("expected *mssql.Conn, got %T", driverConn)
		}

		// Configure bulk options for optimal performance
		// - Tablock: acquire table lock to reduce lock overhead
		// - RowsPerBatch: hint to optimizer for memory allocation
		rowsPerBatch := p.rowsPerBatch
		if rowsPerBatch <= 0 || rowsPerBatch > len(rows) {
			rowsPerBatch = len(rows)
		}

		// Create bulk insert operation directly
		bulk := mssqlConn.CreateBulkContext(ctx, fullTableName, cols)
		bulk.Options.Tablock = true
		bulk.Options.RowsPerBatch = rowsPerBatch

		// Add all rows - this buffers internally and sends efficiently
		// Convert rows to handle []byte values (e.g., decimal from MSSQL source)
		for _, row := range rows {
			err := bulk.AddRow(convertRowForBulkCopy(row))
			if err != nil {
				return fmt.Errorf("adding row: %w", err)
			}
		}

		// Finalize the bulk insert - sends all buffered data
		rowsAffected, err := bulk.Done()
		if err != nil {
			return fmt.Errorf("finalizing bulk insert: %w", err)
		}

		// Verify all rows were inserted
		if rowsAffected != int64(len(rows)) {
			return fmt.Errorf("bulk insert: expected %d rows, got %d", len(rows), rowsAffected)
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

// UpsertChunkWithWriter performs high-performance upsert using per-writer staging tables.
// This approach uses:
// 1. Per-writer #temp table (session-scoped, auto-dropped on disconnect)
// 2. TDS bulk insert into staging (fast, single round-trip)
// 3. MERGE WITH (TABLOCK) immediately after each chunk
// 4. Deadlock retry with linear backoff (5 retries)
// colTypes is used to skip geography/geometry columns from change detection in MERGE
func (p *MSSQLPool) UpsertChunkWithWriter(ctx context.Context, schema, table string,
	cols []string, colTypes []string, pkCols []string, rows [][]any, writerID int, partitionID *int) error {

	if len(rows) == 0 {
		return nil
	}

	if len(pkCols) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	// Acquire a dedicated connection for this chunk
	// #temp tables are session-scoped in SQL Server, so we need a single connection
	// for the entire create->bulk insert->merge workflow.
	// Note: Each chunk gets its own connection, so the temp table is recreated per chunk.
	// This is acceptable because the bulk insert + merge is the expensive operation,
	// and connection pooling minimizes connection overhead.
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()

	// 1. Generate session-scoped temp table name
	// #tables are auto-dropped when connection closes (handles crashes gracefully)
	stagingTable := safeMSSQLStagingName(table, writerID, partitionID)

	// 2. Create temp table (fresh for each chunk since we get a new connection)
	targetTable := qualifyMSSQLTable(schema, table)
	createSQL := fmt.Sprintf(`SELECT TOP 0 * INTO %s FROM %s`, stagingTable, targetTable)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// 2a. For cross-engine migrations (PG→MSSQL), alter geography/geometry columns to nvarchar(max)
	// This allows bulk insert of WKT text, which will be converted back in MERGE
	isCrossEngine := p.sourceType == "postgres"
	if isCrossEngine {
		if err := p.alterSpatialColumnsToText(ctx, conn, stagingTable, colTypes, cols); err != nil {
			return fmt.Errorf("altering spatial columns: %w", err)
		}
	}

	// 2b. Get actual column names from staging table (to handle case mismatches)
	// When source is PostgreSQL (lowercase) and target is MSSQL (mixed case),
	// we need to use the target's column names for bulk insert.
	actualCols, err := p.getStagingTableColumns(ctx, conn, stagingTable)
	if err != nil {
		return fmt.Errorf("getting staging table columns: %w", err)
	}

	// Build case-insensitive mapping from input cols to actual cols
	colMapping := make(map[string]string, len(actualCols))
	for _, ac := range actualCols {
		colMapping[strings.ToLower(ac)] = ac
	}

	// Map input column names to actual staging column names
	mappedCols := make([]string, len(cols))
	for i, c := range cols {
		if actual, ok := colMapping[strings.ToLower(c)]; ok {
			mappedCols[i] = actual
		} else {
			mappedCols[i] = c // fallback to original if not found
		}
	}

	// Map PK column names as well
	mappedPKCols := make([]string, len(pkCols))
	for i, pk := range pkCols {
		if actual, ok := colMapping[strings.ToLower(pk)]; ok {
			mappedPKCols[i] = actual
		} else {
			mappedPKCols[i] = pk // fallback to original if not found
		}
	}

	// 3. Check for identity columns (need IDENTITY_INSERT for merge)
	var hasIdentity bool
	identitySQL := `
		SELECT CASE WHEN EXISTS (
			SELECT 1 FROM sys.columns c
			JOIN sys.tables t ON c.object_id = t.object_id
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND c.is_identity = 1
		) THEN 1 ELSE 0 END`
	if err := conn.QueryRowContext(ctx, identitySQL, schema, table).Scan(&hasIdentity); err != nil {
		hasIdentity = false // Non-fatal, assume no identity
	}

	// 4. Bulk insert into temp staging table using TDS bulk copy
	if err := p.bulkInsertToTempTable(ctx, conn, stagingTable, mappedCols, rows); err != nil {
		return fmt.Errorf("bulk insert to staging: %w", err)
	}

	// 5. Build and execute MERGE with deadlock retry
	// Pass colTypes to skip geography/geometry from change detection
	// Pass isCrossEngine to convert WKT text back to geography for PG→MSSQL
	mergeSQL := buildMSSQLMergeWithTablock(targetTable, stagingTable, mappedCols, colTypes, mappedPKCols, isCrossEngine)
	if err := p.executeMergeWithRetry(ctx, conn, targetTable, mergeSQL, hasIdentity, 5); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	return nil
}

// safeMSSQLStagingName generates a safe #temp table name that handles long identifiers.
// MSSQL temp table names are limited to 116 characters (128 - 12 for system suffix).
// If the name would be too long, falls back to a hash-based name.
func safeMSSQLStagingName(table string, writerID int, partitionID *int) string {
	suffix := fmt.Sprintf("_w%d", writerID)
	if partitionID != nil {
		suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
	}
	base := fmt.Sprintf("#stg_%s", table)
	maxLen := 116 // MSSQL temp table limit (128 - 12 for system suffix)

	if len(base)+len(suffix) > maxLen {
		// Use hash for long names
		hash := sha256.Sum256([]byte(table))
		base = fmt.Sprintf("#stg_%x", hash[:8]) // 16 hex chars
	}
	return base + suffix
}

// getStagingTableColumns retrieves the column names from a #temp staging table.
// This is needed to handle case mismatches between source (e.g., PG lowercase)
// and target (e.g., MSSQL mixed case) column names.
func (p *MSSQLPool) getStagingTableColumns(ctx context.Context, conn *sql.Conn, stagingTable string) ([]string, error) {
	// Use OBJECT_ID which correctly handles session-scoped temp tables
	// OBJECT_ID('tempdb..#tablename') returns the correct object_id for the current session's temp table
	query := `
		SELECT c.name
		FROM tempdb.sys.columns c
		WHERE c.object_id = OBJECT_ID('tempdb..' + @p1)
		ORDER BY c.column_id`

	rows, err := conn.QueryContext(ctx, query, stagingTable)
	if err != nil {
		return nil, fmt.Errorf("querying staging table columns: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("scanning column name: %w", err)
		}
		cols = append(cols, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating columns: %w", err)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found for staging table %s", stagingTable)
	}

	return cols, nil
}

// alterSpatialColumnsToText alters geography/geometry columns in a staging table to nvarchar(max).
// This is needed for cross-engine migrations (PG→MSSQL) where spatial data comes as WKT text.
// The MERGE statement will convert text back to geography using STGeomFromText.
func (p *MSSQLPool) alterSpatialColumnsToText(ctx context.Context, conn *sql.Conn, stagingTable string, colTypes, cols []string) error {
	for i, colType := range colTypes {
		if i >= len(cols) {
			break
		}
		colTypeLower := strings.ToLower(colType)
		if colTypeLower == "geography" || colTypeLower == "geometry" {
			// Get the actual column name from the staging table
			// For temp tables, we need to query tempdb
			alterSQL := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s nvarchar(max)`,
				stagingTable, quoteMSSQLIdent(cols[i]))
			if _, err := conn.ExecContext(ctx, alterSQL); err != nil {
				return fmt.Errorf("altering column %s: %w", cols[i], err)
			}
		}
	}
	return nil
}

// bulkInsertToTempTable performs TDS bulk insert into a #temp table
func (p *MSSQLPool) bulkInsertToTempTable(ctx context.Context, conn *sql.Conn, tempTable string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	// Start transaction for bulk insert
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Use mssql.CopyIn for bulk insert
	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(tempTable, mssql.BulkOptions{
		RowsPerBatch: p.rowsPerBatch,
	}, cols...))
	if err != nil {
		return fmt.Errorf("preparing bulk insert: %w", err)
	}
	defer stmt.Close()

	// Insert all rows
	// Convert rows to handle []byte values (e.g., decimal from MSSQL source)
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, convertRowForBulkCopy(row)...); err != nil {
			return fmt.Errorf("executing bulk insert row: %w", err)
		}
	}

	// Finalize bulk insert
	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("finalizing bulk insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing bulk insert: %w", err)
	}

	return nil
}

// buildMSSQLMergeWithTablock generates a MERGE statement with TABLOCK hint.
// TABLOCK prevents S->X lock conversion deadlocks that are common with MERGE.
// colTypes is used to skip geography/geometry columns from change detection
// (SQL Server doesn't support comparison operators on spatial types).
// isCrossEngine indicates PG→MSSQL migration where spatial data is WKT text that needs conversion.
func buildMSSQLMergeWithTablock(targetTable, stagingTable string, cols, colTypes, pkCols []string, isCrossEngine bool) string {
	// Build ON clause (PK join condition)
	var onClauses []string
	for _, pk := range pkCols {
		onClauses = append(onClauses, fmt.Sprintf("target.%s = source.%s",
			quoteMSSQLIdent(pk), quoteMSSQLIdent(pk)))
	}

	// Build SET clause and change detection (exclude PK columns)
	pkSet := make(map[string]bool)
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	var setClauses []string
	var changeDetection []string
	for i, col := range cols {
		if !pkSet[col] {
			quotedCol := quoteMSSQLIdent(col)
			sourceExpr := fmt.Sprintf("source.%s", quotedCol)

			// Get column type (lowercase for comparison)
			colType := ""
			if i < len(colTypes) {
				colType = strings.ToLower(colTypes[i])
			}

			// For cross-engine migrations (PG→MSSQL), convert WKT text to geography/geometry
			isSpatial := colType == "geography" || colType == "geometry"
			if isCrossEngine && isSpatial {
				// Use STGeomFromText to convert WKT text to spatial type
				// SRID 4326 is WGS84 (standard GPS coordinates)
				sourceExpr = fmt.Sprintf("%s::STGeomFromText(source.%s, 4326)", colType, quotedCol)
			}

			setClauses = append(setClauses, fmt.Sprintf("%s = %s", quotedCol, sourceExpr))

			// Skip geography/geometry from change detection - SQL Server doesn't support <> on spatial types
			if isSpatial {
				continue
			}

			// NULL-safe change detection for MSSQL
			changeDetection = append(changeDetection, fmt.Sprintf(
				"(target.%s <> source.%s OR "+
					"(target.%s IS NULL AND source.%s IS NOT NULL) OR "+
					"(target.%s IS NOT NULL AND source.%s IS NULL))",
				quotedCol, quotedCol, quotedCol, quotedCol, quotedCol, quotedCol))
		}
	}

	// Build INSERT column lists
	quotedCols := make([]string, len(cols))
	sourceCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCol := quoteMSSQLIdent(col)
		quotedCols[i] = quotedCol

		// Get column type
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// For cross-engine migrations, convert WKT text to spatial type in INSERT as well
		isSpatial := colType == "geography" || colType == "geometry"
		if isCrossEngine && isSpatial {
			sourceCols[i] = fmt.Sprintf("%s::STGeomFromText(source.%s, 4326)", colType, quotedCol)
		} else {
			sourceCols[i] = fmt.Sprintf("source.%s", quotedCol)
		}
	}

	var sb strings.Builder

	// MERGE INTO target WITH (TABLOCK) - TABLOCK prevents deadlocks
	sb.WriteString(fmt.Sprintf("MERGE INTO %s WITH (TABLOCK) AS target\n", targetTable))
	sb.WriteString(fmt.Sprintf("USING %s AS source\n", stagingTable))
	sb.WriteString(fmt.Sprintf("ON %s\n", strings.Join(onClauses, " AND ")))

	// WHEN MATCHED AND (changes detected) THEN UPDATE
	if len(setClauses) > 0 {
		sb.WriteString(fmt.Sprintf("WHEN MATCHED AND (%s) THEN UPDATE SET %s\n",
			strings.Join(changeDetection, " OR "),
			strings.Join(setClauses, ", ")))
	}

	// WHEN NOT MATCHED THEN INSERT
	sb.WriteString(fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(quotedCols, ", "),
		strings.Join(sourceCols, ", ")))

	return sb.String()
}

// executeMergeWithRetry executes a MERGE statement with deadlock retry logic.
// Handles IDENTITY_INSERT if the target has identity columns.
func (p *MSSQLPool) executeMergeWithRetry(ctx context.Context, conn *sql.Conn,
	targetTable, mergeSQL string, hasIdentity bool, maxRetries int) error {

	const baseDelayMs = 200

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error

		if hasIdentity {
			// Enable IDENTITY_INSERT, execute MERGE, then disable
			if _, err = conn.ExecContext(ctx, fmt.Sprintf("SET IDENTITY_INSERT %s ON", targetTable)); err != nil {
				return fmt.Errorf("enabling identity insert: %w", err)
			}
			// Use defer-like pattern to ensure IDENTITY_INSERT is disabled even on error
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

		// Check if deadlock (error 1205)
		if !isDeadlockError(err) || attempt == maxRetries {
			return err
		}

		logging.Warn("Deadlock on %s, retry %d/%d", targetTable, attempt, maxRetries)
		time.Sleep(time.Duration(baseDelayMs*attempt) * time.Millisecond)
	}

	return fmt.Errorf("merge failed after %d retries", maxRetries)
}

// convertRowForBulkCopy converts row values for MSSQL bulk copy.
// SQL Server source returns decimal/money values as []byte (ASCII string),
// which the go-mssqldb bulk copy can't handle. This function converts []byte
// values to strings ONLY if they look like ASCII numeric data.
// Binary data (geography, varbinary, etc.) is left unchanged.
func convertRowForBulkCopy(row []any) []any {
	result := make([]any, len(row))
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			// Only convert to string if it looks like ASCII numeric data (decimal/money)
			// Binary data (geography, varbinary) should pass through unchanged
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

// isASCIINumeric checks if a byte slice contains only ASCII numeric characters
// (digits, decimal point, minus sign, plus sign). This is used to distinguish
// decimal/money values (which come as ASCII strings like "3000.00") from
// binary data (geography, varbinary) which contains non-printable bytes.
func isASCIINumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		// Allow: 0-9, decimal point, minus, plus, 'E'/'e' for scientific notation
		if !((c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'E' || c == 'e') {
			return false
		}
	}
	return true
}

// isDeadlockError checks if the error is a SQL Server deadlock (error 1205)
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}

	// Check for mssql-specific error type
	if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
		return mssqlErr.SQLErrorNumber() == 1205
	}

	// Fallback to string matching
	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") || strings.Contains(errStr, "1205")
}

// GenerateMSSQLDDL generates CREATE TABLE statement for SQL Server
// sourceType indicates the source database type ("mssql" or "postgres") for type mapping
// Includes PRIMARY KEY constraint in CREATE TABLE to enable sorted bulk insert
func GenerateMSSQLDDL(t *source.Table, targetSchema string, sourceType string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", qualifyMSSQLTable(targetSchema, t.Name)))

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		// Map data type using unified type mapping
		mssqlType := typemap.MapType(sourceType, "mssql", col.DataType, col.MaxLength, col.Precision, col.Scale)

		sb.WriteString(fmt.Sprintf("    %s %s", quoteMSSQLIdent(col.Name), mssqlType))

		// Add IDENTITY for serial columns
		if col.IsIdentity {
			sb.WriteString(" IDENTITY(1,1)")
		}

		// Nullability
		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		} else {
			sb.WriteString(" NULL")
		}
	}

	// Add PRIMARY KEY constraint if table has one
	// This enables efficient sorted bulk insert (data arrives in PK order)
	if len(t.PrimaryKey) > 0 {
		pkCols := make([]string, len(t.PrimaryKey))
		for i, col := range t.PrimaryKey {
			pkCols[i] = quoteMSSQLIdent(col)
		}
		// Generate constraint name with MSSQL 128-char limit
		pkName := fmt.Sprintf("PK_%s", t.Name)
		if len(pkName) > 128 {
			pkName = pkName[:128]
		}
		sb.WriteString(fmt.Sprintf(",\n    CONSTRAINT %s PRIMARY KEY CLUSTERED (%s)",
			quoteMSSQLIdent(pkName),
			strings.Join(pkCols, ", ")))
	}

	sb.WriteString("\n)")

	return sb.String()
}

// mapReferentialActionMSSQL converts referential action to SQL Server syntax
func mapReferentialActionMSSQL(action string) string {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL", "SET NULL":
		return "SET NULL"
	case "SET_DEFAULT", "SET DEFAULT":
		return "SET DEFAULT"
	case "RESTRICT", "NO_ACTION", "NO ACTION":
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

// convertCheckDefinitionMSSQL converts PostgreSQL CHECK definition to SQL Server
func convertCheckDefinitionMSSQL(def string) string {
	result := def

	// Replace "column" with [column]
	// This is a simplified conversion
	for {
		start := strings.Index(result, `"`)
		if start == -1 {
			break
		}
		end := strings.Index(result[start+1:], `"`)
		if end == -1 {
			break
		}
		colName := result[start+1 : start+1+end]
		result = result[:start] + "[" + colName + "]" + result[start+end+2:]
	}

	// Replace CURRENT_TIMESTAMP with GETDATE()
	result = strings.ReplaceAll(result, "CURRENT_TIMESTAMP", "GETDATE()")
	result = strings.ReplaceAll(result, "current_timestamp", "GETDATE()")

	// Replace true/false with 1/0
	result = strings.ReplaceAll(result, " true", " 1")
	result = strings.ReplaceAll(result, " false", " 0")
	result = strings.ReplaceAll(result, "(true)", "(1)")
	result = strings.ReplaceAll(result, "(false)", "(0)")

	return result
}
