package target

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/typemap"
	mssql "github.com/microsoft/go-mssqldb"
)

// MSSQLPool manages a pool of SQL Server target connections
type MSSQLPool struct {
	db       *sql.DB
	config   *config.TargetConfig
	maxConns int
}

// NewMSSQLPool creates a new SQL Server target connection pool
func NewMSSQLPool(cfg *config.TargetConfig, maxConns int) (*MSSQLPool, error) {
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&TrustServerCertificate=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

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

	return &MSSQLPool{
		db:       db,
		config:   cfg,
		maxConns: maxConns,
	}, nil
}

// Close closes all connections in the pool
func (p *MSSQLPool) Close() {
	p.db.Close()
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

// CreateSchema creates the target schema if it doesn't exist
func (p *MSSQLPool) CreateSchema(ctx context.Context, schema string) error {
	// Check if schema exists
	var exists int
	err := p.db.QueryRowContext(ctx,
		"SELECT 1 FROM sys.schemas WHERE name = @schema",
		sql.Named("schema", schema)).Scan(&exists)
	if err == sql.ErrNoRows {
		// Create schema
		_, err = p.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA [%s]", schema))
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
	ddl := GenerateMSSQLDDL(t, targetSchema)

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
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE [%s].[%s]", schema, table))
	return err
}

// DropTable drops a table if it exists
func (p *MSSQLPool) DropTable(ctx context.Context, schema, table string) error {
	_, err := p.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS [%s].[%s]", schema, table))
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

// CreatePrimaryKey creates a primary key on the table
func (p *MSSQLPool) CreatePrimaryKey(ctx context.Context, t *source.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	pkCols := ""
	for i, col := range t.PrimaryKey {
		if i > 0 {
			pkCols += ", "
		}
		pkCols += fmt.Sprintf("[%s]", col)
	}

	sql := fmt.Sprintf("ALTER TABLE [%s].[%s] ADD PRIMARY KEY (%s)",
		targetSchema, t.Name, pkCols)

	_, err := p.db.ExecContext(ctx, sql)
	return err
}

// GetRowCount returns the row count for a table
func (p *MSSQLPool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM [%s].[%s]", schema, table)).Scan(&count)
	return count, err
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
		fmt.Sprintf("SELECT COALESCE(MAX([%s]), 0) FROM [%s].[%s]", identityCol, schema, t.Name)).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil // Empty table, no need to reset
	}

	// Reseed identity
	_, err = p.db.ExecContext(ctx, fmt.Sprintf("DBCC CHECKIDENT ('[%s].[%s]', RESEED, %d)", schema, t.Name, maxVal))
	return err
}

// CreateIndex creates an index on the target table
func (p *MSSQLPool) CreateIndex(ctx context.Context, t *source.Table, idx *source.Index, targetSchema string) error {
	// Build column list
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = fmt.Sprintf("[%s]", col)
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

	sqlStmt := fmt.Sprintf("CREATE %sINDEX [%s] ON [%s].[%s] (%s)",
		unique, idxName, targetSchema, t.Name, strings.Join(cols, ", "))

	// Add included columns if any
	if len(idx.IncludeCols) > 0 {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			includeCols[i] = fmt.Sprintf("[%s]", col)
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
		cols[i] = fmt.Sprintf("[%s]", col)
	}

	refCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		refCols[i] = fmt.Sprintf("[%s]", col)
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
		ALTER TABLE [%s].[%s]
		ADD CONSTRAINT [%s]
		FOREIGN KEY (%s)
		REFERENCES [%s].[%s] (%s)
		ON DELETE %s
		ON UPDATE %s
	`, targetSchema, t.Name, fkName,
		strings.Join(cols, ", "),
		targetSchema, fk.RefTable, strings.Join(refCols, ", "),
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
		ALTER TABLE [%s].[%s]
		ADD CONSTRAINT [%s]
		CHECK %s
	`, targetSchema, t.Name, chkName, definition)

	_, err := p.db.ExecContext(ctx, sqlStmt)
	return err
}

// WriteChunk writes a chunk of data to the target table using TDS bulk copy protocol
func (p *MSSQLPool) WriteChunk(ctx context.Context, schema, table string, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	// Start a transaction for bulk copy
	txn, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	// Create fully qualified table name
	fullTableName := fmt.Sprintf("[%s].[%s]", schema, table)

	// Prepare bulk copy statement with performance hints
	// - Tablock: acquire table lock to reduce lock overhead
	// - RowsPerBatch: hint to optimizer for memory allocation
	bulkOpts := mssql.BulkOptions{
		Tablock:      true,
		RowsPerBatch: len(rows),
	}
	stmt, err := txn.PrepareContext(ctx, mssql.CopyIn(fullTableName, bulkOpts, cols...))
	if err != nil {
		txn.Rollback()
		return fmt.Errorf("preparing bulk copy: %w", err)
	}

	// Execute for each row
	for _, row := range rows {
		_, err = stmt.ExecContext(ctx, row...)
		if err != nil {
			stmt.Close()
			txn.Rollback()
			return fmt.Errorf("bulk copy row: %w", err)
		}
	}

	// Final exec with no args to flush all rows
	_, err = stmt.ExecContext(ctx)
	if err != nil {
		stmt.Close()
		txn.Rollback()
		return fmt.Errorf("flushing bulk copy: %w", err)
	}

	if err = stmt.Close(); err != nil {
		txn.Rollback()
		return fmt.Errorf("closing bulk copy: %w", err)
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("committing bulk copy: %w", err)
	}

	return nil
}

// GenerateMSSQLDDL generates SQL Server DDL from source table metadata
func GenerateMSSQLDDL(t *source.Table, targetSchema string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE [%s].[%s] (\n", targetSchema, t.Name))

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		// Map data type
		mssqlType := typemap.PostgresToMSSQL(col.DataType, col.MaxLength, col.Precision, col.Scale)

		sb.WriteString(fmt.Sprintf("    [%s] %s", col.Name, mssqlType))

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
