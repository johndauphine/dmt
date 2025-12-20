package target

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
)

// PoolStats contains connection pool statistics
type PoolStats struct {
	MaxConns          int32 // Maximum number of connections
	TotalConns        int32 // Total number of connections
	AcquiredConns     int32 // Connections currently in use
	IdleConns         int32 // Connections currently idle
	AcquireCount      int64 // Total number of successful acquires
	EmptyAcquireCount int64 // Acquires that waited for a connection
}

// Pool manages a pool of PostgreSQL connections
type Pool struct {
	pool     *pgxpool.Pool
	config   *config.TargetConfig
	maxConns int
}

// NewPool creates a new PostgreSQL connection pool
func NewPool(cfg *config.TargetConfig, maxConns int) (*Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing dsn: %w", err)
	}

	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(maxConns / 4)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &Pool{pool: pool, config: cfg, maxConns: maxConns}, nil
}

// Close closes all connections in the pool
func (p *Pool) Close() {
	p.pool.Close()
}

// Pool returns the underlying pgxpool
func (p *Pool) Pool() *pgxpool.Pool {
	return p.pool
}

// Stats returns current connection pool statistics
func (p *Pool) Stats() PoolStats {
	stats := p.pool.Stat()
	return PoolStats{
		MaxConns:          stats.MaxConns(),
		TotalConns:        stats.TotalConns(),
		AcquiredConns:     stats.AcquiredConns(),
		IdleConns:         stats.IdleConns(),
		AcquireCount:      stats.AcquireCount(),
		EmptyAcquireCount: stats.EmptyAcquireCount(),
	}
}

// MaxConns returns the configured maximum connections
func (p *Pool) MaxConns() int {
	return p.maxConns
}

// DBType returns the database type
func (p *Pool) DBType() string {
	return "postgres"
}

// CreateSchema creates the target schema if it doesn't exist
func (p *Pool) CreateSchema(ctx context.Context, schema string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quotePGIdent(schema)))
	return err
}

// CreateTable creates a table from source metadata
func (p *Pool) CreateTable(ctx context.Context, t *source.Table, targetSchema string) error {
	return p.CreateTableWithOptions(ctx, t, targetSchema, false)
}

// CreateTableWithOptions creates a table with optional UNLOGGED
func (p *Pool) CreateTableWithOptions(ctx context.Context, t *source.Table, targetSchema string, unlogged bool) error {
	ddl := GenerateDDLWithOptions(t, targetSchema, unlogged)

	_, err := p.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}

	return nil
}

// SetTableLogged converts an UNLOGGED table to LOGGED
func (p *Pool) SetTableLogged(ctx context.Context, schema, table string) error {
	sql := fmt.Sprintf("ALTER TABLE %s.%q SET LOGGED", schema, table)
	_, err := p.pool.Exec(ctx, sql)
	return err
}

// TruncateTable truncates a table
func (p *Pool) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualifyPGTable(schema, table)))
	return err
}

// DropTable drops a table if it exists
func (p *Pool) DropTable(ctx context.Context, schema, table string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifyPGTable(schema, table)))
	return err
}

// TableExists checks if a table exists in the schema
func (p *Pool) TableExists(ctx context.Context, schema, table string) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)
	`, schema, table).Scan(&exists)
	return exists, err
}

// CreatePrimaryKey creates a primary key on the table
func (p *Pool) CreatePrimaryKey(ctx context.Context, t *source.Table, targetSchema string) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}

	pkCols := ""
	for i, col := range t.PrimaryKey {
		if i > 0 {
			pkCols += ", "
		}
		pkCols += quotePGIdent(col)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)",
		qualifyPGTable(targetSchema, t.Name), pkCols)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// GetRowCount returns the row count for a table
func (p *Pool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifyPGTable(schema, table))).Scan(&count)
	return count, err
}

// ResetSequence resets identity sequence to max value
func (p *Pool) ResetSequence(ctx context.Context, schema string, t *source.Table) error {
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
	err := p.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", quotePGIdent(identityCol), qualifyPGTable(schema, t.Name))).Scan(&maxVal)
	if err != nil {
		return fmt.Errorf("getting max value for %s.%s: %w", t.Name, identityCol, err)
	}

	if maxVal == 0 {
		return nil // Empty table, no need to reset
	}

	// For GENERATED AS IDENTITY columns, use ALTER TABLE ... RESTART WITH
	// This works for both SERIAL and IDENTITY columns
	sql := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d`,
		qualifyPGTable(schema, t.Name), quotePGIdent(identityCol), maxVal+1)

	_, err = p.pool.Exec(ctx, sql)
	if err != nil {
		// Fall back to pg_get_serial_sequence for SERIAL columns
		fallbackSQL := fmt.Sprintf(`
			SELECT setval(
				pg_get_serial_sequence('%s', '%s'),
				%d
			)
		`, qualifyPGTable(schema, t.Name), identityCol, maxVal)
		_, err = p.pool.Exec(ctx, fallbackSQL)
	}
	return err
}

// CreateIndex creates an index on the target table
func (p *Pool) CreateIndex(ctx context.Context, t *source.Table, idx *source.Index, targetSchema string) error {
	// Build column list
	cols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = quotePGIdent(col)
	}

	unique := ""
	if idx.IsUnique {
		unique = "UNIQUE "
	}

	// Generate index name (PostgreSQL has length limits)
	idxName := fmt.Sprintf("idx_%s_%s", t.Name, idx.Name)
	if len(idxName) > 63 {
		idxName = idxName[:63]
	}

	sql := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, quotePGIdent(idxName), qualifyPGTable(targetSchema, t.Name), strings.Join(cols, ", "))

	// Add included columns if any (PostgreSQL INCLUDE syntax)
	if len(idx.IncludeCols) > 0 {
		includeCols := make([]string, len(idx.IncludeCols))
		for i, col := range idx.IncludeCols {
			includeCols[i] = quotePGIdent(col)
		}
		sql += fmt.Sprintf(" INCLUDE (%s)", strings.Join(includeCols, ", "))
	}

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// CreateForeignKey creates a foreign key constraint on the target table
func (p *Pool) CreateForeignKey(ctx context.Context, t *source.Table, fk *source.ForeignKey, targetSchema string) error {
	// Build column lists
	cols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		cols[i] = quotePGIdent(col)
	}

	refCols := make([]string, len(fk.RefColumns))
	for i, col := range fk.RefColumns {
		refCols[i] = quotePGIdent(col)
	}

	// Map SQL Server referential actions to PostgreSQL
	onDelete := mapReferentialAction(fk.OnDelete)
	onUpdate := mapReferentialAction(fk.OnUpdate)

	// Generate FK name
	fkName := fmt.Sprintf("fk_%s_%s", t.Name, fk.Name)
	if len(fkName) > 63 {
		fkName = fkName[:63]
	}

	sql := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		FOREIGN KEY (%s)
		REFERENCES %s (%s)
		ON DELETE %s
		ON UPDATE %s
	`, qualifyPGTable(targetSchema, t.Name), quotePGIdent(fkName),
		strings.Join(cols, ", "),
		qualifyPGTable(targetSchema, fk.RefTable), strings.Join(refCols, ", "),
		onDelete, onUpdate)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// CreateCheckConstraint creates a check constraint on the target table
func (p *Pool) CreateCheckConstraint(ctx context.Context, t *source.Table, chk *source.CheckConstraint, targetSchema string) error {
	// Convert SQL Server CHECK syntax to PostgreSQL
	// Most basic checks are compatible
	definition := convertCheckDefinition(chk.Definition)

	// Generate constraint name
	chkName := fmt.Sprintf("chk_%s_%s", t.Name, chk.Name)
	if len(chkName) > 63 {
		chkName = chkName[:63]
	}

	sql := fmt.Sprintf(`
		ALTER TABLE %s
		ADD CONSTRAINT %s
		CHECK %s
	`, qualifyPGTable(targetSchema, t.Name), quotePGIdent(chkName), definition)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// mapReferentialAction converts SQL Server referential action to PostgreSQL
func mapReferentialAction(action string) string {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL":
		return "SET NULL"
	case "SET_DEFAULT":
		return "SET DEFAULT"
	case "NO_ACTION":
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

// convertCheckDefinition converts SQL Server CHECK definition to PostgreSQL
func convertCheckDefinition(def string) string {
	// Replace SQL Server specific functions/syntax
	// Most basic checks are compatible
	result := def

	// Replace [column] with "column"
	// This is a simplified conversion - complex expressions may need manual review
	for {
		start := strings.Index(result, "[")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "]")
		if end == -1 {
			break
		}
		colName := result[start+1 : start+end]
		result = result[:start] + `"` + colName + `"` + result[start+end+1:]
	}

	// Replace getdate() with CURRENT_TIMESTAMP
	result = strings.ReplaceAll(result, "getdate()", "CURRENT_TIMESTAMP")
	result = strings.ReplaceAll(result, "GETDATE()", "CURRENT_TIMESTAMP")

	return result
}
