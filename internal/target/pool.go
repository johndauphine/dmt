package target

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
)

// Pool manages a pool of PostgreSQL connections
type Pool struct {
	pool   *pgxpool.Pool
	config *config.TargetConfig
}

// NewPool creates a new PostgreSQL connection pool
func NewPool(cfg *config.TargetConfig, maxConns int) (*Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

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

	return &Pool{pool: pool, config: cfg}, nil
}

// Close closes all connections in the pool
func (p *Pool) Close() {
	p.pool.Close()
}

// Pool returns the underlying pgxpool
func (p *Pool) Pool() *pgxpool.Pool {
	return p.pool
}

// CreateSchema creates the target schema if it doesn't exist
func (p *Pool) CreateSchema(ctx context.Context, schema string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	return err
}

// CreateTable creates a table from source metadata (as UNLOGGED for speed)
func (p *Pool) CreateTable(ctx context.Context, t *source.Table, targetSchema string) error {
	ddl := GenerateDDL(t, targetSchema)

	_, err := p.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("creating table %s: %w", t.FullName(), err)
	}

	return nil
}

// TruncateTable truncates a table
func (p *Pool) TruncateTable(ctx context.Context, schema, table string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", schema, table))
	return err
}

// ConvertToLogged converts an UNLOGGED table to LOGGED
func (p *Pool) ConvertToLogged(ctx context.Context, schema, table string) error {
	_, err := p.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s SET LOGGED", schema, table))
	return err
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
		pkCols += fmt.Sprintf("%q", col)
	}

	sql := fmt.Sprintf("ALTER TABLE %s.%q ADD PRIMARY KEY (%s)",
		targetSchema, t.Name, pkCols)

	_, err := p.pool.Exec(ctx, sql)
	return err
}

// GetRowCount returns the row count for a table
func (p *Pool) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.%q", schema, table)).Scan(&count)
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

	// Get sequence name and reset
	sql := fmt.Sprintf(`
		SELECT setval(
			pg_get_serial_sequence('%s.%s', '%s'),
			COALESCE((SELECT MAX(%q) FROM %s.%q), 1)
		)
	`, schema, t.Name, identityCol, identityCol, schema, t.Name)

	_, err := p.pool.Exec(ctx, sql)
	return err
}
