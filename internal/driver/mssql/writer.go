package mssql

import (
	"context"
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
)

// Writer implements driver.Writer for SQL Server.
type Writer struct {
	db                 *sql.DB
	config             *dbconfig.TargetConfig
	maxConns           int
	defaultBatchSize   int
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

	// go-mssqldb errors can quote the raw DSN — see #231. ScrubError
	// strips the password.
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, logging.ScrubError(fmt.Errorf("opening connection: %w", err))
	}

	// Configure connection pool
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, logging.ScrubError(fmt.Errorf("pinging database: %w", err))
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

	// Log mapper initialization. Type-switch to surface which mapper
	// is in use — useful for debugging when AI fallback is/isn't
	// firing (#170).
	driver.LogTypeMapperInit(opts.TypeMapper)

	// Check if type mapper also implements finalization DDL mapper
	finalizationMapper, _ := opts.TypeMapper.(driver.FinalizationDDLMapper)

	w := &Writer{
		db:                 db,
		config:             cfg,
		maxConns:           maxConns,
		defaultBatchSize:   opts.BatchSize,
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
		MaxNVarcharLength:        4000,   // NVARCHAR(n) max is 4000 chars; beyond that use NVARCHAR(MAX)
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
