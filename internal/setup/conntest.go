package setup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"

	// Register SQL drivers for connection testing.
	// mysql/postgres/sqlite/clickhouse register via the catalog-driven
	// generic engine (#509); mssql still self-registers.
	_ "github.com/johndauphine/dmt/internal/driver/generic"
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
)

// sqlDriverNames maps DMT driver names to database/sql driver names.
var sqlDriverNames = map[string]string{
	"mssql":    "sqlserver",
	"postgres": "pgx",
	"mysql":    "mysql",
}

// ConnTestResult holds the result of a connection test.
type ConnTestResult struct {
	Connected bool
	Error     string
	LatencyMs int64
}

// TestConnection tests a database connection using a raw database/sql ping.
// It uses the driver's Dialect.BuildDSN() to build the connection string
// and does NOT call ExtractSchema or create Reader/Writer instances.
func TestConnection(ctx context.Context, dbType, host string, port int, database, user, password string, opts map[string]any) *ConnTestResult {
	start := time.Now()

	canonical := driver.Canonicalize(dbType)

	d, err := driver.Get(canonical)
	if err != nil {
		return &ConnTestResult{Error: fmt.Sprintf("unknown driver: %s", dbType)}
	}

	dsn := d.Dialect().BuildDSN(host, port, database, user, password, opts)

	sqlDriver, ok := sqlDriverNames[canonical]
	if !ok {
		return &ConnTestResult{Error: fmt.Sprintf("no sql driver mapping for: %s", canonical)}
	}

	// sql.Open + PingContext error strings from each driver library
	// can echo the DSN (and thus the password). Scrub before
	// publishing the message into ConnTestResult, which is rendered
	// to the setup-wizard UI and may be logged for support
	// debugging (#231).
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return &ConnTestResult{
			Error:     logging.Scrub(fmt.Sprintf("connection error: %v", err)),
			LatencyMs: time.Since(start).Milliseconds(),
		}
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return &ConnTestResult{
			Error:     logging.Scrub(fmt.Sprintf("ping failed: %v", err)),
			LatencyMs: time.Since(start).Milliseconds(),
		}
	}

	return &ConnTestResult{
		Connected: true,
		LatencyMs: time.Since(start).Milliseconds(),
	}
}
