package stats

import "fmt"

// PoolStats contains connection pool statistics for logging.
// This provides a unified view of pool metrics across different database drivers.
type PoolStats struct {
	DBType      string // "mssql" or "postgres"
	MaxConns    int    // Maximum connections allowed
	ActiveConns int    // Currently active/in-use connections
	IdleConns   int    // Currently idle connections
	WaitCount   int64  // Total number of times a connection was waited for
	WaitTimeMs  int64  // Total time spent waiting for connections (milliseconds)
}

// String returns a formatted string for logging pool stats.
func (s PoolStats) String() string {
	return fmt.Sprintf("%s: %d/%d active, %d idle, %d waits (%.1fms avg)",
		s.DBType, s.ActiveConns, s.MaxConns, s.IdleConns,
		s.WaitCount, float64(s.WaitTimeMs)/float64(max(s.WaitCount, 1)))
}
