package generic

import (
	// Each supported backend is a database/sql driver the catalog can
	// select via connection.backend. The Go SQL driver dependency is
	// the one per-engine cost a catalog cannot eliminate.
	_ "github.com/ClickHouse/clickhouse-go/v2" // backend "clickhouse" (pure Go)
	_ "modernc.org/sqlite"                     // backend "sqlite" (pure Go, no cgo)
)

// knownBackends is the load-time validation set for
// connection.backend. The value is the sql.Open driver name (today
// identical to the key; kept as a map so a backend whose import name
// differs from its registered name stays a one-line change).
var knownBackends = map[string]string{
	"sqlite":     "sqlite",
	"clickhouse": "clickhouse",
}
