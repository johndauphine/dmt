package dbtuning

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

// SystemInfo captures host-level facts a tuning rule may need to compute
// its recommendation (e.g. "shared_buffers should be 25% of RAM"). It's
// detected once per Analyze call and passed to every rule.
type SystemInfo struct {
	HostMemoryMB int64
	CPUCores     int
}

// Role is one of the strings the orchestrator passes for the database
// role being analyzed. Rules can specialize on this — most settings
// don't need to, but a few do (e.g. wal_level=minimal makes sense for
// a non-replicated target but not for a production source).
const (
	RoleSource = "source"
	RoleTarget = "target"
)

// Setting is one parameter the deterministic catalog can analyze.
// Each Setting owns its query shape and rule logic; the dispatcher
// just walks the catalog and aggregates results. Keeping query +
// parse + rule co-located per setting (rather than splitting into
// Query/Parse/Rule fields) lets each entry pick whatever SQL shape
// is most natural for that DB — `SHOW VARIABLES` for MySQL,
// `pg_settings` for Postgres, `sys.configurations` for MSSQL — without
// awkward shared abstractions.
type Setting struct {
	Name     string
	Evaluate EvaluateFunc
}

// EvaluateFunc fetches a setting's current value and, if a rule fires,
// returns a TuningRecommendation. The current value is always recorded
// in the report's CurrentSettings map (when err is nil) so that the
// user can see what dmt looked at, even when no recommendation applies.
//
// Returning (current=nil, rec=nil, err=nil) means "couldn't read the
// setting cleanly but it's not a hard error" (e.g. permission denied
// on a single sys.configurations row); the dispatcher silently skips
// those rather than failing the whole analysis.
type EvaluateFunc func(
	ctx context.Context,
	db *sql.DB,
	role string,
	sys SystemInfo,
	stats SchemaStatistics,
) (current interface{}, rec *TuningRecommendation, err error)

var (
	mu       sync.RWMutex
	catalogs = map[string][]Setting{}
)

// Register adds settings under a driver name. Driver names are stored
// lowercase; lookup is case-insensitive. Driver packages register their
// own settings in an init() so the dispatcher can find them by the
// driver type string the orchestrator passes in.
func Register(driverName string, settings []Setting) {
	mu.Lock()
	defer mu.Unlock()
	key := strings.ToLower(driverName)
	catalogs[key] = append(catalogs[key], settings...)
}

// SettingsFor returns a copy of the catalog for the given driver name
// (after Canonicalize-style alias normalization). Returns nil if no
// catalog is registered. The copy guards callers against accidental
// mutation of the shared slice while iterating.
func SettingsFor(driverName string) []Setting {
	mu.RLock()
	defer mu.RUnlock()
	src := catalogs[strings.ToLower(driverName)]
	if len(src) == 0 {
		return nil
	}
	out := make([]Setting, len(src))
	copy(out, src)
	return out
}

// SettingCount reports how many settings are registered for a driver.
// Used in tests to assert minimum coverage.
func SettingCount(driverName string) int {
	mu.RLock()
	defer mu.RUnlock()
	return len(catalogs[strings.ToLower(driverName)])
}

// runCatalog walks the catalog for driverName, applies every setting's
// Evaluate function, and collects current values + recommendations.
// Per-setting errors are logged and skipped so one bad permission
// doesn't poison the whole report.
func runCatalog(
	ctx context.Context,
	db *sql.DB,
	driverName string,
	role string,
	sys SystemInfo,
	stats SchemaStatistics,
) (map[string]interface{}, []TuningRecommendation) {
	settings := SettingsFor(driverName)
	current := make(map[string]interface{}, len(settings))
	var recs []TuningRecommendation

	for _, s := range settings {
		val, rec, err := s.Evaluate(ctx, db, role, sys, stats)
		if err != nil {
			logging.Debug("dbtuning: %s setting %q skipped: %v", driverName, s.Name, err)
			continue
		}
		if val != nil {
			current[s.Name] = val
		}
		if rec != nil {
			recs = append(recs, *rec)
		}
	}

	return current, recs
}
