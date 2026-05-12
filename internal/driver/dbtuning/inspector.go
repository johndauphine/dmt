package dbtuning

import (
	"context"
	"database/sql"
	"fmt"
)

// SchemaStatistics contains database schema metrics used for tuning recommendations.
type SchemaStatistics struct {
	TotalTables     int
	TotalRows       int64
	AvgRowSizeBytes int64
	EstimatedMemMB  int64
	LargestTable    TableInfo
}

// TableInfo contains information about a specific table.
type TableInfo struct {
	Name      string
	Rows      int64
	SizeBytes int64
}

// DatabaseTuning contains database-specific tuning recommendations.
type DatabaseTuning struct {
	DatabaseType string // "mysql", "postgresql", "mssql", "oracle"
	Role         string // "source" or "target"

	// Current configuration values dmt actually read (one entry per
	// setting examined). Settings the deterministic catalog doesn't
	// cover do not appear here.
	CurrentSettings map[string]interface{}

	// Recommendations the rules emitted, in registration order. An
	// empty list means "config looks reasonable" — see
	// TuningPotential="none" for that case.
	Recommendations []TuningRecommendation

	// Summary
	TuningPotential string // "high", "medium", "low", "none"
	EstimatedImpact string // e.g., "3 recommendation(s); 2 critical, 1 important"
}

// TuningRecommendation represents a single database parameter tuning suggestion.
type TuningRecommendation struct {
	Parameter        string
	CurrentValue     interface{}
	RecommendedValue interface{}
	Impact           string // "high", "medium", "low"
	Reason           string
	Priority         int // 1=critical, 2=important, 3=optional

	// Application method
	CanApplyRuntime bool   // Can use SET GLOBAL or similar
	SQLCommand      string // e.g., "SET GLOBAL innodb_buffer_pool_size = 4294967296;"
	RequiresRestart bool
	ConfigFile      string // Configuration file snippet if restart required
}

// Analyze runs the deterministic tuning catalog for the given database
// and produces a DatabaseTuning report. No AI calls are made — every
// setting the catalog covers is interrogated via a hardcoded query;
// each rule is a Go function.
//
// `sys` describes the *DB server's* RAM and CPU, which the caller is
// expected to populate honestly: pass DetectSystemInfo() values only
// when the DB is on the same host as dmt; otherwise zero out
// HostMemoryMB (rules that size memory based on RAM will skip rather
// than emit wrong recommendations). MSSQL rules additionally
// self-correct by querying sys.dm_os_sys_memory at the DB.
//
// Settings the deterministic catalog doesn't recognize produce no
// recommendation; the user can investigate them via the
// CurrentSettings map.
func Analyze(
	ctx context.Context,
	db *sql.DB,
	dbType string,
	role string,
	stats SchemaStatistics,
	sys SystemInfo,
) (*DatabaseTuning, error) {
	if db == nil {
		return &DatabaseTuning{
			DatabaseType:    dbType,
			Role:            role,
			TuningPotential: "unknown",
			EstimatedImpact: "Database connection not available for analysis.",
		}, nil
	}

	if SettingCount(dbType) == 0 {
		return &DatabaseTuning{
			DatabaseType:    dbType,
			Role:            role,
			TuningPotential: "unknown",
			EstimatedImpact: "No tuning catalog registered for this database type.",
		}, nil
	}

	current, recs := runCatalog(ctx, db, dbType, role, sys, stats)

	// Guard against the "every query failed" case (Codex review on
	// #172). If we read zero settings, the DB-side was unreachable
	// (context expired, role lacks SELECT on the relevant catalogs,
	// etc.); reporting "none / configuration looks reasonable" would
	// be wrong because we never actually inspected anything. Report
	// unknown so the user can investigate.
	if len(current) == 0 && len(recs) == 0 {
		impactMsg := "Could not read any catalog setting — check role privileges on pg_settings / sys.configurations / information_schema, or whether the analysis context timed out."
		if ctx != nil && ctx.Err() != nil {
			impactMsg = fmt.Sprintf("Analysis context error: %v", ctx.Err())
		}
		return &DatabaseTuning{
			DatabaseType:    dbType,
			Role:            role,
			CurrentSettings: current,
			TuningPotential: "unknown",
			EstimatedImpact: impactMsg,
		}, nil
	}

	potential, impact := summarizeRecommendations(recs)
	return &DatabaseTuning{
		DatabaseType:    dbType,
		Role:            role,
		CurrentSettings: current,
		Recommendations: recs,
		TuningPotential: potential,
		EstimatedImpact: impact,
	}, nil
}
