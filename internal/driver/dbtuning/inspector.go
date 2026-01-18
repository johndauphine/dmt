package dbtuning

import (
	"context"
	"database/sql"
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

	// Current configuration
	CurrentSettings map[string]interface{}

	// Recommendations
	Recommendations []TuningRecommendation

	// Summary
	TuningPotential string // "high", "medium", "low", "none"
	EstimatedImpact string // e.g., "2-3x throughput improvement"
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

// Analyze analyzes database configuration using AI.
// Returns DatabaseTuning recommendations or an error if AI is not configured.
func Analyze(
	ctx context.Context,
	db *sql.DB,
	dbType string,
	role string,
	stats SchemaStatistics,
	aiMapper interface{}, // anything with CallAI(ctx, prompt) method
) (*DatabaseTuning, error) {
	// AI is required - no fallback to hardcoded logic
	if aiMapper == nil {
		return &DatabaseTuning{
			DatabaseType:    dbType,
			Role:            role,
			TuningPotential: "unknown",
			EstimatedImpact: "AI not configured - set ai.api_key to enable database tuning recommendations",
		}, nil
	}

	// Check if it's an AIQuerier interface
	if querier, ok := aiMapper.(AIQuerier); ok {
		analyzer := NewAITuningAnalyzer(querier)
		return analyzer.AnalyzeDatabase(ctx, db, dbType, role, stats)
	}

	// Check if it has CallAI method (AITypeMapper)
	if caller, ok := aiMapper.(interface {
		CallAI(ctx context.Context, prompt string) (string, error)
	}); ok {
		analyzer := NewAITuningAnalyzer(&aiCallWrapper{caller})
		return analyzer.AnalyzeDatabase(ctx, db, dbType, role, stats)
	}

	// AI mapper doesn't implement expected interface
	return &DatabaseTuning{
		DatabaseType:    dbType,
		Role:            role,
		TuningPotential: "unknown",
		EstimatedImpact: "AI mapper does not implement required Query/CallAI method",
	}, nil
}

// aiCallWrapper wraps CallAI method to implement AIQuerier interface.
type aiCallWrapper struct {
	caller interface {
		CallAI(ctx context.Context, prompt string) (string, error)
	}
}

func (w *aiCallWrapper) Query(ctx context.Context, prompt string) (string, error) {
	return w.caller.CallAI(ctx, prompt)
}
