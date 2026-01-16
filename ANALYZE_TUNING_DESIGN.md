# Analyze Command Enhancement: Database Tuning Recommendations

## Overview

Enhance the existing `analyze` command to provide **pre-migration database tuning recommendations** for both source and target databases, leveraging historical performance data.

**Key principle**: Recommendations only - no new dmt configuration parameters. Users manually apply database tuning, then use existing dmt migration config.

## Current State

The `analyze` command currently:
- Analyzes source database schema metadata
- Returns migration config recommendations (workers, chunk_size, etc.)
- Uses AI for intelligent parameter suggestions via `SmartConfigAnalyzer`
- Outputs `SmartConfigSuggestions` with migration-level tuning

**Missing capabilities:**
- No source database configuration analysis
- No target database configuration analysis
- No database-specific tuning recommendations
- No historical performance data integration

## Proposed Enhancement

**Scope**: Add database tuning analysis and recommendations to existing analyze output. The recommendations are informational only - users apply them manually to their databases before running migrations.

### 1. Database Configuration Analysis

Add database configuration inspection to identify tuning opportunities:

**For MySQL (source and target):**
- Query current settings: `SHOW VARIABLES`
- Key parameters to check:
  - `innodb_buffer_pool_size` (target: 60-70% of RAM for dedicated server)
  - `innodb_flush_log_at_trx_commit` (target: 2 for migration workloads)
  - `max_connections` (ensure sufficient for dmt workers)
  - `read_buffer_size` (source: 8MB for sequential scans)
  - `table_open_cache` (source: 4000+ for large schemas)
  - `tmp_table_size` (source: 256MB+ for complex queries)
  - `innodb_io_capacity` (target: 2000-10000 for SSDs)
  - `innodb_write_io_threads` (target: 16+ for high concurrency)
  - `bulk_insert_buffer_size` (target: 128MB for bulk loads)

**For PostgreSQL (source and target):**
- Query current settings: `SHOW ALL` or `SELECT * FROM pg_settings`
- Key parameters to check:
  - `shared_buffers` (25% of RAM for dedicated server)
  - `effective_cache_size` (50-75% of RAM)
  - `work_mem` (per-operation memory, adjust for large sorts)
  - `maintenance_work_mem` (for bulk operations, indexes)
  - `checkpoint_segments` / `max_wal_size` (reduce checkpoint frequency)
  - `synchronous_commit` (off for migration workloads)
  - `full_page_writes` (off for migration workloads)
  - `max_connections` (ensure sufficient for dmt workers)
  - `random_page_cost` (1.1 for SSDs vs 4.0 for HDDs)

**For MSSQL (source and target):**
- Query current settings: `sp_configure` and DMVs
- Key parameters to check:
  - `max server memory` (leave 2-4GB for OS)
  - `max degree of parallelism` (MAXDOP, typically 8 for OLTP)
  - `cost threshold for parallelism` (50 recommended)
  - `recovery interval` (target: high for bulk loads)
  - `trace flags` (610, 1117, 1118 for bulk operations)
  - Database-level: `RECOVERY SIMPLE` for target during migration
  - Database-level: `READ_COMMITTED_SNAPSHOT ON` for source

**For Oracle (source and target):**
- Query current settings: `V$PARAMETER`, `V$SYSTEM_PARAMETER`
- Key parameters to check:
  - `sga_target` / `sga_max_size` (60% of RAM for dedicated server)
  - `pga_aggregate_target` (20-30% of RAM)
  - `db_file_multiblock_read_count` (source: 128 for full scans)
  - `optimizer_mode` (ALL_ROWS for batch operations)
  - `parallel_max_servers` (support parallel queries)
  - `db_writer_processes` (target: 4-8 for high write)
  - `log_buffer` (target: 16-32MB)

### 2. Historical Performance Integration

Query `ai_adjustments` table to build performance profiles:

```sql
-- Get effectiveness by action type across all runs
SELECT
    action,
    COUNT(*) as attempts,
    SUM(CASE WHEN effect_percent > 0 THEN 1 ELSE 0 END) as successful,
    AVG(effect_percent) as avg_effect,
    AVG(throughput_after) as avg_throughput
FROM ai_adjustments
GROUP BY action;

-- Get recent adjustment patterns (last 30 days)
SELECT
    action,
    adjustments,
    throughput_before,
    throughput_after,
    effect_percent,
    reasoning
FROM ai_adjustments
WHERE timestamp > datetime('now', '-30 days')
ORDER BY timestamp DESC
LIMIT 50;

-- Identify optimal parameter ranges
SELECT
    json_extract(adjustments, '$.workers') as workers,
    json_extract(adjustments, '$.chunk_size') as chunk_size,
    AVG(throughput_after) as avg_throughput,
    COUNT(*) as sample_size
FROM ai_adjustments
WHERE json_extract(adjustments, '$.workers') IS NOT NULL
GROUP BY workers, chunk_size
HAVING sample_size >= 3
ORDER BY avg_throughput DESC;
```

### 3. New Data Structures

Extend `SmartConfigSuggestions` in `internal/driver/ai_smartconfig.go`:

```go
type SmartConfigSuggestions struct {
    // ... existing fields ...

    // Database tuning recommendations (NEW)
    SourceTuning *DatabaseTuning
    TargetTuning *DatabaseTuning

    // Historical insights (NEW)
    HistoricalProfile *HistoricalPerformanceProfile
}

type DatabaseTuning struct {
    DatabaseType string // "mysql", "postgresql", "mssql", "oracle"
    Role         string // "source" or "target"

    // Current configuration
    CurrentSettings map[string]interface{}

    // Recommendations
    Recommendations []TuningRecommendation

    // Summary
    TuningPotential string // "high", "medium", "low"
    EstimatedImpact string // e.g., "2-3x throughput improvement"
}

type TuningRecommendation struct {
    Parameter    string
    CurrentValue interface{}
    RecommendedValue interface{}
    Impact       string // "high", "medium", "low"
    Reason       string
    Priority     int    // 1=critical, 2=important, 3=optional

    // Application method
    CanApplyRuntime bool   // Can use SET GLOBAL
    SQLCommand      string // e.g., "SET GLOBAL innodb_buffer_pool_size = 4294967296;"
    RequiresRestart bool
}

type HistoricalPerformanceProfile struct {
    TotalRuns int
    TotalAdjustments int

    // Action effectiveness summary
    ActionStats map[string]ActionEffectiveness

    // Optimal parameter ranges discovered
    OptimalRanges map[string]ParameterRange

    // Recent patterns
    RecentPatterns []string // Human-readable insights
}

type ActionEffectiveness struct {
    Action       string
    Attempts     int
    Successful   int
    SuccessRate  float64
    AvgEffect    float64
    AvgThroughput float64
}

type ParameterRange struct {
    Parameter  string
    MinValue   int
    MaxValue   int
    OptimalValue int
    Confidence string // "high", "medium", "low"
    SampleSize int
}
```

### 4. Implementation Components

**4.1. Database Configuration Inspectors**

Create `internal/driver/dbtuning/` package:

```
internal/driver/dbtuning/
├── inspector.go         # DatabaseInspector interface
├── mysql.go            # MySQL configuration inspector
├── postgres.go         # PostgreSQL configuration inspector
├── mssql.go            # MSSQL configuration inspector
├── oracle.go           # Oracle configuration inspector
└── recommendations.go  # Recommendation generation logic
```

**DatabaseInspector interface:**
```go
type DatabaseInspector interface {
    // GetCurrentConfig retrieves current database configuration
    GetCurrentConfig(ctx context.Context, conn *sql.DB) (map[string]interface{}, error)

    // GenerateRecommendations analyzes config and returns tuning suggestions
    GenerateRecommendations(
        ctx context.Context,
        currentConfig map[string]interface{},
        role string, // "source" or "target"
        schemaStats SchemaStatistics,
    ) ([]TuningRecommendation, error)
}

type SchemaStatistics struct {
    TotalTables    int
    TotalRows      int64
    AvgRowSizeBytes int64
    EstimatedMemMB int64
    LargestTable   TableInfo
}
```

**4.2. Historical Performance Analyzer**

Create `internal/checkpoint/history.go`:

```go
// HistoricalAnalyzer queries checkpoint database for performance insights
type HistoricalAnalyzer struct {
    db *sql.DB
}

func (ha *HistoricalAnalyzer) GetPerformanceProfile(ctx context.Context) (*HistoricalPerformanceProfile, error) {
    // Query ai_adjustments table
    // Calculate action effectiveness
    // Identify optimal parameter ranges
    // Generate insights
}

func (ha *HistoricalAnalyzer) GetOptimalParameters(ctx context.Context) (map[string]ParameterRange, error) {
    // Query for parameter correlations with throughput
    // Return statistically significant ranges
}
```

**4.3. Enhanced Analyze Command**

Modify `internal/orchestrator/orchestrator.go`:

```go
func (o *Orchestrator) AnalyzeConfig(ctx context.Context, schema *models.Schema) (*driver.SmartConfigSuggestions, error) {
    // ... existing schema analysis ...

    // NEW: Database configuration analysis
    if o.cfg.Source != nil {
        sourceInspector := dbtuning.NewInspector(o.cfg.Source.Type)
        sourceConfig, err := sourceInspector.GetCurrentConfig(ctx, o.sourceDB)
        if err == nil {
            sourceTuning, _ := sourceInspector.GenerateRecommendations(
                ctx, sourceConfig, "source", schemaStats,
            )
            suggestions.SourceTuning = sourceTuning
        }
    }

    if o.cfg.Target != nil {
        targetInspector := dbtuning.NewInspector(o.cfg.Target.Type)
        targetConfig, err := targetInspector.GetCurrentConfig(ctx, o.targetDB)
        if err == nil {
            targetTuning, _ := targetInspector.GenerateRecommendations(
                ctx, targetConfig, "target", schemaStats,
            )
            suggestions.TargetTuning = targetTuning
        }
    }

    // NEW: Historical performance analysis
    if o.state != nil {
        histAnalyzer := checkpoint.NewHistoricalAnalyzer(o.state.DB())
        profile, err := histAnalyzer.GetPerformanceProfile(ctx)
        if err == nil {
            suggestions.HistoricalProfile = profile
        }
    }

    return suggestions, nil
}
```

### 5. Output Format

Enhance the YAML output from analyze command:

```yaml
# Existing migration config suggestions
migration:
  workers: 8
  chunk_size: 50000
  # ... etc ...

# NEW: Source database tuning recommendations
source_tuning:
  database: mysql
  role: source
  tuning_potential: high
  estimated_impact: "2-3x throughput improvement"

  recommendations:
    - parameter: read_buffer_size
      current: 131072 (128 KB)
      recommended: 8388608 (8 MB)
      impact: high
      priority: 1
      reason: "Sequential table scans benefit from larger read buffers"
      runtime_change: true
      sql: "SET GLOBAL read_buffer_size = 8388608;"

    - parameter: table_open_cache
      current: 2000
      recommended: 4000
      impact: medium
      priority: 2
      reason: "Schema has 150 tables, increase cache to avoid file handle churn"
      runtime_change: true
      sql: "SET GLOBAL table_open_cache = 4000;"

    - parameter: tmp_table_size
      current: 16777216 (16 MB)
      recommended: 268435456 (256 MB)
      impact: medium
      priority: 2
      reason: "Complex queries may benefit from larger temporary tables"
      runtime_change: true
      sql: "SET GLOBAL tmp_table_size = 268435456;"

# NEW: Target database tuning recommendations
target_tuning:
  database: mysql
  role: target
  tuning_potential: high
  estimated_impact: "1.5-2x throughput improvement"

  recommendations:
    - parameter: innodb_buffer_pool_size
      current: 2147483648 (2 GB)
      recommended: 8589934592 (8 GB)
      impact: high
      priority: 1
      reason: "System has 16 GB RAM, allocate 8 GB for buffer pool (50%)"
      runtime_change: false
      requires_restart: true
      config_file: |
        # Add to my.cnf [mysqld] section:
        innodb_buffer_pool_size = 8G

    - parameter: innodb_flush_log_at_trx_commit
      current: 1
      recommended: 2
      impact: high
      priority: 1
      reason: "Migration workload tolerates potential 1-second data loss for 2-3x write performance"
      runtime_change: true
      sql: "SET GLOBAL innodb_flush_log_at_trx_commit = 2;"

    - parameter: innodb_write_io_threads
      current: 4
      recommended: 16
      impact: medium
      priority: 2
      reason: "High concurrent writes benefit from more IO threads"
      runtime_change: false
      requires_restart: true
      config_file: |
        # Add to my.cnf [mysqld] section:
        innodb_write_io_threads = 16

# NEW: Historical performance insights
historical_insights:
  total_runs: 15
  total_adjustments: 87

  action_effectiveness:
    scale_up:
      attempts: 35
      successful: 28
      success_rate: 80%
      avg_effect: +12.4%
      avg_throughput: 145000 rows/sec

    reduce_chunk:
      attempts: 18
      successful: 14
      success_rate: 77.8%
      avg_effect: -8.2% memory, stable throughput
      avg_throughput: 132000 rows/sec

    scale_down:
      attempts: 12
      successful: 8
      success_rate: 66.7%
      avg_effect: -15.3% CPU
      avg_throughput: 98000 rows/sec

  optimal_parameters:
    workers:
      range: 4-8
      optimal: 6
      confidence: high
      sample_size: 25

    chunk_size:
      range: 15000-30000
      optimal: 25000
      confidence: medium
      sample_size: 18

  recent_patterns:
    - "Scale-up most effective when CPU < 60% (8 of 10 recent successes)"
    - "Chunk reduction helps when memory > 80% (5 of 7 recent successes)"
    - "Workers=6 achieved 165K rows/sec average in last 5 runs"
    - "Last successful run 2 hours ago: 106M rows in 12.8 min (138K rows/sec)"
```

### 6. User Workflow

**Phase 1 (Current Implementation)**: Recommendations only

```bash
# Analyze and show recommendations
$ dmt analyze --config config.yaml > tuning-recommendations.yaml

# User manually applies database tuning
$ docker exec mysql-target mysql -e "SET GLOBAL innodb_flush_log_at_trx_commit = 2;"
$ docker exec mysql-target mysql -e "SET GLOBAL innodb_buffer_pool_size = 8589934592;"

# Run migration with tuned databases
$ dmt run --config config.yaml
```

**Phase 2 (Future Enhancement)**: Optional auto-apply functionality could be added later if needed, but is NOT part of this initial implementation.

### 7. Safety Considerations

**Configuration validation:**
- Verify recommended values are safe for system resources
- Check for conflicting settings
- Warn about settings that require restart
- Provide rollback SQL commands

**Example:**
```yaml
recommendations:
  - parameter: innodb_buffer_pool_size
    current: 2147483648
    recommended: 8589934592
    warning: "Ensure system has 16 GB RAM available"
    rollback_sql: "SET GLOBAL innodb_buffer_pool_size = 2147483648;"
```

### 8. Testing Strategy

**Unit tests:**
- Database inspectors for each DB type
- Recommendation generation logic
- Historical data analysis
- YAML formatting

**Integration tests:**
- Connect to test databases
- Query configuration
- Validate SQL commands
- Test --apply functionality

**End-to-end tests:**
- Run analyze on test migrations
- Verify recommendations improve performance
- Test with different database configurations

### 9. Implementation Phases

**Phase 1: Core Infrastructure**
- Create `internal/driver/dbtuning` package
- Implement MySQL inspector and recommendations
- Add database tuning to SmartConfigSuggestions
- Update analyze command to include database tuning
- YAML output formatting

**Phase 2: Historical Integration**
- Create `internal/checkpoint/history.go`
- Implement historical performance analyzer
- Integrate with analyze command output
- Add historical insights to YAML output

**Phase 3: Multi-Database Support**
- Implement PostgreSQL inspector
- Implement MSSQL inspector
- Implement Oracle inspector
- Comprehensive testing across all database types

**Phase 4: Documentation and Examples**
- User guide for applying recommendations
- Example tuning scripts for each database type
- Best practices documentation
- Integration with existing analyze workflow

## Benefits

1. **Empirical guidance**: Recommendations based on actual database configuration, not generic advice
2. **Historical learning**: Leverage past migrations to optimize future ones
3. **Database-specific**: Tailored recommendations for MySQL, PostgreSQL, MSSQL, Oracle
4. **Actionable**: Provides exact SQL commands to apply settings
5. **Safe**: Distinguishes runtime vs restart-required changes
6. **Integrated**: Builds on existing analyze infrastructure

## Example Usage

```bash
# Analyze current configuration and get recommendations
$ dmt analyze --config config.yaml

Analyzing source database (MySQL)...
Analyzing target database (MySQL)...
Querying historical performance data...

Source Database Tuning (HIGH potential, 2-3x improvement):
  ✓ 3 runtime-changeable settings
  ⚠ 2 settings require restart

Target Database Tuning (HIGH potential, 1.5-2x improvement):
  ✓ 1 runtime-changeable setting
  ⚠ 2 settings require restart

Historical Insights:
  • 15 previous runs analyzed
  • Scale-up effective 80% of time (+12% avg throughput)
  • Optimal workers: 6 (high confidence, 25 samples)
  • Last run: 106M rows in 12.8 min (138K rows/sec)

Full recommendations written to: analysis-2026-01-16.yaml

To apply recommendations:
  1. Review: cat analysis-2026-01-16.yaml
  2. Apply runtime settings: see SQL commands in recommendations
  3. Update config files: for settings requiring restart
  4. Run migration: dmt run --config config.yaml
```

## Success Metrics

- Users run `analyze` before migrations and apply recommendations
- Measured throughput improvements match estimates (within 20%)
- Reduction in manual database tuning time
- Higher success rate of first-run migrations
- Historical data accumulates and improves recommendations over time
