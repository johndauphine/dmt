package dbtuning

import (
	"context"
	"database/sql"
	"fmt"
)

// mssqlSettings is the deterministic tuning catalog for SQL Server.
// Server-level settings are read from sys.configurations
// (value_in_use); database-level settings from sys.databases for
// DB_NAME().
var mssqlSettings = []Setting{
	{Name: "max_server_memory_mb", Evaluate: mssqlEvalMaxServerMemory},
	{Name: "max_degree_of_parallelism", Evaluate: mssqlEvalMaxDOP},
	{Name: "cost_threshold_for_parallelism", Evaluate: mssqlEvalCostThreshold},
	{Name: "optimize_for_ad_hoc_workloads", Evaluate: mssqlEvalOptimizeAdHoc},
	{Name: "recovery_model", Evaluate: mssqlEvalRecoveryModel},
	{Name: "auto_close", Evaluate: mssqlEvalAutoClose},
	{Name: "auto_create_statistics", Evaluate: mssqlEvalAutoCreateStats},
	{Name: "auto_update_statistics", Evaluate: mssqlEvalAutoUpdateStats},
	{Name: "target_recovery_time_sec", Evaluate: mssqlEvalTargetRecoveryTime},
}

func init() {
	Register("mssql", mssqlSettings)
	Register("sqlserver", mssqlSettings)
	Register("sql-server", mssqlSettings)
}

// mssqlConfig reads value_in_use from sys.configurations for a server
// configuration option name. Returns the integer setting.
//
// Uses sql.Named because github.com/microsoft/go-mssqldb addresses
// positional parameters via `@name`/`@p1`, not `?` like MySQL/SQLite.
// Codex review on #172 caught an earlier `?` usage that silently
// skipped four MSSQL settings.
func mssqlConfig(ctx context.Context, db *sql.DB, name string) (int64, error) {
	var v int64
	err := db.QueryRowContext(ctx,
		"SELECT CAST(value_in_use AS bigint) FROM sys.configurations WHERE name = @name",
		sql.Named("name", name),
	).Scan(&v)
	return v, err
}

// mssqlEvalMaxServerMemory — default 2147483647 (essentially unlimited).
// On most servers, capping max server memory below total RAM gives the
// OS room for filesystem cache and other processes. Recommend
// db_host_RAM - 4GB (with a 4GB floor).
//
// Self-corrects for the remote-DB case: queries sys.dm_os_sys_memory
// for the actual database server's RAM (MSSQL exposes this; PG and
// MySQL don't). Falls back to the caller-provided sys.HostMemoryMB
// when the DMV is unavailable (older versions, permission denied).
func mssqlEvalMaxServerMemory(ctx context.Context, db *sql.DB, role string, sys SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	currentMB, err := mssqlConfig(ctx, db, "max server memory (MB)")
	if err != nil {
		return nil, nil, err
	}

	dbServerMB := mssqlPhysicalMemoryMB(ctx, db)
	if dbServerMB == 0 {
		dbServerMB = sys.HostMemoryMB
	}
	if dbServerMB <= 0 {
		// Neither DMV nor caller hint usable — show the current value
		// but don't recommend, since we can't size the cap honestly.
		return FormatMB(currentMB), nil, nil
	}

	// PG-style: leave ~4GB for OS on small hosts, more on big ones.
	reservedMB := int64(4096)
	if dbServerMB > 32768 {
		reservedMB = dbServerMB / 8
	}
	recommendedMB := dbServerMB - reservedMB
	if recommendedMB < 4096 {
		recommendedMB = 4096
	}

	// If current is already at-or-below the computed cap, no
	// recommendation. Comparing against dbServerMB (as an earlier
	// revision did) is too loose — a 30GB cap on a 32GB server still
	// leaves only 2GB for the OS, well below the 4GB+ reserve this
	// rule targets (Codex review on #172).
	if currentMB <= recommendedMB {
		return FormatMB(currentMB), nil, nil
	}

	// "Unlimited" only applies to the default sentinel (INT_MAX MB).
	// A user who set max server memory to a finite-but-too-high value
	// should see their actual value, not the misleading "unlimited".
	displayCurrent := FormatMB(currentMB)
	const mssqlUnlimitedSentinel = 2147483647 // INT_MAX MB
	if currentMB == mssqlUnlimitedSentinel {
		displayCurrent = "unlimited (default)"
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "max server memory (MB)",
		CurrentValue:     displayCurrent,
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           fmt.Sprintf("Default max server memory leaves no headroom for the OS or other processes. Cap at ~%dMB on this %dMB DB server to prevent SQL Server from squeezing out filesystem cache and worker threads.", recommendedMB, dbServerMB),
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       fmt.Sprintf("EXEC sp_configure 'max server memory (MB)', %d; RECONFIGURE;", recommendedMB),
	}), nil
}

// mssqlPhysicalMemoryMB queries the DB server's actual physical RAM
// via sys.dm_os_sys_memory. Returns 0 if the DMV is unavailable
// (very old versions) or the query errors for any reason. This is
// the load-bearing self-correction for the remote-DB case: dmt's
// host RAM is irrelevant to "how much memory does SQL Server have."
func mssqlPhysicalMemoryMB(ctx context.Context, db *sql.DB) int64 {
	var mb int64
	err := db.QueryRowContext(ctx,
		"SELECT total_physical_memory_kb / 1024 FROM sys.dm_os_sys_memory",
	).Scan(&mb)
	if err != nil {
		return 0
	}
	return mb
}

// mssqlEvalMaxDOP — for bulk-insert workloads, intra-query parallelism
// adds contention without throughput. Microsoft's published guidance
// is MAXDOP <= 8 for most workloads. dmt's pattern is many concurrent
// chunks of separate INSERTs; a lower MAXDOP (1-2) keeps each chunk
// from spawning extra workers.
//
// Target-only — sources run large SELECT scans that legitimately
// benefit from parallelism (Codex review on #172).
func mssqlEvalMaxDOP(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := mssqlConfig(ctx, db, "max degree of parallelism")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget {
		return current, nil, nil
	}
	// 0 = unlimited (let SQL Server pick). 1-8 are reasonable.
	// >8 typically only helps OLAP, not bulk insert.
	if current >= 1 && current <= 8 {
		return current, nil, nil
	}
	return current, Recommend(RecOpts{
		Parameter:        "max degree of parallelism",
		CurrentValue:     current,
		RecommendedValue: 2,
		Impact:           "medium",
		Reason:           "MAXDOP 0 (unlimited) or very high values cause bulk INSERTs to spawn parallel workers that contend on the same TABLOCK; MAXDOP=2 produces the best throughput in dmt's measurements.",
		Priority:         2,
		CanApplyRuntime:  true,
		SQLCommand:       "EXEC sp_configure 'max degree of parallelism', 2; RECONFIGURE;",
	}), nil
}

// mssqlEvalCostThreshold — minimum estimated query cost before
// parallelism is considered. The default of 5 is widely considered
// too low for modern hardware; 25-50 is the typical recommendation.
func mssqlEvalCostThreshold(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := mssqlConfig(ctx, db, "cost threshold for parallelism")
	if err != nil {
		return nil, nil, err
	}
	if current >= 25 {
		return current, nil, nil
	}
	return current, Recommend(RecOpts{
		Parameter:        "cost threshold for parallelism",
		CurrentValue:     current,
		RecommendedValue: 50,
		Impact:           "low",
		Reason:           "The default 5 is from SQL Server 6.5-era hardware. Modern guidance is 25-50; below that the optimizer parallelizes trivial queries and burns more CPU than it saves.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "EXEC sp_configure 'cost threshold for parallelism', 50; RECONFIGURE;",
	}), nil
}

// mssqlEvalOptimizeAdHoc — caches a one-time-use plan stub instead
// of a full plan. Reduces plan cache bloat on workloads with many
// distinct ad-hoc queries. Default OFF; ON is broadly safe.
func mssqlEvalOptimizeAdHoc(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := mssqlConfig(ctx, db, "optimize for ad hoc workloads")
	if err != nil {
		return nil, nil, err
	}
	if current == 1 {
		return "ON", nil, nil
	}
	return "OFF", Recommend(RecOpts{
		Parameter:        "optimize for ad hoc workloads",
		CurrentValue:     "OFF",
		RecommendedValue: "ON",
		Impact:           "low",
		Reason:           "ON caches a plan stub for one-shot queries instead of the full plan, reducing plan cache pressure from migration's many-distinct-INSERT workload.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "EXEC sp_configure 'optimize for ad hoc workloads', 1; RECONFIGURE;",
	}), nil
}

// mssqlEvalRecoveryModel — for migration targets, SIMPLE recovery
// truncates the transaction log on each checkpoint. Bulk inserts under
// SIMPLE are minimally logged; under FULL every row is logged.
// Switching FULL→SIMPLE typically multiplies bulk-insert throughput by
// 3-5x. Re-enable FULL after cutover for proper point-in-time recovery.
func mssqlEvalRecoveryModel(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	var model string
	err := db.QueryRowContext(ctx,
		"SELECT recovery_model_desc FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&model)
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget || model == "SIMPLE" || model == "BULK_LOGGED" {
		return model, nil, nil
	}
	return model, Recommend(RecOpts{
		Parameter:        "recovery_model",
		CurrentValue:     model,
		RecommendedValue: "SIMPLE",
		Impact:           "high",
		Reason:           "Under FULL recovery, every bulk-insert row writes a log record; SIMPLE recovery uses minimal logging for INSERT BULK and TABLOCK. Switching for the migration window typically improves throughput 3-5x. Restore FULL after cutover if point-in-time recovery is required.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER DATABASE CURRENT SET RECOVERY SIMPLE;",
	}), nil
}

// mssqlEvalAutoClose — closes the DB after the last connection
// disconnects. Used to be useful for embedded SQL Server; on a server
// it just adds reconnect overhead. Always recommend OFF.
func mssqlEvalAutoClose(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	var isOn bool
	err := db.QueryRowContext(ctx,
		"SELECT is_auto_close_on FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&isOn)
	if err != nil {
		return nil, nil, err
	}
	if !isOn {
		return "OFF", nil, nil
	}
	return "ON", Recommend(RecOpts{
		Parameter:        "auto_close",
		CurrentValue:     "ON",
		RecommendedValue: "OFF",
		Impact:           "medium",
		Reason:           "AUTO_CLOSE shuts the database down between connections and pays the open cost on every reconnect; never appropriate for a server workload.",
		Priority:         2,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER DATABASE CURRENT SET AUTO_CLOSE OFF;",
	}), nil
}

// mssqlEvalAutoCreateStats — ON allows the optimizer to create column
// statistics as needed; required for good plans on tables without
// explicit indexes. Default ON; flag only if explicitly disabled.
func mssqlEvalAutoCreateStats(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	var isOn bool
	err := db.QueryRowContext(ctx,
		"SELECT is_auto_create_stats_on FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&isOn)
	if err != nil {
		return nil, nil, err
	}
	if isOn {
		return "ON", nil, nil
	}
	return "OFF", Recommend(RecOpts{
		Parameter:        "auto_create_statistics",
		CurrentValue:     "OFF",
		RecommendedValue: "ON",
		Impact:           "medium",
		Reason:           "Without auto-created column statistics the optimizer picks bad plans for tables that lack explicit covering indexes; this affects validation queries and any post-migration application traffic.",
		Priority:         2,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER DATABASE CURRENT SET AUTO_CREATE_STATISTICS ON;",
	}), nil
}

// mssqlEvalAutoUpdateStats — ON lets the optimizer refresh stale
// stats based on row-count change. Default ON.
func mssqlEvalAutoUpdateStats(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	var isOn bool
	err := db.QueryRowContext(ctx,
		"SELECT is_auto_update_stats_on FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&isOn)
	if err != nil {
		return nil, nil, err
	}
	if isOn {
		return "ON", nil, nil
	}
	return "OFF", Recommend(RecOpts{
		Parameter:        "auto_update_statistics",
		CurrentValue:     "OFF",
		RecommendedValue: "ON",
		Impact:           "low",
		Reason:           "Without auto-updated statistics, plans grow stale as data volume changes; for the migration itself this matters less, but downstream application traffic will degrade.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS ON;",
	}), nil
}

// mssqlEvalTargetRecoveryTime — controls how long indirect checkpoints
// take. Default 60s on SQL 2016+ (was 0 = automatic prior); 60s is
// almost always the right answer. Flag pathological values only.
func mssqlEvalTargetRecoveryTime(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	var sec int
	err := db.QueryRowContext(ctx,
		"SELECT target_recovery_time_in_seconds FROM sys.databases WHERE name = DB_NAME()",
	).Scan(&sec)
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget {
		return sec, nil, nil
	}
	if sec >= 60 && sec <= 300 {
		return sec, nil, nil
	}
	// 0 means automatic checkpoint (older default) — for migration
	// targets, indirect checkpoint at 60s smooths I/O.
	return sec, Recommend(RecOpts{
		Parameter:        "target_recovery_time_in_seconds",
		CurrentValue:     sec,
		RecommendedValue: 60,
		Impact:           "low",
		Reason:           "target_recovery_time=60 uses indirect checkpoint to spread dirty-buffer flushing evenly across the interval, smoothing the throughput stutter automatic checkpoint introduces.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER DATABASE CURRENT SET TARGET_RECOVERY_TIME = 60 SECONDS;",
	}), nil
}
