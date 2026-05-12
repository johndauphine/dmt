package dbtuning

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// postgresSettings is the deterministic tuning catalog for PostgreSQL.
// Settings are ordered by typical impact on bulk-insert throughput so
// the report's most actionable items appear first.
var postgresSettings = []Setting{
	{Name: "shared_buffers", Evaluate: pgEvalSharedBuffers},
	{Name: "work_mem", Evaluate: pgEvalWorkMem},
	{Name: "maintenance_work_mem", Evaluate: pgEvalMaintenanceWorkMem},
	{Name: "wal_buffers", Evaluate: pgEvalWALBuffers},
	{Name: "max_wal_size", Evaluate: pgEvalMaxWALSize},
	{Name: "synchronous_commit", Evaluate: pgEvalSyncCommit},
	{Name: "checkpoint_completion_target", Evaluate: pgEvalCheckpointTarget},
	{Name: "wal_level", Evaluate: pgEvalWALLevel},
	{Name: "effective_cache_size", Evaluate: pgEvalEffectiveCacheSize},
	{Name: "max_parallel_workers_per_gather", Evaluate: pgEvalMaxParallelWorkers},
	{Name: "archive_mode", Evaluate: pgEvalArchiveMode},
}

func init() {
	Register("postgres", postgresSettings)
	Register("postgresql", postgresSettings)
	Register("pg", postgresSettings)
}

// pgSetting fetches a single setting from pg_settings, returning its
// value and the unit string (one of "", "B", "kB", "MB", "8kB", "s",
// "ms", etc). PG stores numeric settings as text in this table; the
// unit lets us reconstruct the byte/time count.
func pgSetting(ctx context.Context, db *sql.DB, name string) (value, unit string, err error) {
	err = db.QueryRowContext(ctx,
		"SELECT setting, COALESCE(unit, '') FROM pg_settings WHERE name = $1",
		name,
	).Scan(&value, &unit)
	return value, unit, err
}

// pgSettingBytes resolves a PG memory setting into bytes. PG quirk:
// shared_buffers is stored in 8kB blocks (unit="8kB"), most other
// memory settings are in kB.
func pgSettingBytes(ctx context.Context, db *sql.DB, name string) (int64, error) {
	rawVal, unit, err := pgSetting(ctx, db, name)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(rawVal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("setting %s: parse %q: %w", name, rawVal, err)
	}
	switch unit {
	case "", "B":
		return n, nil
	case "kB":
		return n * 1024, nil
	case "8kB":
		return n * 8 * 1024, nil
	case "MB":
		return n * 1024 * 1024, nil
	case "GB":
		return n * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("setting %s: unrecognized unit %q", name, unit)
	}
}

// pgEvalSharedBuffers — PG should hold roughly 25% of host RAM in its
// page cache, capped at 16GB on very large hosts (beyond that the
// kernel's page cache usually outperforms it).
func pgEvalSharedBuffers(ctx context.Context, db *sql.DB, role string, sys SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := pgSettingBytes(ctx, db, "shared_buffers")
	if err != nil {
		return nil, nil, err
	}
	currentMB := current / (1024 * 1024)

	if sys.HostMemoryMB <= 0 {
		return FormatMB(currentMB), nil, nil
	}

	recommendedMB := sys.HostMemoryMB / 4
	const maxRecommendedMB = 16 * 1024
	if recommendedMB > maxRecommendedMB {
		recommendedMB = maxRecommendedMB
	}

	// Only recommend if current is meaningfully below target (within
	// 10% is "fine"). Avoid noisy recommendations that nag about a few
	// percent.
	if currentMB*10 >= recommendedMB*9 {
		return FormatMB(currentMB), nil, nil
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "shared_buffers",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           fmt.Sprintf("shared_buffers is %dMB on a host with %dMB RAM; PostgreSQL recommends ~25%% of RAM (capped at 16GB) for the page cache.", currentMB, sys.HostMemoryMB),
		Priority:         1,
		RequiresRestart:  true,
		ConfigFile:       fmt.Sprintf("shared_buffers = %s", FormatMB(recommendedMB)),
		SQLCommand:       fmt.Sprintf("ALTER SYSTEM SET shared_buffers = '%s';", FormatMB(recommendedMB)),
	}), nil
}

// pgEvalWorkMem — per-operation working memory. Default 4MB is too
// small for sort/hash operations that happen during index build and
// constraint checks. 16-64MB is the typical recommendation.
func pgEvalWorkMem(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := pgSettingBytes(ctx, db, "work_mem")
	if err != nil {
		return nil, nil, err
	}
	currentMB := current / (1024 * 1024)

	const recommendedMB = 16
	if currentMB >= recommendedMB {
		return FormatMB(currentMB), nil, nil
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "work_mem",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "medium",
		Reason:           "work_mem is below 16MB; sort and hash operations during index/constraint creation will spill to disk under the default. ALTER ROLE persists the change for dmt's migration user across all sessions; substitute the role name dmt actually connects as.",
		Priority:         2,
		CanApplyRuntime:  true,
		SQLCommand:       fmt.Sprintf("ALTER ROLE <migration_user> SET work_mem = '%dMB';", recommendedMB),
	}), nil
}

// pgEvalMaintenanceWorkMem — memory for CREATE INDEX, VACUUM, etc.
// During TaskCreateIndexes this is the load-bearing parameter; bumping
// it from 64MB to 512MB or 1GB dramatically speeds up secondary-index
// builds on a large table.
//
// Target-only — the source's maintenance_work_mem isn't touched during
// migration (TaskCreateIndexes runs against target).
func pgEvalMaintenanceWorkMem(ctx context.Context, db *sql.DB, role string, sys SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := pgSettingBytes(ctx, db, "maintenance_work_mem")
	if err != nil {
		return nil, nil, err
	}
	currentMB := current / (1024 * 1024)

	if role != RoleTarget {
		return FormatMB(currentMB), nil, nil
	}

	// RAM-sized recommendation: only emit when we know the DB server's
	// RAM (Codex review on #172). For remote DBs HostMemoryMB is 0 and
	// any 512MB-default suggestion would be guesswork on a server we
	// haven't measured.
	if sys.HostMemoryMB <= 0 {
		return FormatMB(currentMB), nil, nil
	}

	// Aim for ~10% of host RAM, capped at 2GB. maintenance_work_mem is
	// per-connection during parallel index builds, so a high cap can
	// multiply across writers.
	recommendedMB := sys.HostMemoryMB / 10
	if recommendedMB > 2048 {
		recommendedMB = 2048
	}
	if recommendedMB < 256 {
		// Tiny hosts: don't try to "recommend" sub-256MB; the default
		// 64MB is fine on a host this small.
		return FormatMB(currentMB), nil, nil
	}

	if currentMB >= recommendedMB {
		return FormatMB(currentMB), nil, nil
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "maintenance_work_mem",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           "maintenance_work_mem governs CREATE INDEX speed; the default 64MB makes secondary-index creation on large tables disk-bound rather than memory-bound. ALTER ROLE persists the change for dmt's migration user across sessions.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       fmt.Sprintf("ALTER ROLE <migration_user> SET maintenance_work_mem = '%s';", FormatMB(recommendedMB)),
	}), nil
}

// pgEvalWALBuffers — buffer for WAL writes. -1 = auto (=1/32 of
// shared_buffers, capped at 16MB). Manual values smaller than 16MB
// hurt bulk-insert throughput. Target-only — source isn't write-heavy
// during migration.
func pgEvalWALBuffers(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	rawVal, _, err := pgSetting(ctx, db, "wal_buffers")
	if err != nil {
		return nil, nil, err
	}

	current, err := pgSettingBytes(ctx, db, "wal_buffers")
	if err != nil {
		return rawVal, nil, nil
	}
	currentMB := current / (1024 * 1024)

	if role != RoleTarget {
		return FormatMB(currentMB), nil, nil
	}

	// Auto-tuned (-1) is fine; PG picks min(16MB, 1/32 shared_buffers).
	if rawVal == "-1" || currentMB >= 16 {
		return FormatMB(currentMB), nil, nil
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "wal_buffers",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: "16MB",
		Impact:           "medium",
		Reason:           "wal_buffers below 16MB serializes WAL writes during bulk inserts; -1 (auto) or 16MB removes that bottleneck.",
		Priority:         2,
		RequiresRestart:  true,
		ConfigFile:       "wal_buffers = 16MB",
	}), nil
}

// pgEvalMaxWALSize — checkpoint trigger size. Small values force
// frequent checkpoints; large values reduce them but lengthen recovery.
// For migrations, a large value (4-8GB) is appropriate.
func pgEvalMaxWALSize(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := pgSettingBytes(ctx, db, "max_wal_size")
	if err != nil {
		return nil, nil, err
	}
	currentMB := current / (1024 * 1024)

	const recommendedMB = 4 * 1024
	if currentMB >= recommendedMB {
		return FormatMB(currentMB), nil, nil
	}

	// Only relevant on the target — source doesn't generate much WAL.
	priority := 1
	if role == RoleSource {
		priority = 3
	}

	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "max_wal_size",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           "max_wal_size below 4GB triggers frequent checkpoints during bulk inserts; each checkpoint flushes dirty buffers and can cause throughput cliffs.",
		Priority:         priority,
		CanApplyRuntime:  true,
		SQLCommand:       fmt.Sprintf("ALTER SYSTEM SET max_wal_size = '%s'; SELECT pg_reload_conf();", FormatMB(recommendedMB)),
	}), nil
}

// pgEvalSyncCommit — synchronous_commit=off lets the backend return
// before the WAL fsync completes, at the cost of up to ~1 transaction
// (typically ms-scale) of data loss on crash. Acceptable for the
// migration window; the data can always be re-loaded from source.
func pgEvalSyncCommit(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, _, err := pgSetting(ctx, db, "synchronous_commit")
	if err != nil {
		return nil, nil, err
	}

	// Source: leave alone (don't recommend weakening durability).
	// Target: recommend off during the migration.
	if role != RoleTarget {
		return val, nil, nil
	}
	if val == "off" || val == "local" {
		return val, nil, nil
	}

	return val, Recommend(RecOpts{
		Parameter:        "synchronous_commit",
		CurrentValue:     val,
		RecommendedValue: "off",
		Impact:           "high",
		Reason:           "synchronous_commit=off skips per-transaction fsync; under bulk inserts this commonly doubles throughput. The risk is losing the most recent ~1s of data on crash — acceptable here because the source has the canonical copy. Use ALTER SYSTEM so dmt's worker connections inherit the change; a plain SET would only affect the one psql session.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER SYSTEM SET synchronous_commit = off; SELECT pg_reload_conf();",
	}), nil
}

// pgEvalCheckpointTarget — 0.9 spreads the checkpoint I/O over more
// of the interval, smoothing throughput. PG 14+ defaults to 0.9
// already; older versions default to 0.5. Target-only — checkpoint
// behavior on the source doesn't affect migration throughput.
func pgEvalCheckpointTarget(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, _, err := pgSetting(ctx, db, "checkpoint_completion_target")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget {
		return val, nil, nil
	}
	n, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return val, nil, nil
	}
	if n >= 0.9 {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "checkpoint_completion_target",
		CurrentValue:     val,
		RecommendedValue: "0.9",
		Impact:           "medium",
		Reason:           "checkpoint_completion_target<0.9 bunches checkpoint I/O at the start of each interval, causing throughput stutter; 0.9 spreads it evenly.",
		Priority:         2,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER SYSTEM SET checkpoint_completion_target = 0.9; SELECT pg_reload_conf();",
	}), nil
}

// pgEvalWALLevel — minimal allows bulk-insert WAL bypass (when
// wal_level=minimal AND archive_mode=off AND max_wal_senders=0) but
// disables physical replication. Recommend minimal only for targets
// that don't need replication.
func pgEvalWALLevel(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, _, err := pgSetting(ctx, db, "wal_level")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget || val == "minimal" {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "wal_level",
		CurrentValue:     val,
		RecommendedValue: "minimal",
		Impact:           "medium",
		Reason:           "wal_level=minimal lets PostgreSQL skip WAL for some bulk-insert paths (COPY into a newly-created or truncated table in the same transaction), reducing WAL volume during the migration. Only safe if this target is not a replication source.",
		Priority:         3,
		RequiresRestart:  true,
		ConfigFile:       "wal_level = minimal\nmax_wal_senders = 0\narchive_mode = off",
	}), nil
}

// pgEvalEffectiveCacheSize — informs the planner; doesn't reserve
// memory. Should be ~75% of RAM. Misleadingly-low values cause the
// planner to pick worse plans for large queries.
func pgEvalEffectiveCacheSize(ctx context.Context, db *sql.DB, role string, sys SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	current, err := pgSettingBytes(ctx, db, "effective_cache_size")
	if err != nil {
		return nil, nil, err
	}
	currentMB := current / (1024 * 1024)
	if sys.HostMemoryMB <= 0 {
		return FormatMB(currentMB), nil, nil
	}
	recommendedMB := sys.HostMemoryMB * 3 / 4

	// Default is 4GB (4194304 / 1024 = 4096 MB); recommend only if
	// significantly below 75% of RAM.
	if currentMB*4 >= recommendedMB*3 {
		return FormatMB(currentMB), nil, nil
	}
	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "effective_cache_size",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "low",
		Reason:           "effective_cache_size is a planner hint; setting it close to total RAM (~75%) makes the planner correctly prefer index scans for queries the kernel page cache can serve.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       fmt.Sprintf("ALTER SYSTEM SET effective_cache_size = '%s'; SELECT pg_reload_conf();", FormatMB(recommendedMB)),
	}), nil
}

// pgEvalMaxParallelWorkers — for bulk inserts, intra-query parallelism
// doesn't help and can hurt (each worker contends for locks). dmt
// parallelizes at the application level; the DB doesn't need to.
func pgEvalMaxParallelWorkers(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, _, err := pgSetting(ctx, db, "max_parallel_workers_per_gather")
	if err != nil {
		return nil, nil, err
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return val, nil, nil
	}
	// Default is 2; anything 0-4 is fine. Only flag pathological values.
	if n <= 4 {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "max_parallel_workers_per_gather",
		CurrentValue:     val,
		RecommendedValue: "2",
		Impact:           "low",
		Reason:           "Intra-query parallel workers don't speed up bulk INSERT and can serialize on shared locks; dmt parallelizes at the application level via write_ahead_writers. ALTER ROLE persists the change for dmt's migration user without affecting other workloads.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "ALTER ROLE <migration_user> SET max_parallel_workers_per_gather = 2;",
	}), nil
}

// pgEvalArchiveMode — WAL archiving writes every WAL segment to a
// secondary location. Heavy I/O cost during bulk-insert workloads.
// If on for a migration target, disabling for the migration window
// often doubles throughput; re-enable after cutover.
func pgEvalArchiveMode(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, _, err := pgSetting(ctx, db, "archive_mode")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget || val == "off" {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "archive_mode",
		CurrentValue:     val,
		RecommendedValue: "off",
		Impact:           "high",
		Reason:           "WAL archiving doubles the disk-write cost of every bulk insert. Disabling for the migration window (and re-enabling after cutover) commonly halves migration time on archive-enabled targets.",
		Priority:         1,
		RequiresRestart:  true,
		ConfigFile:       "archive_mode = off",
	}), nil
}
