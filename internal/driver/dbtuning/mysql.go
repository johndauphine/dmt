package dbtuning

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

// mysqlSettings is the deterministic tuning catalog for MySQL/MariaDB.
// Values come from SHOW VARIABLES; numeric settings parse as bytes
// (most innodb_* settings are bytes-with-no-suffix in modern MySQL).
var mysqlSettings = []Setting{
	{Name: "innodb_buffer_pool_size", Evaluate: mysqlEvalBufferPool},
	{Name: "innodb_log_file_size", Evaluate: mysqlEvalLogFileSize},
	{Name: "innodb_log_buffer_size", Evaluate: mysqlEvalLogBufferSize},
	{Name: "innodb_flush_log_at_trx_commit", Evaluate: mysqlEvalFlushLogAtCommit},
	{Name: "sync_binlog", Evaluate: mysqlEvalSyncBinlog},
	{Name: "innodb_doublewrite", Evaluate: mysqlEvalDoublewrite},
	{Name: "innodb_flush_method", Evaluate: mysqlEvalFlushMethod},
	{Name: "max_allowed_packet", Evaluate: mysqlEvalMaxAllowedPacket},
	{Name: "innodb_buffer_pool_instances", Evaluate: mysqlEvalBufferPoolInstances},
	{Name: "innodb_io_capacity", Evaluate: mysqlEvalIOCapacity},
}

func init() {
	Register("mysql", mysqlSettings)
	Register("mariadb", mysqlSettings)
	Register("maria", mysqlSettings)
}

// mysqlEvalBufferPool — primary cache for InnoDB data pages.
// Recommend 50-70% of RAM. The single highest-impact MySQL tuning knob.
func mysqlEvalBufferPool(ctx context.Context, db *sql.DB, _ string, sys SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_buffer_pool_size")
	if err != nil {
		return nil, nil, err
	}
	currentBytes, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return val, nil, nil
	}
	currentMB := currentBytes / (1024 * 1024)
	if sys.HostMemoryMB <= 0 {
		return FormatMB(currentMB), nil, nil
	}
	recommendedMB := sys.HostMemoryMB * 60 / 100
	// Round to nearest 1GB so the recommendation looks clean.
	if recommendedMB > 1024 {
		recommendedMB = (recommendedMB / 1024) * 1024
	}
	// Skip recommendation when current is already within 10%.
	if currentMB*10 >= recommendedMB*9 {
		return FormatMB(currentMB), nil, nil
	}
	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "innodb_buffer_pool_size",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           fmt.Sprintf("innodb_buffer_pool_size is %dMB on a %dMB host; for a dedicated MySQL server 60%% of RAM is the standard sizing.", currentMB, sys.HostMemoryMB),
		Priority:         1,
		RequiresRestart:  true,
		ConfigFile:       fmt.Sprintf("[mysqld]\ninnodb_buffer_pool_size=%dM", recommendedMB),
		SQLCommand:       fmt.Sprintf("SET GLOBAL innodb_buffer_pool_size = %d;  -- MySQL 5.7.5+ supports online resize", recommendedMB*1024*1024),
	}), nil
}

// mysqlEvalLogFileSize — InnoDB redo log. Larger = fewer checkpoints =
// better bulk-insert throughput. Recommend 25% of buffer pool, capped
// at 2GB (MySQL's redo log file ceiling). Target-only — source isn't
// doing bulk writes during migration.
func mysqlEvalLogFileSize(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_log_file_size")
	if err != nil {
		return nil, nil, err
	}
	currentBytes, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return val, nil, nil
	}
	currentMB := currentBytes / (1024 * 1024)
	if role != RoleTarget {
		return FormatMB(currentMB), nil, nil
	}
	const recommendedMB = 2048 // 2GB — typical max
	if currentMB >= recommendedMB {
		return FormatMB(currentMB), nil, nil
	}
	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "innodb_log_file_size",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: FormatMB(recommendedMB),
		Impact:           "high",
		Reason:           "innodb_log_file_size below 2GB forces frequent log-flush checkpoints during bulk inserts; larger logs let writes batch.",
		Priority:         1,
		RequiresRestart:  true,
		ConfigFile:       "[mysqld]\ninnodb_log_file_size=2G\ninnodb_log_files_in_group=2",
	}), nil
}

// mysqlEvalLogBufferSize — buffer holding pending log writes.
// Recommend 16-64MB for bulk-load workloads. Target-only — source
// isn't write-heavy during migration.
func mysqlEvalLogBufferSize(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_log_buffer_size")
	if err != nil {
		return nil, nil, err
	}
	currentBytes, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return val, nil, nil
	}
	currentMB := currentBytes / (1024 * 1024)
	if role != RoleTarget {
		return FormatMB(currentMB), nil, nil
	}
	if currentMB >= 16 {
		return FormatMB(currentMB), nil, nil
	}
	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "innodb_log_buffer_size",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: "32MB",
		Impact:           "medium",
		Reason:           "innodb_log_buffer_size<16MB forces flushes on each large transaction; 32MB lets bulk inserts batch their log writes. This is a startup-only InnoDB variable — SET GLOBAL doesn't apply; edit my.cnf and restart MySQL (Codex review on #172).",
		Priority:         2,
		RequiresRestart:  true,
		ConfigFile:       "[mysqld]\ninnodb_log_buffer_size=32M",
	}), nil
}

// mysqlEvalFlushLogAtCommit — durability vs throughput knob.
// 1 = fsync on every commit (ACID, slowest).
// 2 = fsync every second (loses ~1s on crash, much faster).
// 0 = no fsync (loses up to log_buffer on crash, fastest).
//
// For a migration target where the source has the canonical data, 0
// or 2 is appropriate. Re-set to 1 after cutover for production.
func mysqlEvalFlushLogAtCommit(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_flush_log_at_trx_commit")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget {
		return val, nil, nil
	}
	if val == "0" || val == "2" {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "innodb_flush_log_at_trx_commit",
		CurrentValue:     val,
		RecommendedValue: "2",
		Impact:           "high",
		Reason:           "innodb_flush_log_at_trx_commit=1 fsyncs every commit and is the dominant bulk-insert bottleneck. Setting to 2 risks losing ~1s of data on crash — acceptable since the source has the canonical copy. Reset to 1 after cutover for production durability.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "SET GLOBAL innodb_flush_log_at_trx_commit = 2;",
	}), nil
}

// mysqlEvalSyncBinlog — binary log fsync frequency. 1 = every commit
// (durable); 0 = none (let OS schedule). For migration targets where
// binlog is purely incidental (not used for replication during
// migration), 0 removes a major fsync source.
func mysqlEvalSyncBinlog(ctx context.Context, db *sql.DB, role string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "sync_binlog")
	if err != nil {
		return nil, nil, err
	}
	if role != RoleTarget || val == "0" {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "sync_binlog",
		CurrentValue:     val,
		RecommendedValue: "0",
		Impact:           "high",
		Reason:           "sync_binlog=1 fsyncs the binary log on every commit. For a migration target not currently feeding a replica, 0 removes that fsync cost (the binlog still exists for post-cutover restoration if needed). Reset to 1 after cutover when binlog durability matters for replication.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "SET GLOBAL sync_binlog = 0;",
	}), nil
}

// mysqlEvalDoublewrite — extra write step that protects against
// torn pages. Default ON. We don't recommend turning off (the safety
// loss outweighs the throughput gain for most operators), just report
// the current state.
func mysqlEvalDoublewrite(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_doublewrite")
	if err != nil {
		return nil, nil, err
	}
	// No recommendation — informational only. Operators who want the
	// extra throughput can disable it themselves; we won't suggest a
	// data-loss-on-power-failure change automatically.
	return val, nil, nil
}

// mysqlEvalFlushMethod — O_DIRECT bypasses the OS page cache, avoiding
// double-buffering with InnoDB's buffer pool. Linux standard.
func mysqlEvalFlushMethod(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_flush_method")
	if err != nil {
		return nil, nil, err
	}
	// Default is fsync on Linux; O_DIRECT is the standard recommendation.
	if val == "O_DIRECT" || val == "O_DIRECT_NO_FSYNC" {
		return val, nil, nil
	}
	return val, Recommend(RecOpts{
		Parameter:        "innodb_flush_method",
		CurrentValue:     val,
		RecommendedValue: "O_DIRECT",
		Impact:           "medium",
		Reason:           "Default fsync double-buffers between InnoDB's buffer pool and the OS page cache. O_DIRECT eliminates that, freeing RAM for the buffer pool to actually use.",
		Priority:         2,
		RequiresRestart:  true,
		ConfigFile:       "[mysqld]\ninnodb_flush_method=O_DIRECT",
	}), nil
}

// mysqlEvalMaxAllowedPacket — single-statement size cap. dmt's bulk
// inserts hit this when chunks × row_size exceeds the cap. Default
// 64MB in modern MySQL is fine for most workloads; flag if low.
func mysqlEvalMaxAllowedPacket(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "max_allowed_packet")
	if err != nil {
		return nil, nil, err
	}
	currentBytes, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return val, nil, nil
	}
	currentMB := currentBytes / (1024 * 1024)
	if currentMB >= 64 {
		return FormatMB(currentMB), nil, nil
	}
	return FormatMB(currentMB), Recommend(RecOpts{
		Parameter:        "max_allowed_packet",
		CurrentValue:     FormatMB(currentMB),
		RecommendedValue: "64MB",
		Impact:           "high",
		Reason:           "max_allowed_packet below 64MB causes 'Got a packet bigger than max_allowed_packet bytes' errors on large bulk-insert chunks; dmt's chunk size has to drop to fit.",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "SET GLOBAL max_allowed_packet = 67108864;  -- 64MB",
	}), nil
}

// mysqlEvalBufferPoolInstances — splits the buffer pool into N
// independent regions to reduce mutex contention. Recommended 8 for
// buffer pools >= 8GB; default is 1 (sometimes 8 on newer MySQL).
func mysqlEvalBufferPoolInstances(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	bpSizeStr, err := ScanShowVariable(ctx, db, "innodb_buffer_pool_size")
	if err != nil {
		return nil, nil, err
	}
	bpBytes, err := strconv.ParseInt(bpSizeStr, 10, 64)
	if err != nil {
		return nil, nil, nil
	}
	bpMB := bpBytes / (1024 * 1024)
	// Only relevant for large buffer pools.
	if bpMB < 8192 {
		val, _ := ScanShowVariable(ctx, db, "innodb_buffer_pool_instances")
		return val, nil, nil
	}

	val, err := ScanShowVariable(ctx, db, "innodb_buffer_pool_instances")
	if err != nil {
		return nil, nil, err
	}
	current, _ := strconv.Atoi(val)
	if current >= 8 {
		return current, nil, nil
	}
	return current, Recommend(RecOpts{
		Parameter:        "innodb_buffer_pool_instances",
		CurrentValue:     current,
		RecommendedValue: 8,
		Impact:           "medium",
		Reason:           "On large buffer pools (>=8GB) a single instance becomes a mutex bottleneck for parallel writers; 8 instances spreads the contention.",
		Priority:         2,
		RequiresRestart:  true,
		ConfigFile:       "[mysqld]\ninnodb_buffer_pool_instances=8",
	}), nil
}

// mysqlEvalIOCapacity — soft limit on background I/O ops per second.
// Default 200 is calibrated for spinning disk. SSD wants 2000+;
// NVMe wants 10000+. We can't detect storage type from SQL, so we
// recommend a modest SSD-class default and let operators adjust.
func mysqlEvalIOCapacity(ctx context.Context, db *sql.DB, _ string, _ SystemInfo, _ SchemaStatistics) (interface{}, *TuningRecommendation, error) {
	val, err := ScanShowVariable(ctx, db, "innodb_io_capacity")
	if err != nil {
		return nil, nil, err
	}
	current, _ := strconv.Atoi(val)
	if current >= 2000 {
		return current, nil, nil
	}
	return current, Recommend(RecOpts{
		Parameter:        "innodb_io_capacity",
		CurrentValue:     current,
		RecommendedValue: 2000,
		Impact:           "medium",
		Reason:           "Default innodb_io_capacity=200 is calibrated for spinning disk; SSDs can sustain 5000-10000+ IOPS. Set to 2000 as a safe modern baseline; raise further if you've measured the disk's actual sustained random-write IOPS.",
		Priority:         3,
		CanApplyRuntime:  true,
		SQLCommand:       "SET GLOBAL innodb_io_capacity = 2000;",
	}), nil
}
