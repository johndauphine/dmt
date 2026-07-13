package checkpoint

import (
	"database/sql"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

// SaveRuntimeAdjustment saves an AI adjustment record for historical analysis.
func (s *State) SaveRuntimeAdjustment(runID string, record RuntimeAdjustmentRecord) error {
	effectMeasured := 0
	var throughputAfter, effectPercent, cpuAfter, memoryAfter any
	if record.EffectMeasured {
		effectMeasured = 1
		throughputAfter = record.ThroughputAfter
		effectPercent = record.EffectPercent
		cpuAfter = record.CPUAfter
		memoryAfter = record.MemoryAfter
	}

	_, err := s.db.Exec(`
		INSERT INTO ai_adjustments (
			run_id, adjustment_number, timestamp, action, adjustments,
			throughput_before, effect_measured, throughput_after, effect_percent,
			cpu_before, cpu_after, memory_before, memory_after,
			reasoning, confidence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id, adjustment_number) DO UPDATE SET
			effect_measured = excluded.effect_measured,
			throughput_after = excluded.throughput_after,
			effect_percent = excluded.effect_percent,
			cpu_after = excluded.cpu_after,
			memory_after = excluded.memory_after
	`, runID, record.AdjustmentNumber, record.Timestamp.Format("2006-01-02 15:04:05"),
		record.Action, record.AdjustmentsJSON(),
		record.ThroughputBefore, effectMeasured, throughputAfter, effectPercent,
		record.CPUBefore, cpuAfter, record.MemoryBefore, memoryAfter,
		record.Reasoning, record.Confidence)
	return err
}

// queryWithOptionalLimit executes a query, appending " LIMIT ?" only when limit > 0.
func (s *State) queryWithOptionalLimit(query string, limit int, args ...any) (*sql.Rows, error) {
	if limit > 0 {
		return s.db.Query(query+" LIMIT ?", append(args, limit)...)
	}
	return s.db.Query(query, args...)
}

// GetRuntimeAdjustments returns the most recent AI adjustment records across all runs.
func (s *State) GetRuntimeAdjustments(limit int) ([]RuntimeAdjustmentRecord, error) {
	rows, err := s.queryWithOptionalLimit(`
		SELECT id, run_id, adjustment_number, timestamp, action, adjustments,
		       throughput_before, effect_measured, throughput_after, effect_percent,
		       cpu_before, cpu_after, memory_before, memory_after,
		       reasoning, confidence
		FROM ai_adjustments
		ORDER BY timestamp DESC`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.scanAIAdjustments(rows)
}

// GetRuntimeAdjustmentsByAction returns AI adjustment records filtered by action type.
func (s *State) GetRuntimeAdjustmentsByAction(action string, limit int) ([]RuntimeAdjustmentRecord, error) {
	rows, err := s.queryWithOptionalLimit(`
		SELECT id, run_id, adjustment_number, timestamp, action, adjustments,
		       throughput_before, effect_measured, throughput_after, effect_percent,
		       cpu_before, cpu_after, memory_before, memory_after,
		       reasoning, confidence
		FROM ai_adjustments
		WHERE action = ?
		ORDER BY timestamp DESC`, limit, action)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.scanAIAdjustments(rows)
}

// scanAIAdjustments scans rows into RuntimeAdjustmentRecord slice.
func (s *State) scanAIAdjustments(rows *sql.Rows) ([]RuntimeAdjustmentRecord, error) {
	var records []RuntimeAdjustmentRecord
	for rows.Next() {
		var r RuntimeAdjustmentRecord
		var timestampStr, adjustmentsStr string
		var reasoning, confidence sql.NullString
		var effectMeasured int
		var throughputBefore, throughputAfter, effectPercent sql.NullFloat64
		var cpuBefore, cpuAfter, memoryBefore, memoryAfter sql.NullFloat64

		if err := rows.Scan(
			&r.ID, &r.RunID, &r.AdjustmentNumber, &timestampStr, &r.Action, &adjustmentsStr,
			&throughputBefore, &effectMeasured, &throughputAfter, &effectPercent,
			&cpuBefore, &cpuAfter, &memoryBefore, &memoryAfter,
			&reasoning, &confidence,
		); err != nil {
			return nil, err
		}

		r.Timestamp, _ = time.Parse("2006-01-02 15:04:05", timestampStr)
		r.Adjustments = ParseAdjustments(adjustmentsStr)

		if throughputBefore.Valid {
			r.ThroughputBefore = throughputBefore.Float64
		}
		if cpuBefore.Valid {
			r.CPUBefore = cpuBefore.Float64
		}
		if memoryBefore.Valid {
			r.MemoryBefore = memoryBefore.Float64
		}
		if effectMeasured == 1 {
			if throughputAfter.Valid && effectPercent.Valid && cpuAfter.Valid && memoryAfter.Valid {
				r.EffectMeasured = true
				r.ThroughputAfter = throughputAfter.Float64
				r.EffectPercent = effectPercent.Float64
				r.CPUAfter = cpuAfter.Float64
				r.MemoryAfter = memoryAfter.Float64
			} else {
				logging.WarnEvent("runtime adjustment marked measured with incomplete after metrics; treating as unmeasured",
					"run_id", r.RunID,
					"adjustment_number", r.AdjustmentNumber,
				)
			}
		}
		if reasoning.Valid {
			r.Reasoning = reasoning.String
		}
		if confidence.Valid {
			r.Confidence = confidence.String
		}

		records = append(records, r)
	}
	return records, rows.Err()
}

// SaveTuningRecord saves tuning parameters from a migration run and returns
// the inserted row ID so the same run can stamp its result onto that row.
func (s *State) SaveTuningRecord(record TuningRecord) (int64, error) {
	wasAIUsed := 0
	if record.WasAIUsed {
		wasAIUsed = 1
	}

	// Helper to convert empty strings / zeros into nullable values so the
	// new regime columns are stored as SQL NULL when unknown rather than
	// as "" or 0 (which the smartconfig render path would otherwise treat
	// as a real value and try to compare against the current run).
	nullStr := func(s string) interface{} {
		if s == "" {
			return nil
		}
		return s
	}
	nullInt := func(v int64) interface{} {
		if v == 0 {
			return nil
		}
		return v
	}

	res, err := s.db.Exec(`
		INSERT INTO ai_tuning_history (
			timestamp, source_db_type, target_db_type,
			total_tables, total_rows, avg_row_size_bytes,
			cpu_cores, memory_gb,
			workers, chunk_size, read_ahead_buffers, write_ahead_writers,
			parallel_readers, max_partitions, large_table_threshold,
			max_source_connections, max_target_connections,
			estimated_memory_mb, ai_reasoning, was_ai_used,
			platform, target_shared_buffers_mb, target_synchronous_commit,
			target_fsync, target_full_page_writes, target_max_wal_size_mb,
			target_wal_level, source_max_server_memory_mb,
			source_host, source_port, source_database, source_schema,
			target_host, target_port, target_database, target_schema
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		record.Timestamp.Format("2006-01-02 15:04:05"),
		record.SourceDBType, record.TargetDBType,
		record.TotalTables, record.TotalRows, record.AvgRowSizeBytes,
		record.CPUCores, record.MemoryGB,
		record.Workers, record.ChunkSize, record.ReadAheadBuffers, record.WriteAheadWriters,
		record.ParallelReaders, record.MaxPartitions, record.LargeTableThreshold,
		record.MaxSourceConns, record.MaxTargetConns,
		record.EstimatedMemoryMB, record.Reasoning, wasAIUsed,
		nullStr(record.Platform), nullInt(record.TargetSharedBuffersMB), nullStr(record.TargetSyncCommit),
		nullStr(record.TargetFsync), nullStr(record.TargetFullPageWrites), nullInt(record.TargetMaxWALSizeMB),
		nullStr(record.TargetWALLevel), nullInt(record.SourceMaxServerMemoryMB),
		// #215 workload identity. All fields nullable; nullStr/nullInt
		// elide empty values so pre-identity records won't ever match
		// the Tier 1 exact-identity filter.
		nullStr(record.SourceHost), nullInt(int64(record.SourcePort)), nullStr(record.SourceDatabase), nullStr(record.SourceSchema),
		nullStr(record.TargetHost), nullInt(int64(record.TargetPort)), nullStr(record.TargetDatabase), nullStr(record.TargetSchema),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// UpdateTuningResult stamps the outcome onto exactly the tuning row created
// for this run. The NULL guard makes duplicate completion calls idempotent.
//
// chunkRetryCount is the cumulative count of transient chunk retries observed
// during the run (RuntimeMetrics.ChunkRetryCount); 0 for a clean run.
//
// adjustedAtRuntime marks the row as runtime-adjusted (#451): the run's
// observed throughput blends multiple configs, so the deterministic tuner
// excludes the row from regression / smoothed-bins / drift cohorts.
func (s *State) UpdateTuningResult(rowID int64, throughput float64, durationSecs float64, chunkRetryCount int, adjustedAtRuntime bool) error {
	adjusted := 0
	if adjustedAtRuntime {
		adjusted = 1
	}
	res, err := s.db.Exec(`
		UPDATE ai_tuning_history
		SET final_throughput = ?, final_duration_seconds = ?, chunk_retry_count = ?, adjusted_at_runtime = ?
		WHERE id = ? AND final_throughput IS NULL
	`, throughput, durationSecs, chunkRetryCount, adjusted, rowID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		logging.Debug("tuning row %d already stamped or missing", rowID)
	}
	return nil
}

// GetTuningHistory returns AI tuning recommendations filtered by migration
// direction (e.g., "mssql"→"postgres"). Pass limit=0 to fetch all records.
func (s *State) GetTuningHistory(limit int, sourceType, targetType string) ([]TuningRecord, error) {
	rows, err := s.queryWithOptionalLimit(`
		SELECT id, timestamp, source_db_type, target_db_type,
		       total_tables, total_rows, avg_row_size_bytes,
		       cpu_cores, memory_gb,
		       workers, chunk_size, read_ahead_buffers, write_ahead_writers,
		       parallel_readers, max_partitions, large_table_threshold,
		       max_source_connections, max_target_connections,
		       estimated_memory_mb, ai_reasoning, was_ai_used,
		       final_throughput, final_duration_seconds, chunk_retry_count,
		       platform, target_shared_buffers_mb, target_synchronous_commit,
		       target_fsync, target_full_page_writes, target_max_wal_size_mb,
		       target_wal_level, source_max_server_memory_mb,
		       source_host, source_port, source_database, source_schema,
		       target_host, target_port, target_database, target_schema,
		       adjusted_at_runtime
		FROM ai_tuning_history
		WHERE source_db_type = ? AND target_db_type = ?
		ORDER BY timestamp DESC`, limit, sourceType, targetType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []TuningRecord
	for rows.Next() {
		var r TuningRecord
		var timestampStr string
		var targetDBType, aiReasoning sql.NullString
		var avgRowSize, cpuCores, memoryGB sql.NullInt64
		var readAhead, writeAhead, parallelReaders, maxPartitions sql.NullInt64
		var largeTableThreshold, maxSourceConns, maxTargetConns, estimatedMem sql.NullInt64
		var wasAIUsed int
		var finalThroughput, finalDurationSecs sql.NullFloat64
		var chunkRetryCount sql.NullInt64
		// #144 regime columns; nullable for older rows that predate the migration.
		var platform, targetSyncCommit, targetFsync, targetFullPageWrites, targetWALLevel sql.NullString
		var targetSharedBuffersMB, targetMaxWALSizeMB, sourceMaxServerMemoryMB sql.NullInt64
		// #215 workload identity; nullable for older rows.
		var sourceHost, sourceDatabase, sourceSchema, targetHost, targetDatabase, targetSchema sql.NullString
		var sourcePort, targetPort sql.NullInt64
		var adjustedAtRuntime sql.NullInt64

		if err := rows.Scan(
			&r.ID, &timestampStr, &r.SourceDBType, &targetDBType,
			&r.TotalTables, &r.TotalRows, &avgRowSize,
			&cpuCores, &memoryGB,
			&r.Workers, &r.ChunkSize, &readAhead, &writeAhead,
			&parallelReaders, &maxPartitions, &largeTableThreshold,
			&maxSourceConns, &maxTargetConns,
			&estimatedMem, &aiReasoning, &wasAIUsed,
			&finalThroughput, &finalDurationSecs, &chunkRetryCount,
			&platform, &targetSharedBuffersMB, &targetSyncCommit,
			&targetFsync, &targetFullPageWrites, &targetMaxWALSizeMB,
			&targetWALLevel, &sourceMaxServerMemoryMB,
			&sourceHost, &sourcePort, &sourceDatabase, &sourceSchema,
			&targetHost, &targetPort, &targetDatabase, &targetSchema,
			&adjustedAtRuntime,
		); err != nil {
			return nil, err
		}
		// #215 workload-identity passthrough.
		if sourceHost.Valid {
			r.SourceHost = sourceHost.String
		}
		if sourcePort.Valid {
			r.SourcePort = int(sourcePort.Int64)
		}
		if sourceDatabase.Valid {
			r.SourceDatabase = sourceDatabase.String
		}
		if sourceSchema.Valid {
			r.SourceSchema = sourceSchema.String
		}
		if targetHost.Valid {
			r.TargetHost = targetHost.String
		}
		if targetPort.Valid {
			r.TargetPort = int(targetPort.Int64)
		}
		if targetDatabase.Valid {
			r.TargetDatabase = targetDatabase.String
		}
		if targetSchema.Valid {
			r.TargetSchema = targetSchema.String
		}
		if chunkRetryCount.Valid {
			r.ChunkRetryCount = int(chunkRetryCount.Int64)
		}
		// #451: NULL (pre-migration row) reads as false — those runs
		// mostly predate runtime tuning and stay eligible for training.
		r.AdjustedAtRuntime = adjustedAtRuntime.Valid && adjustedAtRuntime.Int64 != 0
		if platform.Valid {
			r.Platform = platform.String
		}
		if targetSharedBuffersMB.Valid {
			r.TargetSharedBuffersMB = targetSharedBuffersMB.Int64
		}
		if targetSyncCommit.Valid {
			r.TargetSyncCommit = targetSyncCommit.String
		}
		if targetFsync.Valid {
			r.TargetFsync = targetFsync.String
		}
		if targetFullPageWrites.Valid {
			r.TargetFullPageWrites = targetFullPageWrites.String
		}
		if targetMaxWALSizeMB.Valid {
			r.TargetMaxWALSizeMB = targetMaxWALSizeMB.Int64
		}
		if targetWALLevel.Valid {
			r.TargetWALLevel = targetWALLevel.String
		}
		if sourceMaxServerMemoryMB.Valid {
			r.SourceMaxServerMemoryMB = sourceMaxServerMemoryMB.Int64
		}

		r.Timestamp, _ = time.Parse("2006-01-02 15:04:05", timestampStr)
		r.WasAIUsed = wasAIUsed == 1

		if targetDBType.Valid {
			r.TargetDBType = targetDBType.String
		}
		if aiReasoning.Valid {
			r.Reasoning = aiReasoning.String
		}
		if avgRowSize.Valid {
			r.AvgRowSizeBytes = avgRowSize.Int64
		}
		if cpuCores.Valid {
			r.CPUCores = int(cpuCores.Int64)
		}
		if memoryGB.Valid {
			r.MemoryGB = int(memoryGB.Int64)
		}
		if readAhead.Valid {
			r.ReadAheadBuffers = int(readAhead.Int64)
		}
		if writeAhead.Valid {
			r.WriteAheadWriters = int(writeAhead.Int64)
		}
		if parallelReaders.Valid {
			r.ParallelReaders = int(parallelReaders.Int64)
		}
		if maxPartitions.Valid {
			r.MaxPartitions = int(maxPartitions.Int64)
		}
		if largeTableThreshold.Valid {
			r.LargeTableThreshold = largeTableThreshold.Int64
		}
		if maxSourceConns.Valid {
			r.MaxSourceConns = int(maxSourceConns.Int64)
		}
		if maxTargetConns.Valid {
			r.MaxTargetConns = int(maxTargetConns.Int64)
		}
		if estimatedMem.Valid {
			r.EstimatedMemoryMB = estimatedMem.Int64
		}
		if finalThroughput.Valid {
			r.FinalThroughput = finalThroughput.Float64
		}
		if finalDurationSecs.Valid {
			r.FinalDurationSecs = finalDurationSecs.Float64
		}

		records = append(records, r)
	}
	return records, rows.Err()
}

// GetAITuningAggregatesByWaw aggregates the FULL ai_tuning_history table by
// write_ahead_writers for the given migration direction. Used by smartconfig to
// keep retry-rate denominators honest while the trajectory rows in the prompt
// are bounded (issue #141). Records without a completed run are excluded via
// `final_throughput > 0` — this correctly filters both NULL (the on-disk state
// before UpdateTuningResult fires) and 0 values, matching the prior Go-side
// `if h.FinalThroughput <= 0 { continue }` aggregation behavior.
func (s *State) GetAITuningAggregatesByWaw(sourceType, targetType string) ([]WawAggregateRecord, error) {
	rows, err := s.db.Query(`
		SELECT write_ahead_writers,
		       COUNT(*) AS total_runs,
		       SUM(CASE WHEN chunk_retry_count > 0 THEN 1 ELSE 0 END) AS runs_with_retries,
		       COALESCE(SUM(chunk_retry_count), 0) AS total_retries,
		       COALESCE(MAX(final_throughput), 0) AS peak_throughput,
		       COALESCE(AVG(final_throughput), 0) AS mean_throughput
		FROM ai_tuning_history
		WHERE source_db_type = ? AND target_db_type = ?
		  AND final_throughput > 0
		GROUP BY write_ahead_writers
		ORDER BY write_ahead_writers ASC`, sourceType, targetType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggs []WawAggregateRecord
	for rows.Next() {
		var a WawAggregateRecord
		if err := rows.Scan(
			&a.WriteAheadWriters, &a.TotalRuns, &a.RunsWithRetries,
			&a.TotalRetries, &a.PeakThroughput, &a.MeanThroughput,
		); err != nil {
			return nil, err
		}
		aggs = append(aggs, a)
	}
	return aggs, rows.Err()
}

// GetAITuningAggregatesByChunkSize aggregates the FULL ai_tuning_history table
// by chunk_size for the given migration direction. Same rationale as
// GetAITuningAggregatesByWaw — keeps the chunk-size summary's denominator
// accurate when the trajectory is bounded.
func (s *State) GetAITuningAggregatesByChunkSize(sourceType, targetType string) ([]ChunkSizeAggregateRecord, error) {
	rows, err := s.db.Query(`
		SELECT chunk_size,
		       COUNT(*) AS runs,
		       AVG(final_throughput) AS avg_throughput
		FROM ai_tuning_history
		WHERE source_db_type = ? AND target_db_type = ?
		  AND final_throughput > 0
		GROUP BY chunk_size
		ORDER BY chunk_size ASC`, sourceType, targetType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var aggs []ChunkSizeAggregateRecord
	for rows.Next() {
		var a ChunkSizeAggregateRecord
		if err := rows.Scan(&a.ChunkSize, &a.Runs, &a.AvgThroughput); err != nil {
			return nil, err
		}
		aggs = append(aggs, a)
	}
	return aggs, rows.Err()
}
