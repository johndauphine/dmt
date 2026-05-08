package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/logging"
)

// captureDBTuning queries the source and target databases for the effective
// tuning settings that materially affect bulk-migration throughput. Issue #144
// — the smartconfig prompt's regime classifier was previously CPU/RAM-only,
// which couldn't distinguish same-machine runs that differed only in PG
// shared_buffers, synchronous_commit, fsync, max_wal_size, etc. (the SO2010
// cold-tuner-vs-warm-tuner case + SO2013 durability-off case in
// docs/BENCHMARKS.md). This function captures those settings so the
// trajectory rendering can render per-row deltas against the current run.
//
// All queries are best-effort: a failed lookup logs a Debug line and leaves
// the corresponding field at its zero value. The smartconfig render path
// treats zero values as "unknown" and tells the model to discount comparisons
// against unknown rows.
func captureDBTuning(ctx context.Context, sourceDB, targetDB *sql.DB, sourceType, targetType string) checkpoint.AITuningRecord {
	rec := checkpoint.AITuningRecord{}

	captureCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if targetDB != nil && targetType == "postgres" {
		captureTargetPG(captureCtx, targetDB, &rec)
	}
	if sourceDB != nil && sourceType == "mssql" {
		captureSourceMSSQL(captureCtx, sourceDB, &rec)
	}
	// MySQL source / PG source / MSSQL target: not yet implemented. Leaving
	// fields zero-valued so the smartconfig render path treats them as
	// "unknown" and doesn't pretend to compare against missing data.

	return rec
}

func captureTargetPG(ctx context.Context, db *sql.DB, rec *checkpoint.AITuningRecord) {
	type pgSetting struct {
		name string
		// dest receives the parsed value. For *int64 fields, parsePGSize
		// converts "8GB" / "16384MB" / "1024kB" to MB.
		assignStr  func(string)
		assignSize func(int64)
	}
	settings := []pgSetting{
		{name: "shared_buffers", assignSize: func(mb int64) { rec.TargetSharedBuffersMB = mb }},
		{name: "synchronous_commit", assignStr: func(s string) { rec.TargetSyncCommit = s }},
		{name: "fsync", assignStr: func(s string) { rec.TargetFsync = s }},
		{name: "full_page_writes", assignStr: func(s string) { rec.TargetFullPageWrites = s }},
		{name: "max_wal_size", assignSize: func(mb int64) { rec.TargetMaxWALSizeMB = mb }},
		{name: "wal_level", assignStr: func(s string) { rec.TargetWALLevel = s }},
	}
	for _, s := range settings {
		var raw string
		// SHOW parameter_name. PG returns a single-value result.
		if err := db.QueryRowContext(ctx, "SHOW "+s.name).Scan(&raw); err != nil {
			logging.Debug("captureTargetPG: SHOW %s: %v", s.name, err)
			continue
		}
		raw = strings.TrimSpace(raw)
		if s.assignStr != nil {
			s.assignStr(raw)
		} else if s.assignSize != nil {
			if mb, ok := parsePGSize(raw); ok {
				s.assignSize(mb)
			}
		}
	}
}

func captureSourceMSSQL(ctx context.Context, db *sql.DB, rec *checkpoint.AITuningRecord) {
	// max server memory is the most-load-bearing source-side knob for
	// large-dataset reads (the SO2013 8GB-vs-16GB experiment in BENCHMARKS.md).
	var mb int64
	err := db.QueryRowContext(ctx,
		`SELECT CAST(value_in_use AS bigint) FROM sys.configurations
		 WHERE name = 'max server memory (MB)'`).Scan(&mb)
	if err != nil {
		logging.Debug("captureSourceMSSQL: max server memory: %v", err)
		return
	}
	// MSSQL caps at 2147483647 MB by default ("unlimited"); render as 0/unknown.
	if mb >= 2_000_000_000 {
		return
	}
	rec.SourceMaxServerMemoryMB = mb
}

// parsePGSize converts a PG size string like "8GB", "16384MB", "1024kB",
// "8388608" (bare bytes) into MB. Returns (0, false) if it can't parse.
//
// PG's SHOW returns sizes with a trailing unit when the param has a unit
// (kB / MB / GB / TB) but bare numeric for "blocks" type params. This
// helper handles both forms used by the settings we capture.
func parsePGSize(raw string) (int64, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	// Strip any trailing 'b' (bytes); PG uses lowercase b for byte units.
	for _, suffix := range []struct {
		unit  string
		bytes int64
	}{
		{"tb", 1024 * 1024},
		{"gb", 1024},
		{"mb", 1},
		{"kb", 0}, // sub-MB handled below
	} {
		if strings.HasSuffix(s, suffix.unit) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(s, suffix.unit)), 10, 64)
			if err != nil {
				return 0, false
			}
			if suffix.unit == "kb" {
				return n / 1024, true // round down; sub-MB precision lost
			}
			return n * suffix.bytes, true
		}
	}
	// No unit suffix — bare integer. PG's "shared_buffers" can be reported
	// in 8kB blocks but SHOW always renders with units, so this branch is
	// rarely hit. Treat bare value as bytes and convert to MB.
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n / (1024 * 1024), true
}

// formatTuningSnippet renders a captured tuning record as a compact
// human-readable string, e.g.
// "pg{shared_buffers=8192MB, sync_commit=off, fsync=on, ...}, mssql{max_server_memory=8192MB}".
// Used in the smartconfig prompt to render per-row regime context.
func formatTuningSnippet(r checkpoint.AITuningRecord) string {
	var pg []string
	if r.TargetSharedBuffersMB > 0 {
		pg = append(pg, fmt.Sprintf("shared_buffers=%dMB", r.TargetSharedBuffersMB))
	}
	if r.TargetSyncCommit != "" {
		pg = append(pg, "sync_commit="+r.TargetSyncCommit)
	}
	if r.TargetFsync != "" {
		pg = append(pg, "fsync="+r.TargetFsync)
	}
	if r.TargetFullPageWrites != "" {
		pg = append(pg, "full_page_writes="+r.TargetFullPageWrites)
	}
	if r.TargetMaxWALSizeMB > 0 {
		pg = append(pg, fmt.Sprintf("max_wal=%dMB", r.TargetMaxWALSizeMB))
	}
	if r.TargetWALLevel != "" {
		pg = append(pg, "wal_level="+r.TargetWALLevel)
	}

	var parts []string
	if len(pg) > 0 {
		parts = append(parts, "pg{"+strings.Join(pg, ", ")+"}")
	}
	if r.SourceMaxServerMemoryMB > 0 {
		parts = append(parts, fmt.Sprintf("mssql{max_server_memory=%dMB}", r.SourceMaxServerMemoryMB))
	}
	if len(parts) == 0 {
		return "tuning=unknown"
	}
	return strings.Join(parts, ", ")
}
