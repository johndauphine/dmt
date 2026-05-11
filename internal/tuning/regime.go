package tuning

import "fmt"

// Regime labels classify how comparable a historical run is to the current
// one. Selection logic uses these to discount or exclude history rows that
// ran under materially different conditions (#144).
//
// RegimeDifferentWorkload (#198) covers the case where hardware and DB
// tuning agree but the *workload itself* differs — e.g. yesterday's run
// migrated a 19M-row dataset and today's is 106M rows. Such rows train the
// regression on operating points that don't apply, so they're filtered out
// just like different-hw rows. The label is returned alone (without
// combining with hw/tuning) because filterByRegime drops any of these
// outcomes — the combo distinction would only matter for diagnostics, and
// the deltas slice already carries the per-axis detail.
const (
	RegimeSame                 = "same_regime"
	RegimeDifferentTuning      = "different_tuning"
	RegimeDifferentHW          = "different_hw"
	RegimeDifferentHWAndTuning = "different_hw_and_tuning"
	RegimeDifferentWorkload    = "different_workload"
	RegimeUnknown              = "unknown"
)

// hardwareTolerance: CPU cores must agree within max(2, cores/5); memory
// must agree within 20%. Rows outside either band are flagged as
// different_hw. Lifted verbatim from internal/driver/ai_smartconfig.go.
const hardwareMemoryTolerancePct = 80

// dbTuningSizeTolerancePct: discrete fields like shared_buffers are
// considered "same" within 10% (so 8000 MB vs 8192 MB doesn't count as a
// regime change).
const dbTuningSizeTolerancePct = 10

// workloadRowsToleranceRatio is the max ratio between historical and
// current TotalRows before the row is treated as different_workload.
// 3× is loose enough that week-over-week table growth doesn't trip it
// (a 25% week-over-week grower takes ~5 weeks to exceed 3×); tight enough
// that a 19M-row vs 106M-row mix-up (the #198 repro: 5.5×) is caught.
const workloadRowsToleranceRatio = 3.0

// workloadAvgRowBytesToleranceRatio is the equivalent ratio for
// AvgRowBytes. 2× catches schema swaps (a wide row-size shift signals
// a different table mix) while still tolerating the natural variance
// across tables within one dataset.
const workloadAvgRowBytesToleranceRatio = 2.0

// ClassifyRegime decides which similarity bucket a historical row falls
// into relative to the current run's host + DB-tuning context. Returns
// the regime label plus a list of human-readable deltas suitable for
// diagnostic logging when the regime is non-same.
//
// Lifted verbatim from internal/driver/ai_smartconfig.go classifyRegime
// (#144). Behavior preserved including the unknown-on-missing-fields
// fallback for pre-#144 history rows that don't carry CPU / memory data.
func ClassifyRegime(history HistoryRecord, current Input, currentTuning DBTuning) (label string, deltas []string) {
	if history.CPUCores <= 0 || history.MemoryGB <= 0 || current.CPUCores <= 0 || current.MemoryGB <= 0 {
		return RegimeUnknown, nil
	}

	hwDiffer := false
	cpuDiff := absInt(history.CPUCores - current.CPUCores)
	cpuThreshold := maxInt(2, current.CPUCores/5)
	lowerMem := minInt(history.MemoryGB, current.MemoryGB)
	upperMem := maxInt(history.MemoryGB, current.MemoryGB)
	memorySimilar := lowerMem*100 >= upperMem*hardwareMemoryTolerancePct
	if cpuDiff > cpuThreshold || !memorySimilar {
		hwDiffer = true
		deltas = append(deltas, fmt.Sprintf("hw: history=%dc/%dGB vs current=%dc/%dGB",
			history.CPUCores, history.MemoryGB, current.CPUCores, current.MemoryGB))
	}

	tuningDiffer := false
	addStr := func(label, hist, curr string) {
		if hist == "" || curr == "" || hist == curr {
			return
		}
		tuningDiffer = true
		deltas = append(deltas, fmt.Sprintf("%s: %s→%s", label, hist, curr))
	}
	addSize := func(label string, hist, curr int64, tolerancePct int) {
		if hist == 0 || curr == 0 {
			return
		}
		lower, upper := hist, curr
		if lower > upper {
			lower, upper = upper, lower
		}
		if lower*100 >= upper*int64(100-tolerancePct) {
			return
		}
		tuningDiffer = true
		deltas = append(deltas, fmt.Sprintf("%s: %dMB→%dMB", label, hist, curr))
	}

	addSize("pg_shared_buffers", history.TargetSharedBuffersMB, currentTuning.TargetSharedBuffersMB, dbTuningSizeTolerancePct)
	addStr("pg_sync_commit", history.TargetSyncCommit, currentTuning.TargetSyncCommit)
	addStr("pg_fsync", history.TargetFsync, currentTuning.TargetFsync)
	addStr("pg_full_page_writes", history.TargetFullPageWrites, currentTuning.TargetFullPageWrites)
	addSize("pg_max_wal", history.TargetMaxWALSizeMB, currentTuning.TargetMaxWALSizeMB, dbTuningSizeTolerancePct)
	addStr("pg_wal_level", history.TargetWALLevel, currentTuning.TargetWALLevel)
	addSize("mssql_max_server_memory", history.SourceMaxServerMemoryMB, currentTuning.SourceMaxServerMemoryMB, dbTuningSizeTolerancePct)

	// Workload-population check (#198). Rows whose dataset shape differs
	// materially from the current run are dropped: their throughput-vs-
	// config surface fits a different operating regime (a 40s migration
	// is dominated by startup overhead; a 5m migration is dominated by
	// transfer cost). When workload differs, we return DifferentWorkload
	// alone — the combo with hw/tuning is intentionally collapsed because
	// filterByRegime drops any of these outcomes, and the per-axis detail
	// is preserved in the deltas slice for diagnostics.
	workloadDiffer := false
	addRatio := func(label string, hist, curr int64, maxRatio float64) {
		if hist <= 0 || curr <= 0 {
			return
		}
		lo, hi := hist, curr
		if lo > hi {
			lo, hi = hi, lo
		}
		if float64(hi)/float64(lo) <= maxRatio {
			return
		}
		workloadDiffer = true
		deltas = append(deltas, fmt.Sprintf("%s: %d→%d", label, hist, curr))
	}
	addRatio("total_rows", history.TotalRows, current.TotalRows, workloadRowsToleranceRatio)
	addRatio("avg_row_bytes", history.AvgRowBytes, current.AvgRowBytes, workloadAvgRowBytesToleranceRatio)

	switch {
	case workloadDiffer:
		return RegimeDifferentWorkload, deltas
	case hwDiffer && tuningDiffer:
		return RegimeDifferentHWAndTuning, deltas
	case hwDiffer:
		return RegimeDifferentHW, deltas
	case tuningDiffer:
		return RegimeDifferentTuning, deltas
	default:
		return RegimeSame, nil
	}
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// hasExactIdentity reports whether the Input carries a complete enough
// workload-identity tuple for the Tier 1 classifier (#215) to do an
// equality lookup. Hosts and database names are required; schemas may
// legitimately be empty for drivers that don't expose them (MySQL has
// no schema concept distinct from database). Ports are required because
// SQL NULL ports scan as Go's zero value (0) in the SQLite reader, so
// a missing port would make "unset" indistinguishable from "actual 0"
// — both would then match pre-#215 rows whose ports also scanned as 0
// from NULL. Requiring non-zero ports here keeps the Tier 1 comparison
// honest about which rows it intends to match (Copilot review on
// PR #223).
//
// Returns false on missing required fields → caller skips Tier 1 and
// falls through to the Tier 2 / Tier 3 path (regime filter or baseline).
func hasExactIdentity(in Input) bool {
	if in.SourceHost == "" || in.SourceDatabase == "" {
		return false
	}
	if in.TargetHost == "" || in.TargetDatabase == "" {
		return false
	}
	if in.SourcePort == 0 || in.TargetPort == 0 {
		return false
	}
	return true
}

// filterByExactIdentity returns the subset of rows whose workload
// identity tuple matches the input exactly (#215). All eight fields
// must match by equality — same source host/port/db/schema AND same
// target host/port/db/schema. Pre-#215 rows have empty identity fields
// and naturally fall out of the match (empty string ≠ user's non-empty
// host).
//
// No normalization is applied. `localhost` and `127.0.0.1` are treated
// as distinct hosts. Case-sensitive comparison (PG identifiers are
// case-sensitive by default; users who want loose matching can
// normalize in their config).
func filterByExactIdentity(rows []HistoryRecord, in Input) []HistoryRecord {
	out := make([]HistoryRecord, 0, len(rows))
	for _, r := range rows {
		if r.SourceHost == in.SourceHost &&
			r.SourcePort == in.SourcePort &&
			r.SourceDatabase == in.SourceDatabase &&
			r.SourceSchema == in.SourceSchema &&
			r.TargetHost == in.TargetHost &&
			r.TargetPort == in.TargetPort &&
			r.TargetDatabase == in.TargetDatabase &&
			r.TargetSchema == in.TargetSchema {
			out = append(out, r)
		}
	}
	return out
}
