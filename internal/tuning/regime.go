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

// Physical-regime bands by total bytes (rows × avg_row_bytes). Replaces
// the original ratio-based row-count/avg-row-bytes thresholds (#214)
// because ratios get the danger zone wrong: 1M→3M is the same ratio as
// 30M→90M but very different physics, and there's a discontinuity at
// the threshold (100M vs 305M flags; 100M vs 295M doesn't). What
// actually drives the throughput surface is the physical regime — does
// it fit in RAM, in the target buffer pool, in page cache, or does it
// spill to disk on one or both sides.
//
// Boundaries are decimal multiples (10^9 for GB, 10^12 for TB), aligned
// with the typical buffer-pool / page-cache sizing on the hardware dmt
// targets (8–256 GB RAM hosts). The cost-cliff physics the bands model
// (buffer-pool size, page-cache spill, disk-bound regime) is order-of-
// magnitude, so decimal vs binary doesn't change which cliff a workload
// sits behind. Two runs with the same band are comparable on the
// workload axis regardless of how their raw row counts compare.
const (
	bandTinyMaxBytes   int64 = 100 * 1000 * 1000         // 100 MB
	bandSmallMaxBytes  int64 = 10 * 1000 * 1000 * 1000   // 10 GB
	bandMediumMaxBytes int64 = 100 * 1000 * 1000 * 1000  // 100 GB
	bandLargeMaxBytes  int64 = 1000 * 1000 * 1000 * 1000 // 1 TB
)

// Workload bands (#214). Returned via the regime delta string for
// diagnostics; not exported because no caller outside this package
// needs to switch on them.
const (
	bandTiny    = "tiny"    // < 100 MB — fits any RAM, network-bound
	bandSmall   = "small"   // 100 MB – 10 GB — fits target buffer pool
	bandMedium  = "medium"  // 10 GB – 100 GB — spills target page cache
	bandLarge   = "large"   // 100 GB – 1 TB — spills to disk both sides
	bandHuge    = "huge"    // > 1 TB — streaming-only, network bottleneck
	bandUnknown = "unknown" // missing total_rows or avg_row_bytes
)

// Skew tiers (#214) bucket largest_table_bytes / total_bytes. The
// secondary axis: a 100 GB dataset with one 90 GB table runs very
// differently from a 100 GB dataset with 200 evenly-sized tables
// (parallel-readers utilization, head-of-line blocking).
const (
	skewEven     = "evenly_distributed" // largest < 25% of total
	skewSkewed   = "skewed"             // 25% – 75%
	skewDominant = "dominant"           // > 75%
	skewUnknown  = "unknown"            // largest table size not recorded
)

const (
	skewEvenMaxPct   = 0.25
	skewSkewedMaxPct = 0.75
)

// Count tiers (#214) bucket TotalTables. The tertiary axis — used only
// when band and skew also agree. A 50-table migration and a 5-table
// migration of the same total-bytes can still differ in DDL overhead,
// connection-pool pressure, and scheduling cost.
const (
	countFew     = "few"     // < 10
	countMany    = "many"    // 10 – 100
	countMassive = "massive" // > 100
	countUnknown = "unknown" // TotalTables not recorded
)

const (
	countFewMax  = 10
	countManyMax = 100
)

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

	// Workload-population check (#198 / #214). Rows whose dataset shape
	// differs materially from the current run are dropped: their
	// throughput-vs-config surface fits a different operating regime
	// (a 40s migration is dominated by startup overhead; a 5m migration
	// is dominated by transfer cost). When workload differs, we return
	// DifferentWorkload alone — the combo with hw/tuning is intentionally
	// collapsed because filterByRegime drops any of these outcomes, and
	// the per-axis detail is preserved in the deltas slice for
	// diagnostics.
	//
	// #214 replaced the original per-field ratio gate (3× rows, 2× avg
	// row bytes) with bucket gating on derived metrics. Ratios were the
	// wrong shape — same ratio in different regimes has different physics
	// (1M→3M ≠ 30M→90M), and there's a discontinuity at the threshold
	// (100M and 305M flag; 100M and 295M don't). Bands on total_bytes
	// align with the actual cost cliffs (fits-in-RAM, fits-in-page-
	// cache, spills-to-disk) so they remain stable through smooth
	// dataset growth and fire only when a real regime boundary crosses.
	workloadDiffer := false
	histBand := workloadBand(history.TotalRows, history.AvgRowBytes)
	currBand := workloadBand(current.TotalRows, current.AvgRowBytes)
	if histBand != bandUnknown && currBand != bandUnknown && histBand != currBand {
		workloadDiffer = true
		deltas = append(deltas, fmt.Sprintf("workload_band: %s→%s", histBand, currBand))
	}

	// Secondary axis: skew tier. Only consulted when the primary band
	// matches (or one side is unknown); a mismatched band already gates
	// the row out, and stacking deltas there is just noise.
	if !workloadDiffer {
		histSkew := skewTier(history.LargestTableBytes, totalBytes(history.TotalRows, history.AvgRowBytes))
		currSkew := skewTier(current.LargestTableBytes, totalBytes(current.TotalRows, current.AvgRowBytes))
		if histSkew != skewUnknown && currSkew != skewUnknown && histSkew != currSkew {
			workloadDiffer = true
			deltas = append(deltas, fmt.Sprintf("workload_skew: %s→%s", histSkew, currSkew))
		}
	}

	// Tertiary axis: table-count tier. Only consulted when band and
	// skew both agree (or skew is unknown on either side) — DDL/
	// scheduling cost is a smaller perturbation than the physical-
	// regime cliff or the skew distribution.
	if !workloadDiffer {
		histCount := countTier(history.TotalTables)
		currCount := countTier(current.TotalTables)
		if histCount != countUnknown && currCount != countUnknown && histCount != currCount {
			workloadDiffer = true
			deltas = append(deltas, fmt.Sprintf("workload_count: %s→%s", histCount, currCount))
		}
	}

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

// totalBytes computes the physical dataset size used for workload-band
// classification (#214). Returns 0 when either input is non-positive so
// the caller can fall through to "unknown band" rather than coalescing
// a missing field with a tiny-dataset.
func totalBytes(rows, avgRowBytes int64) int64 {
	if rows <= 0 || avgRowBytes <= 0 {
		return 0
	}
	return rows * avgRowBytes
}

// workloadBand returns the bucket label for the given dataset
// dimensions. Returns bandUnknown when total_bytes can't be computed —
// pre-#214 callers and tests that don't set both fields fall through
// to this case and the regime classifier skips the workload axis for
// that comparison.
func workloadBand(rows, avgRowBytes int64) string {
	tb := totalBytes(rows, avgRowBytes)
	if tb <= 0 {
		return bandUnknown
	}
	switch {
	case tb < bandTinyMaxBytes:
		return bandTiny
	case tb < bandSmallMaxBytes:
		return bandSmall
	case tb < bandMediumMaxBytes:
		return bandMedium
	case tb < bandLargeMaxBytes:
		return bandLarge
	default:
		return bandHuge
	}
}

// skewTier returns the bucket label for largest_table_bytes / total_bytes.
// Returns skewUnknown when either input is non-positive — pre-#214
// callers don't have a recorded largest-table size and we don't want
// to treat zero as "evenly distributed."
func skewTier(largestTableBytes, totalBytes int64) string {
	if largestTableBytes <= 0 || totalBytes <= 0 {
		return skewUnknown
	}
	pct := float64(largestTableBytes) / float64(totalBytes)
	switch {
	case pct < skewEvenMaxPct:
		return skewEven
	case pct < skewSkewedMaxPct:
		return skewSkewed
	default:
		return skewDominant
	}
}

// countTier returns the bucket label for total table count. Returns
// countUnknown when the count is non-positive so pre-#214 rows that
// didn't persist a count don't false-trip the tertiary gate.
func countTier(tableCount int) string {
	if tableCount <= 0 {
		return countUnknown
	}
	switch {
	case tableCount < countFewMax:
		return countFew
	case tableCount <= countManyMax:
		return countMany
	default:
		return countMassive
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
