package tuning

import "fmt"

// Regime labels classify how comparable a historical run is to the current
// one. Selection logic uses these to discount or exclude history rows that
// ran under materially different conditions (#144).
const (
	RegimeSame                 = "same_regime"
	RegimeDifferentTuning      = "different_tuning"
	RegimeDifferentHW          = "different_hw"
	RegimeDifferentHWAndTuning = "different_hw_and_tuning"
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

	switch {
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
