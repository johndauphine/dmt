package orchestrator

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/tuning"
)

// TestReplayValidation_AgainstAITuningHistory replays each historical
// ai_tuning_history row through the deterministic tuner and compares
// the predicted (WAW, chunk_size) to what the AI actually picked. Reports
// per-(source, target) agreement rate. Disagreements are logged with
// the recorded throughput so a reviewer can tell whether the AI's pick
// was empirically faster or slower than what the deterministic tuner
// would have chosen.
//
// Skipped by default — this is a one-shot confidence check, not a
// regression gate. Run against your own local DB:
//
//	DMT_REPLAY_DATADIR=$HOME/.dmt go test -v -run TestReplayValidation \
//	    ./internal/orchestrator
//
// The acceptance bar from #175 is ≥90% agreement on WAW per
// (source, target) pair, or a justified deviation. Chunk_size agreement
// is expected to be lower because the AI usually anchored at 50000
// regardless of avg_row_bytes while the deterministic path scales with
// row size (per #166). That divergence is the point of #166.
func TestReplayValidation_AgainstAITuningHistory(t *testing.T) {
	dataDir := os.Getenv("DMT_REPLAY_DATADIR")
	if dataDir == "" {
		t.Skip("set DMT_REPLAY_DATADIR=path/to/dmt/data/dir to run replay validation")
	}

	state, err := checkpoint.New(dataDir)
	if err != nil {
		t.Fatalf("opening checkpoint state: %v", err)
	}
	defer state.Close()

	// Find every (source, target) pair present in history. GetAITuningHistory
	// with limit=0 returns all rows; we group locally.
	all, err := allHistoryPairs(state)
	if err != nil {
		t.Fatalf("scanning history pairs: %v", err)
	}
	if len(all) == 0 {
		t.Skip("no ai_tuning_history rows found — nothing to replay")
	}

	for pair, rows := range all {
		t.Run(pair.source+"_to_"+pair.target, func(t *testing.T) {
			report := replayPair(rows)
			t.Logf("\n%s", report.format(pair))
			// Report only — no assertion. Acceptance is reviewer judgement
			// per #175 (the ≥90% bar is qualitative, not a CI gate).
		})
	}
}

type historyKey struct {
	source string
	target string
}

// allHistoryPairs returns all history rows the State backend has, grouped
// by (source, target). Uses a hardcoded candidate list of canonical driver
// pairs because checkpoint.State doesn't expose a "list distinct pairs"
// method — adding one is scope creep for PR1. Pairs using non-canonical
// driver names (aliases) or future drivers added without updating this
// list would be silently missed; harmless for the diagnostic-only purpose
// of this test.
func allHistoryPairs(state *checkpoint.State) (map[historyKey][]checkpoint.AITuningRecord, error) {
	pairs := map[historyKey][]checkpoint.AITuningRecord{}
	candidates := []historyKey{
		{"mssql", "postgres"}, {"postgres", "mssql"},
		{"mssql", "mysql"}, {"mysql", "mssql"},
		{"postgres", "mysql"}, {"mysql", "postgres"},
		{"mssql", "mssql"}, {"postgres", "postgres"}, {"mysql", "mysql"},
	}
	for _, k := range candidates {
		rows, err := state.GetAITuningHistory(0, k.source, k.target)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			pairs[k] = rows
		}
	}
	return pairs, nil
}

type replayResult struct {
	totalRows          int
	wawAgree           int
	chunkAgree         int
	disagreementsTotal int          // count across all rows
	disagreements      []disagreement // capped at maxDisagreementSamples to bound memory
}

// maxDisagreementSamples caps how many disagreement records replayPair
// retains. Format prints the first 10; anything past 100 is just memory
// overhead on a busy database.
const maxDisagreementSamples = 100

type disagreement struct {
	timestamp       string
	avgRowBytes     int64
	cpuCores        int
	memoryGB        int
	wasAIUsed       bool
	recordedWAW     int
	predictedWAW    int
	recordedChunk   int
	predictedChunk  int
	finalThroughput float64
	chunkRetryCount int
}

// replayPair walks the rows for one (source, target) pair in chronological
// order, replaying each through tuning.Tune with history-as-of-the-row's-
// timestamp.
func replayPair(rows []checkpoint.AITuningRecord) replayResult {
	// State's GetAITuningHistory orders by Timestamp DESC; we want ASC for
	// chronological replay so each row sees the prior history.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Timestamp.Before(rows[j].Timestamp)
	})

	var result replayResult
	for i, r := range rows {
		input := tuning.Input{
			CPUCores:     r.CPUCores,
			MemoryGB:     r.MemoryGB,
			Platform:     r.Platform,
			SourceDBType: r.SourceDBType,
			TargetDBType: r.TargetDBType,
			AvgRowBytes:  r.AvgRowSizeBytes,
			TotalTables:  r.TotalTables,
			TotalRows:    r.TotalRows,
		}
		profile := buildProfile(r.TargetDBType)
		history := historySliceProvider{rows: convertHistoryRows(rows[:i])}
		dbt := tuning.DBTuning{
			TargetSharedBuffersMB:   r.TargetSharedBuffersMB,
			TargetSyncCommit:        r.TargetSyncCommit,
			TargetFsync:             r.TargetFsync,
			TargetFullPageWrites:    r.TargetFullPageWrites,
			TargetMaxWALSizeMB:      r.TargetMaxWALSizeMB,
			TargetWALLevel:          r.TargetWALLevel,
			SourceMaxServerMemoryMB: r.SourceMaxServerMemoryMB,
		}

		out := tuning.Tune(input, profile, &history, dbt)

		result.totalRows++
		if out.WriteAheadWriters == r.WriteAheadWriters {
			result.wawAgree++
		}
		if out.ChunkSize == r.ChunkSize {
			result.chunkAgree++
		}
		if out.WriteAheadWriters != r.WriteAheadWriters || out.ChunkSize != r.ChunkSize {
			result.disagreementsTotal++
			if len(result.disagreements) < maxDisagreementSamples {
				result.disagreements = append(result.disagreements, disagreement{
					timestamp:       r.Timestamp.Format("2006-01-02 15:04"),
					avgRowBytes:     r.AvgRowSizeBytes,
					cpuCores:        r.CPUCores,
					memoryGB:        r.MemoryGB,
					wasAIUsed:       r.WasAIUsed,
					recordedWAW:     r.WriteAheadWriters,
					predictedWAW:    out.WriteAheadWriters,
					recordedChunk:   r.ChunkSize,
					predictedChunk:  out.ChunkSize,
					finalThroughput: r.FinalThroughput,
					chunkRetryCount: r.ChunkRetryCount,
				})
			}
		}
	}
	return result
}

// historySliceProvider is a tuning.HistoryProvider backed by an in-memory
// slice — used by replay so each row sees only the history that existed
// when it was recorded.
type historySliceProvider struct {
	rows []tuning.HistoryRecord
}

func (h *historySliceProvider) Records(_, _ string) ([]tuning.HistoryRecord, error) {
	return h.rows, nil
}

func convertHistoryRows(rows []checkpoint.AITuningRecord) []tuning.HistoryRecord {
	out := make([]tuning.HistoryRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, tuning.HistoryRecord{
			SourceDBType:            r.SourceDBType,
			TargetDBType:            r.TargetDBType,
			Workers:                 r.Workers,
			ChunkSize:               r.ChunkSize,
			WriteAheadWriters:       r.WriteAheadWriters,
			FinalThroughput:         r.FinalThroughput,
			ChunkRetryCount:         r.ChunkRetryCount,
			CPUCores:                r.CPUCores,
			MemoryGB:                r.MemoryGB,
			Platform:                r.Platform,
			TargetSharedBuffersMB:   r.TargetSharedBuffersMB,
			TargetSyncCommit:        r.TargetSyncCommit,
			TargetFsync:             r.TargetFsync,
			TargetFullPageWrites:    r.TargetFullPageWrites,
			TargetMaxWALSizeMB:      r.TargetMaxWALSizeMB,
			TargetWALLevel:          r.TargetWALLevel,
			SourceMaxServerMemoryMB: r.SourceMaxServerMemoryMB,
		})
	}
	return out
}

func buildProfile(targetType string) tuning.DriverProfile {
	profile := tuning.DriverProfile{Name: targetType, BaselineWAW: 2}
	d, err := driver.Get(targetType)
	if err != nil {
		return profile
	}
	defaults := d.Defaults()
	profile.BaselineWAW = defaults.WriteAheadWriters
	profile.ScaleWritersWithCores = defaults.ScaleWritersWithCores
	profile.OptimumBulkChunkBytes = defaults.OptimumBulkChunkBytes
	// TODO(#166): pass r.AvgRowSizeBytes once HardChunkLimit becomes
	// row-size-dependent (MySQL @@max_allowed_packet probe). All drivers
	// return 0 today regardless of input.
	profile.HardChunkLimit = d.HardChunkLimit(0)
	return profile
}

func (r replayResult) format(pair historyKey) string {
	if r.totalRows == 0 {
		return fmt.Sprintf("%s→%s: no rows", pair.source, pair.target)
	}
	wawPct := 100.0 * float64(r.wawAgree) / float64(r.totalRows)
	chunkPct := 100.0 * float64(r.chunkAgree) / float64(r.totalRows)
	out := fmt.Sprintf("%s→%s: %d rows | WAW agree %d/%d (%.0f%%) | chunk_size agree %d/%d (%.0f%%)\n",
		pair.source, pair.target,
		r.totalRows, r.wawAgree, r.totalRows, wawPct,
		r.chunkAgree, r.totalRows, chunkPct,
	)
	if len(r.disagreements) == 0 {
		return out
	}
	out += "  disagreements (recorded → predicted; throughput rows/s; retries):\n"
	maxShown := len(r.disagreements)
	if maxShown > 10 {
		maxShown = 10
	}
	for _, d := range r.disagreements[:maxShown] {
		out += fmt.Sprintf("  - [%s] avg=%dB cpu=%d mem=%dGB ai=%v | WAW %d→%d | chunk %d→%d | %.0f rows/s, %d retries\n",
			d.timestamp, d.avgRowBytes, d.cpuCores, d.memoryGB, d.wasAIUsed,
			d.recordedWAW, d.predictedWAW,
			d.recordedChunk, d.predictedChunk,
			d.finalThroughput, d.chunkRetryCount,
		)
	}
	if r.disagreementsTotal > maxShown {
		out += fmt.Sprintf("  ... and %d more\n", r.disagreementsTotal-maxShown)
	}
	return out
}
