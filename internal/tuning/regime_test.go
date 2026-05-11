package tuning

import (
	"strings"
	"testing"
)

// TestClassifyRegime_Same verifies the happy path: identical CPU /
// memory / DB-tuning means RegimeSame and no deltas.
func TestClassifyRegime_Same(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{CPUCores: 16, MemoryGB: 48}
	tuning := DBTuning{TargetSharedBuffersMB: 8192, TargetSyncCommit: "off"}
	histTuning := tuning // identical

	hist.TargetSharedBuffersMB = histTuning.TargetSharedBuffersMB
	hist.TargetSyncCommit = histTuning.TargetSyncCommit

	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeSame {
		t.Errorf("label: got %q, want %q", label, RegimeSame)
	}
	if len(deltas) != 0 {
		t.Errorf("deltas: got %v, want none", deltas)
	}
}

// TestClassifyRegime_UnknownOnMissingFields locks in the pre-#144
// fallback: rows without CPU + memory data classify as Unknown so they
// get filtered out (treated as weak evidence) instead of accidentally
// matching as Same.
func TestClassifyRegime_UnknownOnMissingFields(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	cases := []HistoryRecord{
		{CPUCores: 0, MemoryGB: 48},
		{CPUCores: 16, MemoryGB: 0},
		{CPUCores: 0, MemoryGB: 0},
	}
	for i, hist := range cases {
		label, _ := ClassifyRegime(hist, current, DBTuning{})
		if label != RegimeUnknown {
			t.Errorf("case %d: got %q, want %q", i, label, RegimeUnknown)
		}
	}
}

// TestClassifyRegime_HardwareTolerances locks the boundary behavior on
// CPU and memory similarity. CPU threshold is max(2, current/5); memory
// must agree within 20%.
func TestClassifyRegime_HardwareTolerances(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48} // CPU threshold = 16/5 = 3
	cases := []struct {
		name     string
		hist     HistoryRecord
		wantSame bool
	}{
		{"identical", HistoryRecord{CPUCores: 16, MemoryGB: 48}, true},
		{"cpu within threshold", HistoryRecord{CPUCores: 13, MemoryGB: 48}, true},
		{"cpu beyond threshold", HistoryRecord{CPUCores: 12, MemoryGB: 48}, false}, // 4 > 3
		{"memory within 20%", HistoryRecord{CPUCores: 16, MemoryGB: 40}, true},     // 40/48 = 83%
		{"memory beyond 20%", HistoryRecord{CPUCores: 16, MemoryGB: 38}, false},    // 38/48 = 79%
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, deltas := ClassifyRegime(tc.hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("%s: label=%q, isSame=%v, wantSame=%v, deltas=%v",
					tc.name, label, isSame, tc.wantSame, deltas)
			}
		})
	}
}

// TestClassifyRegime_DBTuningDifferences exercises each DB-tuning field
// independently to verify they all contribute to RegimeDifferentTuning.
func TestClassifyRegime_DBTuningDifferences(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	baseHist := HistoryRecord{CPUCores: 16, MemoryGB: 48}
	curr := DBTuning{
		TargetSharedBuffersMB:   8192,
		TargetSyncCommit:        "on",
		TargetFsync:             "on",
		TargetFullPageWrites:    "on",
		TargetMaxWALSizeMB:      4096,
		TargetWALLevel:          "replica",
		SourceMaxServerMemoryMB: 16384,
	}
	cases := []struct {
		name string
		mut  func(*HistoryRecord)
	}{
		{"shared_buffers differs >10%", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = 4096
			// fill the rest from current to isolate this delta
			h.TargetSyncCommit = curr.TargetSyncCommit
			h.TargetFsync = curr.TargetFsync
		}},
		{"sync_commit differs", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = curr.TargetSharedBuffersMB
			h.TargetSyncCommit = "off"
			h.TargetFsync = curr.TargetFsync
		}},
		{"max_wal differs >10%", func(h *HistoryRecord) {
			h.TargetSharedBuffersMB = curr.TargetSharedBuffersMB
			h.TargetSyncCommit = curr.TargetSyncCommit
			h.TargetMaxWALSizeMB = 1024
		}},
		{"mssql_max_server_memory differs >10%", func(h *HistoryRecord) {
			h.SourceMaxServerMemoryMB = 4096
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := baseHist
			tc.mut(&h)
			label, deltas := ClassifyRegime(h, current, curr)
			if label != RegimeDifferentTuning {
				t.Errorf("got %q (deltas=%v), want %q", label, deltas, RegimeDifferentTuning)
			}
		})
	}
}

// TestClassifyRegime_SizeTolerance locks in the 10% size tolerance —
// 8000MB vs 8192MB shouldn't count as a regime change.
func TestClassifyRegime_SizeTolerance(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:              16,
		MemoryGB:              48,
		TargetSharedBuffersMB: 8000,
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192} // 8000/8192 = 97.6% → within 10%
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeSame {
		t.Errorf("got %q (deltas=%v), want %q", label, deltas, RegimeSame)
	}
}

// TestClassifyRegime_DifferentHWAndTuning verifies the combined label
// fires when both axes differ.
func TestClassifyRegime_DifferentHWAndTuning(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:              4, // way fewer cores
		MemoryGB:              48,
		TargetSharedBuffersMB: 1024, // way smaller buffers
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192}
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeDifferentHWAndTuning {
		t.Errorf("got %q, want %q", label, RegimeDifferentHWAndTuning)
	}
	if len(deltas) < 2 {
		t.Errorf("expected at least 2 deltas (hw + tuning); got %d: %v", len(deltas), deltas)
	}
}

// TestClassifyRegime_WorkloadDiffers covers the #198 workload-population
// check: identical hardware and DB tuning, but the dataset shape differs
// materially.
func TestClassifyRegime_WorkloadDiffers(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	cases := []struct {
		name     string
		hist     HistoryRecord
		wantSame bool
	}{
		// Row-count axis. 3.0× tolerance.
		// "just inside 3×" uses 34M not the round 100M/3 = 33,333,333: that
		// integer truncates down so the float ratio comes out as
		// 3.0000000003, tripping the inclusive (≤) boundary. 34M gives ratio
		// 2.94 — comfortably within tolerance and verifies the boundary
		// without depending on float precision.
		{"rows within 1.5×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 67_000_000, AvgRowBytes: 1000}, true},
		{"rows just inside 3×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 34_000_000, AvgRowBytes: 1000}, true},
		{"rows beyond 3× (5×)", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 20_000_000, AvgRowBytes: 1000}, false},
		// Avg-row-bytes axis. 2.0× tolerance.
		{"avg_row_bytes within 1.5×", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 600}, true},
		{"avg_row_bytes at 2× exactly", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 500}, true},
		{"avg_row_bytes beyond 2× (3×)", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 333}, false},
		// Missing values fall through (don't fire the rule).
		{"hist rows missing", HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 0, AvgRowBytes: 1000}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			label, deltas := ClassifyRegime(tc.hist, current, DBTuning{})
			isSame := label == RegimeSame
			if isSame != tc.wantSame {
				t.Errorf("got label=%q, isSame=%v, wantSame=%v, deltas=%v",
					label, isSame, tc.wantSame, deltas)
			}
			if !tc.wantSame && label != RegimeDifferentWorkload {
				t.Errorf("workload diff should yield RegimeDifferentWorkload, got %q", label)
			}
		})
	}
}

// TestClassifyRegime_WorkloadWinsOverHwAndTuning verifies that when
// workload differs alongside hw and/or tuning, the label collapses to
// DifferentWorkload — filterByRegime drops all three semantics so the
// combo distinction would only matter for diagnostics, and the deltas
// slice still carries each axis's delta.
func TestClassifyRegime_WorkloadWinsOverHwAndTuning(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{
		CPUCores:              4,             // hw differs
		MemoryGB:              48,
		TotalRows:             10_000_000,    // workload differs (10×)
		AvgRowBytes:           1000,
		TargetSharedBuffersMB: 1024,          // tuning differs (vs 8192)
	}
	tuning := DBTuning{TargetSharedBuffersMB: 8192}
	label, deltas := ClassifyRegime(hist, current, tuning)
	if label != RegimeDifferentWorkload {
		t.Errorf("got %q, want %q (workload should win)", label, RegimeDifferentWorkload)
	}
	if len(deltas) < 3 {
		t.Errorf("expected ≥3 deltas (hw + tuning + workload); got %d: %v", len(deltas), deltas)
	}
}

// TestClassifyRegime_WorkloadDeltaReadable spot-checks the human-readable
// delta string for the workload axis.
func TestClassifyRegime_WorkloadDeltaReadable(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48, TotalRows: 100_000_000, AvgRowBytes: 1000}
	hist := HistoryRecord{CPUCores: 16, MemoryGB: 48, TotalRows: 19_000_000, AvgRowBytes: 1000}
	_, deltas := ClassifyRegime(hist, current, DBTuning{})
	joined := strings.Join(deltas, "|")
	if !strings.Contains(joined, "total_rows: 19000000→100000000") {
		t.Errorf("workload delta missing or malformed: %q", joined)
	}
}

// TestClassifyRegime_DeltasAreReadable spot-checks the human-readable
// delta strings used in diagnostic logging.
func TestClassifyRegime_DeltasAreReadable(t *testing.T) {
	current := Input{CPUCores: 16, MemoryGB: 48}
	hist := HistoryRecord{
		CPUCores:         4,
		MemoryGB:         16,
		TargetSyncCommit: "off",
	}
	tuning := DBTuning{TargetSyncCommit: "on"}
	_, deltas := ClassifyRegime(hist, current, tuning)
	joined := strings.Join(deltas, "|")
	if !strings.Contains(joined, "history=4c/16GB vs current=16c/48GB") {
		t.Errorf("hw delta missing or malformed: %q", joined)
	}
	if !strings.Contains(joined, "pg_sync_commit: off→on") {
		t.Errorf("tuning delta missing or malformed: %q", joined)
	}
}

// --- Issue #215: exact-identity workload classifier ---

// mkIdentity helper returns an Input with all 8 identity fields set.
// Saves verbosity in the test fixtures below.
func mkIdentity() Input {
	return Input{
		SourceHost:     "src.example.com",
		SourcePort:     1433,
		SourceDatabase: "MyDB",
		SourceSchema:   "dbo",
		TargetHost:     "tgt.example.com",
		TargetPort:     5432,
		TargetDatabase: "mydb",
		TargetSchema:   "public",
		CPUCores:       16, MemoryGB: 48,
	}
}

// TestHasExactIdentity_FullTuple — happy path. All required fields set.
func TestHasExactIdentity_FullTuple(t *testing.T) {
	if !hasExactIdentity(mkIdentity()) {
		t.Error("full identity tuple should be detected as complete")
	}
}

// TestHasExactIdentity_MissingHostFails — missing required field.
// Schemas may legitimately be empty (MySQL has no schema concept), so
// empty schema does NOT disqualify; empty host does.
func TestHasExactIdentity_MissingHostFails(t *testing.T) {
	in := mkIdentity()
	in.SourceHost = ""
	if hasExactIdentity(in) {
		t.Error("missing SourceHost should fail hasExactIdentity")
	}
}

// TestHasExactIdentity_MissingPortFails — port is required because
// SQLite NULL != 0 in equality, so a missing port would silently
// prevent any matches.
func TestHasExactIdentity_MissingPortFails(t *testing.T) {
	in := mkIdentity()
	in.SourcePort = 0
	if hasExactIdentity(in) {
		t.Error("missing SourcePort should fail hasExactIdentity")
	}
}

// TestHasExactIdentity_EmptySchemaAllowed — drivers without schema
// concept (MySQL) leave the schema field empty. That's legitimate;
// the check should pass.
func TestHasExactIdentity_EmptySchemaAllowed(t *testing.T) {
	in := mkIdentity()
	in.SourceSchema = ""
	in.TargetSchema = ""
	if !hasExactIdentity(in) {
		t.Error("empty schemas should not disqualify identity (drivers without schema concept)")
	}
}

// TestFilterByExactIdentity_AllMatchPass — every row matches the input.
func TestFilterByExactIdentity_AllMatchPass(t *testing.T) {
	in := mkIdentity()
	rows := []HistoryRecord{
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
			FinalThroughput: 1_000_000},
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema,
			TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema,
			FinalThroughput: 1_100_000},
	}
	got := filterByExactIdentity(rows, in)
	if len(got) != 2 {
		t.Errorf("expected 2 matching rows, got %d", len(got))
	}
}

// TestFilterByExactIdentity_OneFieldDifferentRejected — exactly one
// field differs on each test row; all must be rejected.
func TestFilterByExactIdentity_OneFieldDifferentRejected(t *testing.T) {
	in := mkIdentity()
	cases := []struct {
		name string
		row  HistoryRecord
	}{
		{"source_host_diff", HistoryRecord{SourceHost: "OTHER", SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_port_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: 9999, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_db_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: "OTHER", SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"source_schema_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: "OTHER", TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_host_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: "OTHER", TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_port_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: 9999, TargetDatabase: in.TargetDatabase, TargetSchema: in.TargetSchema}},
		{"target_db_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: "OTHER", TargetSchema: in.TargetSchema}},
		{"target_schema_diff", HistoryRecord{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase, SourceSchema: in.SourceSchema, TargetHost: in.TargetHost, TargetPort: in.TargetPort, TargetDatabase: in.TargetDatabase, TargetSchema: "OTHER"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterByExactIdentity([]HistoryRecord{tc.row}, in)
			if len(got) != 0 {
				t.Errorf("row with %s should be rejected; got %d matches", tc.name, len(got))
			}
		})
	}
}

// TestFilterByExactIdentity_PreIdentityRowsRejected — rows persisted
// before #215 have empty identity fields. Equality comparison rejects
// them, leaving them out of the Tier 1 cohort. That's correct: their
// identity is unrecoverable, so they shouldn't claim to match anything.
func TestFilterByExactIdentity_PreIdentityRowsRejected(t *testing.T) {
	in := mkIdentity()
	rows := []HistoryRecord{
		{}, // all-zero — represents a pre-#215 row
		{SourceHost: in.SourceHost, SourcePort: in.SourcePort, SourceDatabase: in.SourceDatabase}, // partial — also rejected
	}
	got := filterByExactIdentity(rows, in)
	if len(got) != 0 {
		t.Errorf("pre-#215 rows (empty identity) should not match; got %d", len(got))
	}
}

// TestTune_ExactIdentityCohort_FiresRegression — integration test for
// the Tier 1 dispatch. When the history holds ≥minRowsForRegression
// rows from the exact same identity, the tuner should fire regression
// on those alone (not the broader regime-comparable set).
func TestTune_ExactIdentityCohort_FiresRegression(t *testing.T) {
	current := mkIdentity()
	current.SourceDBType = "mssql"
	current.TargetDBType = "postgres"
	current.AvgRowBytes = 500

	// 30 matching-identity rows + 30 different-identity rows. Tier 1
	// should ignore the different-identity rows entirely.
	rows := make([]HistoryRecord, 0, 60)
	mkRow := func(host string, throughput float64, waw int) HistoryRecord {
		return HistoryRecord{
			SourceHost: host, SourcePort: current.SourcePort, SourceDatabase: current.SourceDatabase, SourceSchema: current.SourceSchema,
			TargetHost: current.TargetHost, TargetPort: current.TargetPort, TargetDatabase: current.TargetDatabase, TargetSchema: current.TargetSchema,
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			WriteAheadWriters: waw, ChunkSize: 50_000, AvgRowBytes: 500,
			ParallelReaders: 2, ReadAheadBuffers: 4, // match baseline()'s defaults so argmax covers a cell (#220/#222)
			FinalThroughput: throughput,
		}
	}
	// Matching identity: 30 rows, throughput peaks at WAW=4.
	for i := 0; i < 30; i++ {
		waw := (i % 6) + 1
		dev := waw - 4
		rows = append(rows, mkRow(current.SourceHost, 1_000_000.0-50_000.0*float64(dev*dev), waw))
	}
	// Different host: 30 rows with very different shape (throughput peaks
	// at WAW=1). If these leaked into the Tier 1 cohort, the regression
	// would mispredict.
	for i := 0; i < 30; i++ {
		waw := (i % 6) + 1
		rows = append(rows, mkRow("other-host.example.com", 100_000.0+200_000.0/float64(waw), waw))
	}

	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Tier 1 should have fired regression; reasoning should reflect that.
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 1 should fire regression on exact-identity cohort; got: %q", out.Reasoning)
	}
	// The "over N filtered rows" count should be 30 (the matching
	// identity cohort), not 60 (all rows). Tier 1 isolation matters.
	if !strings.Contains(out.Reasoning, "over 30 filtered rows") {
		t.Errorf("Tier 1 should train on 30 matching rows, not the full 60; got: %q", out.Reasoning)
	}
}

// TestTune_ExactIdentityCohortTooThin_FallsThroughToTier2 — when the
// exact-identity cohort has fewer than minRowsForRegression rows, the
// dispatch falls through to the Tier 2 regime filter. The rest of the
// rows (regime-comparable but not identity-matching) take over.
func TestTune_ExactIdentityCohortTooThin_FallsThroughToTier2(t *testing.T) {
	current := mkIdentity()
	current.SourceDBType = "mssql"
	current.TargetDBType = "postgres"
	current.AvgRowBytes = 500
	current.TotalRows = 100_000_000

	mkRow := func(host string, totalRows int64, throughput float64, waw int) HistoryRecord {
		return HistoryRecord{
			SourceHost: host, SourcePort: current.SourcePort, SourceDatabase: current.SourceDatabase, SourceSchema: current.SourceSchema,
			TargetHost: current.TargetHost, TargetPort: current.TargetPort, TargetDatabase: current.TargetDatabase, TargetSchema: current.TargetSchema,
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			TotalRows: totalRows, AvgRowBytes: 500,
			WriteAheadWriters: waw, ChunkSize: 50_000,
			ParallelReaders: 2, ReadAheadBuffers: 4, // match baseline()'s defaults (#220/#222)
			FinalThroughput: throughput,
		}
	}

	rows := make([]HistoryRecord, 0, 35)
	// 5 matching-identity rows — below regression floor.
	for i := 0; i < 5; i++ {
		rows = append(rows, mkRow(current.SourceHost, current.TotalRows, 1_000_000, (i%6)+1))
	}
	// 30 different-identity but regime-comparable rows (same workload,
	// different source host). Tier 2 will route through these.
	for i := 0; i < 30; i++ {
		rows = append(rows, mkRow("other-host.example.com", current.TotalRows, 800_000, (i%6)+1))
	}

	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Should NOT be from Tier 1 (only 5 matches); should fall through
	// to Tier 2 (regime-filtered, larger cohort). The "filtered rows"
	// count in the reasoning should be > 5 (some subset of the 35).
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 2 should fire regression on the 35-row cohort; got: %q", out.Reasoning)
	}
}

// TestTune_NoIdentityInInput_FallsThroughToTier2 — when the caller
// doesn't populate the identity fields (e.g., test fixtures or
// pre-#215 callers that haven't been updated), Tier 1 is skipped
// silently and the Tier 2 path takes over. Backward compatibility.
func TestTune_NoIdentityInInput_FallsThroughToTier2(t *testing.T) {
	current := Input{
		SourceDBType: "mssql", TargetDBType: "postgres",
		CPUCores: 16, MemoryGB: 48,
		AvgRowBytes: 500, TotalRows: 100_000_000,
		// No identity fields set.
	}
	rows := make([]HistoryRecord, 30)
	for i := range rows {
		waw := (i % 6) + 1
		rows[i] = HistoryRecord{
			SourceDBType: "mssql", TargetDBType: "postgres",
			CPUCores: 16, MemoryGB: 48,
			TotalRows: current.TotalRows, AvgRowBytes: 500,
			WriteAheadWriters: waw, ChunkSize: 50_000,
			ParallelReaders: 2, ReadAheadBuffers: 4, // baseline defaults (#220/#222)
			FinalThroughput: 800_000,
		}
	}
	profile := DriverProfile{Name: "postgres", BaselineWAW: 2, OptimumBulkChunkBytes: 25_000_000}
	out := Tune(current, profile, &stubHistory{rows: rows}, DBTuning{})

	// Tier 2 fires with the full 30-row cohort.
	if !strings.Contains(out.Reasoning, "regression-selected") {
		t.Errorf("Tier 2 should fire when identity is missing; got: %q", out.Reasoning)
	}
}
