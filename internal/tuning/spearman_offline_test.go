package tuning

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
)

// TestCellSpearmanOffline is a measurement harness for evaluating how well
// the regression *ranks* (WAW, ChunkSize, ParallelReaders, ReadAheadBuffers)
// cells against empirical per-cell mean throughput. Spearman ρ is the right
// loss for a tuner whose job is picking the best cell — R² penalizes the
// (irreducible) within-cell noise, which on real workloads is large enough
// to make a near-optimal ranker look broken.
//
// Loads history from a sqlite-dumped CSV; pipe-delimited; columns must match
// the SELECT in the docstring below. Path is taken from $DMT_SWEEP_CSV;
// skipped if the env var is unset or the file is missing, so the test is
// no-op in CI and on developer machines without a sweep dump.
//
// To produce the input and run the harness:
//
//	sqlite3 -separator '|' ~/.dmt/migrate.db \
//	  "SELECT source_db_type, target_db_type, workers, chunk_size,
//	          write_ahead_writers, parallel_readers, read_ahead_buffers,
//	          avg_row_size_bytes, total_rows, total_tables,
//	          large_table_threshold, COALESCE(final_throughput,0),
//	          ROUND(final_throughput * avg_row_size_bytes, 0),
//	          COALESCE(chunk_retry_count,0), cpu_cores, memory_gb,
//	          COALESCE(platform,''), COALESCE(target_shared_buffers_mb,0),
//	          COALESCE(target_synchronous_commit,''), COALESCE(target_fsync,''),
//	          COALESCE(target_full_page_writes,''),
//	          COALESCE(target_max_wal_size_mb,0), COALESCE(target_wal_level,''),
//	          COALESCE(source_max_server_memory_mb,0),
//	          COALESCE(source_host,''), COALESCE(source_port,0),
//	          COALESCE(source_database,''), COALESCE(source_schema,''),
//	          COALESCE(target_host,''), COALESCE(target_port,0),
//	          COALESCE(target_database,''), COALESCE(target_schema,'')
//	     FROM ai_tuning_history
//	    WHERE source_database='StackOverflow2010'" > /tmp/so2010.csv
//	DMT_SWEEP_CSV=/tmp/so2010.csv go test -v -run TestCellSpearmanOffline ./internal/tuning
func TestCellSpearmanOffline(t *testing.T) {
	csvPath := os.Getenv("DMT_SWEEP_CSV")
	if csvPath == "" {
		t.Skip("DMT_SWEEP_CSV unset — skipping offline harness")
	}
	f, err := os.Open(csvPath)
	if err != nil {
		t.Skipf("offline data missing at %s — skipping (%v)", csvPath, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '|'
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}

	rows := make([]HistoryRecord, 0, len(records))
	for i, rec := range records {
		if len(rec) < 32 {
			t.Fatalf("row %d: got %d fields, want 32", i, len(rec))
		}
		atoi := func(s string) int { v, _ := strconv.Atoi(s); return v }
		atoi64 := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
		atof := func(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

		rows = append(rows, HistoryRecord{
			SourceDBType:            rec[0],
			TargetDBType:            rec[1],
			Workers:                 atoi(rec[2]),
			ChunkSize:               atoi(rec[3]),
			WriteAheadWriters:       atoi(rec[4]),
			ParallelReaders:         atoi(rec[5]),
			ReadAheadBuffers:        atoi(rec[6]),
			AvgRowBytes:             atoi64(rec[7]),
			TotalRows:               atoi64(rec[8]),
			TotalTables:             atoi(rec[9]),
			FinalThroughput:         atof(rec[11]),
			FinalThroughputBytes:    int64(atof(rec[12])),
			ChunkRetryCount:         atoi(rec[13]),
			CPUCores:                atoi(rec[14]),
			MemoryGB:                atoi(rec[15]),
			Platform:                rec[16],
			TargetSharedBuffersMB:   atoi64(rec[17]),
			TargetSyncCommit:        rec[18],
			TargetFsync:             rec[19],
			TargetFullPageWrites:    rec[20],
			TargetMaxWALSizeMB:      atoi64(rec[21]),
			TargetWALLevel:          rec[22],
			SourceMaxServerMemoryMB: atoi64(rec[23]),
			SourceHost:              rec[24],
			SourcePort:              atoi(rec[25]),
			SourceDatabase:          rec[26],
			SourceSchema:            rec[27],
			TargetHost:              rec[28],
			TargetPort:              atoi(rec[29]),
			TargetDatabase:          rec[30],
			TargetSchema:            rec[31],
		})
	}
	t.Logf("loaded %d rows from %s", len(rows), csvPath)

	model, err := fitRegression(rows)
	if err != nil {
		t.Fatalf("fitRegression: %v", err)
	}

	// Group rows by (WAW, ChunkSize, PR, RAB) — that's a "cell" — and
	// compute empirical mean throughput per cell.
	type cellKey struct{ waw, cs, pr, rab int }
	type cellStats struct {
		n           int
		obsTotal    float64
		obsAvgBytes float64
	}
	cells := map[cellKey]*cellStats{}
	for _, r := range rows {
		k := cellKey{r.WriteAheadWriters, r.ChunkSize, r.ParallelReaders, r.ReadAheadBuffers}
		c, ok := cells[k]
		if !ok {
			c = &cellStats{}
			cells[k] = c
		}
		c.n++
		c.obsTotal += float64(r.FinalThroughputBytes)
	}

	type cellRow struct {
		k        cellKey
		predBPS  float64
		obsBPS   float64
		n        int
		predRank int
		obsRank  int
	}
	avgRowBytes := rows[0].AvgRowBytes
	src := rows[0].SourceDBType
	tgt := rows[0].TargetDBType

	cellRows := make([]cellRow, 0, len(cells))
	for k, c := range cells {
		obs := c.obsTotal / float64(c.n)
		csBytes := int64(k.cs) * avgRowBytes
		pred := model.Predict(k.waw, csBytes, k.pr, k.rab, src, tgt, "", avgRowBytes)
		cellRows = append(cellRows, cellRow{
			k: k, predBPS: pred, obsBPS: obs, n: c.n,
		})
	}

	// Assign ranks (1 = lowest). Average ranks for ties — small enough
	// cell count we can hand-roll it.
	assignRanks := func(getter func(i int) float64, setter func(i, r int)) {
		idx := make([]int, len(cellRows))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool { return getter(idx[a]) < getter(idx[b]) })
		for r, i := range idx {
			setter(i, r+1)
		}
	}
	assignRanks(
		func(i int) float64 { return cellRows[i].predBPS },
		func(i, r int) { cellRows[i].predRank = r },
	)
	assignRanks(
		func(i int) float64 { return cellRows[i].obsBPS },
		func(i, r int) { cellRows[i].obsRank = r },
	)

	// Spearman ρ = 1 − 6·Σd² / (n(n²−1))
	n := len(cellRows)
	d2 := 0
	for _, c := range cellRows {
		d := c.predRank - c.obsRank
		d2 += d * d
	}
	spearman := 1.0 - 6.0*float64(d2)/float64(n*(n*n-1))

	// Report
	bytesToMBs := func(b float64) float64 { return b / (1024 * 1024) }
	sort.SliceStable(cellRows, func(a, b int) bool { return cellRows[a].obsBPS > cellRows[b].obsBPS })
	t.Logf("")
	t.Logf("%-30s %4s  %10s %10s  %4s %4s", "cell (WAW,CS,PR,RAB)", "n", "pred MB/s", "obs MB/s", "rPR", "rOB")
	for _, c := range cellRows {
		label := fmt.Sprintf("(%d,%d,%d,%d)", c.k.waw, c.k.cs, c.k.pr, c.k.rab)
		t.Logf("%-30s %4d  %10.1f %10.1f  %4d %4d", label, c.n, bytesToMBs(c.predBPS), bytesToMBs(c.obsBPS), c.predRank, c.obsRank)
	}
	t.Logf("")
	t.Logf("n cells = %d, Σd² = %d", n, d2)
	t.Logf("Spearman ρ (predicted vs empirical-mean throughput, by cell) = %+.3f", spearman)

	if math.IsNaN(spearman) {
		t.Errorf("Spearman returned NaN")
	}
}
