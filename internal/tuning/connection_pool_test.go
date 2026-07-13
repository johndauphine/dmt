package tuning

import "testing"

func TestConnectionPoolSizes(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	overflowWorkers := (maxInt-4)/2 + 1

	tests := []struct {
		name              string
		workers           int
		parallelReaders   int
		writeAheadWriters int
		wantSource        int
		wantTarget        int
	}{
		{name: "normal", workers: 4, parallelReaders: 2, writeAheadWriters: 2, wantSource: 12, wantTarget: 12},
		{name: "asymmetric", workers: 3, parallelReaders: 5, writeAheadWriters: 1, wantSource: 19, wantTarget: 7},
		{name: "nonpositive workers", workers: 0, parallelReaders: 2, writeAheadWriters: 3, wantSource: 4, wantTarget: 4},
		{name: "nonpositive source fanout", workers: 3, parallelReaders: -1, writeAheadWriters: 2, wantSource: 4, wantTarget: 10},
		{name: "nonpositive target fanout", workers: 3, parallelReaders: 2, writeAheadWriters: 0, wantSource: 10, wantTarget: 4},
		{
			name:              "source overflow only",
			workers:           overflowWorkers,
			parallelReaders:   2,
			writeAheadWriters: 1,
			wantSource:        maxInt,
			wantTarget:        overflowWorkers + 4,
		},
		{
			name:              "target overflow only",
			workers:           overflowWorkers,
			parallelReaders:   1,
			writeAheadWriters: 2,
			wantSource:        overflowWorkers + 4,
			wantTarget:        maxInt,
		},
		{name: "both overflow", workers: maxInt, parallelReaders: maxInt, writeAheadWriters: maxInt, wantSource: maxInt, wantTarget: maxInt},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSource, gotTarget := ConnectionPoolSizes(tc.workers, tc.parallelReaders, tc.writeAheadWriters)
			if gotSource != tc.wantSource || gotTarget != tc.wantTarget {
				t.Fatalf("ConnectionPoolSizes(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.workers, tc.parallelReaders, tc.writeAheadWriters,
					gotSource, gotTarget, tc.wantSource, tc.wantTarget)
			}
		})
	}
}

type connectionPoolHistory struct {
	rows []HistoryRecord
}

func (h connectionPoolHistory) Records(_, _ string) ([]HistoryRecord, error) {
	return h.rows, nil
}

func TestTuneFinalConnectionPoolsTrackSelectedTuple(t *testing.T) {
	profile := DriverProfile{
		Name:                  "postgres",
		BaselineWAW:           4,
		OptimumBulkChunkBytes: 25_000_000,
	}
	baseInput := Input{
		CPUCores:               8,
		MemoryGB:               16,
		Platform:               "linux",
		SourceDBType:           "mssql",
		TargetDBType:           "postgres",
		AvgRowBytes:            500,
		RepresentativeRowBytes: 500,
	}

	historyRows := make([]HistoryRecord, 12)
	for i := range historyRows {
		historyRows[i] = HistoryRecord{
			WriteAheadWriters: 3,
			ChunkSize:         20_000,
			ParallelReaders:   2,
			ReadAheadBuffers:  4,
			AvgRowBytes:       500,
			FinalThroughput:   100_000 + float64(i),
			CPUCores:          8,
			MemoryGB:          16,
		}
	}
	identityInput := baseInput
	identityInput.SourceHost = "source"
	identityInput.SourcePort = 1433
	identityInput.SourceDatabase = "source_db"
	identityInput.SourceSchema = "dbo"
	identityInput.TargetHost = "target"
	identityInput.TargetPort = 5432
	identityInput.TargetDatabase = "target_db"
	identityInput.TargetSchema = "public"
	identityRows := append([]HistoryRecord(nil), historyRows...)
	for i := range identityRows {
		identityRows[i].SourceHost = identityInput.SourceHost
		identityRows[i].SourcePort = identityInput.SourcePort
		identityRows[i].SourceDatabase = identityInput.SourceDatabase
		identityRows[i].SourceSchema = identityInput.SourceSchema
		identityRows[i].TargetHost = identityInput.TargetHost
		identityRows[i].TargetPort = identityInput.TargetPort
		identityRows[i].TargetDatabase = identityInput.TargetDatabase
		identityRows[i].TargetSchema = identityInput.TargetSchema
	}

	tests := []struct {
		name                string
		input               Input
		history             HistoryProvider
		wantSelectedWAW     int
		wantSelectedReaders int
	}{
		{
			name:                "baseline",
			input:               baseInput,
			wantSelectedWAW:     4,
			wantSelectedReaders: 2,
		},
		{
			name: "forced exploration",
			input: func() Input {
				in := baseInput
				in.ForceExplore = true
				return in
			}(),
			wantSelectedWAW:     1,
			wantSelectedReaders: 2,
		},
		{
			name:                "history selection",
			input:               baseInput,
			history:             connectionPoolHistory{rows: historyRows},
			wantSelectedWAW:     3,
			wantSelectedReaders: 2,
		},
		{
			name:                "exact-identity history selection",
			input:               identityInput,
			history:             connectionPoolHistory{rows: identityRows},
			wantSelectedWAW:     3,
			wantSelectedReaders: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := Tune(tc.input, profile, tc.history, DBTuning{})
			if out.WriteAheadWriters != tc.wantSelectedWAW || out.ParallelReaders != tc.wantSelectedReaders {
				t.Fatalf("selection path returned WAW/PR=(%d,%d), want (%d,%d)",
					out.WriteAheadWriters, out.ParallelReaders, tc.wantSelectedWAW, tc.wantSelectedReaders)
			}
			wantSource, wantTarget := ConnectionPoolSizes(out.Workers, out.ParallelReaders, out.WriteAheadWriters)
			if out.MaxSourceConnections != wantSource || out.MaxTargetConnections != wantTarget {
				t.Fatalf("final pools = (%d,%d), want (%d,%d) from final tuple %+v",
					out.MaxSourceConnections, out.MaxTargetConnections, wantSource, wantTarget, out)
			}
		})
	}
}
