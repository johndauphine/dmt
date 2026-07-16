package tuning

import "testing"

func useDerivedProjectionConnectionPolicy(in *Input) {
	in.ProjectionConnectionPolicyKnown = true
}

func stampDerivedProjectionConnections(row *HistoryRecord) {
	row.MaxSourceConnections, row.MaxTargetConnections = ConnectionPoolSizes(
		row.Workers,
		row.ParallelReaders,
		row.WriteAheadWriters,
	)
}

func TestFilterProjectedByContextValidatesPerActionConnectionPolicy(t *testing.T) {
	in := Input{
		ProjectionContextFingerprint:    "projection-v2",
		ProjectionConnectionPolicyKnown: true,
	}
	row := HistoryRecord{
		Workers:                      4,
		WriteAheadWriters:            3,
		ParallelReaders:              2,
		SafetyProjected:              true,
		ProjectionContextFingerprint: in.ProjectionContextFingerprint,
	}
	stampDerivedProjectionConnections(&row)

	kept, dropped := filterProjectedByContext([]HistoryRecord{row}, in)
	if len(kept) != 1 || dropped != 0 {
		t.Fatalf("matching derived pool row: kept=%d dropped=%d", len(kept), dropped)
	}

	mismatch := row
	mismatch.MaxTargetConnections++
	kept, dropped = filterProjectedByContext([]HistoryRecord{mismatch}, in)
	if len(kept) != 0 || dropped != 1 {
		t.Fatalf("mismatched derived pool row: kept=%d dropped=%d", len(kept), dropped)
	}

	fixed := in
	fixed.ProjectionMaxSourceConnectionsPinned = true
	fixed.ProjectionMaxSourceConnections = 5
	fixed.ProjectionMaxTargetConnectionsPinned = true
	fixed.ProjectionMaxTargetConnections = 7
	row.MaxSourceConnections = 5
	row.MaxTargetConnections = 7
	kept, dropped = filterProjectedByContext([]HistoryRecord{row}, fixed)
	if len(kept) != 1 || dropped != 0 {
		t.Fatalf("matching fixed pool row: kept=%d dropped=%d", len(kept), dropped)
	}

	unknownPolicy := in
	unknownPolicy.ProjectionConnectionPolicyKnown = false
	kept, dropped = filterProjectedByContext([]HistoryRecord{row}, unknownPolicy)
	if len(kept) != 0 || dropped != 1 {
		t.Fatalf("unknown pool policy did not fail closed: kept=%d dropped=%d", len(kept), dropped)
	}

	ordinary := HistoryRecord{FinalThroughput: 1}
	kept, dropped = filterProjectedByContext([]HistoryRecord{ordinary}, Input{})
	if len(kept) != 1 || dropped != 0 {
		t.Fatalf("ordinary history became projection-gated: kept=%d dropped=%d", len(kept), dropped)
	}
}
