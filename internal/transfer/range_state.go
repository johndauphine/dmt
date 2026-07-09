package transfer

import (
	"bytes"
	"encoding/json"
)

// Per-reader-range checkpoint state (#464). The keyset coordinator already
// tracks one watermark per PK range in memory; persisting all of them lets
// resume continue each range from its own watermark instead of restarting
// every reader at the single safe minimum and deleting the faster readers'
// completed work.

// resumeRange is one PK range's restored progress, decoded from the
// transfer_progress.range_state column.
type resumeRange struct {
	lastPK          any  // watermark or fresh lower bound
	maxPK           any  // original range upper bound (nil = unbounded)
	lastPKInclusive bool // true only when lastPK has not been transferred yet
	complete        bool
}

// rangeProgressJSON is the wire form persisted in range_state. Values
// round-trip through encoding/json exactly like the legacy last_pk column
// so both paths share one type convention (numbers decode as float64 —
// the same convention ProgressSaver.GetProgress has always used).
type rangeProgressJSON struct {
	Last      json.RawMessage `json:"last,omitempty"`
	Max       json.RawMessage `json:"max,omitempty"`
	Inclusive bool            `json:"inclusive,omitempty"`
	Complete  bool            `json:"complete,omitempty"`
}

// encodeKeysetRangeState renders the coordinator's per-range states for
// persistence. Returns "" on any marshal failure — the row then carries
// only the legacy single watermark, which resume handles as before.
func encodeKeysetRangeState(states []readerCheckpointState) string {
	if len(states) == 0 {
		return ""
	}
	wire := make([]rangeProgressJSON, len(states))
	for i := range states {
		wire[i].Complete = states[i].complete
		wire[i].Inclusive = states[i].lastPKInclusive
		if states[i].lastPK != nil {
			b, err := json.Marshal(states[i].lastPK)
			if err != nil {
				return ""
			}
			wire[i].Last = b
		}
		if states[i].maxPK != nil {
			b, err := json.Marshal(states[i].maxPK)
			if err != nil {
				return ""
			}
			wire[i].Max = b
		}
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return ""
	}
	return string(b)
}

// decodeKeysetRangeState parses a persisted range_state value. Returns nil
// for empty or malformed input — resume then falls back to the legacy
// single-watermark behavior rather than failing the run.
func decodeKeysetRangeState(s string) []resumeRange {
	if s == "" {
		return nil
	}
	var wire []rangeProgressJSON
	if err := json.Unmarshal([]byte(s), &wire); err != nil || len(wire) == 0 {
		return nil
	}
	out := make([]resumeRange, len(wire))
	for i, rp := range wire {
		out[i].lastPK = decodePKValue(rp.Last)
		out[i].maxPK = decodePKValue(rp.Max)
		out[i].lastPKInclusive = rp.Inclusive
		out[i].complete = rp.Complete
	}
	return out
}

// decodePKValue decodes one persisted PK bound, preserving int64
// precision (codex review on #464): a plain json.Unmarshal into any
// yields float64, which rounds BIGINT values past 2^53 and could shift
// a restored range bound — skipping rows at the end of the range.
func decodePKValue(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil
	}
	if n, ok := v.(json.Number); ok {
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
		return n.String()
	}
	return v
}

// restoredPKRanges converts decoded resume ranges into the reader range
// set plus per-range completion flags. Each incomplete range resumes from
// its own watermark; completed ranges keep a slot (the coordinator's
// readerID indexes into the full set) but spawn no reader. The persisted
// ranges win over the current parallel_readers setting — ranges converge
// naturally as they complete.
func restoredPKRanges(resumeRanges []resumeRange, fallbackMin any) ([]pkRange, []bool) {
	ranges := make([]pkRange, len(resumeRanges))
	completed := make([]bool, len(resumeRanges))
	for i, rr := range resumeRanges {
		start := rr.lastPK
		if start == nil {
			start = fallbackMin
		}
		ranges[i] = pkRange{minPK: start, maxPK: rr.maxPK, minInclusive: rr.lastPKInclusive}
		completed[i] = rr.complete
	}
	return ranges, completed
}
