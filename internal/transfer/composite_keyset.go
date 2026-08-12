package transfer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/progress"
)

// executeCompositeKeysetPagination pages a tuple-keyset-eligible table —
// composite PKs and single-column tuple-safe PKs not owned by the legacy
// parallel keyset path — with WHERE (a,…) > (?,…) ORDER BY a,… (#616/#629).
// It is a single reader (no range split): O(page) per query with an index,
// unlike the ROW_NUMBER
// fallback whose window re-scans deepen with each chunk. It never issues a
// MIN()/MAX() boundary query (the first chunk reads unbounded), so types
// without those aggregates (pg uuid) work. Eligible component types are
// vetted per engine in driver.tupleKeysetSafeComponent — collation-consistent
// text, uuid, decimal, and int64-safe integers. Date/time types stay on
// ROW_NUMBER because their driver and engine-specific binding semantics can
// make ORDER BY and the bound-watermark comparison disagree. Resume replays
// through duplicate-safe writes rather than a tuple range-DELETE, which keeps
// target-side comparison (and target collation) out of the picture entirely.
func executeCompositeKeysetPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeTuple []any,
	resumeRowsDone int64,
	targetTableName string,
	tuner RuntimeTuner,
	writeErrorAdjuster WriteErrorAdjuster,
) (*TransferStats, error) {
	db := sourceQueryerForJob(ctx, srcPool.DB(), job)

	srcDialect := driver.GetDialect(srcPool.DBType())
	if srcDialect == nil {
		return nil, fmt.Errorf("no dialect registered for source DB type %q", srcPool.DBType())
	}
	colList := srcDialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	valueConvs := srcDialect.ValueConverters(colTypes, tgtPool.DBType())
	convIdx := buildConvIdx(valueConvs)
	tableHint := srcDialect.TableHint(cfg.Migration.StrictConsistency)

	// Resolve the row-index of each PK column so the producer can extract the
	// watermark tuple from each scanned row.
	pkIdxs := make([]int, len(job.Table.PrimaryKey))
	for i, pk := range job.Table.PrimaryKey {
		pkIdxs[i] = -1
		for j, c := range cols {
			if c == pk {
				pkIdxs[i] = j
				break
			}
		}
		if pkIdxs[i] < 0 {
			return nil, fmt.Errorf("composite keyset: PK column %q not found in column list", pk)
		}
	}

	var partitionID *int
	partitionRows := job.Table.RowCount
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		partitionRows = job.Partition.RowCount
	}

	// Replayed rows on resume become no-ops via duplicate-safe writes (#616),
	// mirroring the ROW_NUMBER path — no tuple range-DELETE needed.
	idempotentOnDup := (job.IsResume || job.ReplayPossible) &&
		cfg.Migration.TargetMode != "upsert" && job.Table.HasPK()

	producer := &compositeKeysetProducer{
		db:          db,
		dialect:     srcDialect,
		colList:     colList,
		tableHint:   tableHint,
		job:         job,
		pkCols:      job.Table.PrimaryKey,
		pkIdxs:      pkIdxs,
		srcDBType:   srcPool.DBType(),
		valueConvs:  valueConvs,
		convIdx:     convIdx,
		numCols:     len(cols),
		resumeTuple: resumeTuple,
	}

	var coord *compositeCheckpointCoordinator
	return runPipeline(ctx, pipelineConfig{
		cfg:             cfg,
		job:             job,
		tgtPool:         tgtPool,
		prog:            prog,
		tuner:           tuner,
		adjuster:        writeErrorAdjuster,
		producer:        producer,
		targetTableName: targetTableName,
		targetCols:      targetCols,
		colTypes:        colTypes,
		colSRIDs:        colSRIDs,
		idempotentOnDup: idempotentOnDup,
		resumeRowsDone:  resumeRowsDone,
		newAckHandler: func(cb tunerCallbacks, saver ProgressSaver) func(writeAck) ackRelease {
			coord = newCompositeCheckpointCoordinator(saver, job, partitionID, partitionRows, resumeRowsDone, cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		saveFinal: func(last chunkResult, totalTransferred int64) (bool, error) {
			if job.Saver == nil || job.TaskID <= 0 {
				return false, nil
			}
			finalTuple := coord.finalTuple(last.tuple())
			if finalTuple == nil {
				return false, nil
			}
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalTuple, totalTransferred, partitionRows, encodeCompositeTuple(finalTuple)); err != nil {
				return false, err
			}
			return true, nil
		},
	})
}

// compositeKeysetProducer is the single reader for tuple keyset pagination.
type compositeKeysetProducer struct {
	db         sourceQueryer
	dialect    driver.Dialect
	colList    string
	tableHint  string
	job        Job
	pkCols     []string
	pkIdxs     []int
	srcDBType  string
	valueConvs []func(any) any
	convIdx    []int
	numCols    int

	resumeTuple []any // resume watermark tuple, nil for a fresh transfer
}

func (p *compositeKeysetProducer) readerCount() int { return 1 }

func (p *compositeKeysetProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	lastPK := p.resumeTuple
	firstUnbounded := p.resumeTuple == nil
	seq := int64(0)

	for {
		select {
		case <-ctx.Done():
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return
		default:
		}

		if !env.memGuard.waitIfNeeded(ctx) {
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return
		}

		chunkSize := env.chunkSize()

		// The first fresh chunk reads unbounded (a tuple minimum can't be
		// decremented for a strict ">"), then pages with the real tuple
		// watermark (#616).
		unbounded := firstUnbounded && seq == 0
		hasLowerBound := !unbounded
		query := p.dialect.BuildCompositeKeysetQuery(p.colList, p.pkCols, p.job.Table.Schema, p.job.Table.Name, p.tableHint, hasLowerBound, p.job.DateFilter)
		args := p.dialect.BuildCompositeKeysetArgs(lastPK, chunkSize, hasLowerBound, p.job.DateFilter)

		queryStart := time.Now()
		rows, err := p.db.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("composite keyset query: %w", err)})
			return
		}

		scanStart := time.Now()
		chunk, chunkBytes, err := scanRows(rows, p.numCols, p.valueConvs, p.convIdx)
		rows.Close()
		scanTime := time.Since(scanStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("scanning rows: %w", err)})
			return
		}

		if len(chunk) == 0 {
			sendChunkOrCancel(ctx, out, chunkResult{done: true})
			return
		}

		// Extract the watermark tuple from the last row, normalizing each
		// component to a JSON-round-trippable, bind-safe Go value (#629).
		lastRow := chunk[len(chunk)-1]
		newTuple := make([]any, len(p.pkIdxs))
		for i, idx := range p.pkIdxs {
			newTuple[i] = normalizeTupleValue(lastRow[idx], p.srcDBType)
		}

		reserved, ok := env.acquireMem(ctx, chunkBytes)
		if !ok {
			if e := ctx.Err(); e != nil {
				sendChunkOrCancel(ctx, out, chunkResult{err: e})
			}
			return
		}

		if !sendChunkOrCancel(ctx, out, chunkResult{
			rows:      chunk,
			lastPK:    newTuple, // tuple travels as lastPK; retrieved via chunkResult.tuple()
			readerID:  0,
			seq:       seq,
			bytes:     reserved,
			queryTime: queryTime,
			scanTime:  scanTime,
			readEnd:   time.Now(),
		}) {
			return
		}
		lastPK = newTuple
		seq++

		if len(chunk) < chunkSize {
			sendChunkOrCancel(ctx, out, chunkResult{done: true})
			return
		}
	}
}

// tuple returns the PK watermark tuple carried by a composite chunk (stored
// in lastPK), or nil.
func (r chunkResult) tuple() []any {
	if t, ok := r.lastPK.([]any); ok {
		return t
	}
	return nil
}

// compositeCheckpointCoordinator persists the tuple watermark from ordered
// write acks. Like the ROW_NUMBER coordinator it is single-sequence; the
// watermark is the PK tuple rather than a row number, and it is stored in the
// range_state column via encodeCompositeTuple to preserve int64 precision
// (the legacy last_pk column round-trips through float64).
type compositeCheckpointCoordinator struct {
	saver          ProgressSaver
	taskID         int64
	tableName      string
	partitionID    *int
	rowsTotal      int64
	resumeRowsDone int64
	checkpointFreq func() int

	seq             ackSequencer
	completedChunks int
	lastTuple       []any

	// ackedRows counts rows from acks applied in sequence order — the rows
	// the persisted tuple watermark covers. See the keyset coordinator's
	// field for why the pool's write counter must not feed rows_done (#632).
	ackedRows int64
}

func newCompositeCheckpointCoordinator(saver ProgressSaver, job Job, partitionID *int, rowsTotal, resumeRowsDone int64, checkpointFreq func() int) *compositeCheckpointCoordinator {
	if saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq == nil {
		checkpointFreq = func() int { return 10 }
	}
	return &compositeCheckpointCoordinator{
		saver:          saver,
		taskID:         job.TaskID,
		tableName:      job.Table.Name,
		partitionID:    partitionID,
		rowsTotal:      rowsTotal,
		resumeRowsDone: resumeRowsDone,
		checkpointFreq: checkpointFreq,
	}
}

func (c *compositeCheckpointCoordinator) onAck(ack writeAck) ackRelease {
	if c == nil {
		return ackRelease{jobs: 1}
	}
	return c.seq.feed(ack, func(a writeAck) {
		if t, ok := a.lastPK.([]any); ok {
			c.lastTuple = t
		}
		c.ackedRows += a.rows
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 && c.lastTuple != nil {
			rowsDone := c.resumeRowsDone + c.ackedRows
			if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, c.lastTuple, rowsDone, c.rowsTotal, encodeCompositeTuple(c.lastTuple)); err != nil {
				logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
			}
		}
	})
}

func (c *compositeCheckpointCoordinator) finalTuple(fallback []any) []any {
	if c == nil || c.lastTuple == nil {
		return fallback
	}
	return c.lastTuple
}

// normalizeTupleValue converts a scanned PK component into the canonical Go
// value the tuple path uses for watermarks (#629). Non-SQLite []byte values
// become strings: MySQL text/decimal scan as []byte, which go-mssqldb would
// bind back as varbinary and which must compare under the source column type.
// SQLite keeps []byte as []byte, because a BLOB storage-class value in a
// text-affinity PK must bind back as BLOB for ORDER BY and strict-`>` to agree.
func normalizeTupleValue(v any, srcDBType string) any {
	if b, ok := v.([]byte); ok {
		if strings.EqualFold(srcDBType, "sqlite") {
			return b
		}
		return string(b)
	}
	return v
}

// convertersTouchPK reports whether the source dialect registers a value
// converter (#477) for any primary-key column (#629). The watermark tuple is
// extracted after converters run, so a converter-rewritten PK value (e.g.
// mssql uniqueidentifier []byte→string, or the datetime family's pre-year-1
// →nil) may no longer match the source column — such tables must stay on
// ROW_NUMBER. The static type allowlist already excludes today's converter
// types; this runtime gate keeps the invariant if catalogs ever grow custom
// converters.
func convertersTouchPK(d driver.Dialect, cols, colTypes []string, targetDBType string, pkCols []string) bool {
	if d == nil {
		return true // no dialect → cannot verify → be safe
	}
	convs := d.ValueConverters(colTypes, targetDBType)
	for _, pk := range pkCols {
		for i, c := range cols {
			if c == pk {
				if i < len(convs) && convs[i] != nil {
					return true
				}
				break
			}
		}
	}
	return false
}

// Tuple watermark persistence (#616/#629). The range_state column stores the
// tuple as a JSON array of type-tagged components so every eligible PK type
// round-trips exactly through a crash-resume:
//
//	{"t":"i","v":123}                          int64 (json.Number decode — a
//	                                           plain unmarshal yields float64,
//	                                           rounding BIGINT past 2^53)
//	{"t":"s","v":"abc"}                        UTF-8 string (text/uuid/decimal)
//	{"t":"rs","v":"..."}                       raw string bytes for invalid
//	                                           UTF-8 SQLite TEXT values
//	{"t":"b","v":"..."}                        []byte for SQLite BLOB
//	                                           storage-class PK values
//	{"t":"tm","v":"2024-01-15T10:30:00.5Z"}    time.Time (RFC3339Nano —
//	                                           preserves the instant; decode
//	                                           yields a time.Time that binds
//	                                           natively, avoiding string-
//	                                           format pitfalls per engine)
//	{"t":"f","v":1.5}                          float64 (sqlite NUMERIC-affinity
//	                                           scans; float64 IS the stored
//	                                           value there, so it is exact)
//
// decode also accepts the legacy pre-#629 format — a plain array of numbers
// (integer-composite checkpoints written by PR #628) — so an in-flight
// migration resumes across the upgrade.
const (
	tupleTagInt       = "i"
	tupleTagString    = "s"
	tupleTagRawString = "rs"
	tupleTagBytes     = "b"
	tupleTagTime      = "tm"
	tupleTagFloat     = "f"
)

type tupleComponentJSON struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

// encodeCompositeTuple renders a watermark tuple for persistence. Returns ""
// when the tuple is empty or holds a type the codec cannot round-trip
// exactly — the checkpoint row then carries only the legacy last_pk column,
// and resume degrades to its float64/string fallback rather than persisting
// a value that would decode wrong.
func encodeCompositeTuple(tuple []any) string {
	if len(tuple) == 0 {
		return ""
	}
	wire := make([]tupleComponentJSON, len(tuple))
	for i, v := range tuple {
		var tag string
		var val any
		switch x := v.(type) {
		case int64:
			tag, val = tupleTagInt, x
		case int32:
			tag, val = tupleTagInt, int64(x)
		case int:
			tag, val = tupleTagInt, int64(x)
		case string:
			if utf8.ValidString(x) {
				tag, val = tupleTagString, x
			} else {
				tag, val = tupleTagRawString, base64.StdEncoding.EncodeToString([]byte(x))
			}
		case []byte:
			tag, val = tupleTagBytes, base64.StdEncoding.EncodeToString(x)
		case time.Time:
			tag, val = tupleTagTime, x.Format(time.RFC3339Nano)
		case float64:
			tag, val = tupleTagFloat, x
		default:
			return "" // unknown component type — fall back to legacy last_pk
		}
		raw, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		wire[i] = tupleComponentJSON{T: tag, V: raw}
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return ""
	}
	return string(b)
}

// decodeCompositeTuple parses a persisted watermark tuple; nil for empty,
// malformed, or foreign input (e.g. the integer-keyset per-range watermark
// blob, which also lives in range_state but is an array of objects without
// a "t" tag).
func decodeCompositeTuple(s string) []any {
	if s == "" {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var raw []any
	if err := dec.Decode(&raw); err != nil || len(raw) == 0 {
		return nil
	}
	out := make([]any, len(raw))
	for i, v := range raw {
		switch x := v.(type) {
		case map[string]any:
			comp, ok := decodeTupleComponent(x)
			if !ok {
				return nil
			}
			out[i] = comp
		case json.Number:
			// Legacy #628 integer-composite format: plain number array.
			iv, err := x.Int64()
			if err != nil {
				return nil
			}
			out[i] = iv
		case string:
			// Legacy defensive path: plain string component.
			out[i] = x
		default:
			return nil
		}
	}
	return out
}

func decodeTupleComponent(m map[string]any) (any, bool) {
	tag, _ := m["t"].(string)
	val, hasVal := m["v"]
	if !hasVal {
		return nil, false
	}
	switch tag {
	case tupleTagInt:
		n, ok := val.(json.Number)
		if !ok {
			return nil, false
		}
		iv, err := n.Int64()
		if err != nil {
			return nil, false
		}
		return iv, true
	case tupleTagString:
		s, ok := val.(string)
		return s, ok
	case tupleTagRawString:
		s, ok := val.(string)
		if !ok {
			return nil, false
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, false
		}
		return string(b), true
	case tupleTagBytes:
		s, ok := val.(string)
		if !ok {
			return nil, false
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, false
		}
		return b, true
	case tupleTagTime:
		s, ok := val.(string)
		if !ok {
			return nil, false
		}
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, false
		}
		return tm, true
	case tupleTagFloat:
		n, ok := val.(json.Number)
		if !ok {
			return nil, false
		}
		fv, err := n.Float64()
		if err != nil {
			return nil, false
		}
		return fv, true
	}
	return nil, false
}
