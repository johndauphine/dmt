package transfer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// executeCompositeKeysetPagination pages an all-integer composite-PK table
// with tuple keyset — WHERE (a,b) > (?,?) ORDER BY a,b (#616). It is a single
// reader (no range split): O(page) per query with an index, unlike the
// ROW_NUMBER fallback whose window re-scans deepen with each chunk. Integer
// components keep it collation-free and aggregate-free, so it is safe across
// engines. Resume replays through duplicate-safe writes rather than a tuple
// range-DELETE, which keeps target-side comparison out of the picture.
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
	db := srcPool.DB()

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
		newAckHandler: func(wp *writerPool, cb tunerCallbacks, saver ProgressSaver) func(writeAck) {
			coord = newCompositeCheckpointCoordinator(saver, job, partitionID, partitionRows, resumeRowsDone, wp.written, cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		saveFinal: func(last chunkResult, totalTransferred int64) {
			if job.Saver == nil || job.TaskID <= 0 {
				return
			}
			finalTuple := coord.finalTuple(last.tuple())
			if finalTuple == nil {
				return
			}
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalTuple, totalTransferred, partitionRows, encodeCompositeTuple(finalTuple)); err != nil {
				logging.Warn("Checkpoint save failed for %s: %v", job.Table.Name, err)
			}
		},
	})
}

// compositeKeysetProducer is the single reader for tuple keyset pagination.
type compositeKeysetProducer struct {
	db         *sql.DB
	dialect    driver.Dialect
	colList    string
	tableHint  string
	job        Job
	pkCols     []string
	pkIdxs     []int
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

		// Extract the watermark tuple from the last row.
		lastRow := chunk[len(chunk)-1]
		newTuple := make([]any, len(p.pkIdxs))
		for i, idx := range p.pkIdxs {
			newTuple[i] = lastRow[idx]
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
	written        func() int64
	checkpointFreq func() int

	seq             ackSequencer
	completedChunks int
	lastTuple       []any
}

func newCompositeCheckpointCoordinator(saver ProgressSaver, job Job, partitionID *int, rowsTotal, resumeRowsDone int64, written func() int64, checkpointFreq func() int) *compositeCheckpointCoordinator {
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
		written:        written,
		checkpointFreq: checkpointFreq,
	}
}

func (c *compositeCheckpointCoordinator) onAck(ack writeAck) {
	if c == nil {
		return
	}
	c.seq.feed(ack, func(a writeAck) {
		if t, ok := a.lastPK.([]any); ok {
			c.lastTuple = t
		}
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 && c.lastTuple != nil {
			rowsDone := c.resumeRowsDone + c.written()
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

// encodeCompositeTuple / decodeCompositeTuple round-trip an integer PK tuple
// through JSON while preserving int64 precision (json.Number, like the #464
// per-range watermark decode) — a plain unmarshal into []any yields float64,
// which rounds BIGINT keys past 2^53 and could shift the resume watermark.
func encodeCompositeTuple(tuple []any) string {
	if len(tuple) == 0 {
		return ""
	}
	b, err := json.Marshal(tuple)
	if err != nil {
		return ""
	}
	return string(b)
}

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
		if n, ok := v.(json.Number); ok {
			if iv, err := n.Int64(); err == nil {
				out[i] = iv
				continue
			}
		}
		out[i] = v
	}
	return out
}
