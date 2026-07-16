package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// compositeResumeRange is one leading-component range restored from a
// versioned tuple range_state envelope. A completed range is retained by ID
// but never re-queued, preserving per-range ack sequencing across resume.
type compositeResumeRange struct {
	min          int64
	max          int64
	minInclusive bool
	tuple        []any
	complete     bool
}

// executeParallelCompositeKeysetPagination is the conservative parallel
// extension of tuple keyset (#667). It only claims a job after the leading PK
// component proved int64-safe at runtime; every other shape falls through to
// the established single-reader tuple path.
func executeParallelCompositeKeysetPagination(
	ctx context.Context,
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	cols, targetCols, colTypes []string,
	colSRIDs []int,
	prog *progress.Tracker,
	resumeRanges []compositeResumeRange,
	resumeRowsDone int64,
	targetTableName string,
	tuner RuntimeTuner,
	writeErrorAdjuster WriteErrorAdjuster,
) (*TransferStats, bool, error) {
	if cfg.Migration.ParallelReaders <= 1 || job.Partition != nil {
		return nil, false, nil
	}
	if !compositeRangeLeadingTypeEligible(job.Table) {
		return nil, false, nil
	}

	dialect := driver.GetDialect(srcPool.DBType())
	if dialect == nil || !dialect.SupportsCompositeRangeKeyset() {
		return nil, false, nil
	}
	db := sourceQueryerForJob(ctx, srcPool.DB(), job)
	tableHint := dialect.TableHint(cfg.Migration.StrictConsistency)
	numReaders := cfg.Migration.ParallelReaders
	var queryerForWorker sourceQueryerFactory
	if cfg.Migration.StrictConsistency {
		strategyName, strategy, err := resolveStrictReaderStrategyForScope(srcPool.DBType(), cfg.Migration.StrictConsistencyScope)
		if err != nil {
			return nil, true, err
		}
		if strategy == nil || !strategy.perJobParallel() {
			return nil, false, nil
		}
		queryerForWorker = sourceQueryerFactoryForJob(ctx, job)
		if queryerForWorker == nil {
			return nil, false, nil
		}
		var joins, clamped bool
		numReaders, joins, clamped = strictKeysetReaderPlan(true, strategy, cfg.Migration.ParallelReaders, cfg.Migration.MaxSourceConnections)
		if !joins {
			return nil, false, nil
		}
		if clamped {
			logging.Warn("Table %s: strict_consistency composite readers clamped from %d to %d for strategy %s and max_source_connections=%d", job.Table.Name, cfg.Migration.ParallelReaders, numReaders, strategyName, cfg.Migration.MaxSourceConnections)
		}
	}

	pkIdxs := compositePKIndexes(job.Table.PrimaryKey, cols)
	if pkIdxs == nil {
		return nil, true, fmt.Errorf("composite keyset: primary-key column missing from select list")
	}

	ranges := resumeRanges
	if len(ranges) == 0 {
		min, max, eligible, err := compositeLeadingBounds(ctx, db, dialect, job, tableHint)
		if err != nil {
			return nil, true, err
		}
		if !eligible {
			return nil, false, nil
		}
		numRanges := keysetWorkRangeCount(min, max, cfg.Migration.ParallelReaders, cfg.Migration.ChunkSize)
		for _, r := range splitPKRange(min, max, numRanges, true) {
			lo, okLo := parseNumericPK(r.minPK)
			hi, okHi := parseNumericPK(r.maxPK)
			if !okLo || !okHi {
				return nil, false, nil
			}
			ranges = append(ranges, compositeResumeRange{min: lo, max: hi, minInclusive: r.minInclusive})
		}
	}
	if len(ranges) == 0 {
		return nil, false, nil
	}

	if numReaders > len(ranges) {
		numReaders = len(ranges)
	}
	if numReaders < 1 {
		return nil, false, nil
	}

	colList := dialect.ColumnListForSelect(cols, colTypes, tgtPool.DBType())
	valueConvs := dialect.ValueConverters(colTypes, tgtPool.DBType())
	producer := &compositeParallelProducer{
		db:               db,
		queryerForWorker: queryerForWorker,
		dialect:          dialect,
		colList:          colList,
		tableHint:        tableHint,
		job:              job,
		pkCols:           job.Table.PrimaryKey,
		pkIdxs:           pkIdxs,
		srcDBType:        srcPool.DBType(),
		valueConvs:       valueConvs,
		convIdx:          buildConvIdx(valueConvs),
		numCols:          len(cols),
		ranges:           ranges,
		numReaders:       numReaders,
	}

	var partitionID *int
	rowsTotal := job.Table.RowCount
	if job.Partition != nil {
		partitionID = &job.Partition.PartitionID
		rowsTotal = job.Partition.RowCount
	}
	idempotentOnDup := (job.IsResume || job.ReplayPossible) && cfg.Migration.TargetMode != "upsert" && job.Table.HasPK()

	var coord *compositeRangeCheckpointCoordinator
	stats, err := runPipeline(ctx, pipelineConfig{
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
			coord = newCompositeRangeCheckpointCoordinator(saver, job, ranges, partitionID, rowsTotal, resumeRowsDone, cb.checkpointFreq)
			if coord == nil {
				return nil
			}
			return coord.onAck
		},
		onRangeDone: func(readerID int, nextSeq int64) {
			coord.markRangeDone(readerID, nextSeq)
		},
		saveFinal: func(last chunkResult, totalTransferred int64) (bool, error) {
			if job.Saver == nil || job.TaskID <= 0 {
				return false, nil
			}
			finalTuple := coord.finalTuple(last.tuple())
			if finalTuple == nil {
				return false, nil
			}
			coord.markComplete()
			if err := job.Saver.SaveProgress(job.TaskID, job.Table.Name, partitionID, finalTuple, totalTransferred, rowsTotal, coord.rangeState()); err != nil {
				return false, err
			}
			return true, nil
		},
	})
	return stats, true, err
}

func compositePKIndexes(pkCols, cols []string) []int {
	idxs := make([]int, len(pkCols))
	for i, pk := range pkCols {
		idxs[i] = -1
		for j, col := range cols {
			if col == pk {
				idxs[i] = j
				break
			}
		}
		if idxs[i] < 0 {
			return nil
		}
	}
	return idxs
}

// compositeRangeLeadingTypeEligible keeps the range split numeric even when a
// text PK happens to contain digit strings. Text ordering/collation and
// numeric range predicates are different orderings, so runtime ParseInt alone
// would be insufficiently conservative for values such as "10" and "2".
// The bound probe below still proves the actual MIN/MAX values fit int64.
func compositeRangeLeadingTypeEligible(table driver.Table) bool {
	if len(table.PrimaryKey) == 0 {
		return false
	}
	for _, col := range table.Columns {
		if col.Name != table.PrimaryKey[0] {
			continue
		}
		typeName := strings.ToLower(strings.TrimSpace(col.DataType))
		if i := strings.IndexAny(typeName, " ("); i >= 0 {
			typeName = typeName[:i]
		}
		switch typeName {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
			"int2", "int4", "int8", "serial", "smallserial", "bigserial",
			"decimal", "numeric", "number", "real", "float", "double", "money", "smallmoney":
			return true
		default:
			return false
		}
	}
	return false
}

// compositeLeadingBounds is intentionally a narrow eligibility probe. The
// single-reader tuple path needs no aggregates, so an empty or non-int64-safe
// leading component simply preserves that path instead of changing behavior.
func compositeLeadingBounds(ctx context.Context, db sourceQueryer, dialect driver.Dialect, job Job, tableHint string) (int64, int64, bool, error) {
	if len(job.Table.PrimaryKey) == 0 {
		return 0, 0, false, nil
	}
	col := dialect.QuoteIdentifier(job.Table.PrimaryKey[0])
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s %s", col, col, dialect.QualifyTable(job.Table.Schema, job.Table.Name), tableHint)
	var min, max any
	if err := db.QueryRowContext(ctx, query).Scan(&min, &max); err != nil {
		return 0, 0, false, fmt.Errorf("composite keyset range boundaries: %w", err)
	}
	lo, okLo := parseNumericPK(min)
	hi, okHi := parseNumericPK(max)
	if !okLo || !okHi || hi < lo {
		return 0, 0, false, nil
	}
	return lo, hi, true, nil
}

type compositeParallelProducer struct {
	db               sourceQueryer
	queryerForWorker sourceQueryerFactory
	dialect          driver.Dialect
	colList          string
	tableHint        string
	job              Job
	pkCols           []string
	pkIdxs           []int
	srcDBType        string
	valueConvs       []func(any) any
	convIdx          []int
	numCols          int
	ranges           []compositeResumeRange
	numReaders       int

	rangesPerWorker []int64
	rowsPerWorker   []int64
}

func (p *compositeParallelProducer) readerCount() int { return p.numReaders }

func (p *compositeParallelProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	queue := make(chan int, len(p.ranges))
	for rangeID, r := range p.ranges {
		if !r.complete {
			queue <- rangeID
		}
	}
	close(queue)
	p.rangesPerWorker = make([]int64, p.numReaders)
	p.rowsPerWorker = make([]int64, p.numReaders)

	var wg sync.WaitGroup
	for i := 0; i < p.numReaders; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			queryer := p.db
			if p.queryerForWorker != nil {
				var release func()
				var err error
				queryer, release, err = p.queryerForWorker(ctx, workerID)
				if err != nil {
					sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("starting composite reader %d: %w", workerID, err)})
					return
				}
				defer release()
			}
			for rangeID := range queue {
				p.rangesPerWorker[workerID]++
				rows, ok := p.readRange(ctx, queryer, env, out, rangeID, p.ranges[rangeID])
				p.rowsPerWorker[workerID] += rows
				if !ok {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	logging.Debug("Composite tuple ranges for %s: ranges_per_worker=%v rows_per_worker=%v", p.job.Table.Name, p.rangesPerWorker, p.rowsPerWorker)
}

func (p *compositeParallelProducer) readRange(ctx context.Context, queryer sourceQueryer, env pipelineEnv, out chan<- chunkResult, rangeID int, r compositeResumeRange) (int64, bool) {
	lastTuple := r.tuple
	seq := int64(0)
	var rowsRead int64
	for {
		select {
		case <-ctx.Done():
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return rowsRead, false
		default:
		}
		if !env.memGuard.waitIfNeeded(ctx) {
			sendChunkOrCancel(ctx, out, chunkResult{err: ctx.Err()})
			return rowsRead, false
		}
		chunkSize := env.chunkSize()
		hasLowerBound := len(lastTuple) > 0
		query := p.dialect.BuildCompositeKeysetRangeQuery(p.colList, p.pkCols, p.job.Table.Schema, p.job.Table.Name, p.tableHint, hasLowerBound, r.minInclusive, p.job.DateFilter)
		args := p.dialect.BuildCompositeKeysetRangeArgs(lastTuple, r.min, r.max, chunkSize, hasLowerBound, p.job.DateFilter)
		queryStart := time.Now()
		rows, err := queryer.QueryContext(ctx, query, args...)
		queryTime := time.Since(queryStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("composite range query: %w", err)})
			return rowsRead, false
		}
		scanStart := time.Now()
		chunk, chunkBytes, err := scanRows(rows, p.numCols, p.valueConvs, p.convIdx)
		rows.Close()
		scanTime := time.Since(scanStart)
		if err != nil {
			sendChunkOrCancel(ctx, out, chunkResult{err: fmt.Errorf("scanning composite range: %w", err)})
			return rowsRead, false
		}
		if len(chunk) == 0 {
			return rowsRead, p.finishRange(ctx, out, rangeID, seq)
		}
		rowsRead += int64(len(chunk))
		newTuple := make([]any, len(p.pkIdxs))
		for i, idx := range p.pkIdxs {
			newTuple[i] = normalizeTupleValue(chunk[len(chunk)-1][idx], p.srcDBType)
		}
		reserved, ok := env.acquireMem(ctx, chunkBytes)
		if !ok {
			if err := ctx.Err(); err != nil {
				sendChunkOrCancel(ctx, out, chunkResult{err: err})
			}
			return rowsRead, false
		}
		if !sendChunkOrCancel(ctx, out, chunkResult{rows: chunk, lastPK: newTuple, readerID: rangeID, seq: seq, bytes: reserved, queryTime: queryTime, scanTime: scanTime, readEnd: time.Now()}) {
			return rowsRead, false
		}
		lastTuple = newTuple
		seq++
		if len(chunk) < chunkSize {
			return rowsRead, p.finishRange(ctx, out, rangeID, seq)
		}
	}
}

// finishRange tells the shared consumer that this range has no more source
// rows. It follows every data chunk from the same producer, so nextSeq lets
// the checkpoint coordinator wait for the final write acknowledgement.
func (p *compositeParallelProducer) finishRange(ctx context.Context, out chan<- chunkResult, rangeID int, nextSeq int64) bool {
	return sendChunkOrCancel(ctx, out, chunkResult{readerID: rangeID, seq: nextSeq, rangeDone: true})
}

type compositeRangeCheckpointState struct {
	min          int64
	max          int64
	minInclusive bool
	tuple        []any
	complete     bool
	seq          ackSequencer
	// readDone records that the producer sent its final marker. nextSeq is the
	// first sequence which cannot exist, so every prior data chunk must be
	// acknowledged before this range is checkpoint-complete.
	readDone bool
	nextSeq  int64
}

type compositeRangeCheckpointCoordinator struct {
	mu              sync.Mutex
	saver           ProgressSaver
	taskID          int64
	tableName       string
	partitionID     *int
	rowsTotal       int64
	resumeRowsDone  int64
	checkpointFreq  func() int
	states          []compositeRangeCheckpointState
	ackedRows       int64
	completedChunks int
}

func newCompositeRangeCheckpointCoordinator(saver ProgressSaver, job Job, ranges []compositeResumeRange, partitionID *int, rowsTotal, resumeRowsDone int64, checkpointFreq func() int) *compositeRangeCheckpointCoordinator {
	if saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq == nil {
		checkpointFreq = func() int { return 10 }
	}
	states := make([]compositeRangeCheckpointState, len(ranges))
	for i, r := range ranges {
		states[i] = compositeRangeCheckpointState{min: r.min, max: r.max, minInclusive: r.minInclusive, tuple: r.tuple, complete: r.complete}
	}
	return &compositeRangeCheckpointCoordinator{saver: saver, taskID: job.TaskID, tableName: job.Table.Name, partitionID: partitionID, rowsTotal: rowsTotal, resumeRowsDone: resumeRowsDone, checkpointFreq: checkpointFreq, states: states}
}

func (c *compositeRangeCheckpointCoordinator) onAck(ack writeAck) ackRelease {
	if c == nil {
		return ackRelease{jobs: 1}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onAckLocked(ack)
}

func (c *compositeRangeCheckpointCoordinator) onAckLocked(ack writeAck) ackRelease {
	if c == nil || ack.readerID < 0 || ack.readerID >= len(c.states) {
		return ackRelease{jobs: 1}
	}
	state := &c.states[ack.readerID]
	wasComplete := state.complete
	released := state.seq.feed(ack, func(a writeAck) {
		if tuple, ok := a.lastPK.([]any); ok {
			state.tuple = tuple
		}
		c.ackedRows += a.rows
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 {
			c.saveCheckpointLocked()
		}
	})
	// ackSequencer advances nextSeq after its apply callback returns. Check
	// completion here so a marker that arrived before the final ack is not
	// promoted one acknowledgement late.
	c.updateCompleteLocked(state)
	if !wasComplete && state.complete {
		c.saveCheckpointLocked()
	}
	return released
}

// markRangeDone runs on the consumer goroutine while onAck runs on the writer
// acknowledgement goroutine. The shared mutex makes state publication safe
// when the final marker outruns the last asynchronous write acknowledgement.
func (c *compositeRangeCheckpointCoordinator) markRangeDone(readerID int, nextSeq int64) {
	if c == nil || nextSeq < 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if readerID < 0 || readerID >= len(c.states) {
		return
	}
	state := &c.states[readerID]
	wasComplete := state.complete
	state.readDone = true
	state.nextSeq = nextSeq
	c.updateCompleteLocked(state)
	if !wasComplete && state.complete {
		c.saveCheckpointLocked()
	}
}

func (c *compositeRangeCheckpointCoordinator) updateCompleteLocked(state *compositeRangeCheckpointState) {
	if state.readDone && state.seq.nextSeq >= state.nextSeq {
		state.complete = true
	}
}

func (c *compositeRangeCheckpointCoordinator) saveCheckpointLocked() {
	safeTuple := c.safeTupleLocked()
	if safeTuple == nil {
		return
	}
	if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, safeTuple, c.resumeRowsDone+c.ackedRows, c.rowsTotal, c.rangeStateLocked()); err != nil {
		logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
	}
}

func (c *compositeRangeCheckpointCoordinator) markComplete() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.states {
		c.states[i].complete = true
	}
}

func (c *compositeRangeCheckpointCoordinator) rangeState() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rangeStateLocked()
}

func (c *compositeRangeCheckpointCoordinator) rangeStateLocked() string {
	ranges := make([]compositeResumeRange, len(c.states))
	for i, s := range c.states {
		ranges[i] = compositeResumeRange{min: s.min, max: s.max, minInclusive: s.minInclusive, tuple: s.tuple, complete: s.complete}
	}
	return encodeCompositeRangeState(ranges)
}

func (c *compositeRangeCheckpointCoordinator) finalTuple(fallback []any) []any {
	if c == nil {
		return fallback
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if safeTuple := c.safeTupleLocked(); safeTuple != nil {
		return safeTuple
	}
	return fallback
}

// safeTupleLocked is the only value copied to legacy last_pk. It advances
// only across fully acknowledged lower ranges, so it is a safe fallback for
// current code if the rich envelope is unavailable. If the next range is
// empty and has no tuple yet, the last completed lower tuple is still a safe
// frontier. The envelope remains authoritative: pre-#667 binaries must not
// resume it because their legacy JSON last_pk decoding rounds large int64s.
func (c *compositeRangeCheckpointCoordinator) safeTupleLocked() []any {
	var completedPrefixTuple []any
	for i := range c.states {
		if c.states[i].complete {
			if c.states[i].tuple != nil {
				completedPrefixTuple = c.states[i].tuple
			}
			continue
		}
		if c.states[i].tuple != nil {
			return c.states[i].tuple
		}
		return completedPrefixTuple
	}
	return nil
}

type compositeRangeStateEnvelope struct {
	Version int                         `json:"version"`
	Ranges  []compositeRangeStateRecord `json:"ranges"`
}

type compositeRangeStateRecord struct {
	Range struct {
		Min       int64 `json:"min"`
		Max       int64 `json:"max"`
		Inclusive bool  `json:"inclusive"`
		Complete  bool  `json:"complete"`
	} `json:"range"`
	Tuple json.RawMessage `json:"tuple,omitempty"`
}

// encodeCompositeRangeState is a new envelope, intentionally distinct from
// both legacy plain tuple arrays and integer-keyset range arrays. New binaries
// accept all older forms. The new envelope is intentionally not downgrade
// compatible with pre-#667 binaries; resume such tasks with this or newer code
// so typed BIGINT watermarks remain exact.
func encodeCompositeRangeState(ranges []compositeResumeRange) string {
	if len(ranges) == 0 {
		return ""
	}
	wire := compositeRangeStateEnvelope{Version: 1, Ranges: make([]compositeRangeStateRecord, len(ranges))}
	for i, r := range ranges {
		wire.Ranges[i].Range.Min = r.min
		wire.Ranges[i].Range.Max = r.max
		wire.Ranges[i].Range.Inclusive = r.minInclusive
		wire.Ranges[i].Range.Complete = r.complete
		if encoded := encodeCompositeTuple(r.tuple); encoded != "" {
			wire.Ranges[i].Tuple = json.RawMessage(encoded)
		}
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return ""
	}
	return string(b)
}

func decodeCompositeRangeState(s string) []compositeResumeRange {
	if s == "" {
		return nil
	}
	var wire compositeRangeStateEnvelope
	if err := json.Unmarshal([]byte(s), &wire); err != nil || wire.Version != 1 || len(wire.Ranges) == 0 {
		return nil
	}
	out := make([]compositeResumeRange, len(wire.Ranges))
	for i, r := range wire.Ranges {
		if r.Range.Max < r.Range.Min {
			return nil
		}
		out[i] = compositeResumeRange{min: r.Range.Min, max: r.Range.Max, minInclusive: r.Range.Inclusive, complete: r.Range.Complete}
		if len(r.Tuple) > 0 {
			out[i].tuple = decodeCompositeTuple(string(r.Tuple))
			if out[i].tuple == nil {
				return nil
			}
		}
	}
	return out
}
