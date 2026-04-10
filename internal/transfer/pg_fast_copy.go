package transfer

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/progress"
)

// pgFastCopyEligible reports whether a Job can be migrated via the PG→PG
// binary COPY relay fast path.
//
// Requirements:
//   - source driver is postgres and implements driver.BinaryCopyReader
//   - target driver is postgres and implements driver.BinaryCopyWriter
//   - target mode is drop_recreate (not upsert)
//   - no DateFilter (incremental sync is not yet supported on the fast path)
//   - no resume state (chunk-level resume is not yet supported)
//   - for partitioned jobs, Partition.MinPK and Partition.MaxPK must be
//     integer types (int / int32 / int64), since COPY ... TO STDOUT does not
//     support parameter binding and the WHERE clause must inline literals
//
// If activation fails, the caller must fall through to the generic pipeline.
func pgFastCopyEligible(
	srcPool pool.SourcePool,
	tgtPool pool.TargetPool,
	cfg *config.Config,
	job Job,
	resumeLastPK any,
) (driver.BinaryCopyReader, driver.BinaryCopyWriter, bool) {
	if cfg.Migration.PgFastCopyDisabled {
		return nil, nil, false
	}
	if srcPool.DBType() != "postgres" || tgtPool.DBType() != "postgres" {
		return nil, nil, false
	}
	if cfg.Migration.TargetMode == "upsert" {
		return nil, nil, false
	}
	if job.DateFilter != nil {
		return nil, nil, false
	}
	if resumeLastPK != nil {
		return nil, nil, false
	}
	if job.Partition != nil {
		// Partitioned jobs require inlineable integer bounds. Non-integer
		// partitions (shouldn't happen — keyset pagination already restricts
		// to integer PKs — but be defensive) fall back to the generic path.
		if _, ok := pkAsInt64(job.Partition.MinPK); !ok {
			return nil, nil, false
		}
		if _, ok := pkAsInt64(job.Partition.MaxPK); !ok {
			return nil, nil, false
		}
	}

	bcr, ok := srcPool.(driver.BinaryCopyReader)
	if !ok {
		return nil, nil, false
	}
	bcw, ok := tgtPool.(driver.BinaryCopyWriter)
	if !ok {
		return nil, nil, false
	}
	return bcr, bcw, true
}

// pkAsInt64 coerces a PK value into int64 if it is one of the supported
// integer types. Returns (0, false) for any other type.
func pkAsInt64(pk any) (int64, bool) {
	switch v := pk.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	}
	return 0, false
}

// byteCountingReader wraps an io.Reader and atomically counts bytes passed
// through, so the caller goroutine can observe progress while the relay is
// in flight. This is a cheap substitute for row-level progress — binary COPY
// does not expose row boundaries mid-stream.
type byteCountingReader struct {
	r     io.Reader
	bytes *int64
}

func (b *byteCountingReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		atomic.AddInt64(b.bytes, int64(n))
	}
	return n, err
}

// executePgFastCopy streams one table (or one partition of a table) from a
// PG source to a PG target using the binary COPY relay.
//
// For partitioned jobs with integer PKs we sub-batch the partition into
// chunk_size-sized ranges and run one COPY per sub-range. This is essential
// for throughput and stability: a single COPY for an entire partition holds
// one giant transaction open on the target, which overwhelms WAL/checkpoint
// pressure when multiple partitions run concurrently and eventually stalls
// or crashes the connection with "protocol synchronization lost". Short
// transactions let the target interleave WAL writes and checkpoints cleanly,
// mirroring how the generic pipeline chunks work.
//
// Non-partitioned jobs still run as a single COPY — they are small enough
// that sub-batching adds overhead for no benefit.
//
// Returns TransferStats filled with Rows and WriteTime.
func executePgFastCopy(
	ctx context.Context,
	bcr driver.BinaryCopyReader,
	bcw driver.BinaryCopyWriter,
	cfg *config.Config,
	job Job,
	cols []string,
	targetTableName string,
	prog *progress.Tracker,
) (*TransferStats, error) {
	stats := &TransferStats{}

	tgtOpts := driver.CopyBinaryOptions{
		Schema:  cfg.Target.Schema,
		Table:   targetTableName,
		Columns: cols, // writer re-sanitizes each column name
	}

	// Build the list of sub-range WHERE clauses to process. For non-
	// partitioned jobs this is a single empty string (unfiltered COPY).
	// For partitioned jobs we slice [minPK, maxPK] into chunk_size-row
	// windows, each becoming its own short transaction.
	subRanges := buildFastCopySubRanges(job, cfg.Migration.ChunkSize)

	var totalRows int64
	for i, where := range subRanges {
		srcOpts := driver.CopyBinaryOptions{
			Schema:  job.Table.Schema,
			Table:   job.Table.Name,
			Columns: cols,
			Where:   where,
		}
		rows, writeTime, err := runFastCopyPair(ctx, bcr, bcw, srcOpts, tgtOpts)
		stats.WriteTime += writeTime
		if err != nil {
			return stats, fmt.Errorf("sub-batch %d/%d: %w", i+1, len(subRanges), err)
		}
		totalRows += rows

		if prog != nil && rows > 0 {
			prog.Add(rows)
		}
	}

	stats.Rows = totalRows

	partitionStr := ""
	if job.Partition != nil {
		partitionStr = fmt.Sprintf(" partition=%d", job.Partition.PartitionID)
	}
	logging.Debug(
		"pg fast copy: table=%s%s rows=%d sub_batches=%d write_time=%s",
		job.Table.Name, partitionStr, totalRows, len(subRanges), stats.WriteTime,
	)

	return stats, nil
}

// runFastCopyPair executes a single source→pipe→target COPY relay for one
// WHERE filter and returns the number of rows copied and the wall time
// spent on the target write.
func runFastCopyPair(
	ctx context.Context,
	bcr driver.BinaryCopyReader,
	bcw driver.BinaryCopyWriter,
	srcOpts, tgtOpts driver.CopyBinaryOptions,
) (int64, time.Duration, error) {
	// Buffered pipe decouples source and target so their network I/O can
	// overlap. 256KB chunks × 16 capacity = up to 4MB in flight.
	pw, pr := newBufferedPipe(256*1024, 16)

	var bytesStreamed int64
	var readerRows, writerRows int64
	var writeDuration time.Duration

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		n, err := bcr.CopyBinaryTo(gctx, pw, srcOpts)
		if err != nil {
			_ = pw.CloseWithError(err)
			return fmt.Errorf("source COPY TO: %w", err)
		}
		atomic.StoreInt64(&readerRows, n)
		return pw.Close()
	})

	g.Go(func() error {
		defer func() {
			_ = pr.Close()
		}()
		counted := &byteCountingReader{r: pr, bytes: &bytesStreamed}
		writeStart := time.Now()
		n, err := bcw.CopyBinaryFrom(gctx, counted, tgtOpts)
		writeDuration = time.Since(writeStart)
		if err != nil {
			_ = pr.CloseWithError(err)
			return fmt.Errorf("target COPY FROM: %w", err)
		}
		atomic.StoreInt64(&writerRows, n)
		return nil
	})

	if err := g.Wait(); err != nil {
		return 0, writeDuration, err
	}

	rows := atomic.LoadInt64(&writerRows)
	if rows == 0 {
		rows = atomic.LoadInt64(&readerRows)
	}
	return rows, writeDuration, nil
}

// buildFastCopySubRanges returns the WHERE clauses to apply for one job.
// For non-partitioned jobs it returns a single empty WHERE (unfiltered
// COPY). For partitioned jobs it slices [minPK, maxPK] into chunkSize-row
// windows, so each window becomes an independent short-lived COPY
// transaction.
//
// The slicing is inclusive on both ends and non-overlapping:
//   - window 0: pk >= minPK       AND pk <= minPK + chunkSize - 1
//   - window 1: pk >= minPK + chunkSize AND pk <= minPK + 2*chunkSize - 1
//   - ...
//   - last:     pk >= ... AND pk <= maxPK
//
// Note: because pk values are dense-integer (NTILE partitioning uses ORDER
// BY pk), chunkSize rows ≈ chunkSize pk units for typical SO2013 tables.
// If PKs are sparse this will under-fill chunks but will still be correct.
func buildFastCopySubRanges(job Job, chunkSize int) []string {
	if job.Partition == nil || len(job.Table.PrimaryKey) == 0 {
		return []string{""}
	}
	minPK, ok := pkAsInt64(job.Partition.MinPK)
	if !ok {
		return []string{""}
	}
	maxPK, ok := pkAsInt64(job.Partition.MaxPK)
	if !ok {
		return []string{""}
	}

	if chunkSize <= 0 {
		chunkSize = 50000
	}

	pkQuoted := `"` + job.Table.PrimaryKey[0] + `"`
	var ranges []string
	cur := minPK
	for cur <= maxPK {
		end := cur + int64(chunkSize) - 1
		if end > maxPK {
			end = maxPK
		}
		ranges = append(ranges, fmt.Sprintf("%s >= %d AND %s <= %d", pkQuoted, cur, pkQuoted, end))
		// Guard against overflow when end is very close to int64 max.
		if end == maxPK {
			break
		}
		cur = end + 1
	}
	return ranges
}

