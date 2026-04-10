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
// Phase 1 requires:
//   - source driver is postgres and implements driver.BinaryCopyReader
//   - target driver is postgres and implements driver.BinaryCopyWriter
//   - target mode is drop_recreate (not upsert)
//   - no DateFilter (incremental sync is not yet supported on the fast path)
//   - no resume state (chunk-level resume is not yet supported)
//   - the job is NOT partitioned (partitions will land in Phase 2)
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
		// Partitioned jobs deferred to Phase 2.
		return nil, nil, false
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

// executePgFastCopy streams one non-partitioned table from a PG source to a
// PG target using the binary COPY relay. All columns of the table are copied
// via a single COPY TO STDOUT → io.Pipe → COPY FROM STDIN relay. The source
// and target goroutines run concurrently under an errgroup so the reader
// stalls whenever the writer can't keep up (natural backpressure via the
// unbuffered pipe).
//
// Returns TransferStats filled with Rows and WriteTime. Other timing fields
// (QueryTime, ScanTime) are not meaningful on the fast path — binary COPY
// conflates query execution with streaming — so they are left zero.
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

	// The source COPY uses original source column names; the target Writer
	// re-sanitizes identifiers internally, so we can pass the same column
	// slice to both sides.
	srcOpts := driver.CopyBinaryOptions{
		Schema:  job.Table.Schema,
		Table:   job.Table.Name,
		Columns: cols,
	}
	tgtOpts := driver.CopyBinaryOptions{
		Schema:  cfg.Target.Schema,
		Table:   targetTableName,
		Columns: cols, // writer re-sanitizes each column name
	}

	// Buffered pipe: decouples source and target so the source's COPY TO can
	// run ahead of the target's COPY FROM. io.Pipe would serialize them.
	// 256KB chunks × 16 capacity = up to 4MB in flight — matches the magnitude
	// of the generic pipeline's read-ahead buffer depth without wasting memory.
	pw, pr := newBufferedPipe(256*1024, 16)

	var bytesStreamed int64
	var readerRows, writerRows int64

	g, gctx := errgroup.WithContext(ctx)

	// Reader goroutine: COPY TO STDOUT → buffered pipe writer
	g.Go(func() error {
		// Close the writer on normal completion so the reader sees EOF.
		// On error, CloseWithError propagates the failure to the consumer.
		n, err := bcr.CopyBinaryTo(gctx, pw, srcOpts)
		if err != nil {
			_ = pw.CloseWithError(err)
			return fmt.Errorf("source COPY TO: %w", err)
		}
		atomic.StoreInt64(&readerRows, n)
		return pw.Close()
	})

	// Writer goroutine: buffered pipe reader (byte-counted) → COPY FROM STDIN
	g.Go(func() error {
		defer func() {
			_ = pr.Close()
		}()
		counted := &byteCountingReader{r: pr, bytes: &bytesStreamed}
		writeStart := time.Now()
		n, err := bcw.CopyBinaryFrom(gctx, counted, tgtOpts)
		stats.WriteTime += time.Since(writeStart)
		if err != nil {
			_ = pr.CloseWithError(err)
			return fmt.Errorf("target COPY FROM: %w", err)
		}
		atomic.StoreInt64(&writerRows, n)
		return nil
	})

	if err := g.Wait(); err != nil {
		return stats, err
	}

	rows := atomic.LoadInt64(&writerRows)
	if rows == 0 {
		rows = atomic.LoadInt64(&readerRows)
	}
	stats.Rows = rows
	bytesOut := atomic.LoadInt64(&bytesStreamed)

	// Push row count into the progress tracker in a single bump — the fast
	// path has no row-level granularity inside the COPY stream.
	if prog != nil && rows > 0 {
		prog.Add(rows)
	}

	logging.Debug(
		"pg fast copy: table=%s rows=%d bytes=%d write_time=%s",
		job.Table.Name, rows, bytesOut, stats.WriteTime,
	)

	return stats, nil
}

