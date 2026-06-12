package generic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// LOAD DATA LOCAL INFILE bulk strategy for MySQL targets (#531). The
// batched multi-row INSERT path pays per-row placeholder parsing and
// is bind-variable-capped; LOAD DATA streams the whole chunk through
// the protocol's file-transfer frames in one statement. Rows are
// rendered as escaped TSV into an in-memory buffer served by a
// registered reader handler (go-sql-driver's Reader:: scheme — no
// filesystem involved, and allowed without AllowAllFiles).
//
// IdempotentOnDup replay keeps the batched-INSERT path: LOAD DATA's
// IGNORE modifier also downgrades data-conversion errors to warnings,
// which is exactly the silent-data-drop the #227 design rejected with
// INSERT IGNORE.

func init() {
	bulkStrategies["mysql_load_data"] = mysqlLoadDataWrite
}

// mysqlInfileState caches the per-writer @@local_infile probe; when the
// server has it disabled, the strategy degrades to batched INSERTs with
// one audited warning instead of failing every chunk.
type mysqlInfileState struct {
	once     sync.Once
	disabled bool
}

// infileSeq distinguishes concurrent reader-handler registrations —
// the driver's registry is process-global and keyed by name.
var infileSeq atomic.Uint64

func mysqlLoadDataWrite(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if opts.IdempotentOnDup {
		return batchedInsert(ctx, env, opts)
	}

	if env.myState != nil {
		env.myState.once.Do(func() {
			var enabled int
			if err := env.db.QueryRowContext(ctx, "SELECT @@local_infile").Scan(&enabled); err != nil || enabled != 1 {
				env.myState.disabled = true
				logging.Warn("mysql local_infile is disabled on the server — falling back to batched INSERTs. Enable with: SET PERSIST local_infile=1 (typically 2x+ faster bulk loads)")
			}
		})
		if env.myState.disabled {
			return batchedInsert(ctx, env, opts)
		}
	}

	data, err := renderInfileTSV(opts.Rows, env.convert)
	if err != nil {
		return err
	}

	name := fmt.Sprintf("dmt_chunk_%d", infileSeq.Add(1))
	mysql.RegisterReaderHandler(name, func() io.Reader { return bytes.NewReader(data) })
	defer mysql.DeregisterReaderHandler(name)

	stmt := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' (%s)",
		name,
		env.dialect.QualifyTable(opts.Schema, opts.Table),
		env.dialect.ColumnList(opts.Columns),
	)
	// One autocommitted statement per chunk — the same resume
	// granularity as the non-transactional batched-INSERT path.
	if _, err := env.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("LOAD DATA into %s: %w", opts.Table, err)
	}
	return nil
}

// renderInfileTSV renders rows in LOAD DATA's default-compatible TSV
// form: fields tab-separated, lines newline-terminated, backslash
// escaping, \N for NULL.
func renderInfileTSV(rows [][]any, convert func([]any) []any) ([]byte, error) {
	var buf bytes.Buffer
	// Rough preallocation: 64 bytes per value avoids most regrowth.
	if len(rows) > 0 {
		buf.Grow(len(rows) * len(rows[0]) * 64)
	}
	for _, row := range rows {
		for i, v := range convert(row) {
			if i > 0 {
				buf.WriteByte('\t')
			}
			if err := writeInfileValue(&buf, v); err != nil {
				return nil, err
			}
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func writeInfileValue(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case nil:
		buf.WriteString(`\N`)
	case string:
		writeInfileEscaped(buf, []byte(val))
	case []byte:
		writeInfileEscaped(buf, val)
	case int64:
		buf.WriteString(strconv.FormatInt(val, 10))
	case int:
		buf.WriteString(strconv.Itoa(val))
	case int32:
		buf.WriteString(strconv.FormatInt(int64(val), 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(val, 10))
	case float64:
		buf.WriteString(strconv.FormatFloat(val, 'g', -1, 64))
	case float32:
		buf.WriteString(strconv.FormatFloat(float64(val), 'g', -1, 32))
	case bool:
		// Normally pre-converted by the bool_to_int row converter;
		// belt-and-braces for callers without it.
		if val {
			buf.WriteByte('1')
		} else {
			buf.WriteByte('0')
		}
	case time.Time:
		buf.WriteString(val.Format("2006-01-02 15:04:05.000000"))
	default:
		// Unknown driver-specific type: defer to fmt, escaped.
		writeInfileEscaped(buf, []byte(fmt.Sprintf("%v", val)))
	}
	return nil
}

// writeInfileEscaped escapes the bytes LOAD DATA's default ESCAPED BY
// '\' scheme requires: the escape char itself, field/line terminators,
// CR, and NUL.
func writeInfileEscaped(buf *bytes.Buffer, b []byte) {
	for _, c := range b {
		switch c {
		case '\\':
			buf.WriteString(`\\`)
		case '\t':
			buf.WriteString(`\t`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case 0x00:
			buf.WriteString(`\0`)
		default:
			buf.WriteByte(c)
		}
	}
}
