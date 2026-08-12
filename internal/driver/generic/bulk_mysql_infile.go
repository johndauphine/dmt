package generic

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
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
	// One autocommitted statement per chunk — the same resume granularity as
	// the non-transactional batched-INSERT path. Pin a connection so the
	// post-load warning check reads THIS statement's diagnostics.
	conn, err := env.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("LOAD DATA into %s: acquiring connection: %w", opts.Table, err)
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("LOAD DATA into %s: %w", opts.Table, err)
	}

	// LOAD DATA LOCAL runs with implicit-IGNORE semantics: duplicate-key rows
	// are silently discarded and data-conversion errors are demoted to
	// warnings (strict sql_mode does not apply with LOCAL). The batched-INSERT
	// path errors on those, so verify the load matches that path's strictness
	// before acking the chunk — a dropped row (RowsAffected mismatch) or an
	// Error/Warning-level conversion is otherwise undetectable and even passes
	// row-count validation (#544).
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("LOAD DATA into %s: reading affected rows: %w", opts.Table, err)
	}
	if affected != int64(len(opts.Rows)) {
		return fmt.Errorf("LOAD DATA into %s loaded %d of %d rows: rows were silently dropped (duplicate key or unparseable) under LOAD DATA's implicit IGNORE",
			opts.Table, affected, len(opts.Rows))
	}
	if n, detail := mysqlLoadDataWarnings(ctx, conn); n > 0 {
		return fmt.Errorf("LOAD DATA into %s raised %d warning(s): values were silently adjusted (truncation/clamping/default substitution): %s",
			opts.Table, n, detail)
	}
	return nil
}

// mysqlLoadDataWarnings reports how many warnings the last statement on conn
// raised, plus a short sample of their messages. SHOW WARNINGS reads the
// connection's diagnostics area without clearing it, so it must run on the same
// connection immediately after the LOAD DATA. Best-effort: a query error yields
// 0, since the RowsAffected check already guards against dropped rows.
func mysqlLoadDataWarnings(ctx context.Context, conn *sql.Conn) (int, string) {
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return 0, ""
	}
	defer rows.Close()

	var count int
	var sample []string
	for rows.Next() {
		var level, message string
		var code int
		if err := rows.Scan(&level, &code, &message); err != nil {
			break
		}
		// Note-level diagnostics (e.g. decimal fractional rounding like
		// 1.505 -> DECIMAL(_,2)) are tolerated by the strict batched-INSERT
		// path as well, so only Error/Warning count as silent loss. Failing
		// on Note too would make the load stricter than its own fallback and
		// make success depend on @@local_infile (#544 review).
		if strings.EqualFold(level, "Note") {
			continue
		}
		count++
		if len(sample) < 3 {
			sample = append(sample, fmt.Sprintf("%s %d: %s", level, code, message))
		}
	}
	return count, strings.Join(sample, "; ")
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
		buf.WriteString(val.UTC().Format("2006-01-02 15:04:05.000000"))
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
