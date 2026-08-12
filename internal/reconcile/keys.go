package reconcile

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/driver/shared"
)

const defaultKeyBatchSize = 10000

// KeyBatchFunc receives one batch of key tuples. Implementations must not
// retain or mutate the slice after returning.
type KeyBatchFunc func(keys [][]any) error

// KeyDiffOptions describes the table and primary-key columns to compare.
type KeyDiffOptions struct {
	SourceSchema string
	TargetSchema string
	Table        string
	KeyColumns   []string
	BatchSize    int
}

// FindTargetOnlyKeys scans source and target primary keys and sends target
// keys missing from the source to handleMissing in bounded batches.
func FindTargetOnlyKeys(
	ctx context.Context,
	sourceDB, targetDB *sql.DB,
	sourceDialect, targetDialect driver.Dialect,
	opts KeyDiffOptions,
	handleMissing KeyBatchFunc,
) (int64, error) {
	if handleMissing == nil {
		return 0, fmt.Errorf("find target-only keys: missing-key handler is nil")
	}

	sourceKeys := make(map[string]struct{})
	if err := ScanKeys(ctx, sourceDB, sourceDialect, opts.SourceSchema, opts.Table, opts.KeyColumns, opts.BatchSize,
		func(keys [][]any) error {
			for _, key := range keys {
				fingerprint, err := KeyFingerprint(key)
				if err != nil {
					return err
				}
				sourceKeys[fingerprint] = struct{}{}
			}
			return nil
		}); err != nil {
		return 0, fmt.Errorf("scan source keys: %w", err)
	}

	var missing int64
	batchSize := effectiveBatchSize(opts.BatchSize)
	pending := make([][]any, 0, batchSize)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := handleMissing(pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	err := ScanKeys(ctx, targetDB, targetDialect, opts.TargetSchema, opts.Table, opts.KeyColumns, batchSize,
		func(keys [][]any) error {
			for _, key := range keys {
				fingerprint, err := KeyFingerprint(key)
				if err != nil {
					return err
				}
				if _, ok := sourceKeys[fingerprint]; ok {
					continue
				}
				pending = append(pending, cloneKey(key))
				missing++
				if len(pending) >= batchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return nil
		})
	if err != nil {
		return missing, fmt.Errorf("scan target keys: %w", err)
	}
	if err := flush(); err != nil {
		return missing, err
	}
	return missing, nil
}

// ScanKeys scans a table's primary-key columns in deterministic key order.
func ScanKeys(
	ctx context.Context,
	db *sql.DB,
	dialect driver.Dialect,
	schema, table string,
	keyColumns []string,
	batchSize int,
	handleBatch KeyBatchFunc,
) error {
	if db == nil {
		return fmt.Errorf("scan keys: db is nil")
	}
	if dialect == nil {
		return fmt.Errorf("scan keys: dialect is nil")
	}
	if handleBatch == nil {
		return fmt.Errorf("scan keys: handler is nil")
	}
	query, err := shared.OrderedKeySelect(dialect, dialect, schema, table, keyColumns)
	if err != nil {
		return err
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([][]any, 0, effectiveBatchSize(batchSize))
	for rows.Next() {
		key, err := scanKeyRow(rows, len(keyColumns))
		if err != nil {
			return err
		}
		batch = append(batch, key)
		if len(batch) >= cap(batch) {
			if err := handleBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		return handleBatch(batch)
	}
	return nil
}

// DeleteKeys hard-deletes key tuples from a target table in parameter-bounded
// batches. The returned count is the database-reported affected row total.
func DeleteKeys(
	ctx context.Context,
	db *sql.DB,
	dialect driver.Dialect,
	schema, table string,
	keyColumns []string,
	keys [][]any,
	batchSize int,
) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("delete keys: db is nil")
	}
	if dialect == nil {
		return 0, fmt.Errorf("delete keys: dialect is nil")
	}
	if len(keys) == 0 {
		return 0, nil
	}

	step := EffectiveDeleteBatchSize(dialect.DBType(), len(keyColumns), batchSize)
	var total int64
	for start := 0; start < len(keys); start += step {
		end := start + step
		if end > len(keys) {
			end = len(keys)
		}
		affected, err := deleteKeyBatch(ctx, db, dialect, schema, table, keyColumns, keys[start:end])
		if err != nil {
			return total, err
		}
		total += affected
	}
	return total, nil
}

// EffectiveDeleteBatchSize caps delete batches so parameterized DELETE
// statements stay below common driver/server limits.
func EffectiveDeleteBatchSize(dbType string, keyColumnCount, requested int) int {
	size := effectiveBatchSize(requested)
	if keyColumnCount <= 0 {
		return size
	}

	var maxParams int
	switch strings.ToLower(dbType) {
	case "mssql", "sqlserver":
		maxParams = 2000
	case "sqlite":
		maxParams = 30000
	default:
		maxParams = 60000
	}
	maxRows := maxParams / keyColumnCount
	if maxRows < 1 {
		maxRows = 1
	}
	if size > maxRows {
		return maxRows
	}
	return size
}

func deleteKeyBatch(
	ctx context.Context,
	db *sql.DB,
	dialect driver.Dialect,
	schema, table string,
	keyColumns []string,
	keys [][]any,
) (int64, error) {
	query, _, err := shared.DeleteByKeyBatch(
		dialect, dialect, dialect,
		schema, table, keyColumns,
		len(keys), 1,
	)
	if err != nil {
		return 0, err
	}
	args, err := shared.FlattenRows(keys, len(keyColumns))
	if err != nil {
		return 0, err
	}
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func scanKeyRow(rows *sql.Rows, columnCount int) ([]any, error) {
	values := make([]any, columnCount)
	dest := make([]any, columnCount)
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	for i := range values {
		values[i] = normalizeValue(values[i])
	}
	return values, nil
}

func effectiveBatchSize(size int) int {
	if size <= 0 {
		return defaultKeyBatchSize
	}
	return size
}

func cloneKey(key []any) []any {
	out := make([]any, len(key))
	for i, value := range key {
		out[i] = normalizeValue(value)
	}
	return out
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case []byte:
		if len(v) == 16 {
			return formatMixedEndianUUID(v)
		}
		return canonicalStringValue(string(v))
	case string:
		return canonicalStringValue(v)
	case time.Time:
		return v.UTC()
	default:
		return v
	}
}

func canonicalStringValue(value string) any {
	if value == "" || value != strings.TrimSpace(value) {
		return value
	}
	if uuid, ok := canonicalUUIDString(value); ok {
		return uuid
	}
	if i, ok := canonicalIntegerString(value); ok {
		return i
	}
	if ts, ok := canonicalTimeString(value); ok {
		return ts
	}
	return value
}

func canonicalIntegerString(value string) (any, bool) {
	if value == "" {
		return nil, false
	}
	if strings.HasPrefix(value, "+") {
		return nil, false
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil && strconv.FormatInt(i, 10) == value {
		return i, true
	}
	if u, err := strconv.ParseUint(value, 10, 64); err == nil && strconv.FormatUint(u, 10) == value {
		return u, true
	}
	return nil, false
}

func canonicalTimeString(value string) (time.Time, bool) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

func canonicalUUIDString(value string) (string, bool) {
	if len(value) != 36 {
		return "", false
	}
	for i, r := range value {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return "", false
			}
		default:
			if !isHexRune(r) {
				return "", false
			}
		}
	}
	return strings.ToLower(value), true
}

func isHexRune(r rune) bool {
	return ('0' <= r && r <= '9') || ('a' <= r && r <= 'f') || ('A' <= r && r <= 'F')
}

func formatMixedEndianUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15])
}

// KeyFingerprint returns a stable, length-delimited representation of one key
// tuple suitable for cross-database set comparison.
func KeyFingerprint(key []any) (string, error) {
	if len(key) == 0 {
		return "", fmt.Errorf("key fingerprint: key has no columns")
	}

	var b strings.Builder
	for _, value := range key {
		part, err := keyPartFingerprint(value)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%d:%s;", len(part), part)
	}
	return b.String(), nil
}

func keyPartFingerprint(value any) (string, error) {
	switch v := normalizeValue(value).(type) {
	case nil:
		return "", fmt.Errorf("key fingerprint: primary-key value is nil")
	case string:
		return "s:" + v, nil
	case int:
		return "n:" + strconv.FormatInt(int64(v), 10), nil
	case int8:
		return "n:" + strconv.FormatInt(int64(v), 10), nil
	case int16:
		return "n:" + strconv.FormatInt(int64(v), 10), nil
	case int32:
		return "n:" + strconv.FormatInt(int64(v), 10), nil
	case int64:
		return "n:" + strconv.FormatInt(v, 10), nil
	case uint:
		return "n:" + strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return "n:" + strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return "n:" + strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return "n:" + strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		if v > math.MaxInt64 {
			return "u:" + strconv.FormatUint(v, 10), nil
		}
		return "n:" + strconv.FormatUint(v, 10), nil
	case float32:
		return "f:" + strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64), nil
	case bool:
		return "b:" + strconv.FormatBool(v), nil
	case time.Time:
		return "t:" + v.UTC().Format(time.RFC3339Nano), nil
	case []byte:
		return "x:" + base64.StdEncoding.EncodeToString(v), nil
	default:
		return fmt.Sprintf("%T:%v", v, v), nil
	}
}
