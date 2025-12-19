package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/mssql-pg-migrate/internal/config"
	"github.com/johndauphine/mssql-pg-migrate/internal/progress"
	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/target"
)

// Job represents a data transfer job
type Job struct {
	Table     source.Table
	Partition *source.Partition
}

// Execute runs a transfer job
func Execute(
	ctx context.Context,
	srcPool *source.Pool,
	tgtPool *target.Pool,
	cfg *config.Config,
	job Job,
	prog *progress.Tracker,
) error {
	// Truncate target on first partition or non-partitioned
	if job.Partition == nil || job.Partition.PartitionID == 1 {
		if err := tgtPool.TruncateTable(ctx, cfg.Target.Schema, job.Table.Name); err != nil {
			// Ignore truncate errors (table might not exist)
		}
	}

	// Build column list
	cols := make([]string, len(job.Table.Columns))
	for i, c := range job.Table.Columns {
		cols[i] = c.Name
	}

	// Transfer in chunks
	offset := int64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := readChunk(ctx, srcPool.DB(), &job, cols, offset, cfg.Migration.ChunkSize)
		if err != nil {
			return fmt.Errorf("reading chunk at offset %d: %w", offset, err)
		}

		if len(rows) == 0 {
			break
		}

		if err := writeChunk(ctx, tgtPool.Pool(), cfg.Target.Schema, job.Table.Name, cols, rows); err != nil {
			return fmt.Errorf("writing chunk at offset %d: %w", offset, err)
		}

		prog.Add(int64(len(rows)))
		offset += int64(len(rows))

		if len(rows) < cfg.Migration.ChunkSize {
			break
		}
	}

	return nil
}

func readChunk(ctx context.Context, db *sql.DB, job *Job, cols []string, offset int64, limit int) ([][]any, error) {
	colList := "[" + strings.Join(cols, "], [") + "]"

	var query string
	var args []any

	if job.Partition != nil {
		// Partitioned query with PK range
		pkCol := job.Table.PrimaryKey[0]
		query = fmt.Sprintf(`
			SELECT %s FROM [%s].[%s] WITH (NOLOCK)
			WHERE [%s] >= @minPK AND [%s] <= @maxPK
			ORDER BY [%s]
			OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
		`, colList, job.Table.Schema, job.Table.Name, pkCol, pkCol, pkCol)
		args = []any{
			sql.Named("minPK", job.Partition.MinPK),
			sql.Named("maxPK", job.Partition.MaxPK),
			sql.Named("offset", offset),
			sql.Named("limit", limit),
		}
	} else {
		// Non-partitioned query
		query = fmt.Sprintf(`
			SELECT %s FROM [%s].[%s] WITH (NOLOCK)
			ORDER BY (SELECT NULL)
			OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
		`, colList, job.Table.Schema, job.Table.Name)
		args = []any{
			sql.Named("offset", offset),
			sql.Named("limit", limit),
		}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	numCols := len(cols)
	var result [][]any

	for rows.Next() {
		row := make([]any, numCols)
		ptrs := make([]any, numCols)
		for i := range row {
			ptrs[i] = &row[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, rows.Err()
}

func writeChunk(ctx context.Context, pool *pgxpool.Pool, schema, table string, cols []string, rows [][]any) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Use COPY for bulk insert
	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{schema, table},
		cols,
		pgx.CopyFromRows(rows),
	)

	return err
}
