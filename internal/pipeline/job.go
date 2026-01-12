package pipeline

import (
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// ProgressSaver is an interface for saving transfer progress.
type ProgressSaver interface {
	SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error
	GetProgress(taskID int64) (lastPK any, rowsDone int64, err error)
}

// Job represents a data transfer job for a single table or partition.
type Job struct {
	// Table is the source table to transfer.
	Table driver.Table

	// Partition specifies a partition to transfer (nil for whole table).
	Partition *driver.Partition

	// TaskID is the checkpoint task ID for resume support.
	TaskID int64

	// Saver is the progress saver for checkpointing (nil to disable).
	Saver ProgressSaver

	// DateFilter is an optional filter for incremental sync.
	DateFilter *DateFilter
}

// DateFilter specifies a filter on a date/timestamp column.
type DateFilter struct {
	// Column is the name of the date column to filter on.
	Column string

	// Timestamp is the minimum value (rows where column > timestamp are included).
	Timestamp time.Time
}

// chunkResult holds a chunk of data from the read-ahead pipeline.
type chunkResult struct {
	rows      [][]any
	lastPK    any
	rowNum    int64 // for ROW_NUMBER pagination progress tracking
	readerID  int
	seq       int64
	queryTime time.Duration
	scanTime  time.Duration
	readEnd   time.Time // when this chunk finished reading
	err       error
	done      bool // signals end of data
}

// writeJob holds a chunk of data to be written.
type writeJob struct {
	rows     [][]any
	lastPK   any
	rowNum   int64
	readerID int
	seq      int64
}

// writeAck acknowledges a completed write for checkpoint coordination.
type writeAck struct {
	readerID int
	seq      int64
	lastPK   any
	rowNum   int64
}

// pkRange represents a primary key range for parallel reading.
type pkRange struct {
	minPK any // inclusive (start from > minPK)
	maxPK any // inclusive (read up to <= maxPK)
}
