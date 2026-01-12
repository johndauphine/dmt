// Package pipeline provides the Reader → Writer data transfer orchestration.
package pipeline

import (
	"fmt"
	"time"
)

// Stats tracks timing statistics for profiling a transfer operation.
type Stats struct {
	// QueryTime is total time spent executing source queries.
	QueryTime time.Duration

	// ScanTime is total time spent scanning rows from query results.
	ScanTime time.Duration

	// WriteTime is total time spent writing to the target.
	WriteTime time.Duration

	// Rows is the total number of rows transferred.
	Rows int64
}

// String returns a formatted summary of the stats.
func (s *Stats) String() string {
	total := s.QueryTime + s.ScanTime + s.WriteTime
	if total == 0 {
		return "no data"
	}
	return fmt.Sprintf("query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%), rows=%d",
		s.QueryTime.Seconds(), float64(s.QueryTime)/float64(total)*100,
		s.ScanTime.Seconds(), float64(s.ScanTime)/float64(total)*100,
		s.WriteTime.Seconds(), float64(s.WriteTime)/float64(total)*100,
		s.Rows)
}

// TotalTime returns the sum of all timing components.
func (s *Stats) TotalTime() time.Duration {
	return s.QueryTime + s.ScanTime + s.WriteTime
}

// RowsPerSecond calculates the throughput.
func (s *Stats) RowsPerSecond() float64 {
	total := s.TotalTime()
	if total == 0 {
		return 0
	}
	return float64(s.Rows) / total.Seconds()
}
