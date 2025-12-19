package progress

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar/v3"
)

// Tracker tracks migration progress
type Tracker struct {
	bar       *progressbar.ProgressBar
	total     int64
	current   atomic.Int64
	startTime time.Time
}

// New creates a new progress tracker
func New() *Tracker {
	return &Tracker{
		startTime: time.Now(),
	}
}

// SetTotal sets the total number of rows to transfer
func (t *Tracker) SetTotal(total int64) {
	t.total = total
	t.bar = progressbar.NewOptions64(
		total,
		progressbar.OptionSetDescription("Transferring"),
		progressbar.OptionShowBytes(false),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
	)
}

// Add increments the progress counter
func (t *Tracker) Add(n int64) {
	t.current.Add(n)
	if t.bar != nil {
		t.bar.Add64(n)
	}
}

// Current returns the current count
func (t *Tracker) Current() int64 {
	return t.current.Load()
}

// Finish marks the progress as complete
func (t *Tracker) Finish() {
	if t.bar != nil {
		t.bar.Finish()
	}

	elapsed := time.Since(t.startTime)
	rowsPerSec := float64(t.current.Load()) / elapsed.Seconds()

	fmt.Println()
	fmt.Printf("Transferred %d rows in %s (%.0f rows/sec)\n",
		t.current.Load(), elapsed.Round(time.Second), rowsPerSec)
}
