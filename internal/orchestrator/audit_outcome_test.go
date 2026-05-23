package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestClassifyRunOutcome pins the codex-review fix: the deferred audit
// handler classifies (runErr, recover()) into a 3-tuple. Errors must
// be captured (not silently dropped), panics must come through as
// "panic", and context cancellation must mark the run resumable so
// the audit file stays writable for the next `dmt resume`.
func TestClassifyRunOutcome(t *testing.T) {
	cases := []struct {
		name     string
		runErr   error
		rec      any
		wantStat string
		wantErr  string
		wantRes  bool
	}{
		{
			name:     "success",
			runErr:   nil,
			rec:      nil,
			wantStat: "success",
			wantRes:  false,
		},
		{
			name:     "hard failure",
			runErr:   errors.New("schema extraction failed: dial tcp: connection refused"),
			rec:      nil,
			wantStat: "failed",
			wantErr:  "schema extraction failed: dial tcp: connection refused",
			wantRes:  false,
		},
		{
			name:     "panic preempts the named-return error",
			runErr:   errors.New("ignored"),
			rec:      "boom",
			wantStat: "panic",
			wantErr:  "panic: boom",
			wantRes:  false,
		},
		{
			name:     "context cancelled — resumable",
			runErr:   context.Canceled,
			rec:      nil,
			wantStat: "cancelled",
			wantErr:  context.Canceled.Error(),
			wantRes:  true,
		},
		{
			name:     "context deadline — resumable",
			runErr:   context.DeadlineExceeded,
			rec:      nil,
			wantStat: "cancelled",
			wantErr:  context.DeadlineExceeded.Error(),
			wantRes:  true,
		},
		{
			name:     "wrapped context.Canceled — still resumable",
			runErr:   fmt.Errorf("transfer: %w", context.Canceled),
			rec:      nil,
			wantStat: "cancelled",
			wantErr:  "transfer: context canceled",
			wantRes:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStat, gotErr, gotRes := classifyRunOutcome(tc.runErr, tc.rec)
			if gotStat != tc.wantStat {
				t.Errorf("status = %q, want %q", gotStat, tc.wantStat)
			}
			if gotErr != tc.wantErr {
				t.Errorf("errStr = %q, want %q", gotErr, tc.wantErr)
			}
			if gotRes != tc.wantRes {
				t.Errorf("resumable = %v, want %v", gotRes, tc.wantRes)
			}
		})
	}
}
