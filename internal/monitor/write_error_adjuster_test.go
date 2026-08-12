// Tests for RuleWriteErrorAdjuster. Direct unit tests against the
// known-error patterns and the halving response.

package monitor

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/transfer"
)

func TestRuleWriteErrorAdjuster_StructuralErrors_HalveChunk(t *testing.T) {
	a := NewRuleWriteErrorAdjuster()
	cases := []struct {
		name     string
		errMsg   string
		rowCount int
		want     int
	}{
		{"mysql_placeholders", "Error 1390: Prepared statement contains too many placeholders", 5000, 2500},
		{"mssql_parameters", "The incoming request has too many parameters. The server supports a maximum of 2100 parameters.", 5000, 2500},
		{"mysql_packet", "Got a packet bigger than 'max_allowed_packet' bytes", 1000, 500},
		{"mssql_request_size", "Request size exceeds the maximum allowed length", 2000, 1000},
		{"case_insensitive_match", "TOO MANY PLACEHOLDERS in statement", 100, 50},
		{"floor_at_1", "too many placeholders", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := a.EvaluateWriteError(context.Background(), transfer.WriteErrorContext{
				ErrorMessage: tc.errMsg,
				RowCount:     tc.rowCount,
			})
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRuleWriteErrorAdjuster_UnknownError_NoAdjustment(t *testing.T) {
	a := NewRuleWriteErrorAdjuster()
	cases := []struct {
		name   string
		errMsg string
	}{
		{"network_timeout", "context deadline exceeded"},
		{"deadlock", "Deadlock found when trying to get lock"},
		{"missing_table", "ERROR: relation \"users\" does not exist"},
		{"empty", ""},
		{"unrelated", "syntax error at or near \"FOO\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := a.EvaluateWriteError(context.Background(), transfer.WriteErrorContext{
				ErrorMessage: tc.errMsg,
				RowCount:     5000,
			})
			if got != 0 {
				t.Errorf("unknown error should return 0; got %d", got)
			}
		})
	}
}

func TestIsStructuralLimitError_Patterns(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"too many placeholders", true},
		{"too many parameters", true},
		{"max_allowed_packet bytes", true},
		{"request size exceeds the maximum", true},
		{"some other error", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.msg, func(t *testing.T) {
			if got := isStructuralLimitError(tc.msg); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
