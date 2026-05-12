package dbtuning

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// DetectSystemInfo pulls host facts via gopsutil. Failures degrade
// gracefully: an unreadable RAM count produces a zero, and any rule
// that depends on RAM declines to recommend (rather than recommending
// based on bogus data).
func DetectSystemInfo() SystemInfo {
	var sys SystemInfo
	if vm, err := mem.VirtualMemory(); err == nil {
		sys.HostMemoryMB = int64(vm.Total / (1024 * 1024))
	}
	if n, err := cpu.Counts(true); err == nil {
		sys.CPUCores = n
	}
	return sys
}

// ScanSingleString runs a one-row, one-column query and returns the
// scanned string. Used by the many tuning settings that look like
// "SHOW <name>" or "SELECT setting FROM pg_settings WHERE name=...".
func ScanSingleString(ctx context.Context, db *sql.DB, query string, args ...interface{}) (string, error) {
	var v string
	err := db.QueryRowContext(ctx, query, args...).Scan(&v)
	if err != nil {
		return "", err
	}
	return v, nil
}

// ScanShowVariable handles MySQL's `SHOW VARIABLES LIKE 'X'` which
// returns two columns (Variable_name, Value). Returns the value
// portion as a string.
func ScanShowVariable(ctx context.Context, db *sql.DB, varName string) (string, error) {
	var name, value string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE ?", varName).Scan(&name, &value)
	if err != nil {
		return "", err
	}
	return value, nil
}

// ParseBytes parses a size string with optional unit suffix into bytes.
// Accepts forms like "128MB", "8 kB", "16384" (raw), "2GB". Empty unit
// is interpreted as bytes.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}
	// Split number from unit.
	re := regexp.MustCompile(`^(-?[0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)$`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("unrecognized size %q", s)
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, err
	}
	unit := strings.ToLower(m[2])
	switch unit {
	case "", "b":
		return int64(n), nil
	case "k", "kb":
		return int64(n * 1024), nil
	case "m", "mb":
		return int64(n * 1024 * 1024), nil
	case "g", "gb":
		return int64(n * 1024 * 1024 * 1024), nil
	case "t", "tb":
		return int64(n * 1024 * 1024 * 1024 * 1024), nil
	default:
		return 0, fmt.Errorf("unrecognized unit %q in %q", unit, s)
	}
}

// FormatMB renders a megabyte count as a human-friendly size string
// (e.g. 8192 → "8GB", 256 → "256MB"). Used to make recommendation
// values readable in TUI output.
func FormatMB(mb int64) string {
	switch {
	case mb >= 1024*1024 && mb%(1024*1024) == 0:
		return fmt.Sprintf("%dTB", mb/(1024*1024))
	case mb >= 1024 && mb%1024 == 0:
		return fmt.Sprintf("%dGB", mb/1024)
	default:
		return fmt.Sprintf("%dMB", mb)
	}
}

// ParseInt handles the common "setting is a plain integer" case with
// graceful handling of whitespace.
func ParseInt(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// Recommend builds a TuningRecommendation with sensible defaults for
// the fields most callers fill the same way. The variadic "options"
// pattern would be cleaner but we have so few callers that an explicit
// constructor is easier to read.
type RecOpts struct {
	Parameter        string
	CurrentValue     interface{}
	RecommendedValue interface{}
	Impact           string // "high" | "medium" | "low"
	Reason           string
	Priority         int // 1=critical, 2=important, 3=optional
	CanApplyRuntime  bool
	SQLCommand       string
	RequiresRestart  bool
	ConfigFile       string
}

func Recommend(o RecOpts) *TuningRecommendation {
	return &TuningRecommendation{
		Parameter:        o.Parameter,
		CurrentValue:     o.CurrentValue,
		RecommendedValue: o.RecommendedValue,
		Impact:           o.Impact,
		Reason:           o.Reason,
		Priority:         o.Priority,
		CanApplyRuntime:  o.CanApplyRuntime,
		SQLCommand:       o.SQLCommand,
		RequiresRestart:  o.RequiresRestart,
		ConfigFile:       o.ConfigFile,
	}
}

// summarizeRecommendations rolls up a list of recommendations into the
// report-level TuningPotential / EstimatedImpact summary fields.
// "high" / "medium" / "low" / "none" mirrors the AI analyzer's output
// vocabulary so downstream consumers (TUI, smartconfig formatter)
// don't have to learn new strings.
func summarizeRecommendations(recs []TuningRecommendation) (potential string, impact string) {
	if len(recs) == 0 {
		return "none", "Configuration looks reasonable for the migration workload — no high-impact tuning recommended."
	}

	var pri1, pri2 int
	for _, r := range recs {
		switch r.Priority {
		case 1:
			pri1++
		case 2:
			pri2++
		}
	}

	switch {
	case pri1 >= 3:
		potential = "high"
	case pri1 >= 1:
		potential = "medium"
	case pri2 >= 1:
		potential = "low"
	default:
		potential = "low"
	}

	impact = fmt.Sprintf("%d recommendation(s); %d critical, %d important", len(recs), pri1, pri2)
	return potential, impact
}
