package driver

import (
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

// ErrorDiagnosis is dmt's analysis of a migration error: a one-line
// cause statement plus a small list of actionable suggestions, with
// confidence and category metadata for callers that want to filter
// or sort. Produced today by the deterministic catalog in
// internal/driver/errordiag (#173); historically also by an AI
// fallback (removed because the long-tail value did not justify the
// PII / complexity / availability cost of sending error messages to a
// third-party API).
//
// Category is one of: type_mismatch, constraint, permission,
// connection, data_quality, other. Confidence is one of: high,
// medium, low.
type ErrorDiagnosis struct {
	Cause       string   `json:"cause"`
	Suggestions []string `json:"suggestions"`
	Confidence  string   `json:"confidence"`
	Category    string   `json:"category"`
}

// ErrorContext carries the information the diagnoser needs to reason
// about an error. The dispatcher in errordiag_dispatch.go pattern-matches
// on ErrorMessage; the column/schema fields are unused today but are
// preserved so a future enrichment pass (e.g. "the failing column was
// declared as VARCHAR(50) in the source") can read them without a
// signature change.
type ErrorContext struct {
	ErrorMessage string
	TableName    string
	TableSchema  string
	Columns      []Column
	SourceDBType string
	TargetDBType string
	TargetMode   string
}

// DiagnosisHandler is a callback that surfaces a finished diagnosis.
// The TUI registers one of these so it can render diagnoses inside a
// boxed UI element; the CLI/headless path uses the logging fallback.
type DiagnosisHandler func(diagnosis *ErrorDiagnosis)

var (
	diagnosisHandler   DiagnosisHandler
	diagnosisHandlerMu sync.RWMutex
)

// SetDiagnosisHandler registers a callback for diagnosis events.
// Passing nil unregisters the handler and reinstates the logging
// fallback.
func SetDiagnosisHandler(handler DiagnosisHandler) {
	diagnosisHandlerMu.Lock()
	defer diagnosisHandlerMu.Unlock()
	diagnosisHandler = handler
}

// EmitDiagnosis sends a diagnosis to the registered handler, or logs it
// (boxed) if no handler is set. Safe to call from any goroutine.
func EmitDiagnosis(diagnosis *ErrorDiagnosis) {
	if diagnosis == nil {
		return
	}
	diagnosisHandlerMu.RLock()
	handler := diagnosisHandler
	diagnosisHandlerMu.RUnlock()

	if handler != nil {
		handler(diagnosis)
		return
	}
	logging.Warn("\n%s", diagnosis.FormatBox())
}

// Format renders a diagnosis as plain text suitable for terminals
// without box-drawing characters or as a substring in larger logs.
func (diag *ErrorDiagnosis) Format() string {
	var sb strings.Builder
	sb.WriteString("Error Diagnosis\n\n")
	fmt.Fprintf(&sb, "Cause: %s\n\n", diag.Cause)
	sb.WriteString("Suggestions:\n")
	for i, s := range diag.Suggestions {
		fmt.Fprintf(&sb, "  %d. %s\n", i+1, s)
	}
	fmt.Fprintf(&sb, "\nConfidence: %s  |  Category: %s\n", diag.Confidence, diag.Category)
	return sb.String()
}

// FormatBox renders a diagnosis inside a Unicode box, used for prominent
// surfacing in CLI/log output when no TUI handler is registered.
func (diag *ErrorDiagnosis) FormatBox() string {
	const (
		topLeft     = "┌"
		topRight    = "┐"
		bottomLeft  = "└"
		bottomRight = "┘"
		horizontal  = "─"
		vertical    = "│"
		width       = 72
	)
	var sb strings.Builder

	writePadded := func(content string) {
		if len(content) > width-4 {
			content = truncateUTF8Bytes(content, width-7) + "..."
		}
		padding := width - 4 - len(content)
		if padding < 0 {
			padding = 0
		}
		sb.WriteString(vertical + " " + content + strings.Repeat(" ", padding) + " " + vertical + "\n")
	}

	title := " Error Diagnosis "
	leftPad := (width - 2 - len(title)) / 2
	rightPad := width - 2 - len(title) - leftPad
	sb.WriteString(topLeft + strings.Repeat(horizontal, leftPad) + title + strings.Repeat(horizontal, rightPad) + topRight + "\n")

	writePadded("")
	writePadded("Cause: " + diag.Cause)
	writePadded("")
	writePadded("Suggestions:")
	for i, s := range diag.Suggestions {
		writePadded(fmt.Sprintf("  %d. %s", i+1, s))
	}
	writePadded("")
	writePadded(fmt.Sprintf("Confidence: %s  |  Category: %s", diag.Confidence, diag.Category))
	sb.WriteString(bottomLeft + strings.Repeat(horizontal, width-2) + bottomRight)

	return sb.String()
}

func truncateUTF8Bytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	if maxBytes <= 0 {
		return ""
	}

	for maxBytes > 0 && !utf8.ValidString(s[:maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}
