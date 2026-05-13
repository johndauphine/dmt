package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Level represents logging verbosity level
type Level int

const (
	// LevelError only logs errors
	LevelError Level = iota
	// LevelWarn logs warnings and errors
	LevelWarn
	// LevelInfo logs info, warnings, and errors (default)
	LevelInfo
	// LevelDebug logs everything including debug messages
	LevelDebug
)

// Format represents the logging output format
type Format string

const (
	FormatText Format = "text"
	FormatJSON Format = "json"
)

// Logger provides leveled logging with optional structured fields (#229).
// Structured fields flow two ways:
//   - "Base attributes" are set once per run via SetBaseAttr / WithFields and
//     appear on every subsequent log line. The orchestrator uses these to
//     stamp run_id, phase, source_db, target_db etc.
//   - "Event fields" are passed per-call via the Event family and only
//     decorate that one line — used for chunk-completion, errors, etc.
//
// The existing printf-style API (Info / Warn / Error / Debug) is preserved
// and works exactly as before. JSON output merges base attrs and per-event
// fields into the same map; text output appends them as space-separated
// `key=value` pairs after the message.
type Logger struct {
	mu         sync.Mutex
	level      Level
	output     io.Writer
	format     Format
	simpleMode bool // When true, skip timestamps and level prefixes (for TUI)
	baseAttrs  map[string]any
}

var (
	defaultLogger = &Logger{
		level:     LevelInfo,
		output:    os.Stdout,
		format:    FormatText,
		baseAttrs: map[string]any{},
	}
)

// ParseLevel converts a string to a Level
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(s) {
	case "error":
		return LevelError, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "info":
		return LevelInfo, nil
	case "debug":
		return LevelDebug, nil
	default:
		return LevelInfo, fmt.Errorf("unknown verbosity level: %s (valid: debug, info, warn, error)", s)
	}
}

// String returns the string representation of a level
func (l Level) String() string {
	switch l {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// SetLevel sets the global log level
func SetLevel(level Level) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = level
}

// SetOutput sets the output destination for logging
func SetOutput(w io.Writer) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.output = w
}

// SetFormat sets the output format for logging (text or json)
func SetFormat(format string) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	if format == "json" {
		defaultLogger.format = FormatJSON
	} else {
		defaultLogger.format = FormatText
	}
}

// GetLevel returns the current log level
func GetLevel() Level {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	return defaultLogger.level
}

// SetSimpleMode enables/disables simple mode (no timestamps or level prefixes)
// Used by TUI to get clean output without timestamps cluttering the display
func SetSimpleMode(enabled bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.simpleMode = enabled
}

// IsSimpleMode returns whether simple mode is enabled
func IsSimpleMode() bool {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	return defaultLogger.simpleMode
}

// SetBaseAttr installs a key=value pair that decorates every subsequent
// log line. Used by the orchestrator to stamp run_id, phase, and database
// types onto the structured output. SetBaseAttr is safe for concurrent
// callers (the orchestrator updates `phase` from the main goroutine while
// transfer goroutines log against it).
//
// Setting value to nil removes the attribute. Use ClearBaseAttrs to drop
// everything between runs (the orchestrator does this in a defer).
func SetBaseAttr(key string, value any) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	if value == nil {
		delete(defaultLogger.baseAttrs, key)
		return
	}
	defaultLogger.baseAttrs[key] = value
}

// ClearBaseAttrs removes every base attribute. Called by Run/Resume on exit
// so a subsequent invocation in the same process (long-running TUI, tests)
// doesn't leak run_id / phase from a prior run.
func ClearBaseAttrs() {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.baseAttrs = map[string]any{}
}

// BaseAttrs returns a snapshot of the current base attributes. Safe for
// callers that need to attach the same context to a different sink (e.g.
// notifier formatting). Returns a copy so the caller can mutate freely.
func BaseAttrs() map[string]any {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	out := make(map[string]any, len(defaultLogger.baseAttrs))
	for k, v := range defaultLogger.baseAttrs {
		out[k] = v
	}
	return out
}

// WithFields is a convenience wrapper for batch-setting base attributes.
// All keys in the map become base attributes; nil values delete keys.
func WithFields(fields map[string]any) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	for k, v := range fields {
		if v == nil {
			delete(defaultLogger.baseAttrs, k)
		} else {
			defaultLogger.baseAttrs[k] = v
		}
	}
}

// Debug logs a debug message
func Debug(format string, args ...interface{}) {
	defaultLogger.log(LevelDebug, format, args, nil)
}

// Info logs an info message
func Info(format string, args ...interface{}) {
	defaultLogger.log(LevelInfo, format, args, nil)
}

// Warn logs a warning message
func Warn(format string, args ...interface{}) {
	defaultLogger.log(LevelWarn, format, args, nil)
}

// Error logs an error message
func Error(format string, args ...interface{}) {
	defaultLogger.log(LevelError, format, args, nil)
}

// Event emits a structured log line at the given level. The msg is a fixed
// string (not a printf format); fields are alternating key/value pairs:
//
//	logging.Event(logging.LevelInfo, "chunk completed", "table", "users", "rows", 1000)
//
// In JSON mode each field becomes a top-level key alongside the base attrs.
// In text mode fields are appended after the message as space-separated
// key=value pairs, sorted alphabetically for log-diff stability.
//
// An odd number of arguments is logged as-is with a "_dangling" key — the
// log keeps flowing rather than panicking on a mis-counted call.
func Event(level Level, msg string, fields ...any) {
	defaultLogger.log(level, "%s", []any{msg}, fields)
}

// InfoEvent / WarnEvent / ErrorEvent / DebugEvent are level-specific
// shorthands for Event. Prefer these in new code over the printf-style
// API; they make hot-path logs queryable in log aggregators.
func InfoEvent(msg string, fields ...any) {
	Event(LevelInfo, msg, fields...)
}

// WarnEvent emits a structured warn-level log line.
func WarnEvent(msg string, fields ...any) {
	Event(LevelWarn, msg, fields...)
}

// ErrorEvent emits a structured error-level log line.
func ErrorEvent(msg string, fields ...any) {
	Event(LevelError, msg, fields...)
}

// DebugEvent emits a structured debug-level log line.
func DebugEvent(msg string, fields ...any) {
	Event(LevelDebug, msg, fields...)
}

// Print always prints regardless of level (for progress bars, summaries)
func Print(format string, args ...interface{}) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	fmt.Fprintf(defaultLogger.output, format, args...)
}

// Println always prints with newline regardless of level
func Println(args ...interface{}) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	fmt.Fprintln(defaultLogger.output, args...)
}

// log is the single emit path. printfArgs and eventFields are mutually
// exclusive in practice: printf-style callers leave eventFields nil; Event
// callers pass the msg via printfArgs and the structured pairs via
// eventFields. Keeping them separate lets us preserve the existing message
// format for printf calls while still merging structured fields when present.
func (l *Logger) log(level Level, format string, printfArgs []any, eventFields []any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level > l.level {
		return
	}

	msg := fmt.Sprintf(format, printfArgs...)
	attrs := mergeAttrs(l.baseAttrs, eventFields)

	if l.format == FormatJSON {
		emitJSON(l.output, level, msg, attrs)
		return
	}

	emitText(l.output, level, msg, attrs, l.simpleMode)
}

// mergeAttrs combines the persistent baseAttrs with a per-call key/value
// list. Returns nil when both sides are empty so the caller can skip
// attribute formatting entirely. Odd-length eventFields are tolerated:
// the dangling key becomes "_dangling" and the value is the dangling arg.
func mergeAttrs(base map[string]any, fields []any) map[string]any {
	if len(base) == 0 && len(fields) == 0 {
		return nil
	}
	out := make(map[string]any, len(base)+len(fields)/2)
	for k, v := range base {
		out[k] = v
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", fields[i])
		}
		out[key] = fields[i+1]
	}
	if len(fields)%2 == 1 {
		out["_dangling"] = fields[len(fields)-1]
	}
	return out
}

func emitJSON(w io.Writer, level Level, msg string, attrs map[string]any) {
	logEntry := map[string]any{
		"ts":    time.Now().Format(time.RFC3339),
		"level": strings.ToLower(level.String()),
		"msg":   strings.TrimSpace(msg),
	}
	// Merge attrs LAST so callers can't accidentally override ts/level/msg.
	// Reserved-key collisions land under attrs.* so they're still visible.
	for k, v := range attrs {
		if _, reserved := logEntry[k]; reserved {
			logEntry["attrs."+k] = v
			continue
		}
		logEntry[k] = v
	}
	data, err := json.Marshal(logEntry)
	if err != nil {
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		fallback := fmt.Sprintf("%s [%s] %s (json marshal error: %v)", timestamp, level.String(), strings.TrimSpace(msg), err)
		fmt.Fprintln(w, fallback)
		return
	}
	fmt.Fprintln(w, string(data))
}

func emitText(w io.Writer, level Level, msg string, attrs map[string]any, simpleMode bool) {
	if strings.HasPrefix(msg, "\n") {
		msg = strings.TrimPrefix(msg, "\n")
		fmt.Fprint(w, "\n")
	}

	suffix := formatAttrsForText(attrs)
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + suffix + "\n"
	} else if suffix != "" {
		msg = strings.TrimSuffix(msg, "\n") + suffix + "\n"
	}

	if simpleMode {
		fmt.Fprint(w, msg)
		return
	}
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fmt.Fprintf(w, "%s [%s] %s", timestamp, level.String(), msg)
}

// formatAttrsForText turns a key/value map into the trailing " k=v k=v"
// segment for human-readable output. Sorted by key for deterministic
// ordering — matters for log diffs and golden-file tests. Returns an
// empty string when attrs is empty so emitText can omit the leading space.
func formatAttrsForText(attrs map[string]any) string {
	if len(attrs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteByte(' ')
		sb.WriteString(k)
		sb.WriteByte('=')
		fmt.Fprintf(&sb, "%v", attrs[k])
	}
	return sb.String()
}

// IsDebug returns true if debug level is enabled
func IsDebug() bool {
	return GetLevel() >= LevelDebug
}

// IsInfo returns true if info level is enabled
func IsInfo() bool {
	return GetLevel() >= LevelInfo
}
