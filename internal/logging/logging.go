package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
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

// Logger provides leveled logging
type Logger struct {
	mu         sync.Mutex
	level      Level
	output     io.Writer
	format     Format
	simpleMode bool // When true, skip timestamps and level prefixes (for TUI)
}

var (
	defaultLogger = &Logger{
		level:  LevelInfo,
		output: os.Stdout,
		format: FormatText,
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

// Debug logs a debug message
func Debug(format string, args ...interface{}) {
	defaultLogger.log(LevelDebug, format, args...)
}

// Info logs an info message
func Info(format string, args ...interface{}) {
	defaultLogger.log(LevelInfo, format, args...)
}

// Warn logs a warning message
func Warn(format string, args ...interface{}) {
	defaultLogger.log(LevelWarn, format, args...)
}

// Error logs an error message
func Error(format string, args ...interface{}) {
	defaultLogger.log(LevelError, format, args...)
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

func (l *Logger) log(level Level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level > l.level {
		return
	}

	msg := fmt.Sprintf(format, args...)

	// JSON format output
	if l.format == FormatJSON {
		logEntry := map[string]interface{}{
			"ts":    time.Now().Format(time.RFC3339),
			"level": strings.ToLower(level.String()),
			"msg":   strings.TrimSpace(msg),
		}
		data, err := json.Marshal(logEntry)
		if err != nil {
			// Fallback to text output if JSON marshaling fails
			timestamp := time.Now().Format("2006-01-02 15:04:05")
			fallback := fmt.Sprintf("%s [%s] %s (json marshal error: %v)", timestamp, level.String(), strings.TrimSpace(msg), err)
			fmt.Fprintln(l.output, fallback)
			return
		}
		fmt.Fprintln(l.output, string(data))
		return
	}

	// Text format output
	if strings.HasPrefix(msg, "\n") {
		// Handle leading newlines (preserve blank line formatting)
		msg = strings.TrimPrefix(msg, "\n")
		fmt.Fprint(l.output, "\n")
	}

	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}

	if l.simpleMode {
		// Simple mode: just output the message without timestamps or level prefixes
		fmt.Fprint(l.output, msg)
	} else {
		// Full mode: include timestamp and level prefix
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		fmt.Fprintf(l.output, "%s [%s] %s", timestamp, level.String(), msg)
	}
}

// IsDebug returns true if debug level is enabled
func IsDebug() bool {
	return GetLevel() >= LevelDebug
}

// IsInfo returns true if info level is enabled
func IsInfo() bool {
	return GetLevel() >= LevelInfo
}
