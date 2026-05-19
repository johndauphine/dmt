package driver

import (
	"fmt"
	"strings"
	"time"
)

// ParseDateValue converts database driver timestamp values into UTC time.
// Date-column watermarks can come from native timestamp types or text-backed
// sources such as SQLite, so this keeps the parsing rules in one place.
func ParseDateValue(value any) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case time.Time:
		t := v.UTC()
		return &t, nil
	case string:
		return parseDateString(v)
	case []byte:
		return parseDateString(string(v))
	default:
		return nil, fmt.Errorf("unsupported date value type %T", value)
	}
}

func parseDateString(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			utc := t.UTC()
			return &utc, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("parse date value %q: %w", value, lastErr)
}
