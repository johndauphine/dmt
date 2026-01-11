// Package util provides shared utility functions used across the codebase.
package util

import "strings"

// SplitCSV splits a comma-separated string into a slice, trimming whitespace.
// Returns nil for empty strings.
func SplitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
