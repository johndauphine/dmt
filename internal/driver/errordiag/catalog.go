// Package errordiag provides deterministic, regex-matched diagnoses for
// common database errors emitted during migrations. Each registered
// driver contributes a catalog of patterns; Lookup returns the first
// match for a given error message. Callers that need richer analysis
// can fall back to the AI diagnoser in the parent driver package
// when Lookup returns no match.
//
// This package intentionally does not import the parent driver package
// to avoid an import cycle. The parent package adapts the local
// Diagnosis type into driver.ErrorDiagnosis at the boundary.
package errordiag

import (
	"regexp"
	"strings"
	"sync"
)

// Diagnosis is the deterministic equivalent of driver.ErrorDiagnosis.
// Confidence is one of "high", "medium", "low". Category is one of
// "type_mismatch", "constraint", "permission", "connection",
// "data_quality", "other" — same set the AI path normalizes to.
type Diagnosis struct {
	Cause       string
	Suggestions []string
	Confidence  string
	Category    string
}

// Pattern is one entry in a driver's catalog: a regex applied to the
// raw error message and a function that builds the Diagnosis from any
// captured groups.
type Pattern struct {
	Name     string
	Regex    *regexp.Regexp
	Diagnose func(matches []string) Diagnosis
}

// Match is what Lookup returns when a pattern fires. PatternName lets
// callers record which pattern matched for telemetry (#176).
type Match struct {
	Diagnosis   Diagnosis
	PatternName string
}

var (
	mu       sync.RWMutex
	catalogs = map[string][]Pattern{}
)

// Register adds patterns under one or more driver names. Driver names
// are stored lowercase; lookup is case-insensitive. Callers should
// register both the canonical driver name and any aliases they want to
// match (the parent driver package's Canonicalize is preferred at the
// lookup site, but registering aliases too is harmless).
func Register(driverName string, patterns []Pattern) {
	mu.Lock()
	defer mu.Unlock()
	key := strings.ToLower(driverName)
	catalogs[key] = append(catalogs[key], patterns...)
}

// Lookup returns the first matching pattern's diagnosis for the given
// driver. If no pattern matches (or no catalog is registered for the
// driver), returns ok=false. Matching order is the order patterns were
// registered — earliest match wins, so put more specific patterns
// before more general ones in each driver file.
func Lookup(driverName, errMsg string) (Match, bool) {
	mu.RLock()
	defer mu.RUnlock()
	patterns := catalogs[strings.ToLower(driverName)]
	for _, p := range patterns {
		if m := p.Regex.FindStringSubmatch(errMsg); m != nil {
			return Match{Diagnosis: p.Diagnose(m), PatternName: p.Name}, true
		}
	}
	return Match{}, false
}

// PatternCount reports how many patterns are registered for a driver.
// Useful for tests that assert minimum catalog coverage.
func PatternCount(driverName string) int {
	mu.RLock()
	defer mu.RUnlock()
	return len(catalogs[strings.ToLower(driverName)])
}
