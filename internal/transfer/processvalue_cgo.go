//go:build cgo

package transfer

import "github.com/godror/godror"

// processOracleValue handles Oracle-specific type conversions.
// Returns the converted value and true if handled, or original value and false if not.
func processOracleValue(val any) (any, bool) {
	if n, ok := val.(godror.Number); ok {
		return n.String(), true
	}
	return val, false
}
