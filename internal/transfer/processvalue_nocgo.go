//go:build !cgo

package transfer

// processOracleValue is a no-op when CGO is disabled (Oracle not supported).
func processOracleValue(val any) (any, bool) {
	return val, false
}
