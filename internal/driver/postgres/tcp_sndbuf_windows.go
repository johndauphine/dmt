//go:build windows

package postgres

import (
	"fmt"
	"net"
)

// tcpSendBufSize is not supported on Windows — returns an error so the caller
// falls back to fallbackCopyBytes.
func tcpSendBufSize(_ net.Conn) (int, error) {
	return 0, fmt.Errorf("SO_SNDBUF probe not supported on Windows")
}
