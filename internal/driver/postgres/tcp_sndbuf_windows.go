//go:build windows

package postgres

import (
	"fmt"
	"net"
)

// tcpSendBufSize is not supported on Windows — the syscall probe requires Unix,
// so the caller falls back to fallbackCopyBytes.
func tcpSendBufSize(c net.Conn) (int, error) {
	return 0, fmt.Errorf("TCP send buffer probe not supported on Windows")
}
