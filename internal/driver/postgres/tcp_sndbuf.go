//go:build !windows

package postgres

import (
	"fmt"
	"net"
	"syscall"
)

// tcpSendBufSize returns the SO_SNDBUF size for the underlying TCP connection.
// Unwraps TLS connections via the NetConn() interface to reach the raw socket.
func tcpSendBufSize(c net.Conn) (int, error) {
	// Unwrap TLS or other connection wrappers that implement NetConn().
	type netConner interface {
		NetConn() net.Conn
	}
	for {
		if u, ok := c.(netConner); ok {
			c = u.NetConn()
		} else {
			break
		}
	}

	tc, ok := c.(*net.TCPConn)
	if !ok {
		return 0, fmt.Errorf("not a TCP connection: %T", c)
	}

	raw, err := tc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("getting raw conn: %w", err)
	}

	var sndbuf int
	var sysErr error
	err = raw.Control(func(fd uintptr) {
		sndbuf, sysErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
	})
	if err != nil {
		return 0, err
	}
	if sysErr != nil {
		return 0, sysErr
	}
	return sndbuf, nil
}
