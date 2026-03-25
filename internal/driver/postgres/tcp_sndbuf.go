package postgres

import (
	"fmt"
	"net"
	"syscall"
)

// tcpSendBufSize returns the SO_SNDBUF size for the underlying TCP connection.
// Works on Linux, macOS, and other Unix-like systems.
func tcpSendBufSize(c net.Conn) (int, error) {
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
