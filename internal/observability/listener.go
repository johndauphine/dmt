package observability

import "net"

// newListener creates a TCP listener for the metrics endpoint. Exists as
// a separate function so tests can use port 0 + the returned listener's
// Addr() to discover the bound port without racing on a hardcoded port.
func newListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
