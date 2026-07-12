//go:build !linux

package systemmemory

type hostOnlyReader struct {
	hostRead hostReadFunc
}

func newPlatformReader(hostRead hostReadFunc) Reader {
	return &hostOnlyReader{hostRead: hostRead}
}

func (r *hostOnlyReader) Read() (Snapshot, error) {
	host, err := r.hostRead()
	if err != nil {
		return Snapshot{}, err
	}
	return hostSnapshot(host), nil
}
