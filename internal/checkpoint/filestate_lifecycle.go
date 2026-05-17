package checkpoint

func (fs *FileState) Close() error {
	return nil
}

// Path returns the state file path.
func (fs *FileState) Path() string {
	return fs.path
}

// Ensure FileState implements StateBackend
var _ StateBackend = (*FileState)(nil)
