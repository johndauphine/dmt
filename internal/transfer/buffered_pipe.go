package transfer

import (
	"errors"
	"io"
	"sync"
)

// bufferedPipe is an in-memory, single-producer/single-consumer byte pipe
// backed by a bounded channel of []byte chunks. It decouples the producer
// from the consumer so the producer may run ahead of the consumer by up to
// (chunkBytes × capacity) bytes before blocking.
//
// This exists because io.Pipe is strictly synchronous: every Write blocks
// until a matching Read consumes the bytes. For network-bound relays that
// is fatal — each side must wait for the other to complete network I/O
// before the next transfer begins, which means source and target cannot
// overlap their work. bufferedPipe restores pipelining by interposing a
// small in-memory queue.
//
// Producer calls Write on the bufferedPipeWriter; consumer calls Read on
// the bufferedPipeReader. Writer.Close signals graceful EOF; either side
// can abort with CloseWithError which propagates to the other.
type bufferedPipe struct {
	chunks chan []byte // never closed; drained on EOF via done signal
	done   chan struct{}

	closeOnce sync.Once

	mu  sync.Mutex
	err error
	// writerDone is true once the writer has signaled graceful EOF.
	// Distinguishes "writer finished cleanly, reader should drain then EOF"
	// from "reader aborted, writer should fail".
	writerDone bool
}

// newBufferedPipe returns a Writer/Reader pair that share a bounded internal
// buffer. capacity is the number of chunks held in flight, chunkBytes is the
// max size of each chunk. Total buffer footprint is roughly
// capacity * chunkBytes bytes.
func newBufferedPipe(chunkBytes, capacity int) (*bufferedPipeWriter, *bufferedPipeReader) {
	if chunkBytes <= 0 {
		chunkBytes = 256 * 1024
	}
	if capacity <= 0 {
		capacity = 8
	}
	bp := &bufferedPipe{
		chunks: make(chan []byte, capacity),
		done:   make(chan struct{}),
	}
	return &bufferedPipeWriter{bp: bp, chunkBytes: chunkBytes}, &bufferedPipeReader{bp: bp}
}

// signalDone closes the done channel exactly once, optionally recording a
// first-wins error and a writerDone flag.
func (bp *bufferedPipe) signalDone(err error, writerFinished bool) {
	bp.closeOnce.Do(func() {
		bp.mu.Lock()
		if err != nil && bp.err == nil {
			bp.err = err
		}
		bp.writerDone = writerFinished
		bp.mu.Unlock()
		close(bp.done)
	})
}

// currentErr returns the stored closure error, if any.
func (bp *bufferedPipe) currentErr() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.err
}

// writerFinishedCleanly reports whether the writer signaled graceful EOF
// (vs the reader aborting with CloseWithError).
func (bp *bufferedPipe) writerFinishedCleanly() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.writerDone && bp.err == nil
}

// bufferedPipeWriter is the producer side.
type bufferedPipeWriter struct {
	bp         *bufferedPipe
	chunkBytes int
	buf        []byte // staging buffer for small writes
}

// Write stages bytes into an internal buffer and flushes full chunks onto
// the channel. Returns io.ErrClosedPipe if the consumer has aborted.
func (w *bufferedPipeWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	written := 0
	for len(p) > 0 {
		free := w.chunkBytes - len(w.buf)
		if free <= 0 {
			if err := w.flushChunk(); err != nil {
				return written, err
			}
			continue
		}
		n := free
		if n > len(p) {
			n = len(p)
		}
		w.buf = append(w.buf, p[:n]...)
		p = p[n:]
		written += n
		if len(w.buf) >= w.chunkBytes {
			if err := w.flushChunk(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// flushChunk pushes the staged bytes onto the channel, transferring ownership.
// Respects the done signal so an aborting reader unblocks the writer.
func (w *bufferedPipeWriter) flushChunk() error {
	if len(w.buf) == 0 {
		return nil
	}
	chunk := w.buf
	w.buf = nil
	select {
	case w.bp.chunks <- chunk:
		return nil
	case <-w.bp.done:
		if err := w.bp.currentErr(); err != nil {
			return err
		}
		return io.ErrClosedPipe
	}
}

// Close flushes any remaining buffered bytes and signals graceful EOF so
// the reader returns io.EOF after draining the channel.
func (w *bufferedPipeWriter) Close() error {
	if err := w.flushChunk(); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			// Reader already aborted — just mirror its closure without
			// overriding its error.
			w.bp.signalDone(nil, false)
			return nil
		}
		w.bp.signalDone(err, true)
		return err
	}
	w.bp.signalDone(nil, true)
	return nil
}

// CloseWithError signals an abnormal producer-side failure to the consumer.
func (w *bufferedPipeWriter) CloseWithError(err error) error {
	if err == nil {
		return w.Close()
	}
	w.bp.signalDone(err, true)
	return nil
}

// bufferedPipeReader is the consumer side.
type bufferedPipeReader struct {
	bp        *bufferedPipe
	remainder []byte // leftover bytes from a chunk not fully consumed by the last Read
}

// Read pulls bytes from the channel, returning io.EOF when the producer has
// finished gracefully and the channel is drained, or a propagated error if
// the producer aborted.
func (r *bufferedPipeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.remainder) == 0 {
		select {
		case chunk := <-r.bp.chunks:
			r.remainder = chunk
		case <-r.bp.done:
			// Writer finished (or aborted). Drain any remaining buffered
			// chunks before returning EOF — the writer may have flushed
			// bytes into the channel between the final Write and Close.
			select {
			case chunk := <-r.bp.chunks:
				r.remainder = chunk
			default:
				if err := r.bp.currentErr(); err != nil {
					return 0, err
				}
				if r.bp.writerFinishedCleanly() {
					return 0, io.EOF
				}
				// Reader's own CloseWithError path: return pipe-closed.
				return 0, io.ErrClosedPipe
			}
		}
	}
	n := copy(p, r.remainder)
	r.remainder = r.remainder[n:]
	return n, nil
}

// Close signals the producer that no more bytes will be read. In-flight
// bytes in the channel are discarded.
func (r *bufferedPipeReader) Close() error {
	r.bp.signalDone(nil, false)
	return nil
}

// CloseWithError signals consumer-side failure so the producer's next Write
// returns the error.
func (r *bufferedPipeReader) CloseWithError(err error) error {
	if err == nil {
		return r.Close()
	}
	r.bp.signalDone(err, false)
	return nil
}
