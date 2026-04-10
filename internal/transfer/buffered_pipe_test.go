package transfer

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestBufferedPipe_RoundTrip(t *testing.T) {
	w, r := newBufferedPipe(64, 4)

	payload := []byte("the quick brown fox jumps over the lazy dog")
	var got bytes.Buffer

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := io.Copy(&got, r); err != nil {
			t.Errorf("reader copy: %v", err)
		}
	}()

	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wg.Wait()

	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got.Bytes(), payload)
	}
}

func TestBufferedPipe_LargePayloadManyChunks(t *testing.T) {
	w, r := newBufferedPipe(1024, 8)

	// 2 MB of random data — forces many chunk flushes and channel turns.
	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}

	var got bytes.Buffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := io.Copy(&got, r); err != nil {
			t.Errorf("reader copy: %v", err)
		}
	}()

	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-done

	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("large payload mismatch: got %d bytes want %d", got.Len(), len(payload))
	}
}

func TestBufferedPipe_WriterCloseWithErrorPropagates(t *testing.T) {
	w, r := newBufferedPipe(64, 2)

	sentinel := errors.New("producer blew up")

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, r)
		done <- err
	}()

	_, _ = w.Write([]byte("partial"))
	_ = w.CloseWithError(sentinel)

	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected sentinel error to propagate, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader blocked after CloseWithError")
	}
}

func TestBufferedPipe_ReaderCloseStopsWriter(t *testing.T) {
	// chunkBytes=16, capacity=1 — so one 16-byte chunk can land in the
	// channel buffer, and any further flush will block.
	w, r := newBufferedPipe(16, 1)

	// First 16 bytes fill one full chunk and push it into the 1-slot channel.
	if _, err := w.Write(make([]byte, 16)); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	writeDone := make(chan error, 1)
	go func() {
		// This write forces another chunk flush with no free channel slot —
		// it will block until the reader closes.
		_, err := w.Write(make([]byte, 16))
		writeDone <- err
	}()

	// Give the writer time to block on flushChunk.
	time.Sleep(50 * time.Millisecond)

	// Reader closes without draining — writer should unblock with an error.
	_ = r.Close()

	select {
	case err := <-writeDone:
		if err == nil {
			t.Fatal("expected write to fail after reader closed, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer still blocked after reader Close")
	}
}

func TestBufferedPipe_ConcurrentProducerRunsAhead(t *testing.T) {
	// Verifies the point of bufferedPipe: the writer can flush several
	// chunks into the channel before the reader reads any of them. With a
	// synchronous io.Pipe the writer would block on the first Write.
	w, r := newBufferedPipe(16, 4)

	flushed := make(chan int, 4)
	go func() {
		for i := 0; i < 4; i++ {
			if _, err := w.Write(make([]byte, 16)); err != nil {
				return
			}
			flushed <- i
		}
		_ = w.Close()
	}()

	// All 4 chunks should be flushed into the channel before we ever read.
	for i := 0; i < 4; i++ {
		select {
		case <-flushed:
		case <-time.After(2 * time.Second):
			t.Fatalf("writer blocked at chunk %d without reader", i)
		}
	}

	var got bytes.Buffer
	if _, err := io.Copy(&got, r); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got.Len() != 64 {
		t.Fatalf("expected 64 bytes, got %d", got.Len())
	}
}
