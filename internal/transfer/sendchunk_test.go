package transfer

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSendChunkOrCancel_Delivers(t *testing.T) {
	ch := make(chan chunkResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !sendChunkOrCancel(ctx, ch, chunkResult{seq: 1}) {
		t.Fatal("returned false on an open channel with no cancel")
	}
	select {
	case got := <-ch:
		if got.seq != 1 {
			t.Errorf("seq = %d, want 1", got.seq)
		}
	default:
		t.Fatal("nothing landed in the channel")
	}
}

func TestSendChunkOrCancel_AlreadyCancelled(t *testing.T) {
	// Full buffer + already-cancelled ctx means only the cancel branch
	// is ever runnable. (If the channel had space, Go's select can
	// pick either branch — this test pins the cancel-only case.)
	ch := make(chan chunkResult, 1)
	ch <- chunkResult{} // fill it

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sendChunkOrCancel(ctx, ch, chunkResult{seq: 99}) {
		t.Fatal("returned true on a full channel with a cancelled ctx")
	}
}

// TestSendChunkOrCancel_UnblocksMidSend is the core regression guard
// for #250: a reader goroutine blocked on `chunkChan <- r` must
// unblock the moment the per-transfer context is cancelled, even if
// nothing is draining the channel. Before the fix, bare sends would
// leak the goroutine plus the close-channel goroutine waiting on it.
func TestSendChunkOrCancel_UnblocksMidSend(t *testing.T) {
	ch := make(chan chunkResult) // unbuffered → producer blocks immediately
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	var delivered bool
	go func() {
		defer wg.Done()
		delivered = sendChunkOrCancel(ctx, ch, chunkResult{seq: 1})
	}()

	// Give the goroutine a moment to land on the select.
	time.Sleep(20 * time.Millisecond)

	// Cancel without anyone consuming from ch. The producer must exit
	// via the ctx.Done() branch.
	cancel()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		// good
	case <-time.After(time.Second):
		t.Fatal("producer goroutine did not unblock after cancel — #250 regression")
	}

	if delivered {
		t.Error("delivered = true after cancel without consumer; want false")
	}
}

// TestSendChunkOrCancel_NoLeakUnderWriterFailurePattern simulates the
// shape of the #250 bug: N producers push into a buffered channel; the
// consumer breaks out of its loop after a few chunks (writer-failure
// analogue) and cancels the producer context. Without the fix,
// producers would remain blocked on a full channel forever.
func TestSendChunkOrCancel_NoLeakUnderWriterFailurePattern(t *testing.T) {
	const numReaders = 4
	const bufCap = 2

	ch := make(chan chunkResult, bufCap)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; ; j++ {
				if !sendChunkOrCancel(ctx, ch, chunkResult{readerID: id, seq: int64(j)}) {
					return
				}
			}
		}(i)
	}

	// Consumer drains only a few chunks, then "fails" and cancels.
	for i := 0; i < 3; i++ {
		<-ch
	}
	cancel()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	startGoroutines := runtime.NumGoroutine()
	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatalf("%d producer goroutines did not exit after cancel — #250 regression (numGoroutine=%d)",
			numReaders, runtime.NumGoroutine()-startGoroutines)
	}
}
