package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriterPoolAckBackpressureDoesNotDropAck(t *testing.T) {
	writeDone := make(chan struct{})
	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters:    1,
		BufferSize:    1,
		JobBufferSize: 1,
		EnableAck:     true,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			close(writeDone)
			return nil
		},
	})
	fillAckChan(t, wp)

	wp.Start()
	if ok := wp.Submit(WriteJob{
		Rows:     [][]any{{"row"}},
		ReaderID: 7,
		Seq:      42,
		LastPK:   "pk-42",
		RowNum:   99,
	}); !ok {
		t.Fatal("Submit returned false before cancellation")
	}

	select {
	case <-writeDone:
	case <-time.After(time.Second):
		wp.Cancel()
		t.Fatal("timed out waiting for write to complete")
	}

	waitDone := make(chan struct{})
	go func() {
		wp.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("writer pool finished while ack channel was full; ack send should backpressure")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-wp.ackChan:
	case <-time.After(time.Second):
		wp.Cancel()
		t.Fatal("timed out draining saturated ack channel")
	}

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		wp.Cancel()
		t.Fatal("writer pool did not finish after ack channel had capacity")
	}

	foundJobAck := false
	for ack := range wp.ackChan {
		if ack.ReaderID == 7 && ack.Seq == 42 && ack.LastPK == "pk-42" && ack.RowNum == 99 {
			foundJobAck = true
		}
	}
	if !foundJobAck {
		t.Fatal("completed write ack was not delivered")
	}
}

func TestWriterPoolAckBackpressureCancellationUnblocks(t *testing.T) {
	writeDone := make(chan struct{})
	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters:    1,
		BufferSize:    1,
		JobBufferSize: 1,
		EnableAck:     true,
		WriteFunc: func(ctx context.Context, writerID int, rows [][]any) error {
			close(writeDone)
			return nil
		},
	})
	fillAckChan(t, wp)

	wp.Start()
	if ok := wp.Submit(WriteJob{
		Rows:     [][]any{{"row"}},
		ReaderID: 3,
		Seq:      11,
		RowNum:   12,
	}); !ok {
		t.Fatal("Submit returned false before cancellation")
	}

	select {
	case <-writeDone:
	case <-time.After(time.Second):
		wp.Cancel()
		t.Fatal("timed out waiting for write to complete")
	}

	wp.Cancel()
	waitDone := make(chan struct{})
	go func() {
		wp.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("writer pool did not exit after context cancellation")
	}
}

func fillAckChan(t *testing.T, wp *WriterPool) {
	t.Helper()
	if wp.ackChan == nil {
		t.Fatal("ack channel is nil")
	}
	for i := 0; i < cap(wp.ackChan); i++ {
		wp.ackChan <- WriteAck{ReaderID: -1, Seq: int64(i)}
	}
}

func TestOrderedAckWindowBoundsPendingWithoutByteBudget(t *testing.T) {
	firstMayFinish := make(chan struct{})
	var releasedBytes atomic.Int64

	wp := NewWriterPool(context.Background(), WriterPoolConfig{
		NumWriters:    2,
		JobBufferSize: 3,
		EnableAck:     true,
		WriteFunc: func(_ context.Context, _ int, rows [][]any) error {
			if rows[0][0].(int) == 0 {
				<-firstMayFinish
			}
			return nil
		},
		OnComplete: func(bytes int64) {
			releasedBytes.Add(bytes)
		},
	})

	nextSeq := int64(0)
	pending := make(map[int64]WriteAck)
	wp.StartOrderedAckProcessor(func(ack WriteAck) (released AckRelease) {
		if ack.Seq != nextSeq {
			pending[ack.Seq] = ack
			return released
		}
		for {
			released.Jobs++
			released.Bytes += ack.Bytes
			nextSeq++
			next, ok := pending[nextSeq]
			if !ok {
				return released
			}
			delete(pending, nextSeq)
			ack = next
		}
	})
	wp.Start()

	window := cap(wp.ackSlots)
	minimumWindow := cap(wp.jobChan) + maxWriterPoolSize
	if window < minimumWindow {
		t.Fatalf("ack window = %d, want at least %d so a runtime writer upscale is not starved", window, minimumWindow)
	}
	for i := 0; i < window; i++ {
		if ok := wp.Submit(WriteJob{Rows: [][]any{{i}}, Seq: int64(i), Bytes: 10}); !ok {
			t.Fatalf("Submit(%d) failed before window filled", i)
		}
	}
	if got := len(wp.ackSlots); got != window {
		t.Fatalf("outstanding ack slots = %d, want full structural bound %d", got, window)
	}

	extraSubmitted := make(chan bool, 1)
	go func() {
		extraSubmitted <- wp.Submit(WriteJob{Rows: [][]any{{window}}, Seq: int64(window), Bytes: 10})
	}()
	select {
	case <-extraSubmitted:
		wp.Cancel()
		close(firstMayFinish)
		t.Fatal("submission passed a full ordered-ack window while seq0 was blocked")
	case <-time.After(50 * time.Millisecond):
	}
	if got := releasedBytes.Load(); got != 0 {
		wp.Cancel()
		close(firstMayFinish)
		t.Fatalf("released %d bytes before the missing first acknowledgement", got)
	}

	close(firstMayFinish)
	select {
	case ok := <-extraSubmitted:
		if !ok {
			wp.Cancel()
			t.Fatal("submission did not resume after the sequence gap closed")
		}
	case <-time.After(time.Second):
		wp.Cancel()
		t.Fatal("submission stayed blocked after the sequence gap closed")
	}

	wp.Wait()
	wantReleased := int64(window+1) * 10
	if got := releasedBytes.Load(); got != wantReleased {
		t.Fatalf("released bytes = %d, want %d after ordered drain", got, wantReleased)
	}
	if got := len(wp.ackSlots); got != 0 {
		t.Fatalf("ack slots still held after ordered drain: %d", got)
	}
}
