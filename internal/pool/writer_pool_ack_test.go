package pool

import (
	"context"
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
