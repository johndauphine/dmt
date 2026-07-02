package tui

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCaptureToStringReleasesLockOnPanic pins the review fix for #556: a
// panicking fn must not leave stdoutMu wedged or os.Stdout stuck on the closed
// capture pipe — otherwise the next capture (and the next migration) deadlock.
func TestCaptureToStringReleasesLockOnPanic(t *testing.T) {
	orig := os.Stdout

	_, err := CaptureToString(func() error { panic("boom") })
	if err == nil {
		t.Fatal("expected an error from a panicking fn")
	}
	if os.Stdout != orig {
		t.Fatal("os.Stdout not restored after fn panic")
	}

	// The mutex must be free: a subsequent capture must not deadlock.
	done := make(chan struct{})
	go func() {
		_, _ = CaptureToString(func() error { fmt.Print("ok"); return nil })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stdoutMu wedged after fn panic — the next capture deadlocked")
	}
}

// TestCaptureToStringLargeOutputDoesNotDeadlock pins #556 defect 1: output
// larger than the OS pipe buffer (~64KB) must not block the writer forever.
// Before the fix, reading started only after fn() returned, so a large write
// deadlocked and the command goroutine (and its orch.Close()) hung forever.
func TestCaptureToStringLargeOutputDoesNotDeadlock(t *testing.T) {
	const size = 256 * 1024 // well past the 64KB Linux pipe capacity

	type result struct {
		out string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := CaptureToString(func() error {
			fmt.Print(strings.Repeat("x", size))
			return nil
		})
		ch <- result{out, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("CaptureToString: %v", r.err)
		}
		if len(r.out) != size {
			t.Fatalf("captured %d bytes, want %d", len(r.out), size)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CaptureToString deadlocked on output larger than the pipe buffer")
	}
}

// TestCaptureToStringConcurrentCapturesAreIsolated pins #556 defect 2: the
// os.Stdout redirect is serialized (stdoutMu), so concurrent captures don't
// race on the global or leak each other's output. Run under -race.
func TestCaptureToStringConcurrentCapturesAreIsolated(t *testing.T) {
	const n = 16
	results := make([]string, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := fmt.Sprintf("capture-%02d-payload", i)
			results[i], errs[i] = CaptureToString(func() error {
				fmt.Print(want)
				return nil
			})
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("capture %d: %v", i, errs[i])
			continue
		}
		want := fmt.Sprintf("capture-%02d-payload", i)
		if results[i] != want {
			t.Errorf("capture %d = %q, want %q (concurrent captures leaked into each other)", i, results[i], want)
		}
	}
}
