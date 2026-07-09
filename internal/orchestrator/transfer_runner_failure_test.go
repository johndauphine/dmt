package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/progress"
)

func TestCopyDeadlineRetriesThenClassifiesAsTableFailure(t *testing.T) {
	runner := &TransferRunner{progress: progress.New()}
	copyTimeout := fmt.Errorf("copy batch [0:1000]: %w", context.DeadlineExceeded)
	if !isRetryableError(copyTimeout) {
		t.Fatal("COPY deadline must remain retryable before exhaustion")
	}
	// executeJob sends to errCh only after its retry loop is exhausted.
	errCh := make(chan tableError, 1)
	errCh <- tableError{tableName: "orders", err: copyTimeout}
	close(errCh)

	failures, err := runner.collectFailures(context.Background(), errCh)
	if err != nil {
		t.Fatalf("collectFailures returned global interruption %v for a local COPY timeout", err)
	}
	if len(failures) != 1 || failures[0].TableName != "orders" || !errors.Is(failures[0].Error, context.DeadlineExceeded) {
		t.Fatalf("failures = %+v, want orders COPY deadline failure", failures)
	}
}

func TestCollectFailuresLocalDeadlineDoesNotDiscardOtherFailures(t *testing.T) {
	runner := &TransferRunner{progress: progress.New()}
	sentinel := errors.New("target constraint failure")
	errCh := make(chan tableError, 2)
	errCh <- tableError{tableName: "orders", err: fmt.Errorf("copy batch [0:1000]: %w", context.DeadlineExceeded)}
	errCh <- tableError{tableName: "customers", err: sentinel}
	close(errCh)

	failures, err := runner.collectFailures(context.Background(), errCh)
	if err != nil {
		t.Fatalf("collectFailures: %v", err)
	}
	got := make(map[string]error, len(failures))
	for _, failure := range failures {
		got[failure.TableName] = failure.Error
	}
	if len(got) != 2 || !errors.Is(got["orders"], context.DeadlineExceeded) || !errors.Is(got["customers"], sentinel) {
		t.Fatalf("failures = %+v, want both local timeout and constraint failure", failures)
	}
}

func TestCollectFailuresParentInterruptionWins(t *testing.T) {
	tests := []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		want    error
	}{
		{
			name: "canceled",
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			want: context.Canceled,
		},
		{
			name: "deadline exceeded",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Unix(1, 0))
			},
			want: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.context()
			defer cancel()
			runner := &TransferRunner{progress: progress.New()}
			errCh := make(chan tableError, 1)
			errCh <- tableError{tableName: "orders", err: fmt.Errorf("copy batch: %w", context.DeadlineExceeded)}
			close(errCh)

			failures, err := runner.collectFailures(ctx, errCh)
			if !errors.Is(err, tt.want) {
				t.Fatalf("collectFailures error = %v, want %v", err, tt.want)
			}
			if failures != nil {
				t.Fatalf("failures = %+v, want nil for interrupted parent", failures)
			}
		})
	}
}
