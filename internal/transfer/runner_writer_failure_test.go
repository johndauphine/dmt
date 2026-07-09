package transfer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
)

type writerFailureBlockingProducer struct {
	canceled chan struct{}
}

func (p *writerFailureBlockingProducer) readerCount() int { return 1 }

func (p *writerFailureBlockingProducer) produce(ctx context.Context, env pipelineEnv, out chan<- chunkResult) {
	reserved, ok := env.acquireMem(ctx, 64)
	if !ok {
		return
	}
	if !sendChunkOrCancel(ctx, out, chunkResult{
		rows:     [][]any{{int64(1), "payload"}},
		lastPK:   int64(1),
		bytes:    reserved,
		readEnd:  time.Now(),
		readerID: 0,
		seq:      0,
	}) {
		return
	}

	<-ctx.Done()
	close(p.canceled)
}

func TestRunPipelineWriterFailureCancelsBlockedProducer(t *testing.T) {
	for _, withBudget := range []bool{false, true} {
		t.Run(map[bool]string{false: "without_budget", true: "with_budget"}[withBudget], func(t *testing.T) {
			sentinel := errors.New("sentinel writer failure")
			producer := &writerFailureBlockingProducer{canceled: make(chan struct{})}
			target := &keysetRuntimeTargetPool{
				updated:   true,
				writeErr:  sentinel,
				failAfter: 0,
			}
			table := driver.Table{
				Name:             "items",
				PrimaryKey:       []string{"id"},
				EstimatedRowSize: 32,
			}
			table.PopulatePKColumns()
			job := Job{Table: table}
			if withBudget {
				job.MemBudget = NewMemBudget(1024)
			}
			cfg := &config.Config{
				Target: config.TargetConfig{Schema: ""},
				Migration: config.MigrationConfig{
					ChunkSize:         1,
					ParallelReaders:   1,
					WriteAheadWriters: 1,
					TargetMode:        "drop_recreate",
				},
			}

			done := make(chan error, 1)
			go func() {
				_, err := runPipeline(context.Background(), pipelineConfig{
					cfg:             cfg,
					job:             job,
					tgtPool:         target,
					prog:            progress.New(),
					producer:        producer,
					targetTableName: "items",
					targetCols:      []string{"id", "payload"},
					colTypes:        []string{"integer", "text"},
					colSRIDs:        []int{0, 0},
				})
				done <- err
			}()

			select {
			case err := <-done:
				if !errors.Is(err, sentinel) {
					t.Fatalf("runPipeline error = %v, want sentinel writer failure", err)
				}
				if errors.Is(err, context.Canceled) {
					t.Fatalf("runPipeline returned induced context cancellation: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("runPipeline did not cancel the producer after writer failure")
			}

			select {
			case <-producer.canceled:
			default:
				t.Fatal("producer context was not canceled")
			}
			if job.MemBudget != nil && !job.MemBudget.fullyReleased() {
				t.Fatal("memory budget was not fully released")
			}
		})
	}
}
