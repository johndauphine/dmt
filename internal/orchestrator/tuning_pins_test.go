package orchestrator

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

type recordingTuningPinSink struct {
	workers, chunk, waw, pr, rab int
}

func (s *recordingTuningPinSink) SetPinnedWorkers(v int)           { s.workers = v }
func (s *recordingTuningPinSink) SetPinnedChunkSize(v int)         { s.chunk = v }
func (s *recordingTuningPinSink) SetPinnedWriteAheadWriters(v int) { s.waw = v }
func (s *recordingTuningPinSink) SetPinnedParallelReaders(v int)   { s.pr = v }
func (s *recordingTuningPinSink) SetPinnedReadAheadBuffers(v int)  { s.rab = v }

func TestConfigureAnalyzerPinsConnectsConfigProvenanceToEveryCandidateAxis(t *testing.T) {
	cfg, err := config.LoadBytes([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  workers: 3
  chunk_size: 1234
  write_ahead_writers: 5
  parallel_readers: 7
  read_ahead_buffers: 9
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	sink := &recordingTuningPinSink{}

	configureAnalyzerPins(cfg, sink)

	if sink.workers != 3 || sink.chunk != 1_234 || sink.waw != 5 || sink.pr != 7 || sink.rab != 9 {
		t.Fatalf("wired pins = workers:%d chunk:%d WAW:%d PR:%d RAB:%d, want 3/1234/5/7/9",
			sink.workers, sink.chunk, sink.waw, sink.pr, sink.rab)
	}
}
