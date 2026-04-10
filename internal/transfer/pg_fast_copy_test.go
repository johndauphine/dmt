package transfer

import (
	"testing"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

// stubReaderNonPG implements pool.SourcePool (= driver.Reader) minimally
// enough for pgFastCopyEligible with DBType()=="mysql". The fast path must
// refuse this.
type stubReaderNonPG struct{ driver.Reader }

func (stubReaderNonPG) DBType() string { return "mysql" }

// stubReaderPGNoCopy is a PG source that DOES NOT implement BinaryCopyReader.
// Must also be rejected.
type stubReaderPGNoCopy struct{ driver.Reader }

func (stubReaderPGNoCopy) DBType() string { return "postgres" }

// stubWriterPGNoCopy is a PG target that DOES NOT implement BinaryCopyWriter.
type stubWriterPGNoCopy struct{ driver.Writer }

func (stubWriterPGNoCopy) DBType() string { return "postgres" }

// stubReaderPGWithCopy is a PG source that implements BinaryCopyReader.
type stubReaderPGWithCopy struct {
	driver.Reader
	driver.BinaryCopyReader
}

func (stubReaderPGWithCopy) DBType() string { return "postgres" }

// stubWriterPGWithCopy is a PG target that implements BinaryCopyWriter.
type stubWriterPGWithCopy struct {
	driver.Writer
	driver.BinaryCopyWriter
}

func (stubWriterPGWithCopy) DBType() string { return "postgres" }

func baseCfg() *config.Config {
	return &config.Config{
		Migration: config.MigrationConfig{
			TargetMode: "drop_recreate",
		},
	}
}

func baseJob() Job {
	return Job{
		Table: source.Table{
			Name:   "users",
			Schema: "public",
		},
	}
}

func TestPgFastCopyEligible(t *testing.T) {
	cfg := baseCfg()
	job := baseJob()

	t.Run("rejects non-postgres source", func(t *testing.T) {
		var src pool.SourcePool = stubReaderNonPG{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, job, nil); ok {
			t.Error("expected non-postgres source to be rejected")
		}
	})

	t.Run("rejects postgres source without BinaryCopyReader", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGNoCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, job, nil); ok {
			t.Error("expected source without BinaryCopyReader capability to be rejected")
		}
	})

	t.Run("rejects postgres target without BinaryCopyWriter", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGNoCopy{}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, job, nil); ok {
			t.Error("expected target without BinaryCopyWriter capability to be rejected")
		}
	})

	t.Run("rejects upsert mode", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		c := baseCfg()
		c.Migration.TargetMode = "upsert"
		if _, _, ok := pgFastCopyEligible(src, tgt, c, job, nil); ok {
			t.Error("expected upsert mode to be rejected")
		}
	})

	t.Run("rejects job with date filter", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.DateFilter = &DateFilter{Column: "updated_at"}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); ok {
			t.Error("expected date filter to be rejected")
		}
	})

	t.Run("rejects resume state", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, job, int64(123)); ok {
			t.Error("expected resume state to be rejected (phase 1)")
		}
	})

	t.Run("rejects when disabled via config flag", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		c := baseCfg()
		c.Migration.PgFastCopyDisabled = true
		if _, _, ok := pgFastCopyEligible(src, tgt, c, job, nil); ok {
			t.Error("expected pg_fast_copy_disabled to reject eligibility")
		}
	})

	t.Run("rejects partitioned job", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.Partition = &driver.Partition{}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); ok {
			t.Error("expected partitioned job to be rejected (phase 1)")
		}
	})

	t.Run("accepts pg→pg drop_recreate non-partitioned no-resume", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		bcr, bcw, ok := pgFastCopyEligible(src, tgt, cfg, job, nil)
		if !ok {
			t.Fatal("expected eligibility to succeed")
		}
		if bcr == nil || bcw == nil {
			t.Error("expected non-nil capability returns on success")
		}
	})
}
