package transfer

import (
	"fmt"
	"math"
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

func TestBuildFastCopySubRanges(t *testing.T) {
	t.Run("non-partitioned returns single empty clause", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 || got[0] != "" {
			t.Errorf("expected single empty clause, got %v", got)
		}
	})

	t.Run("partition smaller than chunk size produces one sub-range", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{MinPK: int64(1), MaxPK: int64(1000)}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 {
			t.Fatalf("expected 1 sub-range, got %d: %v", len(got), got)
		}
		want := `"id" >= 1 AND "id" <= 1000`
		if got[0] != want {
			t.Errorf("got %q, want %q", got[0], want)
		}
	})

	t.Run("partition exactly chunkSize rows is one sub-range", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{MinPK: int64(1), MaxPK: int64(50000)}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 {
			t.Fatalf("expected 1 sub-range for exact chunk boundary, got %d: %v", len(got), got)
		}
		want := `"id" >= 1 AND "id" <= 50000`
		if got[0] != want {
			t.Errorf("got %q, want %q", got[0], want)
		}
	})

	t.Run("partition splits across multiple sub-ranges non-overlapping", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		// 125001 - 1 + 1 = 125001 rows, chunkSize=50000 → 3 sub-ranges
		j.Partition = &driver.Partition{MinPK: int64(1), MaxPK: int64(125001)}
		got := buildFastCopySubRanges(j, 50000)
		want := []string{
			`"id" >= 1 AND "id" <= 50000`,
			`"id" >= 50001 AND "id" <= 100000`,
			`"id" >= 100001 AND "id" <= 125001`,
		}
		if len(got) != len(want) {
			t.Fatalf("expected %d sub-ranges, got %d: %v", len(want), len(got), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("sub-range %d: got %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("negative and wide partitions still slice correctly", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"badge_id"}
		j.Partition = &driver.Partition{MinPK: int64(-5), MaxPK: int64(4)}
		got := buildFastCopySubRanges(j, 4)
		want := []string{
			`"badge_id" >= -5 AND "badge_id" <= -2`,
			`"badge_id" >= -1 AND "badge_id" <= 2`,
			`"badge_id" >= 3 AND "badge_id" <= 4`,
		}
		if len(got) != len(want) {
			t.Fatalf("expected %d sub-ranges, got %d: %v", len(want), len(got), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("sub-range %d: got %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("int32 bounds are coerced", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{MinPK: int32(1), MaxPK: int32(100)}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 || got[0] != `"id" >= 1 AND "id" <= 100` {
			t.Errorf("int32 coercion failed: %v", got)
		}
	})

	t.Run("string bounds fall back to empty single clause", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{MinPK: "a", MaxPK: "z"}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 || got[0] != "" {
			t.Errorf("expected single empty clause for string bounds, got %v", got)
		}
	})

	t.Run("zero chunkSize uses default", func(t *testing.T) {
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{MinPK: int64(1), MaxPK: int64(100000)}
		got := buildFastCopySubRanges(j, 0)
		// default chunkSize is 50000, so 100000 rows = 2 sub-ranges
		if len(got) != 2 {
			t.Fatalf("expected 2 sub-ranges with default chunkSize, got %d: %v", len(got), got)
		}
	})

	t.Run("partition near int64 max does not overflow", func(t *testing.T) {
		// Regression: cur + (chunkSize - 1) must not overflow int64 even
		// when cur is close to math.MaxInt64. Must terminate cleanly.
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{
			MinPK: int64(math.MaxInt64 - 100),
			MaxPK: int64(math.MaxInt64),
		}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 {
			t.Fatalf("expected exactly one sub-range near int64 max, got %d: %v", len(got), got)
		}
		want := fmt.Sprintf(`"id" >= %d AND "id" <= %d`, int64(math.MaxInt64-100), int64(math.MaxInt64))
		if got[0] != want {
			t.Errorf("got %q, want %q", got[0], want)
		}
	})

	t.Run("exact int64 max upper bound terminates", func(t *testing.T) {
		// Ensure the loop terminates when end == math.MaxInt64 without
		// attempting cur = end + 1 which would wrap.
		j := baseJob()
		j.Table.PrimaryKey = []string{"id"}
		j.Partition = &driver.Partition{
			MinPK: int64(math.MaxInt64 - 5),
			MaxPK: int64(math.MaxInt64),
		}
		got := buildFastCopySubRanges(j, 3)
		// 6 rows total (MaxInt-5 .. MaxInt), chunkSize=3 → 2 sub-ranges
		if len(got) != 2 {
			t.Fatalf("expected 2 sub-ranges at int64 max boundary, got %d: %v", len(got), got)
		}
	})

	t.Run("pk identifier with embedded quote is escaped via dialect", func(t *testing.T) {
		// Regression: manual '"' + ident + '"' quoting would leave embedded
		// double quotes unescaped and produce invalid SQL. The postgres
		// dialect's QuoteIdentifier doubles any embedded " to "".
		j := baseJob()
		j.Table.PrimaryKey = []string{`weird"id`}
		j.Partition = &driver.Partition{MinPK: int64(1), MaxPK: int64(10)}
		got := buildFastCopySubRanges(j, 50000)
		if len(got) != 1 {
			t.Fatalf("expected 1 sub-range, got %d", len(got))
		}
		// Postgres identifier quoting doubles embedded " → "weird""id"
		want := `"weird""id" >= 1 AND "weird""id" <= 10`
		if got[0] != want {
			t.Errorf("identifier escape failed: got %q, want %q", got[0], want)
		}
	})
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

	t.Run("accepts partitioned job with integer bounds", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.Partition = &driver.Partition{
			PartitionID: 1,
			MinPK:       int64(1),
			MaxPK:       int64(1000),
		}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); !ok {
			t.Error("expected partitioned job with int64 bounds to be accepted (phase 2)")
		}
	})

	t.Run("accepts partitioned job with plain int bounds", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.Partition = &driver.Partition{
			PartitionID: 1,
			MinPK:       int(1),
			MaxPK:       int(1000),
		}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); !ok {
			t.Error("expected partitioned job with int bounds to be accepted")
		}
	})

	t.Run("rejects partitioned job with non-integer bounds", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.Partition = &driver.Partition{
			PartitionID: 1,
			MinPK:       "abc",
			MaxPK:       "xyz",
		}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); ok {
			t.Error("expected string-keyed partition to be rejected")
		}
	})

	t.Run("rejects partitioned job with nil bounds", func(t *testing.T) {
		var src pool.SourcePool = stubReaderPGWithCopy{}
		var tgt pool.TargetPool = stubWriterPGWithCopy{}
		j := baseJob()
		j.Partition = &driver.Partition{
			PartitionID: 1,
			MinPK:       nil,
			MaxPK:       nil,
		}
		if _, _, ok := pgFastCopyEligible(src, tgt, cfg, j, nil); ok {
			t.Error("expected nil-bound partition to be rejected")
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
