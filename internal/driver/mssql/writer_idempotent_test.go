package mssql

import (
	"strings"
	"testing"
)

// TestBuildMerge_InsertOnlyOmitsMatchedBranch pins the resume-safe shape added
// for #227: when insertOnly=true, the MERGE must contain only WHEN NOT MATCHED
// (replayed already-present rows are silent no-ops). When insertOnly=false the
// upsert path's WHEN MATCHED ... THEN UPDATE branch must still be emitted —
// regressing the upsert target mode is not acceptable.
func TestBuildMerge_InsertOnlyOmitsMatchedBranch(t *testing.T) {
	w := &Writer{dialect: &Dialect{}}

	cols := []string{"id", "title", "body"}
	pkCols := []string{"id"}
	target := "[dbo].[posts]"
	staging := "#stg_posts_w0"

	insertOnly := w.buildMerge(target, staging, cols, pkCols, nil, false, true)
	if strings.Contains(insertOnly, "WHEN MATCHED") {
		t.Errorf("insertOnly=true MERGE must not contain WHEN MATCHED:\n%s", insertOnly)
	}
	if !strings.Contains(insertOnly, "WHEN NOT MATCHED THEN INSERT") {
		t.Errorf("insertOnly=true MERGE must contain WHEN NOT MATCHED THEN INSERT:\n%s", insertOnly)
	}
	if !strings.Contains(insertOnly, "ON target.[id] = source.[id]") {
		t.Errorf("insertOnly=true MERGE must contain ON-clause for PK:\n%s", insertOnly)
	}

	upsert := w.buildMerge(target, staging, cols, pkCols, nil, false, false)
	if !strings.Contains(upsert, "WHEN MATCHED") {
		t.Errorf("insertOnly=false (upsert) MERGE must contain WHEN MATCHED — regression:\n%s", upsert)
	}
	if !strings.Contains(upsert, "WHEN NOT MATCHED THEN INSERT") {
		t.Errorf("insertOnly=false MERGE must contain WHEN NOT MATCHED THEN INSERT:\n%s", upsert)
	}
}

// TestSafeStagingName_PartitionedNamesAreUnique guards the resume-safe path:
// concurrent partitions on the same writer connection must produce different
// staging table names. Without partitionID in the suffix, two partitions on
// the same writer would collide on the local #temp table.
func TestSafeStagingName_PartitionedNamesAreUnique(t *testing.T) {
	w := &Writer{dialect: &Dialect{}}

	p1, p2 := 1, 2
	n1 := w.safeStagingName("posts", 0, &p1)
	n2 := w.safeStagingName("posts", 0, &p2)
	if n1 == n2 {
		t.Errorf("partitioned staging names must differ: p1=%q p2=%q", n1, n2)
	}
	// And the partition-less form must differ from a partitioned one
	// so old upsert callers don't collide with new idempotent-write
	// partitions on the same writer ID.
	nNil := w.safeStagingName("posts", 0, nil)
	if nNil == n1 {
		t.Errorf("partitioned and unpartitioned staging names must differ: nil=%q p1=%q", nNil, n1)
	}
}
