package tuning

import (
	"math"
	"testing"
)

func TestMemoryModelTinyWideLookupDoesNotGloballyBind(t *testing.T) {
	profile := NewMemoryProfile([]TableMemoryStat{
		{Name: "tiny_lookup", RowCount: 2, AvgRowBytes: 36_864},
		{Name: "large_table", RowCount: 1_000_000, AvgRowBytes: 100},
	})
	model := NewMemoryModel(profile, 36_864)

	if !model.UsesTableProfile() || model.TableCount() != 2 {
		t.Fatalf("profile state = complete %v/count %d, want true/2", model.UsesTableProfile(), model.TableCount())
	}
	if got, want := model.SafeChunkSize(1, 1, 1, 0), int64(10_485); got != want {
		t.Fatalf("table-aware cap = %d, want %d", got, want)
	}
	if scalar := SafeChunkSize(1, 1, 1, 0, 36_864); scalar != 28 {
		t.Fatalf("scalar control cap = %d, want 28", scalar)
	}
	if model.ExceedsBudget(1, 1, 1, 0, 10_485) {
		t.Fatal("returned cap does not fit")
	}
	if !model.ExceedsBudget(1, 1, 1, 0, 10_486) {
		t.Fatal("cap + 1 unexpectedly fits")
	}
}

func TestMemoryModelLargeWideTableStillBinds(t *testing.T) {
	profile := NewMemoryProfile([]TableMemoryStat{
		{Name: "tiny_lookup", RowCount: 2, AvgRowBytes: 36_864},
		{Name: "large_wide_table", RowCount: 1_000_000, AvgRowBytes: 36_864},
	})
	model := NewMemoryModel(profile, 36_864)

	if got, want := model.SafeChunkSize(1, 1, 1, 0), int64(28); got != want {
		t.Fatalf("large-wide cap = %d, want %d", got, want)
	}
}

func TestMemoryModelObservedMSSQLProfileKeepsReachableCandidates(t *testing.T) {
	profile := NewMemoryProfile([]TableMemoryStat{
		{Name: "votes", RowCount: 10_143_364, AvgRowBytes: 38},
		{Name: "comments", RowCount: 3_875_183, AvgRowBytes: 359},
		{Name: "posts", RowCount: 3_729_195, AvgRowBytes: 1_908},
		{Name: "badges", RowCount: 1_102_019, AvgRowBytes: 50},
		{Name: "users", RowCount: 299_398, AvgRowBytes: 208},
		{Name: "postlinks", RowCount: 161_519, AvgRowBytes: 36},
		{Name: "votetypes", RowCount: 15, AvgRowBytes: 4_915},
		{Name: "posttypes", RowCount: 8, AvgRowBytes: 9_216},
		{Name: "linktypes", RowCount: 2, AvgRowBytes: 36_864},
	})
	model := NewMemoryModel(profile, 36_864)

	if got, want := model.SafeChunkSize(8_556, 12, 4, 1), int64(78_368); got != want {
		t.Fatalf("observed profile cap = %d, want %d", got, want)
	}
	if scalar := SafeChunkSize(8_556, 12, 4, 1, 36_864); scalar != 4_056 {
		t.Fatalf("scalar control cap = %d, want 4056", scalar)
	}
	for _, candidate := range []int{26_824, 53_648} {
		if model.ExceedsBudget(8_556, 12, 4, 1, candidate) {
			t.Fatalf("reachable candidate %d was falsely modeled over budget", candidate)
		}
	}
}

func TestMemoryModelIncompleteProfileFallsBackToScalar(t *testing.T) {
	profile := NewMemoryProfile([]TableMemoryStat{
		{Name: "unknown_cardinality", RowCount: 0, AvgRowBytes: 2_000},
	})
	model := NewMemoryModel(profile, 2_000)

	if profile.Complete() {
		t.Fatal("zero catalog cardinality was treated as complete evidence")
	}
	if got, want := model.SafeChunkSize(1, 1, 1, 0), int64(524); got != want {
		t.Fatalf("scalar fallback cap = %d, want %d", got, want)
	}
	if got, want := model.EstimatedMemMB(1, 1, 0, 1_000), EstimatedMemMB(1, 1, 0, 1_000, 2_000); got != want {
		t.Fatalf("scalar fallback estimate = %d, want %d", got, want)
	}

	missing := NewMemoryProfileForTableCount([]TableMemoryStat{
		{Name: "only_returned_table", RowCount: 10, AvgRowBytes: 100},
	}, 2)
	if missing.Complete() {
		t.Fatal("profile missing an expected table was treated as complete")
	}
	duplicate := NewMemoryProfile([]TableMemoryStat{
		{Name: "duplicate", RowCount: 10, AvgRowBytes: 100},
		{Name: "duplicate", RowCount: 20, AvgRowBytes: 200},
	})
	if duplicate.Complete() {
		t.Fatal("duplicate table records were treated as complete evidence")
	}
}

func TestMemoryProfileRejectsCaseVariantPartialCatalog(t *testing.T) {
	// SetTableNameFilter collapses orders/ORDERS to one case-insensitive
	// identity. A partial catalog containing both variants must not use those
	// two records to satisfy the expected orders+customers count.
	profile := NewMemoryProfileForTableCount([]TableMemoryStat{
		{Name: "orders", RowCount: 10, AvgRowBytes: 100},
		{Name: "ORDERS", RowCount: 20, AvgRowBytes: 200},
	}, 2)

	if profile.Complete() {
		t.Fatal("case-variant duplicate hid a missing in-scope table")
	}
	model := NewMemoryModel(profile, 2_000)
	if model.UsesTableProfile() {
		t.Fatal("case-variant partial catalog authorized table-aware modeling")
	}
}

func TestMemoryModelOneRowCanRemainOverBudget(t *testing.T) {
	model := NewMemoryModel(NewMemoryProfile([]TableMemoryStat{
		{Name: "wide", RowCount: 1, AvgRowBytes: 2 * 1024 * 1024},
	}), 2*1024*1024)

	rows, minimumExceeds := model.safeChunkSizeDetail(1, 1, 1, 0)
	if rows != 1 || !minimumExceeds {
		t.Fatalf("one-row fallback = (%d,%v), want (1,true)", rows, minimumExceeds)
	}
	if !model.ExceedsBudget(1, 1, 1, 0, 1) {
		t.Fatal("one-row over-budget state was lost")
	}
}

func TestMemoryModelFullySaturatedProfileHasNoBindingCap(t *testing.T) {
	model := NewMemoryModel(NewMemoryProfile([]TableMemoryStat{
		{Name: "small_a", RowCount: 2, AvgRowBytes: 100},
		{Name: "small_b", RowCount: 4, AvgRowBytes: 50},
	}), 100)

	if got := model.SafeChunkSize(1, 1, 1, 0); got != math.MaxInt64 {
		t.Fatalf("fully fitting profile cap = %d, want MaxInt64", got)
	}
	if model.EstimatedMemMB(1, 1, 0, 1_000_000) != 1 {
		t.Fatal("chunk beyond all cardinalities did not remain on the saturated footprint")
	}
}

func TestMemoryModelExtremeInputsDoNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	model := NewMemoryModel(NewMemoryProfile([]TableMemoryStat{
		{Name: "huge", RowCount: math.MaxInt64, AvgRowBytes: math.MaxInt64},
	}), math.MaxInt64)

	if !model.ExceedsBudget(math.MaxInt64, maxInt, maxInt, maxInt, maxInt) {
		t.Fatal("overflowing table profile appeared to fit")
	}
	rows, minimumExceeds := model.safeChunkSizeDetail(1, maxInt, maxInt, maxInt)
	if rows != 1 || !minimumExceeds {
		t.Fatalf("overflowing table profile cap = (%d,%v), want (1,true)", rows, minimumExceeds)
	}
	if got := model.EstimatedMemMB(maxInt, maxInt, maxInt, maxInt); got <= 0 {
		t.Fatalf("overflowing estimate = %d, want positive saturation", got)
	}
}

func TestMemoryProfileDefensivelyCopiesTables(t *testing.T) {
	tables := []TableMemoryStat{{Name: "orders", RowCount: 10, AvgRowBytes: 100}}
	profile := NewMemoryProfile(tables)
	tables[0].RowCount = 0
	copyOut := profile.Tables()
	copyOut[0].AvgRowBytes = 0

	got := profile.Tables()[0]
	if !profile.Complete() || got.RowCount != 10 || got.AvgRowBytes != 100 {
		t.Fatalf("profile was mutated through an external slice: complete=%v table=%+v", profile.Complete(), got)
	}
}
